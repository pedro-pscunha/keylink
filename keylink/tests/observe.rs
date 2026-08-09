//! The turn-level extension point, used from outside the crate.
//!
//! RFC-06's [`Observer`] watches a **turn against the state**: it is called
//! inside the turn, after the method and before the reply, and it sees the state
//! and how the turn finished — never a request and never a reply payload, which
//! is `Middleware`'s half. This file is the first consumer that is not the
//! design document, so it is also the proof that the port can be implemented
//! against the published surface alone.
//!
//! What is pinned here is §2.6, the table that says who gets an answer:
//!
//! | the turn  | an observer | the caller of a local call gets                |
//! |-----------|-------------|------------------------------------------------|
//! | completed | accepted    | the value                                      |
//! | panicked  | refused     | the panic, resumed — and the actor then ends   |
//! | completed | refused     | `Undelivered`, with the turn not committed   |
//!
//! Three rows and three tests, because they are three different promises and an
//! implementation that collapses any two of them still passes a test written for
//! one of the two. Everything else here guards a seam that table stands on: that
//! the observer really does run before the reply, that `ended` can be awaited
//! without keeping the actor alive, and that a read is not a turn.
//!
//! There is no node in this file and no `mod common`. An observer belongs to
//! `LocalActor`, a keyed actor mounted through `System` has none at all, and
//! every call below is a local one.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::io;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use keylink::{
    Caller, Ended, FinishFut, Observer, Stop, TurnFut, TurnInfo, TurnOutcome, Undelivered,
};
use tokio::sync::{mpsc, oneshot};

keylink::service! {
    service Ledger;

    rpc Post(entry: u64) -> u64;
    /// Panics, so a turn that ends badly is reachable from a handle.
    rpc Boom() -> u64;
}

/// The obvious code, with no observer concern in sight. The state does not know
/// it is watched, and that is the property the port exists to keep.
#[derive(Default)]
struct Ledger {
    entries: Vec<u64>,
}

impl ledger::Actor for Ledger {
    async fn post(&mut self, _cx: Caller<'_>, entry: u64) -> u64 {
        self.entries.push(entry);
        self.entries.len() as u64
    }

    async fn boom(&mut self, _cx: Caller<'_>) -> u64 {
        panic!("bad entry")
    }
}

// ───────────────────────────────────────────────────────────────── the observers

/// Where a [`Recording`] observer says it ran.
///
/// A `Vec` of names and not a counter, because two of the tests below are about
/// the order of a chain and one is about a count, and the `Vec` answers all
/// three without a second fixture.
type Ran = Arc<Mutex<Vec<&'static str>>>;

fn ran() -> Ran {
    Arc::new(Mutex::new(Vec::new()))
}

/// Writes its own name into a log the test kept, and optionally gives up.
///
/// One type rather than three, because every test that cares about observers at
/// all cares about the same two facts: whether this one ran, and where it ran
/// relative to its neighbours. `refuses` is the only other axis, and it is the
/// axis §2.6's third row is about.
struct Recording {
    name: &'static str,
    ran: Ran,
    refuses: bool,
}

impl Recording {
    fn accepting(name: &'static str, ran: &Ran) -> Self {
        Self {
            name,
            ran: ran.clone(),
            refuses: false,
        }
    }

    fn refusing(name: &'static str, ran: &Ran) -> Self {
        Self {
            name,
            ran: ran.clone(),
            refuses: true,
        }
    }
}

impl Observer<Ledger> for Recording {
    fn name(&self) -> &'static str {
        self.name
    }

    fn after_turn<'a>(&'a mut self, _state: &'a Ledger, _turn: TurnInfo<'a>) -> TurnFut<'a> {
        let name = self.name;
        let ran = self.ran.clone();
        let refuses = self.refuses;
        // Recorded INSIDE the future rather than beside it, which is the
        // stronger of the two claims available: a loop that built the future and
        // dropped it unawaited would still have called `after_turn`, and only
        // this placement can tell the two apart.
        Box::pin(async move {
            ran.lock().unwrap().push(name);
            if refuses {
                return Err(Stop::new(format!("{name} refused")));
            }
            Ok(())
        })
    }
}

/// Reads the state, says what it saw, and then waits to be let go.
///
/// The gate is taken on the first turn only, so an actor carrying this observer
/// is held exactly once and runs normally afterwards.
struct Held {
    saw: mpsc::UnboundedSender<usize>,
    gate: Option<oneshot::Receiver<()>>,
}

impl Observer<Ledger> for Held {
    fn name(&self) -> &'static str {
        "held"
    }

    fn after_turn<'a>(&'a mut self, state: &'a Ledger, _turn: TurnInfo<'a>) -> TurnFut<'a> {
        // Synchronously, BEFORE the future exists, which is what `after_turn`
        // being a plain `fn` rather than an `async fn` buys an implementor:
        // nothing holds `&Ledger` across an await here, so this observer needs
        // no `Ledger: Sync` and neither does the design.
        let entries = state.entries.len();
        let saw = self.saw.clone();
        let gate = self.gate.take();
        Box::pin(async move {
            let _ = saw.send(entries);
            if let Some(gate) = gate {
                let _ = gate.await;
            }
            Ok(())
        })
    }
}

/// Unwinds where the loop can see it.
struct Exploding;

impl Observer<Ledger> for Exploding {
    fn name(&self) -> &'static str {
        "exploding"
    }

    fn after_turn<'a>(&'a mut self, _state: &'a Ledger, _turn: TurnInfo<'a>) -> TurnFut<'a> {
        Box::pin(async { panic!("the observer fell over") })
    }
}

/// Refuses a turn that panicked and accepts one that completed.
///
/// What a durability adapter does, and the reason row two of §2.6 has a row of
/// its own: a panic can leave the state torn halfway through a mutation, so
/// there is nothing safe to store and nothing to do but end.
struct Strict;

impl Observer<Ledger> for Strict {
    fn name(&self) -> &'static str {
        "strict"
    }

    fn after_turn<'a>(&'a mut self, _state: &'a Ledger, turn: TurnInfo<'a>) -> TurnFut<'a> {
        Box::pin(async move {
            match turn.outcome {
                TurnOutcome::Completed => Ok(()),
                TurnOutcome::Panicked { message } => {
                    Err(Stop::new(format!("refusing a torn state: {message}")))
                }
            }
        })
    }
}

/// Records the **method name** of every turn it sees, rather than its own name.
///
/// A separate observer from [`Recording`] because the two log different things
/// into the same `Vec`, and a test that read a mixture of names and methods out
/// of one log would pass against a `TurnInfo` that reported either.
struct Naming {
    ran: Ran,
}

impl Observer<Ledger> for Naming {
    fn name(&self) -> &'static str {
        "naming"
    }

    fn after_turn<'a>(&'a mut self, _state: &'a Ledger, turn: TurnInfo<'a>) -> TurnFut<'a> {
        let ran = self.ran.clone();
        let method = turn.method;
        Box::pin(async move {
            ran.lock().unwrap().push(method);
            Ok(())
        })
    }
}

/// How a [`Finishing`] observer falls over, and where.
///
/// Two halves rather than one, for the reason `after_turn` has two: a plain `fn`
/// returning a boxed future can panic before the future exists, and `guarded`
/// takes a built future and therefore cannot see that half.
enum Falls {
    /// Not at all.
    No,
    /// Before the future is built — the half a synchronous catch has to cover.
    BeforeItYields,
    /// Inside the future — the half `guarded` covers.
    InsideTheFuture,
}

/// Implements both halves of the port, into two logs: `ran` gets its name once
/// per turn, `ending` gets one line per `finish` naming what the state held and
/// which [`Ended`] it was handed.
///
/// Two logs and not one, because every 2d claim is about the *relationship*
/// between the two — that `finish` happens after the last turn, that it happens
/// even when the turn half never ran, that it happens once.
struct Finishing {
    name: &'static str,
    ran: Ran,
    ending: Arc<Mutex<Vec<String>>>,
    falls: Falls,
}

impl Finishing {
    fn new(name: &'static str, ran: &Ran, ending: &Arc<Mutex<Vec<String>>>) -> Self {
        Self {
            name,
            ran: ran.clone(),
            ending: ending.clone(),
            falls: Falls::No,
        }
    }

    fn falling(
        name: &'static str,
        ran: &Ran,
        ending: &Arc<Mutex<Vec<String>>>,
        falls: Falls,
    ) -> Self {
        Self {
            falls,
            ..Self::new(name, ran, ending)
        }
    }
}

impl Observer<Ledger> for Finishing {
    fn name(&self) -> &'static str {
        self.name
    }

    fn after_turn<'a>(&'a mut self, _state: &'a Ledger, _turn: TurnInfo<'a>) -> TurnFut<'a> {
        let name = self.name;
        let ran = self.ran.clone();
        Box::pin(async move {
            ran.lock().unwrap().push(name);
            Ok(())
        })
    }

    fn finish<'a>(&'a mut self, state: &'a Ledger, ended: &'a Ended) -> FinishFut<'a> {
        if matches!(self.falls, Falls::BeforeItYields) {
            panic!("{} fell over before it yielded", self.name);
        }
        // Read synchronously, before the future exists — which is what `finish`
        // being a plain `fn` buys, and the reason the call site must build the
        // future and only then await it. An `async` block holding `&Ledger`
        // across an await would need `Ledger: Sync`.
        let entries = state.entries.len();
        let name = self.name;
        let log = self.ending.clone();
        let falls = matches!(self.falls, Falls::InsideTheFuture);
        // Rendered here rather than held: what the test asserts is the reason,
        // and `Ended::Observer` carries an `Arc<dyn Error>` that no equality
        // check can reach.
        let seen = format!("{ended:?}");
        Box::pin(async move {
            // Yielded first, and both halves of that are deliberate. A `finish`
            // that the loop built and never awaited would write nothing, which
            // is what makes the log a claim about the *await* rather than about
            // the call. And it forces the write to the far side of a suspension
            // point, so a `finish` moved after the end was published could not
            // land before `stop()` returned — which is what
            // `finish_runs_once_after_the_last_turn_and_before_stop_returns`
            // asserts and could otherwise pass on a lucky poll order.
            tokio::task::yield_now().await;
            log.lock().unwrap().push(format!("{name}:{entries}:{seen}"));
            assert!(!falls, "{name} fell over inside its future");
        })
    }
}

/// The two routes into a [`Stop`], which exist for different reasons.
enum GivesUp {
    /// [`Stop::new`], for an observer that fails with a message.
    WithMessage,
    /// The blanket `From<E>`, for one that fails with an error type.
    WithError,
}

impl Observer<Ledger> for GivesUp {
    fn name(&self) -> &'static str {
        match self {
            Self::WithMessage => "message",
            Self::WithError => "error",
        }
    }

    fn after_turn<'a>(&'a mut self, _state: &'a Ledger, _turn: TurnInfo<'a>) -> TurnFut<'a> {
        match self {
            Self::WithMessage => Box::pin(async { Err(Stop::new("the store said no")) }),
            // Through the route the blanket impl exists for: an ordinary error
            // type converted at the call site, with `Stop` named nowhere.
            Self::WithError => {
                Box::pin(async { Err(io::Error::other("the store is gone").into()) })
            }
        }
    }
}

// ───────────────────────────────────────────────────────────────────── the port

/// The port itself: an observer is called once for every turn, and the actor
/// needs to know nothing about what it does.
///
/// The first thing that breaks without this is the wiring. An `actor_with` that
/// takes a `Vec` and never lends it to a turn compiles perfectly, keeps the
/// whole suite green, and silently gives a caller who asked for durability none
/// at all.
#[tokio::test]
async fn an_observer_runs_once_for_every_turn() {
    let ran = ran();
    let ledger = ledger::actor_with(
        Ledger::default(),
        vec![Box::new(Recording::accepting("counter", &ran))],
    );

    for entry in 1..=3 {
        assert_eq!(ledger.post(entry).await.expect("running"), entry);
    }

    assert_eq!(*ran.lock().unwrap(), ["counter", "counter", "counter"]);
}

/// Position: the observer runs **before** the caller is answered, which is the
/// whole reason this port is inside the turn loop rather than a task beside it.
///
/// Proving it needs the observer to stop mid-flight. The cheap version — assert
/// afterwards that the observer saw the finished turn — passes just as happily
/// against a loop that replies first and observes second, because by the time
/// the assertion runs both have happened either way. So this one holds the
/// observer on a gate and asserts the caller still has no answer while it is
/// held. A durability adapter that stored after the reply would hand a peer a
/// value no restart can reproduce, and only this ordering rules that out.
#[tokio::test]
async fn an_observer_runs_before_the_caller_is_answered() {
    let (saw, mut seen) = mpsc::unbounded_channel();
    let (release, gate) = oneshot::channel();
    let ledger = ledger::actor_with(
        Ledger::default(),
        vec![Box::new(Held {
            saw,
            gate: Some(gate),
        })],
    );

    let answered = Arc::new(AtomicBool::new(false));
    let call = {
        let ledger = ledger.clone();
        let answered = answered.clone();
        tokio::spawn(async move {
            let entries = ledger.post(1).await.expect("running");
            answered.store(true, Ordering::SeqCst);
            entries
        })
    };

    // No clock: this returns exactly when the observer sends, which is after it
    // has read the state and before it parks on the gate.
    let observed = seen.recv().await.expect("the observer ran");
    assert!(
        !answered.load(Ordering::SeqCst),
        "the caller was answered while the observer was still running"
    );

    release.send(()).expect("the observer still holds the gate");
    assert_eq!(
        call.await.expect("no panic"),
        observed as u64,
        "the observer saw the same finished turn the caller was then told about"
    );
}

/// A panicking observer is not a surviving observer: it ends the actor, and
/// `ended` names it.
///
/// The chain runs inside the same guard as the handler, and that placement is
/// what this pins. Outside it, an observer that unwound would escape a detached
/// task and be swallowed — the actor would go on serving turns with a durability
/// adapter that has stopped working, and nobody would ever be told.
#[tokio::test]
async fn a_panicking_observer_ends_the_actor_and_is_named() {
    let ledger = ledger::actor_with(Ledger::default(), vec![Box::new(Exploding)]);

    assert_eq!(ledger.post(1).await, Err(Undelivered::Gone));

    match ledger.ended().await {
        Ended::Observer { name, error, .. } => {
            assert_eq!(name, "exploding");
            assert!(
                error.to_string().contains("the observer fell over"),
                "got {error}"
            );
        }
        other => panic!("got {other:?}"),
    }
}

/// §2.6 row two: a panicked turn is still answered — with the panic, resumed in
/// the caller — and only then does the actor end, as `Ended::Panicked` and not
/// as `Ended::Observer`.
///
/// Both halves are load-bearing. Withholding the reply here would reverse what
/// `turns.rs` promises about a panicking call, and the caller would be told
/// `Undelivered` — "the actor is gone" — about a message that in fact ran and
/// exploded. And reporting the refusing observer instead of the panic would
/// point an operator at the durability adapter for a fault in a `service!`
/// method, which is why the two variants are not one.
#[tokio::test]
async fn a_panicked_turn_reaches_its_caller_and_then_ends_the_actor() {
    let ledger = ledger::actor_with(Ledger::default(), vec![Box::new(Strict)]);

    // Spawned, because the panic is resumed in whoever awaited the call and this
    // test is not willing to be that task.
    let boom = {
        let ledger = ledger.clone();
        tokio::spawn(async move { ledger.boom().await })
    };
    let failure = boom.await.expect_err("the panic reaches the caller");
    assert!(failure.is_panic(), "got {failure:?}");

    match ledger.ended().await {
        Ended::Panicked { message } => assert!(message.contains("bad entry"), "got {message}"),
        other => panic!("got {other:?}"),
    }
}

/// §2.6 row three: an observer that refuses a turn which **completed** costs the
/// caller its value.
///
/// The turn ran and was not committed, so handing back the value would publish a
/// state no restart can reproduce — the exact fault the port exists to prevent,
/// arrived at through the reply instead of through the store. A local caller
/// fails one way, so it is told `Undelivered`, and `ended` is the only place
/// the reason survives.
#[tokio::test]
async fn a_refused_turn_answers_its_caller_with_nothing() {
    let ran = ran();
    let ledger = ledger::actor_with(
        Ledger::default(),
        vec![Box::new(Recording::refusing("saver", &ran))],
    );

    assert_eq!(ledger.post(1).await, Err(Undelivered::Gone));
    assert_eq!(
        *ran.lock().unwrap(),
        ["saver"],
        "the observer ran; it is the commit that did not happen"
    );

    match ledger.ended().await {
        Ended::Observer { name, error, .. } => {
            assert_eq!(name, "saver");
            assert!(error.to_string().contains("saver refused"), "got {error}");
        }
        other => panic!("got {other:?}"),
    }
}

/// `ended` holds a watch receiver and no mailbox sender, so awaiting it does not
/// itself keep the actor alive.
///
/// The design this rules out is the obvious one: a future built on
/// `mail.closed()`, or on anything else that clones the `mpsc::Sender`. It reads
/// correctly and it makes "the last handle was dropped" unreachable for exactly
/// as long as somebody is waiting to hear about it. Note the failure mode: an
/// actor kept alive by its own end-notification does not fail this test, it
/// hangs it, which is why the await is plain and has no clock to hide behind.
#[tokio::test]
async fn awaiting_the_end_does_not_keep_the_actor_alive() {
    let ledger = ledger::actor(Ledger::default());
    let ended = ledger.ended();
    drop(ledger);

    assert!(matches!(ended.await, Ended::Stopped));
}

/// The other half of that promise: `ended` on a **live** actor stays pending.
///
/// A watch channel initialised with a reason reports that reason immediately,
/// which is why the value is an `Option` starting at `None`. Get it wrong and
/// every caller waiting to hear about a durability failure is told about one at
/// once, on an actor that is running perfectly — and the test above would go on
/// passing, because it never asks whether the answer arrived too early.
///
/// The timeout is a bound and not a measurement: the assertion is "still
/// pending", and no length of wall clock makes that claim stronger or weaker.
#[tokio::test]
async fn a_live_actor_has_not_ended() {
    let ledger = ledger::actor(Ledger::default());
    assert_eq!(ledger.post(1).await.expect("running"), 1);

    let asked = tokio::time::timeout(Duration::from_millis(50), ledger.ended()).await;
    assert!(asked.is_err(), "a running actor reported {asked:?}");
}

/// A read is not a turn, so it runs no observers.
///
/// `with_state` takes `&A` and cannot mutate, so storing after it is waste. It
/// also closes a loop a background writer would otherwise create: a writer that
/// read the state through a turn would mark the actor dirty and schedule another
/// read, forever. Routing `with_state` through `ask` instead of `read` is a
/// one-word edit that nothing else in the suite notices.
#[tokio::test]
async fn a_read_runs_no_observers() {
    let ran = ran();
    let ledger = ledger::actor_with(
        Ledger::default(),
        vec![Box::new(Recording::accepting("saver", &ran))],
    );

    assert_eq!(ledger.post(1).await.expect("running"), 1);
    assert_eq!(*ran.lock().unwrap(), ["saver"]);

    // The read really happens — it comes back with what the turn wrote — and
    // still nothing is observed. Both assertions are needed: a `with_state` that
    // silently did nothing would also leave the log alone.
    let entries = ledger
        .with_state(|state: &Ledger| state.entries.clone())
        .await
        .expect("running");

    assert_eq!(entries, vec![1]);
    assert_eq!(*ran.lock().unwrap(), ["saver"], "a read is not a turn");
}

/// Both routes into a `Stop` reach an ending, and they exist for different
/// reasons.
///
/// `Stop::new` is not a convenience. The blanket `From<E>` makes every other
/// `From` impl a coherence error, so `From<String>` and `From<&str>` can never
/// be added and an observer failing with a plain message would otherwise have to
/// box it by hand. The blanket is what lets one fail with an ordinary error type
/// and write `Err(error.into())`. Dropping either is a source-compatible change
/// to this crate that only a call site outside it notices — so the call site
/// lives here.
#[tokio::test]
async fn an_observer_can_give_up_with_a_message_or_with_an_error() {
    let with_message = ledger::actor_with(Ledger::default(), vec![Box::new(GivesUp::WithMessage)]);
    assert_eq!(with_message.post(1).await, Err(Undelivered::Gone));
    match with_message.ended().await {
        Ended::Observer { name, error, .. } => {
            assert_eq!(name, "message");
            assert_eq!(error.to_string(), "the store said no");
        }
        other => panic!("got {other:?}"),
    }

    let with_error = ledger::actor_with(Ledger::default(), vec![Box::new(GivesUp::WithError)]);
    assert_eq!(with_error.post(1).await, Err(Undelivered::Gone));
    match with_error.ended().await {
        Ended::Observer { name, error, .. } => {
            assert_eq!(name, "error");
            assert_eq!(error.to_string(), "the store is gone");
        }
        other => panic!("got {other:?}"),
    }
}

/// Observers run in the order they were given, and the first `Err` stops the
/// rest.
///
/// Order is what makes a chain composable at all: an adapter that stores and one
/// that reports have to agree on which of them happened first, and a loop that
/// iterated the `Vec` in any other order would make the pair unusable without
/// saying so. The short circuit is the second half — once one observer has
/// decided the actor is ending, running the rest asks them to work against a
/// state nobody will commit, which for a durability adapter means writing bytes
/// that the refusal exists to withhold.
#[tokio::test]
async fn observers_run_in_order_and_the_first_refusal_stops_the_rest() {
    let ran = ran();
    let ledger = ledger::actor_with(
        Ledger::default(),
        vec![
            Box::new(Recording::accepting("first", &ran)),
            Box::new(Recording::refusing("second", &ran)),
            Box::new(Recording::accepting("third", &ran)),
        ],
    );

    assert_eq!(ledger.post(1).await, Err(Undelivered::Gone));
    assert_eq!(
        *ran.lock().unwrap(),
        ["first", "second"],
        "the chain ran in order and stopped at the refusal"
    );

    match ledger.ended().await {
        Ended::Observer { name, .. } => assert_eq!(name, "second"),
        other => panic!("got {other:?}"),
    }
}

/// An observer that panics **before it returns a future** is caught too.
///
/// `after_turn` is a plain `fn` and not an `async fn`, and that is deliberate:
/// the signature exists to invite synchronous work with the state before the
/// future is built, which is what keeps `A: Sync` out of the whole design. So
/// the prelude is not an edge case — it is the half the signature advertises.
///
/// It was also the half that was not caught. `guarded` takes a *built* future,
/// so `guarded(observer.after_turn(..))` evaluates the call first and catches
/// only what happens after it yields. A panic here unwound out of a detached
/// task, the end channel was never filled, and `ended()` reported `Vanished` —
/// "the loop never reached its exit" — for an actor whose observer had simply
/// fallen over. The `Ended::Observer` arm was dead for every panic of this
/// shape, and `a_panicking_observer_ends_the_actor_and_is_named` above could not
/// see it, because that one panics inside the future.
struct ExplodesBeforeItYields;

impl Observer<Ledger> for ExplodesBeforeItYields {
    fn name(&self) -> &'static str {
        "early"
    }

    fn after_turn<'a>(&'a mut self, _state: &'a Ledger, _turn: TurnInfo<'a>) -> TurnFut<'a> {
        panic!("the observer fell over before it yielded");
    }
}

#[tokio::test]
async fn an_observer_that_panics_before_it_yields_is_still_named() {
    let ledger = ledger::actor_with(Ledger::default(), vec![Box::new(ExplodesBeforeItYields)]);

    assert_eq!(ledger.post(1).await, Err(Undelivered::Gone));

    match ledger.ended().await {
        Ended::Observer { name, error, .. } => {
            assert_eq!(name, "early");
            assert!(
                error.to_string().contains("fell over before it yielded"),
                "got {error}"
            );
        }
        // `Vanished` is the specific wrong answer this test exists for: it is
        // what an uncaught unwind out of the detached task produces.
        other => panic!("expected the observer to be named, got {other:?}"),
    }
}

// ────────────────────────────────────────────── what the turn was called (2b)

/// An observer is told which declared method it just watched.
///
/// Absent until the mailbox stopped being a boxed closure, because a closure has
/// no name. What makes this worth a test of its own rather than an assertion
/// inside another one is that the field can be *plausibly wrong*: the trait
/// method is `post` and the wire name is `Post`, and an observer that reported
/// the snake-case spelling would disagree with every `Middleware` watching the
/// same call, silently, in a metrics label.
#[tokio::test]
async fn an_observer_is_told_which_method_the_turn_ran() {
    let ran = ran();
    let ledger = ledger::actor_with(
        Ledger::default(),
        vec![Box::new(Naming { ran: ran.clone() })],
    );

    assert_eq!(ledger.post(1).await.expect("running"), 1);

    // A panicking turn is observed too, and it is observed under its own name —
    // `TurnOutcome` says how it went and `method` says what went.
    let boom = {
        let ledger = ledger.clone();
        tokio::spawn(async move { ledger.boom().await })
    };
    assert!(boom
        .await
        .expect_err("the panic reaches the caller")
        .is_panic());

    assert_eq!(*ran.lock().unwrap(), ["Post", "Boom"]);
}

/// The defect RFC-07 §1.8 recorded, tested from the position that has it.
///
/// `tests/` is a separate crate, so `#[non_exhaustive]` bites here exactly as it
/// bites a downstream author — and the thing being proved is that such an author
/// can unit-test their own `Observer` with **no actor anywhere in sight**, which
/// is the whole point of a port. Before `TurnInfo::for_test` the argument to
/// `after_turn` was unconstructible from out here and this test could not be
/// written at all.
///
/// The `compile_fail` doctest on `TurnInfo` is the other half: it holds that the
/// literal is still shut, so this helper is the one door rather than a second one.
///
/// Made red by `scripts/break-once.sh keylink/src/observe.rs` on `for_test`'s
/// `Self { outcome, method }` → `Self { outcome, method: "Post" }` — a
/// constructor that drops the caller's argument, which would make the helper
/// unable to test what it claims to. Watched: `left: ["Post", "Post"]` against
/// `right: ["Post", "Boom"]`.
#[tokio::test]
async fn an_observer_can_be_unit_tested_without_an_actor() {
    let ran = ran();
    let mut naming = Naming { ran: ran.clone() };
    let state = Ledger { entries: vec![7] };

    naming
        .after_turn(&state, TurnInfo::for_test(TurnOutcome::Completed, "Post"))
        .await
        .expect("the observer accepted the turn");
    naming
        .after_turn(
            &state,
            TurnInfo::for_test(TurnOutcome::Panicked { message: "torn" }, "Boom"),
        )
        .await
        .expect("the observer accepted the fault");

    assert_eq!(*ran.lock().unwrap(), ["Post", "Boom"]);

    // The other half of the port, from the same position and with no actor.
    let mut finishing = Finishing::new("solo", &ran, &Arc::new(Mutex::new(Vec::new())));
    finishing.finish(&state, &Ended::Stopped).await;
}

// ────────────────────────────────────────────────────── the lifecycle end (2d)

/// `finish` runs once, after the last turn, and **before `stop` returns**.
///
/// The ordering is the whole feature. `finish` exists so a policy that batches
/// can flush, and a flush is only a flush if it happens while the state is still
/// there and before anybody is told the actor ended — which is what makes
/// `stop().await` returning continue to mean any claim an observer held is free.
/// A `finish` placed after the three-drop teardown would satisfy "it ran" and
/// none of that, and every assertion below would still pass but the last.
#[tokio::test]
async fn finish_runs_once_after_the_last_turn_and_before_stop_returns() {
    let ran = ran();
    let ending = Arc::new(Mutex::new(Vec::new()));
    let ledger = ledger::actor_with(
        Ledger::default(),
        vec![Box::new(Finishing::new("saver", &ran, &ending))],
    );

    for entry in 1..=3 {
        assert_eq!(ledger.post(entry).await.expect("running"), entry);
    }
    assert!(
        ending.lock().unwrap().is_empty(),
        "finish ran while the actor was still serving: {:?}",
        ending.lock().unwrap()
    );

    ledger
        .stop()
        .await
        .expect("a stop from outside the actor's own turn is never refused");

    // Read the instant `stop` returns, with nothing awaited in between: a
    // `finish` that ran after the end was published would not be here yet.
    assert_eq!(*ran.lock().unwrap(), ["saver", "saver", "saver"]);
    assert_eq!(
        *ending.lock().unwrap(),
        ["saver:3:Stopped"],
        "once, with the state as the last turn left it"
    );
}

/// `finish` gets the same `Ended` every waiter is about to be told.
///
/// Two observers, and the first refuses. That makes the ending
/// `Ended::Observer`, which is the interesting case for two reasons: the reason
/// is not the default one, and the second observer's `after_turn` never ran at
/// all — the chain short-circuited above it. It still gets a `finish`, because a
/// `finish` is about the actor ending and not about that observer having
/// participated. An adapter that had already written bytes for earlier turns
/// needs its flush whether or not it was consulted about the last one.
#[tokio::test]
async fn finish_receives_the_same_ending_the_actor_reports() {
    let ran = ran();
    let ending = Arc::new(Mutex::new(Vec::new()));
    let ledger = ledger::actor_with(
        Ledger::default(),
        vec![
            Box::new(Recording::refusing("vetoing", &ran)),
            Box::new(Finishing::new("saver", &ran, &ending)),
        ],
    );

    assert_eq!(ledger.post(1).await, Err(Undelivered::Gone));

    let reported = ledger.ended().await;
    match &reported {
        Ended::Observer { name, .. } => assert_eq!(*name, "vetoing"),
        other => panic!("got {other:?}"),
    }

    let seen = ending.lock().unwrap().clone();
    let [line] = seen.as_slice() else {
        panic!("expected one finish, got {seen:?}")
    };
    assert!(
        line.starts_with("saver:1:Observer"),
        "finish saw a different ending from the one reported: {line}"
    );
    assert!(
        line.contains("vetoing"),
        "finish was not told which observer refused: {line}"
    );
    assert_eq!(
        *ran.lock().unwrap(),
        ["vetoing"],
        "the chain short-circuited, and finish ran anyway"
    );
}

/// Every observer gets its `finish`, in order, even when an earlier one falls
/// over — in either of the two places it can.
///
/// This is why there is no single `guarded` around the finish loop and two
/// catches inside it instead. One guard around the loop would stop at the first
/// panic, so an adapter that fell over would cost the next adapter its flush;
/// and being a *future* guard it would need an `async` block holding `&Ledger`
/// across an await, which is `Send` only when `Ledger: Sync` — the bound the
/// whole port is shaped to avoid.
///
/// `early` panics before it returns a future and `late` panics inside one. The
/// first of those is the half a `guarded` alone cannot see, and it is the half
/// `an_observer_that_panics_before_it_yields_is_still_named` exists for one rung
/// up.
#[tokio::test]
async fn finish_runs_on_every_observer_even_when_an_earlier_one_panics() {
    let ran = ran();
    let ending = Arc::new(Mutex::new(Vec::new()));
    let ledger = ledger::actor_with(
        Ledger::default(),
        vec![
            Box::new(Finishing::falling(
                "early",
                &ran,
                &ending,
                Falls::BeforeItYields,
            )),
            Box::new(Finishing::falling(
                "late",
                &ran,
                &ending,
                Falls::InsideTheFuture,
            )),
            Box::new(Finishing::new("last", &ran, &ending)),
        ],
    );

    assert_eq!(ledger.post(1).await.expect("running"), 1);
    ledger
        .stop()
        .await
        .expect("a stop from outside the actor's own turn is never refused");

    // `early` never reaches its log, because it panics before the future that
    // writes it exists. `late` does, and then panics. `last` is the assertion:
    // it is downstream of both and it ran.
    assert_eq!(
        *ending.lock().unwrap(),
        ["late:1:Stopped", "last:1:Stopped"],
        "a panicking finish cost a later observer its flush"
    );

    // And a panic in `finish` does not rewrite the ending. It cannot: the reason
    // was decided before the loop, and two observers of one ending must not be
    // told different things about it.
    assert!(
        matches!(ledger.ended().await, Ended::Stopped),
        "a panicking finish changed why the actor ended"
    );
}

/// `finish` runs on `stop` and does **not** run on `abort`.
///
/// Not a policy — a consequence. `abort` is `task.abort()` and nothing else, so
/// the loop's future is dropped at its next await and never reaches its exit;
/// anything placed after the loop is unreachable by construction. Both halves are
/// in one test on purpose: the negative claim alone would pass against a `finish`
/// that never ran at all, and the positive half is what proves this fixture can
/// see one.
///
/// One documented hole, and it is `abort`'s own: a turn that never reaches an
/// await is not interrupted either, so an actor aborted during such a turn can
/// still exit its loop normally afterwards, and `finish` then runs. Nothing here
/// claims otherwise.
#[tokio::test]
async fn finish_runs_on_stop_and_not_on_abort() {
    let ran = ran();

    let aborted_log = Arc::new(Mutex::new(Vec::new()));
    let aborted = ledger::actor_with(
        Ledger::default(),
        vec![Box::new(Finishing::new("saver", &ran, &aborted_log))],
    );
    assert_eq!(aborted.post(1).await.expect("running"), 1);
    aborted.abort();
    // The abort is cooperative, so wait for it to have landed rather than
    // asserting against a clock.
    for _ in 0..100 {
        if !aborted.is_running() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    assert!(!aborted.is_running(), "the abort never landed");
    assert!(
        aborted_log.lock().unwrap().is_empty(),
        "finish ran on an aborted actor: {:?}",
        aborted_log.lock().unwrap()
    );

    let stopped_log = Arc::new(Mutex::new(Vec::new()));
    let stopped = ledger::actor_with(
        Ledger::default(),
        vec![Box::new(Finishing::new("saver", &ran, &stopped_log))],
    );
    assert_eq!(stopped.post(1).await.expect("running"), 1);
    stopped
        .stop()
        .await
        .expect("a stop from outside the actor's own turn is never refused");
    assert_eq!(
        *stopped_log.lock().unwrap(),
        ["saver:1:Stopped"],
        "the same fixture, stopped rather than aborted"
    );
}
