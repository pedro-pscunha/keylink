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
//! | completed | refused     | `MailboxClosed`, with the turn not committed   |
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

use keylink::{Caller, Ended, MailboxClosed, Observer, Stop, TurnFut, TurnInfo, TurnOutcome};
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

    assert_eq!(ledger.post(1).await, Err(MailboxClosed));

    match ledger.ended().await {
        Ended::Observer { name, error } => {
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
/// `local.rs` promises about a panicking call, and the caller would be told
/// `MailboxClosed` — "the actor is gone" — about a message that in fact ran and
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
/// fails one way, so it is told `MailboxClosed`, and `ended` is the only place
/// the reason survives.
#[tokio::test]
async fn a_refused_turn_answers_its_caller_with_nothing() {
    let ran = ran();
    let ledger = ledger::actor_with(
        Ledger::default(),
        vec![Box::new(Recording::refusing("saver", &ran))],
    );

    assert_eq!(ledger.post(1).await, Err(MailboxClosed));
    assert_eq!(
        *ran.lock().unwrap(),
        ["saver"],
        "the observer ran; it is the commit that did not happen"
    );

    match ledger.ended().await {
        Ended::Observer { name, error } => {
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
    assert_eq!(with_message.post(1).await, Err(MailboxClosed));
    match with_message.ended().await {
        Ended::Observer { name, error } => {
            assert_eq!(name, "message");
            assert_eq!(error.to_string(), "the store said no");
        }
        other => panic!("got {other:?}"),
    }

    let with_error = ledger::actor_with(Ledger::default(), vec![Box::new(GivesUp::WithError)]);
    assert_eq!(with_error.post(1).await, Err(MailboxClosed));
    match with_error.ended().await {
        Ended::Observer { name, error } => {
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

    assert_eq!(ledger.post(1).await, Err(MailboxClosed));
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

    assert_eq!(ledger.post(1).await, Err(MailboxClosed));

    match ledger.ended().await {
        Ended::Observer { name, error } => {
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
