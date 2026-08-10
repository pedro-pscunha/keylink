//! `#[defer]`: a reply that outlives the turn that received it.
//!
//! Wave 2c. The wire does not change — a deferred method is an ordinary `rpc` to
//! the caller, which blocks on its stream exactly as before — so everything here is
//! about who holds the reply channel and when.
//!
//! Six claims, and each of the last five is a case a reader would otherwise have
//! to guess at:
//!
//! - a parked reply is answered from a **later turn**, and the value reaches the
//!   original caller;
//! - an answer given **inside** the turn is still gated by the observer chain, so a
//!   `#[defer]` method does not quietly cost a durable actor its commit;
//! - **and so is a parked one**, which is what `parking.rs` closed: a value handed
//!   to `Deferred::send` during a turn waits for that turn's verdict, so a turn
//!   that panics after sending gives its parked callers nothing;
//! - **a send from off the turn's task is not gated**, and that is the one thing
//!   left of the `#[defer]` exception — nothing ran, so there is no commit for the
//!   answer to wait for;
//! - a parked reply whose actor **ends** answers rather than hanging;
//! - a handler that **panics** before consuming its reply costs its caller the
//!   answer and does not cost the actor.
//!
//! And the one that needs two machines: a peer's deferred call, parked past its own
//! turn and answered from a local one. That is the only path where the value has to
//! become the service's own `Response` on the way out, which is the whole reason
//! `Reply::wrapping` exists.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use keylink::{
    ActorId, Allowlist, Caller, Deferred, Ended, Eviction, Observer, RemoteError, Replied, Reply,
    Server, Stop, TurnFut, TurnInfo, Unanswered, Withheld,
};

mod common;
use common::local_node;

keylink::service! {
    service Orders;

    /// Answers every parked order, and reports how many it answered.
    rpc Fill(price: u64) -> u64;

    /// How many orders are parked. An ordinary `rpc`, so a test can wait for a
    /// park to have happened by asking rather than by sleeping.
    rpc Parked() -> u64;

    /// Parked, and answered from a later `Fill`.
    #[defer]
    rpc Place(size: u64) -> u64;

    /// Answered inside its own turn, through the same channel a park would have
    /// taken away. This is the arm the observer chain still has to gate.
    #[defer]
    rpc Quote(size: u64) -> u64;

    /// Panics before it consumes its reply.
    #[defer]
    rpc Boom(size: u64) -> u64;

    /// Takes the reply out and **drops** it, without panicking and without
    /// ending the actor. The sanctioned way a live actor loses one reply —
    /// `Deferred`'s own `#[must_use]` says so — and the only cause of a dead
    /// reply channel that is neither a fault nor an ending.
    #[defer]
    rpc Abandon(size: u64) -> u64;

    /// Answers every parked order **and then panics**. The sends happen inside
    /// the turn, so nothing that turn produced crosses.
    rpc FillThenBoom() -> u64;

    /// Hands every parked reply to a spawned task and **then panics**. Those
    /// sends happen off the turn's task, so the turn's verdict does not reach
    /// them — the one thing left of the `#[defer]` exception.
    rpc DetachThenBoom() -> u64;
}

/// An order book that answers when a price arrives, not when an order does.
#[derive(Default)]
struct Book {
    waiting: Vec<(u64, Deferred<u64>)>,
}

impl orders::Actor for Book {
    async fn fill(&mut self, _cx: Caller<'_>, price: u64) -> u64 {
        let filled = self.waiting.len() as u64;
        for (size, deferred) in self.waiting.drain(..) {
            deferred.send(size * price);
        }
        filled
    }

    async fn parked(&mut self, _cx: Caller<'_>) -> u64 {
        self.waiting.len() as u64
    }

    async fn place(&mut self, _cx: Caller<'_>, reply: Reply<u64>, size: u64) -> Replied<u64> {
        let (deferred, replied) = reply.later();
        self.waiting.push((size, deferred));
        replied
    }

    async fn quote(&mut self, _cx: Caller<'_>, reply: Reply<u64>, size: u64) -> Replied<u64> {
        reply.send(size * 2)
    }

    async fn boom(&mut self, _cx: Caller<'_>, _reply: Reply<u64>, _size: u64) -> Replied<u64> {
        panic!("no price for that");
    }

    async fn abandon(&mut self, _cx: Caller<'_>, reply: Reply<u64>, _size: u64) -> Replied<u64> {
        let (deferred, replied) = reply.later();
        // Saying it out loud, which is what the `#[must_use]` asks for.
        drop(deferred);
        replied
    }

    async fn fill_then_boom(&mut self, _cx: Caller<'_>) -> u64 {
        for (size, deferred) in self.waiting.drain(..) {
            deferred.send(size * 5);
        }
        panic!("the book was half filled");
    }

    async fn detach_then_boom(&mut self, _cx: Caller<'_>) -> u64 {
        for (size, deferred) in self.waiting.drain(..) {
            // Off this task, so the send is not this turn's output and there is
            // no verdict for it to wait for.
            tokio::spawn(async move { deferred.send(size * 5) });
        }
        panic!("the book was half filled");
    }
}

/// Wait for `want` orders to be parked, by asking the actor rather than sleeping.
///
/// A `Parked()` call takes a mailbox permit like anything else, so this can only
/// answer once the `place` turn has *finished* — which is the fact being waited for.
async fn parked(book: &orders::Handle<Book>, want: u64) {
    for _ in 0..500 {
        if book.parked().await.expect("the actor is running") == want {
            return;
        }
        tokio::task::yield_now().await;
    }
    panic!("{want} orders never got parked");
}

// ───────────────────────────────────────────────── the answer from a later turn

/// The criterion: a `#[defer]` handler that parks its reply and answers from a
/// later turn returns the value to the ORIGINAL caller.
///
/// `size * price` rather than a constant, so the answer can only have come from the
/// pairing of the two turns: 3 from the parked `place` and 7 from the `fill` that
/// released it.
///
/// Made red by `scripts/break-once.sh`, replacing `Deferred::send`'s
/// `Verdict::Committed => reply.answer(Answered::Ran(value)),` with
/// `Verdict::Committed => drop((reply, value)),` — a park that takes the channel
/// away and never gives it back. Watched: this fails at the answer,
/// `the actor is running: Dropped`, which is what a caller sees when a **live**
/// actor lets the channel die unanswered. It prints rather than hangs because the
/// wait is bounded by the sender being dropped, not by a timeout.
///
/// **Both halves of that were re-measured on 2026-08-09 rather than carried, and
/// both had moved.** The literal read `self.0.answer(Answered::Ran(value));`, which
/// is not in the tree: `send` binds the reply and parks a callback, so the answer
/// sits on a `Verdict` arm and the old quote could not be applied at all. And the
/// watched word read `Undelivered` — the panic is `expect`'s, so it prints the
/// error's `Debug`, which was the type's name while it was a unit struct and is the
/// variant's name now. Control first, 5 of 5 green; then 2 failed under the break,
/// this test and its sibling below.
#[tokio::test]
async fn a_parked_reply_is_answered_from_a_later_turn() {
    let book = orders::actor(Book::default());

    let placing = {
        let book = book.clone();
        tokio::spawn(async move { book.place(3).await })
    };
    parked(&book, 1).await;

    // The `place` turn is over and its caller is still waiting, which is the whole
    // shape of the feature: one turn took the call, another answers it.
    assert_eq!(book.fill(7).await.expect("running"), 1);

    let answer = n0_future::time::timeout(Duration::from_secs(5), placing)
        .await
        .expect("a parked reply must not hang once it is answered")
        .expect("the caller is not itself panicking");
    assert_eq!(answer.expect("the actor is running"), 21);
}

/// Two parked replies are two channels, and answering one does not answer the
/// other. Enough to catch a design that kept "the" deferred reply per actor.
///
/// **Reddened, but not on its distinguishing claim, and that is worth saying.** The
/// break is the one its sibling above uses — `Deferred::send`'s
/// `Verdict::Committed` arm, answering replaced by a drop — watched here as
/// `running: Dropped` at the first order, in the same run. That fails both
/// tests, so it proves the parked channel is load-bearing and proves nothing about
/// *two* of them. No one-literal edit isolates the plural: a `Reply` is built per
/// call by generated code and `later` consumes it, so "the deferred reply per actor"
/// is not a state this crate can be edited into — it would have to be designed in.
/// This test is therefore a design pin against a shape that was never built, and it
/// is that rather than a guard with a counter-example.
#[tokio::test]
async fn two_parked_replies_are_two_answers() {
    let book = orders::actor(Book::default());

    let first = {
        let book = book.clone();
        tokio::spawn(async move { book.place(2).await })
    };
    parked(&book, 1).await;
    let second = {
        let book = book.clone();
        tokio::spawn(async move { book.place(5).await })
    };
    parked(&book, 2).await;

    assert_eq!(book.fill(10).await.expect("running"), 2);
    assert_eq!(
        first.await.expect("no panic").expect("running"),
        20,
        "the first order was answered with the second one's size"
    );
    assert_eq!(second.await.expect("no panic").expect("running"), 50);
}

// ─────────────────────────────────── an answer inside the turn is still committed

/// One observer, which refuses whatever it is shown.
struct Vetoing {
    saw: Arc<AtomicUsize>,
}

impl Observer<Book> for Vetoing {
    fn name(&self) -> &'static str {
        "vetoing"
    }

    fn after_turn<'a>(&'a mut self, _state: &'a Book, _turn: TurnInfo<'a>) -> TurnFut<'a> {
        let saw = self.saw.clone();
        Box::pin(async move {
            saw.fetch_add(1, Ordering::Relaxed);
            Err(Stop::new("vetoing refused"))
        })
    }
}

/// **The property `Reply::send` exists to keep.** An answer given *inside* a
/// deferred turn is the turn's output, so the observer chain gates it exactly as it
/// gates an ordinary method's return value — the caller is told there is no answer
/// rather than handed one the observers refused.
///
/// This is the case `#[defer]` would have broken if `send` wrote to the channel:
/// a durability adapter refuses, the store never accepted the write, and the caller
/// walks away with the value anyway.
///
/// Red-tested by making `Reply::send` answer immediately — see the commit.
#[tokio::test]
async fn an_answer_given_inside_a_deferred_turn_is_withheld_when_an_observer_refuses() {
    let saw = Arc::new(AtomicUsize::new(0));
    let book = orders::actor_with(
        Book::default(),
        vec![Box::new(Vetoing { saw: saw.clone() })],
    );

    let quoted = book.quote(21).await;
    assert!(
        quoted.is_err(),
        "the observer refused this turn, so its answer must not reach the caller: got {quoted:?}"
    );
    assert_eq!(saw.load(Ordering::Relaxed), 1, "the observer did run");

    match n0_future::time::timeout(Duration::from_secs(5), book.ended())
        .await
        .expect("the actor ends when an observer refuses")
    {
        Ended::Observer { name, .. } => assert_eq!(name, "vetoing"),
        other => panic!("expected the observer to be named: got {other:?}"),
    }
}

// ──────────────────────────────────────────── a parked reply outlives nothing

/// The criterion: a deferred reply whose actor ends before answering resolves
/// rather than hanging.
///
/// The channel is in the state, the state is owned by the loop's task, and the loop
/// exiting drops both — so the caller's wait ends because the sender is gone. Inside
/// keylink that is `TurnLost::Turn(Withheld::Gone)`; through a generated
/// `Handle` it is `Unanswered::Turn(Withheld::Gone)` — the same value, carried
/// by the type the door answers.
///
/// **The timeout is the assertion.** A hang is not a failure to a test harness, so
/// the wait is bounded and the bound is what goes red. `turns.rs`'s own unit test
/// asserts the `Withheld::Gone` underneath, where the variant is visible.
///
/// Red-tested by `::core::mem::forget(state)` in place of the loop's own drop, which
/// is precisely the property under test — the parked channel dies because the state
/// does. Observed: `a parked reply must not outlive its actor as a hang:
/// Elapsed(())`, at five seconds.
#[tokio::test]
async fn a_parked_reply_whose_actor_ends_is_answered_rather_than_hanging() {
    let book = orders::actor(Book::default());

    let placing = {
        let book = book.clone();
        tokio::spawn(async move { book.place(3).await })
    };
    parked(&book, 1).await;

    book.stop()
        .await
        .expect("a stop from outside the actor's own turn is never refused");

    let answer = n0_future::time::timeout(Duration::from_secs(5), placing)
        .await
        .expect("a parked reply must not outlive its actor as a hang")
        .expect("the caller is not itself panicking");
    assert!(
        answer.is_err(),
        "an actor that ended with a reply parked owes its caller a refusal: got {answer:?}"
    );
}

/// And the panicking case, which is the one thing `#[defer]` costs against every
/// other kind.
///
/// A handler that panics before consuming its `Reply` drops it inside the unwinding
/// future, so there is nothing left to answer with: the caller is told `Dropped`
/// instead of being handed the panic the way `ask` hands one on every other kind.
/// The actor survives it, which is the half that keeps the trade worth making — and
/// which is why the answer is *dropped* and not *gone*.
///
/// **Asserted on the variant rather than on `is_err`.** Both halves of the trade
/// are then one assertion apart: the answer names an actor that is serving, and the
/// call below it is served. An `is_err` here passed identically while the crate
/// told this caller to stop calling.
///
/// Made red by `scripts/break-once.sh keylink/src/turns.rs` on `answered`'s
/// `Standing::Running => Withheld::Dropped,` → `Standing::Running =>
/// Withheld::Gone,` — the fold this variant was split out of. Watched: `a handler
/// that panicked before answering owes its caller a refusal: got Err(Turn(Gone))`.
/// The `is_err` this replaced stayed **green** through that same break.
///
/// **Hand the harness that literal with its twelve spaces of indentation.** The
/// bare form counts 2 in `turns.rs`, whose own `mod tests` quotes the arm
/// unindented inside a `///` line, and the harness aborts at "found 2".
///
/// Measured at `81eabe9`, the commit that wrote this record and that `mod tests`
/// one together: the bare form already counted **2** there, so the quote above has
/// never been runnable as written and the watched run must have been given some
/// other spelling of it. Nothing here is unchecked as a result — the sentence is
/// still what the break prints — but the clause a reader would copy was not.
#[tokio::test]
async fn a_deferred_handler_that_panics_costs_its_caller_the_answer_and_not_the_actor() {
    let book = orders::actor(Book::default());

    let booming = {
        let book = book.clone();
        tokio::spawn(async move { book.boom(1).await })
    };
    let answer = n0_future::time::timeout(Duration::from_secs(5), booming)
        .await
        .expect("a panicking deferred handler must not hang its caller")
        .expect("the panic is NOT resumed in the caller, unlike every other kind");
    assert!(
        matches!(answer, Err(Unanswered::Turn(Withheld::Dropped))),
        "a handler that panicked before answering owes its caller a refusal: got {answer:?}"
    );

    // The half that matters: one bad message cost one message.
    assert_eq!(book.fill(7).await.expect("still running"), 0);
}

/// The sanctioned loss, through the local door: a handler that takes
/// `reply.later()` and **drops** the `Deferred` owes its caller `Dropped`.
///
/// No panic, no `stop()`, no supervisor. `Book::abandon` says `drop` out loud,
/// which is what `Deferred`'s own `#[must_use]` asks for, so this is the one cause
/// of a dead reply channel that is neither a fault nor an ending — and the actor is
/// untouched by it.
///
/// # The two assertions are one guard, and the second is not decoration
///
/// The variant alone can pass with its meaning inverted. `answered` reads
/// `Fate` to choose between the three, and a `Fate` left at `Standing::Running`
/// for an actor that is in fact gone answers `Dropped` — *the actor is serving* —
/// about a dead actor. That is the mirror of the defect this variant was split out
/// to remove, and no assertion on the variant can see it, because the variant is
/// the one it expects. The `fill` below refuses it: a gone actor answers nothing.
///
/// So neither half stands alone. The `fill` on its own was true before this
/// change, is true after it and would be true after a revert; the variant on its
/// own cannot tell *serving* from *reported as serving*.
///
/// Made red by `scripts/break-once.sh keylink/src/turns.rs` on `answered`'s
/// `Standing::Running => Withheld::Dropped,` → `Standing::Running =>
/// Withheld::Gone,` — the fold the variant was split out of. **Hand the harness
/// the literal with its sixteen spaces of indentation.** The bare form counts 2 in
/// `turns.rs`: that file's own `mod tests` records the same break against
/// `a_released_reply_is_dropped_when_the_turn_that_released_it_panicked`, and its
/// quote sits unindented inside a `///` line. Same-file collision arriving from
/// the target's side, and the harness aborts at "found 2".
///
/// Watched: `a dropped `Deferred` costs the caller its answer and not the actor`,
/// `left: Err(Turn(Gone))`, `right: Err(Turn(Dropped))` — the variant assertion,
/// which is the
/// one the name rests on. The `fill` is never reached under this break, so it is
/// asserted here and guarded by the ordinary `#[defer]` tests above.
#[tokio::test]
async fn a_live_actor_that_drops_a_deferred_is_told_dropped_and_not_gone() {
    let book = orders::actor(Book::default());

    assert_eq!(
        book.abandon(3).await,
        Err(Unanswered::Turn(Withheld::Dropped)),
        "a dropped `Deferred` costs the caller its answer and not the actor"
    );

    // What makes the line above a sentence about a LIVE actor rather than a
    // sentence a dead one could also produce.
    assert_eq!(
        book.fill(7).await.expect("the actor is still serving"),
        0,
        "the actor answers the call after the one whose reply it threw away"
    );
}

/// **The decision Wave 2a's `Faulted` forces: a reply parked before a fault stays
/// parked, and a later turn can still answer it.**
///
/// `Faulted` is that turn's answer to that turn's caller. The actor survives a
/// panic, so the state survives, so every promise the state was holding survives —
/// and answering a parked reply `Faulted` would tell one caller about another
/// caller's fault, with a `Panic` that cannot be cloned to tell two.
///
/// Ordered rather than raced: `place` is parked before `boom` is sent, `boom` is
/// awaited to completion before `fill` runs, and `fill` reports how many it answered.
#[tokio::test]
async fn a_reply_parked_before_a_fault_is_still_answered_after_it() {
    let book = orders::actor(Book::default());

    let placing = {
        let book = book.clone();
        tokio::spawn(async move { book.place(3).await })
    };
    parked(&book, 1).await;

    // A different message explodes. The actor survives it, and so does the promise
    // it was already holding.
    assert!(book.boom(1).await.is_err(), "the faulted turn has no value");
    assert_eq!(
        book.parked().await.expect("the actor survived the fault"),
        1,
        "a fault must not resolve a reply parked before it"
    );

    assert_eq!(book.fill(7).await.expect("running"), 1);
    let answer = n0_future::time::timeout(Duration::from_secs(5), placing)
        .await
        .expect("still answerable after the fault")
        .expect("the caller is not itself panicking");
    assert_eq!(answer.expect("the actor is running"), 21);
}

// ───────────────────────────────── inside the turn waits; off the turn does not

/// **The commit point, at the macro's own surface.** A turn that answers parked
/// callers and then panics gives them nothing: the sends waited for that turn's
/// verdict, and a turn that panicked produced nothing committable — a durability
/// adapter refuses one, so releasing its values would hand out answers about
/// state that was never stored.
///
/// `Dropped` and not `Faulted`, and that is not a near miss: there is one `Panic`,
/// it is the original payload rather than a message, and it belongs to the turn's
/// own caller. The actor caught that panic and is still serving, which is what
/// makes the answer *dropped* rather than *gone*. `turns.rs`'s
/// `a_released_reply_is_dropped_when_the_turn_that_released_it_panicked` asserts
/// the same variant one layer down, where a `TurnLost` is visible.
///
/// Made red by `scripts/break-once.sh keylink/src/turns.rs` on `Deferred::send`'s
/// `Verdict::Faulted => drop(reply),` → `Verdict::Faulted =>
/// reply.answer(Answered::Ran(value)),`. Watched: `a panicking turn's answer
/// crossed anyway: Ok(15)`.
///
/// **A second break, because the first proves only that nothing crossed.** The
/// assertion also names *which* answer, and no break above it can go red on the
/// wrong one. `answered`'s `Standing::Running => Withheld::Dropped,` →
/// `Standing::Running => Withheld::Gone,` is the fold, and it is red here:
/// `a panicking turn's answer crossed anyway: Err(Turn(Gone))`. The `is_err` this
/// assertion replaced stayed green through it.
///
/// **Hand the harness that literal with its sixteen spaces of indentation.** The
/// bare form counts 2 in `turns.rs` — the arm itself, and that file's own
/// `mod tests` quoting the same break unindented inside a `///` line — so the
/// harness aborts at "found 2". Sibling records above carry the same clause; this
/// one did not, and the quote it hands a reader was unrunnable as written.
#[tokio::test]
async fn an_answer_sent_inside_a_turn_that_then_panics_does_not_cross() {
    let book = orders::actor(Book::default());

    let placing = {
        let book = book.clone();
        tokio::spawn(async move { book.place(3).await })
    };
    parked(&book, 1).await;

    let filling = {
        let book = book.clone();
        tokio::spawn(async move { book.fill_then_boom().await })
    };
    assert!(
        filling
            .await
            .expect_err("an rpc resumes the panic")
            .is_panic(),
        "the filling turn's own caller gets the panic"
    );

    let answer = n0_future::time::timeout(Duration::from_secs(5), placing)
        .await
        .expect("a withheld answer must not hang its caller")
        .expect("the caller is not itself panicking");
    assert!(
        matches!(answer, Err(Unanswered::Turn(Withheld::Dropped))),
        "a panicking turn's answer crossed anyway: {answer:?}"
    );

    // The half that keeps the trade worth making: the actor survived.
    assert_eq!(book.parked().await.expect("still running"), 0);
}

/// **And the one thing left of the exception, pinned rather than only written
/// down.** A `Deferred` is `Send` and `'static`, so it can be handed to a spawned
/// task — and a send from there is answering with something no turn produced.
/// There is no lot in scope, so it crosses immediately.
///
/// The same turn as the test above, panicking the same way, differing in one
/// line: the send goes through `tokio::spawn`. The parked caller gets its value.
/// That is the honest scope of *"an answered call is a stored call"* — it holds
/// for every answer a **turn** produced, and a `Deferred` answered from outside
/// every turn is not one.
///
/// Withholding those would be the wrong fix rather than a stricter one: nothing
/// ran, no observer saw anything, and there is no commit for the answer to wait
/// for — so the value would wait for a turn that may never come.
///
/// Made red by `scripts/break-once.sh keylink/src/parking.rs` on `park`'s
/// `if let Some(answer) = carried {` → `if let Some(answer) = Option::<Parked>::None {`,
/// which drops an off-turn answer instead of giving it. Watched: `a send from
/// another task must not be gated by the turn that spawned it: Dropped`.
///
/// **The harness cannot express that break, so it was applied by hand.** The
/// literal counts **2** in `parking.rs` — `park`'s own arm, and a record in that
/// same file quoting it — so `break-once.sh` aborts at its count assert before a
/// backup exists, and no row here can register it. Re-measured on 2026-08-09
/// with the substitution written into `park`'s arm directly: control green, then
/// exit 101 on this one test. The watched word read `Undelivered` before that,
/// and the panic is `expect`'s, so it prints the error's `Debug` — a variant's
/// name, never the type's.
#[tokio::test]
async fn a_deferred_sent_from_another_task_is_not_gated_by_the_turn_that_spawned_it() {
    let book = orders::actor(Book::default());

    let placing = {
        let book = book.clone();
        tokio::spawn(async move { book.place(3).await })
    };
    parked(&book, 1).await;

    let detaching = {
        let book = book.clone();
        tokio::spawn(async move { book.detach_then_boom().await })
    };
    assert!(
        detaching
            .await
            .expect_err("an rpc resumes the panic")
            .is_panic(),
        "the detaching turn's own caller gets the panic"
    );

    let answer = n0_future::time::timeout(Duration::from_secs(5), placing)
        .await
        .expect("a detached answer must not hang its caller")
        .expect("the caller is not itself panicking");
    assert_eq!(
        answer.expect("a send from another task must not be gated by the turn that spawned it"),
        15,
    );
}

// ─────────────────────────────────────────────────────── and over the wire

/// The served path, on two real nodes, which is the only one where a deferred value
/// has to become the service's own `Response` on the way out.
///
/// A peer calls `place`, the turn parks its reply, the turn ends, and a **local**
/// `fill` answers it. So the value crosses `Reply::wrapping`'s boxed conversion,
/// installed by a generated arm and invoked from a turn that ran later — which is
/// the whole of what the served half of `#[defer]` is.
///
/// Made red by `scripts/break-once.sh`, replacing `Reply::wrapping`'s
/// `Answerer::Wrapped(Box::new(move |answered| self.answer(answered.map(wrap))))`
/// with a closure that drops all three — the conversion installed and then not
/// performed. Watched, when this was written: `the call succeeded:
/// Remote(NoSuchActor { id: "Orders" })`. **Re-watched after the losses became
/// variants**, and it now prints `the call succeeded: Remote(Withheld(Dropped))`.
///
/// **That error named the wrong fault, and it was a finding rather than a detail.**
/// The actor exists, was reached, and ran the turn; a served reply that died
/// unanswered was reported to the peer as though the service had never been mounted.
/// The test went red either way — the finding was about what an operator reads.
/// **Fixed:** `deliver_to_singleton` now splits the dead-channel arm three ways, and the
/// same break watched under it printed `Remote(Failed("dropped: the reply was taken
/// out of the turn and never answered"))`. **That is a transcript and not the
/// current text.** `lost`'s `Standing::Running` arm has been rewritten since — its
/// own comment records that the sentence was wrong twice, both times by naming a
/// cause — so the prefix is what survives from that run and the words after it are
/// read off `lost`. `a_local_caller_and_a_peer_agree_about_an_abandoned_deferred`
/// reaches the same rendering through the public API with nothing broken at all,
/// and it matches on the **prefix** for exactly this reason.
#[tokio::test]
async fn a_peer_gets_an_answer_parked_past_its_own_turn() {
    let node = local_node().await;
    let caller = local_node().await;
    let book = orders::actor(Book::default());
    let _server = Server::builder(&node)
        .service(book.service(Allowlist::anyone()).build())
        .spawn()
        .expect("serves");
    let client = orders::Client::connect(&caller, node.endpoint().addr())
        .await
        .expect("dials");

    let placing = tokio::spawn(async move { client.place(3).await });
    parked(&book, 1).await;
    assert_eq!(book.fill(7).await.expect("running"), 1);

    let answer = n0_future::time::timeout(Duration::from_secs(10), placing)
        .await
        .expect("a peer must not hang on a parked reply")
        .expect("the caller is not itself panicking");
    assert_eq!(answer.expect("the call succeeded"), 21);
}

/// One actor, one cause, two doors, two answers that agree — and a **live** actor
/// must not be reported to a peer as absent.
///
/// The served path had one `Err(_)` arm for a dead reply channel and rendered it
/// [`RemoteError::NoSuchActor`] — *there is nothing here, stop asking*. `#[defer]`
/// added a cause that channel can die from **while the actor is running**: a handler
/// takes `reply.later()` and drops the `Deferred`. That is sanctioned rather than
/// misuse — `Deferred`'s own `#[must_use]` says "dropping a deferred reply answers
/// the caller `Dropped`; that is sometimes right, so say so with `drop`" — so the
/// answer a peer reads for it has to be true, and so does the answer a local caller
/// reads for the same cause on the same actor.
///
/// # The old name promised a setup that cannot exist
///
/// It was `a_live_actor_does_not_tell_a_peer_no_such_actor`, which is half of what
/// this now holds, and the pairing it was extended into cannot be written the way
/// the obvious reading of "both sides see it" asks for: **a reply has exactly one
/// destination**, so a peer's abandoned reply and a local caller's abandoned reply
/// are two replies and never one seen twice. The achievable claim is the one in the
/// first line, and it is the one worth having — a name describing an impossible
/// setup invites an implementer to approximate it and call the approximation done.
///
/// # Both halves assert what they got, and neither asserts `is_err`
///
/// Two `Err`s compared against `Err` is the wrong-reason pass this pairing is most
/// exposed to: every wrong answer here is also an error. So the peer half matches
/// on the `"dropped: "` **prefix** and the local half on the **variant**, and a
/// break that collapses either door back onto *gone* moves one of the two.
///
/// No supervision, no panic, no `stop()`. The actor answers the very next call on
/// the same connection, which is the half `NoSuchActor` denies.
///
/// Made red twice, once per door, because either break leaves the other half
/// green — which is the same evidence that makes the pairing worth asserting:
///
/// - `scripts/break-once.sh keylink/src/turns.rs` on the served arm itself,
///   `lost`'s whole `Standing::Running` arm — the two-line `format!` and its
///   closing `)),` — replaced by
///   `Standing::Running => Withheld::Gone,`, which collapses the split back onto
///   the answer that means *stop calling*. It cannot be the pre-fix
///   `NoSuchActor` any more: all three arms are `Withheld` now, so the fold has
///   to happen inside that vocabulary. Watched:
///   `a live actor must not be reported as absent: Remote(Withheld(Gone))`.
///   Quote the arm with the comment line above it — `        // the message, and
///   it will answer the next one.` — because the bare arm text also sits inside
///   `answered` one indent deeper and counts 2.
/// - and on the local arm, `answered`'s
///   `Standing::Running => Withheld::Dropped,` → `Standing::Running =>
///   Withheld::Gone,`. Quote that literal with its **sixteen spaces** of
///   indentation; the bare form counts 2 in `turns.rs`, which aborts the harness.
///   Watched: `the local door names the same cause the peer's does`,
///   `left: Err(Turn(Gone))`, `right: Err(Turn(Dropped))` — and the peer half passed
///   in that same run, which is the measurement behind "either break leaves the
///   other half green".
#[tokio::test]
async fn a_local_caller_and_a_peer_agree_about_an_abandoned_deferred() {
    let node = local_node().await;
    let caller = local_node().await;
    let book = orders::actor(Book::default());
    let _server = Server::builder(&node)
        .service(book.service(Allowlist::anyone()).build())
        .spawn()
        .expect("serves");
    let client = orders::Client::connect(&caller, node.endpoint().addr())
        .await
        .expect("dials");

    let lost = client
        .abandon(3)
        .await
        .expect_err("a dropped `Deferred` answers nobody");
    match lost {
        keylink::Error::Remote(RemoteError::Withheld(Withheld::Dropped)) => {}
        other => panic!("a live actor must not be reported as absent: {other:?}"),
    }

    // The other door, on the same actor and the same cause. Two replies rather
    // than one, for the reason the doc gives — what is asserted is that the two
    // answers agree about the actor, not that they share a channel.
    assert_eq!(
        book.abandon(3).await,
        Err(Unanswered::Turn(Withheld::Dropped)),
        "the local door names the same cause the peer's prefix does"
    );

    // Same connection, immediately after. This is what makes the finding a
    // defect rather than a wording preference.
    assert_eq!(
        client.parked().await.expect("the actor is still serving"),
        0,
        "the actor answers the call after the one it was reported absent for"
    );
    assert!(book.is_running(), "and nothing ended");
}

/// The other half: an actor that really has **stopped** still answers `NoSuchActor`.
///
/// The split above must not cost the case it was carved out of. A stopped actor's
/// parked reply is a *gone* actor and not an abandoned one, and the peer's next
/// action — stop asking — is the right one.
///
/// **Which line answers here is now the plain one**, and that is the change worth
/// recording: the `ended` channel is not consulted at all. `stop()` breaks the loop
/// with [`Ended::Stopped`], `run_loop` asks its inbox what that means, and the
/// default answer for an inbox with no supervisor above it is *gone* — settled
/// before the state that holds this reply is dropped. The old split read the
/// `Life` channel and `is_running()` and was right here by luck of timing rather
/// than by ordering; see the twin below for the case where that luck ran out.
///
/// Made red by `scripts/break-once.sh keylink/src/turns.rs` on the default
/// `Inbox::ending` — anchored on its whole three-line body, since a bare
/// `Standing::Gone` is **not unique** in that file and the harness aborts on an
/// ambiguous anchor.
///
/// **That clause carried a count — "occurs fourteen times" — and the count is the
/// part that rotted.** Measured across the commits that touched it: 14 at
/// `3628f14`, where the clause was written and where it was **right**; 16 from
/// `99ce53f`; 17 at `81eabe9`, one commit into this wave. So this is not a figure
/// somebody got wrong — it is a figure that was correct, was never re-derived, and
/// went stale under three unrelated commits while reading like evidence.
///
/// **Replaced by the property, which is what the anchor actually needs.**
/// `Standing::Gone` is one arm of a standing `turns.rs` matches on in the
/// supervisor, in the served renderer, in `answered` and in its own tests, so
/// *more than one* follows from how the file is written rather than from a
/// population anyone must keep. The harness aborts at any count that is not 1,
/// which makes uniqueness the whole requirement and the exact number a fact this
/// record never needed. A count is what a reader trusts and nobody re-runs.
/// Watched: `a stopped actor is gone, not merely
/// unanswering: Remote(Withheld(Dropped))`.
#[tokio::test]
async fn a_stopped_actor_still_tells_a_peer_the_actor_is_gone() {
    let node = local_node().await;
    let caller = local_node().await;
    let book = orders::actor(Book::default());
    let _server = Server::builder(&node)
        .service(book.service(Allowlist::anyone()).build())
        .spawn()
        .expect("serves");
    let client = orders::Client::connect(&caller, node.endpoint().addr())
        .await
        .expect("dials");

    let placing = tokio::spawn(async move { client.place(3).await });
    parked(&book, 1).await;
    book.stop()
        .await
        .expect("a stop from outside the actor's own turn is never refused");

    let lost = n0_future::time::timeout(Duration::from_secs(10), placing)
        .await
        .expect("a peer must not hang on a reply its actor took to the grave")
        .expect("the caller is not itself panicking")
        .expect_err("the state died holding it");
    assert!(
        matches!(
            &lost,
            keylink::Error::Remote(RemoteError::Withheld(Withheld::Gone))
        ),
        "a stopped actor is gone, not merely unanswering: {lost:?}"
    );
}

/// And an **aborted** actor, which is the case nothing inside the loop can answer.
///
/// `abort()` destroys the task, so the loop never reaches the hook where an ending
/// says what it means: an aborted actor's cell would read *running* for the rest of
/// the process, and every peer holding a parked reply would be told a destroyed
/// actor merely dropped their answer and is still there. So `LocalActor::abort`
/// publishes *gone* itself, **before** the request lands on the runtime — the
/// runtime drops the future only after it has seen the abort, and drops the state
/// after that, so the reply this kills cannot die before the answer for it exists.
///
/// The old split leaned on `!mail.is_closed()` here instead, which happens to be
/// right for an unsupervised actor only because a cancelled future's *parameters*
/// drop in reverse declaration order — a fact about rustc, not about this crate,
/// and false outright for a supervised one, whose receiver belongs to the
/// supervisor's frame.
///
/// Made red by `scripts/break-once.sh keylink/src/turns.rs` on `Floor`'s own
/// `Drop`, `self.fate.settle(Standing::Gone);` → `Standing::Running`. Watched:
/// `an aborted actor is gone, and its own loop will never say so:
/// Remote(Withheld(Dropped))`.
///
/// **`LocalActor::abort` used to settle this itself, and that line is deleted.**
/// Once `run_loop` carried a floor, no break of the settle in `abort` could be
/// made red — this test stays green with the whole line removed, because the
/// floor is strictly earlier in the same drop. A line that cannot be made red is
/// not a guard.
#[tokio::test]
async fn an_aborted_actor_tells_a_peer_the_actor_is_gone() {
    let node = local_node().await;
    let caller = local_node().await;
    let book = orders::actor(Book::default());
    let _server = Server::builder(&node)
        .service(book.service(Allowlist::anyone()).build())
        .spawn()
        .expect("serves");
    let client = orders::Client::connect(&caller, node.endpoint().addr())
        .await
        .expect("dials");

    let placing = tokio::spawn(async move { client.place(3).await });
    parked(&book, 1).await;
    book.abort();

    let lost = n0_future::time::timeout(Duration::from_secs(10), placing)
        .await
        .expect("a peer must not hang on a reply its actor was killed holding")
        .expect("the caller is not itself panicking")
        .expect_err("the state was destroyed holding it");
    assert!(
        matches!(
            &lost,
            keylink::Error::Remote(RemoteError::Withheld(Withheld::Gone))
        ),
        "an aborted actor is gone, and its own loop will never say so: {lost:?}"
    );
}

// ─────────────────────────── the same three answers, from a KEYED actor

/// A keyed book, and the one id every test below addresses.
fn book_id() -> ActorId {
    ActorId::new("obra/4471").expect("a valid id")
}

/// A served keyed system holding one `Book`, and a client connected to it.
///
/// The keyed twin of the fixture the singleton tests build inline. It hands the
/// system back as well as the client, because the interesting endings on this
/// path are the ones a *registry* causes.
async fn keyed(nodes: &[keylink::Node; 2]) -> (orders::System, orders::KeyedClient, Server) {
    let system: orders::System = orders::System::builder(Eviction::Never).build();
    system
        .mount(book_id(), orders::spawn(Book::default()))
        .expect("the id is free");
    let server = Server::builder(&nodes[0])
        .service(orders::keyed_service(&system, Allowlist::anyone()).build())
        .spawn()
        .expect("one service, one ALPN");
    let client = orders::KeyedClient::connect(&nodes[1], nodes[0].endpoint().addr())
        .await
        .expect("the client dials the actor system");
    (system, client, server)
}

/// **The finding this wave exists for.** A live *keyed* actor must not be
/// reported to a peer as absent.
///
/// The keyed served path had one `Err(_)` arm for a dead reply channel and
/// rendered every cause of it as [`RemoteError::NoSuchActor`] — *no actor is
/// mounted as X*. Here there is no supervisor, no panic, no `stop()`, no
/// eviction and no registry event of any kind: an ordinary keyed actor takes
/// `reply.later()` and drops the `Deferred`, which is the move that type's own
/// `#[must_use]` asks a handler to make. It is mounted, it ran the turn, and it
/// answers the very next call on the same connection.
///
/// The twin of `a_local_caller_and_a_peer_agree_about_an_abandoned_deferred` one path over,
/// and the pair is the point: `deliver_to_singleton` and `deliver_to_actor` render
/// through one function now, so a condition cannot be answered two ways.
///
/// Made red by `scripts/break-once.sh keylink/src/actor/remote.rs` on the arm
/// itself, `.map_err(|_| lost(fate.standing()))` →
/// `.map_err(|_| no_such_actor(&actor))`, which is the pre-fix line word for
/// word. Watched: `a live keyed actor must not be reported as absent:
/// Remote(NoSuchActor { id: "obra/4471" })`.
#[tokio::test]
async fn a_live_keyed_actor_does_not_tell_a_peer_no_such_actor() {
    let nodes = [local_node().await, local_node().await];
    let (system, client, _server) = keyed(&nodes).await;

    let lost = orders::at(&client, book_id())
        .abandon(3)
        .await
        .expect_err("a dropped `Deferred` answers nobody");
    match lost {
        keylink::Error::Remote(RemoteError::Withheld(Withheld::Dropped)) => {}
        other => panic!("a live keyed actor must not be reported as absent: {other:?}"),
    }

    // Same connection, same id, immediately after. This is what makes the
    // finding a defect rather than a wording preference.
    assert_eq!(
        orders::at(&client, book_id())
            .parked()
            .await
            .expect("the actor is still serving"),
        0,
        "the actor answers the call after the one it was reported absent for"
    );
    assert!(
        system.contains(&book_id()),
        "and nothing unmounted it: the registry never heard about any of this"
    );
}

/// The other half, on the keyed path: an actor that really is gone still answers
/// [`RemoteError::NoSuchActor`].
///
/// The split must not cost the case it was carved out of. `abort` destroys the
/// task, so the loop never reaches the hook where an ending settles its own
/// verdict, and two lines answer instead: `System::abort` publishes before it asks
/// the runtime for the destruction, and `Mailbox`'s `Drop` is the floor under
/// whatever the loop was.
///
/// **Which of the two answers *here* was measured, and it is the floor.** An
/// unsupervised keyed actor's mailbox is `run_loop`'s own parameter, so a
/// destroyed future drops it before the state, and its `Drop` overwrites what
/// `System::abort` published a moment earlier. Breaking `System::abort`'s settle
/// alone therefore leaves this test **green** — recorded because it is exactly
/// the sort of belt that reads like a guard. That line earns its place on the
/// *supervised* keyed path, where the mailbox belongs to the supervisor's frame
/// and its `Drop` is too late; it is held by
/// `registry::tests::abort_settles_the_served_answer_before_it_destroys_the_actor`,
/// which observes inside the drop rather than after it.
///
/// Made red by `scripts/break-once.sh keylink/src/actor/registry.rs` on
/// `Mailbox`'s `Drop`, `self.fate.settle(Standing::Gone);` →
/// `self.fate.settle(Standing::Running);`. Watched: `an aborted keyed actor is
/// gone, and its own loop will never say so: Remote(Withheld(Dropped))`.
#[tokio::test]
async fn an_aborted_keyed_actor_tells_a_peer_the_actor_is_gone() {
    let nodes = [local_node().await, local_node().await];
    let (system, client, _server) = keyed(&nodes).await;

    let placing = {
        let client = client.clone();
        tokio::spawn(async move { orders::at(&client, book_id()).place(3).await })
    };
    keyed_parked(&client, 1).await;
    assert!(system.abort(&book_id()).await, "it was mounted");

    let lost = n0_future::time::timeout(Duration::from_secs(10), placing)
        .await
        .expect("a peer must not hang on a reply its actor was killed holding")
        .expect("the caller is not itself panicking")
        .expect_err("the state was destroyed holding it");
    assert!(
        matches!(
            &lost,
            keylink::Error::Remote(RemoteError::Withheld(Withheld::Gone))
        ),
        "an aborted keyed actor is gone, and its own loop will never say so: {lost:?}"
    );
}

/// And the third door out of a registry: a `shutdown` that **drains** an actor
/// holding a parked remote reply.
///
/// **This test asserted the opposite until `deliver_to_actor` stopped holding a
/// mailbox sender across the wait.** Its rustdoc read *"a peer waiting on a parked
/// reply holds a clone of the actor's mailbox sender for as long as it waits, so
/// the loop never sees its senders go and the drain always runs out its budget"*,
/// and it asserted `Some(Ended::Vanished)` against fifty milliseconds. True of the
/// tree and false of the design: `drain` drops the registry's sender and leaves
/// the task alive *"while any other sender does"* so that a message **already in
/// flight** can finish, and a parked `#[defer]` reply is not one — it was
/// delivered, and its turn ran to completion. A keyed actor holding one could not
/// be stopped gracefully at all. `deliver_to_actor` drops its `Route` the moment
/// the send lands now, and this is the door that proves it.
///
/// **The elapsed time is asserted and not only the variant**, because a `Stopped`
/// that cost the whole budget is the same defect wearing a bigger number: fifty
/// milliseconds reached `Vanished`, and thirty seconds would have reached
/// `Stopped` after hanging for thirty seconds.
///
/// The bound is a sixth of the budget, and the number under it was **measured**
/// rather than chosen: six consecutive runs spent between **213 µs and 513 µs** of
/// the thirty seconds, on an M5 Max MacBook Pro, macOS 27.0, 2026-08-07, with a
/// debug build. Re-run it with:
///
/// ```sh
/// cargo test -p keylink --test defer \
///   a_keyed_actor_holding_a_parked_reply_drains_inside_its_budget -- --nocapture
/// ```
///
/// Five seconds is therefore about ten thousand times the slowest of those, which
/// is the headroom a loaded box gets. Load can only make a real drain slower —
/// never faster, and never instant, which is the direction the defect went.
///
/// The expired-budget door did not disappear with the premise, and is no longer
/// held here. `actors::a_shutdown_whose_budget_expires_destroys_the_actor` drives
/// a hand-written loop that genuinely never returns, and
/// `registry::tests::an_expired_shutdown_budget_settles_the_served_answer_before_the_abort`
/// holds `drain`'s own settle. What this test added over its aborted twin above is
/// the **peer's** answer, and that is still what its second half asserts.
///
/// Made red by `scripts/break-once.sh keylink/src/actor/remote.rs`, `    drop(route);`
/// → `    let _held = route;` — the defect restored, since the clone then lives to
/// the end of the function again. Watched: `a keyed actor holding a parked remote
/// reply must drain: got Some(Vanished)`.
///
/// **That break reddens the VARIANT assertion, and the elapsed bound below it is
/// never evaluated on the broken tree.** Nothing releases the clone under the edit,
/// so the drain burns the whole budget, `shutdown` answers `Vanished`, and the first
/// `assert!` fires before `spent` is ever compared. So the two assertions are not
/// both exercised by one replay, and a replayer who sees red has watched one of
/// them.
///
/// The elapsed bound guards a **different shape**: a drain that finishes, but
/// slowly. No one-literal edit here produces that — the defect is a hang, not a
/// slowdown, and a hang is caught by the variant. The bound is what stops the same
/// defect returning behind a *larger* budget, where `Stopped` would be the answer
/// and the wall time the only tell. It is unwatched, and it is carried on the
/// measured 213–513 µs above rather than on a break.
#[tokio::test]
async fn a_keyed_actor_holding_a_parked_reply_drains_inside_its_budget() {
    let nodes = [local_node().await, local_node().await];
    let (system, client, _server) = keyed(&nodes).await;

    let placing = {
        let client = client.clone();
        tokio::spawn(async move { orders::at(&client, book_id()).place(3).await })
    };
    keyed_parked(&client, 1).await;

    // Deliberately far more than the drain needs, which is the whole instrument:
    // the defect spent every budget it was given, so a small one hid inside the
    // noise of a loaded box and a large one cannot.
    let budget = Duration::from_secs(30);
    let started = std::time::Instant::now();
    let ended = system.shutdown(&book_id(), budget).await;
    let spent = started.elapsed();

    assert!(
        matches!(ended, Some(Ended::Stopped)),
        "a keyed actor holding a parked remote reply must drain: got {ended:?}"
    );
    // Captured by the harness unless `--nocapture`, so the gate pays nothing for
    // it and the number in the documentation above stays re-runnable rather than
    // remembered.
    println!("── drained in {spent:?} of a {budget:?} budget");
    assert!(
        spent < budget / 6,
        "a Stopped that cost its whole budget is the sender clone still being held: \
         {spent:?} of {budget:?}"
    );

    let lost = n0_future::time::timeout(Duration::from_secs(10), placing)
        .await
        .expect("a peer must not hang on a reply its actor drained holding")
        .expect("the caller is not itself panicking")
        .expect_err("the state was dropped holding it");
    assert!(
        matches!(
            &lost,
            keylink::Error::Remote(RemoteError::Withheld(Withheld::Gone))
        ),
        "a drained actor is gone, and its parked caller is told so: {lost:?}"
    );
}

/// The same drain read from the peer's end, with a [`keylink::Timeout`] in front
/// of it — which is what makes *not* `TimedOut` an assertion rather than a hope.
///
/// Without a layer both trees end at `NoSuchActor` and the broken one is merely a
/// budget late, so the sibling above has to read a clock to tell them apart. A
/// `Timeout` turns that delay into a **different answer**: the layer drops the
/// wait at two seconds and renders [`RemoteError::TimedOut`], so the peer is told
/// its call expired when what happened is that its actor was stopped and the
/// honest answer was there to be given.
///
/// It is also why the sibling above must **not** have a layer. Dropping the wait
/// drops `deliver_to_actor`'s frame and the sender clone inside it, so on the
/// broken tree the drain finishes *because the peer gave up* — two seconds in
/// rather than thirty — and an elapsed assertion passes. One defect, two shapes,
/// and neither shape sees both halves of it.
///
/// Made red by the same edit the sibling records, `scripts/break-once.sh
/// keylink/src/actor/remote.rs`, `    drop(route);` → `    let _held = route;`.
/// Watched: `a drained actor must answer for itself, and a TimedOut means the peer
/// waited out the layer instead: Remote(TimedOut { millis: 2000 })`.
#[tokio::test]
async fn a_peer_whose_parked_reply_is_drained_is_not_told_its_call_timed_out() {
    let nodes = [local_node().await, local_node().await];
    // Built inline rather than through `keyed`, because the layer is the subject.
    let system: orders::System = orders::System::builder(Eviction::Never).build();
    system
        .mount(book_id(), orders::spawn(Book::default()))
        .expect("the id is free");
    let _server = Server::builder(&nodes[0])
        .service(
            orders::keyed_service(&system, Allowlist::anyone())
                .layer(keylink::Timeout::after(Duration::from_secs(2)))
                .build(),
        )
        .spawn()
        .expect("one service, one ALPN");
    let client = orders::KeyedClient::connect(&nodes[1], nodes[0].endpoint().addr())
        .await
        .expect("the client dials the actor system");

    let placing = {
        let client = client.clone();
        tokio::spawn(async move { orders::at(&client, book_id()).place(3).await })
    };
    keyed_parked(&client, 1).await;
    let ended = system.shutdown(&book_id(), Duration::from_secs(30)).await;
    assert!(
        matches!(ended, Some(Ended::Stopped)),
        "the drain is the sibling's claim, and this test rests on it: got {ended:?}"
    );

    let lost = n0_future::time::timeout(Duration::from_secs(10), placing)
        .await
        .expect("a peer must not hang on a reply its actor drained holding")
        .expect("the caller is not itself panicking")
        .expect_err("the state was dropped holding it");
    assert!(
        matches!(
            &lost,
            keylink::Error::Remote(RemoteError::Withheld(Withheld::Gone))
        ),
        "a drained actor must answer for itself, and a TimedOut means the peer \
         waited out the layer instead: {lost:?}"
    );
}

/// **The other half of the dropped arm, which had no served test at all.**
///
/// `a_deferred_handler_that_panics_costs_its_caller_the_answer_and_not_the_actor`
/// is a **local** call: it asserts `is_err()` on a `Unanswered` and never
/// reaches `deliver_to_singleton`, so nothing anywhere asserted what a *peer* reads
/// for this. The arm's own comment concedes the case — "a handler that panics
/// after taking `reply.later()` arrives here too" — and the sentence it rendered
/// said "the reply was taken out of the turn and never answered", which names the
/// sanctioned drop and is false of this one — as was the reword that followed it,
/// "the reply left its turn", since here the reply never left the turn at all: it
/// was a `Reply` that died *inside* the handler, and never a `Deferred`. The
/// sentence names the actor now and nothing else.
///
/// So the sentence names no cause now, and this is what pins that it covers both:
/// a handler that panicked **without** consuming its reply reads exactly what a
/// handler that dropped one deliberately reads. The panic message is gone, and
/// that is the honest outcome rather than a gap — the `Reply` died in the unwind,
/// so there is no channel left to render a `Panic` down.
///
/// Made red by `scripts/break-once.sh keylink/src/turns.rs` on `lost`'s
/// `Standing::Running` arm → `RemoteError::Failed(format!("{}bad", RemoteError::PANICKED))`,
/// which is the answer this case would have if the fault reached the peer as a
/// fault. Watched: `a panicking deferred handler loses the reply, and that is
/// what the peer must read: "panicked: bad"`.
#[tokio::test]
async fn a_peer_whose_deferred_handler_panicked_reads_the_same_dropped_answer() {
    let node = local_node().await;
    let caller = local_node().await;
    let book = orders::actor(Book::default());
    let _server = Server::builder(&node)
        .service(book.service(Allowlist::anyone()).build())
        .spawn()
        .expect("serves");
    let client = orders::Client::connect(&caller, node.endpoint().addr())
        .await
        .expect("dials");

    let lost = client
        .boom(1)
        .await
        .expect_err("the handler panicked holding the reply");
    match lost {
        keylink::Error::Remote(RemoteError::Withheld(Withheld::Dropped)) => {}
        other => panic!("the actor is alive and this is one lost reply: {other:?}"),
    }

    // Same connection: one bad message cost one message, exactly as the local
    // twin asserts for a local caller.
    assert_eq!(client.parked().await.expect("still serving"), 0);
}

/// [`parked`]'s keyed twin: wait by asking the actor rather than by sleeping.
async fn keyed_parked(client: &orders::KeyedClient, want: u64) {
    for _ in 0..500 {
        if orders::at(client, book_id())
            .parked()
            .await
            .expect("the actor is running")
            == want
        {
            return;
        }
        tokio::task::yield_now().await;
    }
    panic!("{want} orders never got parked");
}

/// And the ordinary shape over the wire: answered inside the turn, so the peer sees
/// nothing different from any other `rpc`.
///
/// Reddened by the same `Reply::wrapping` break its sibling above uses, watched here
/// as "answers: Remote(Withheld(Dropped))" at the **first** assertion.
/// So the break proves the deferred answer crosses the wire and proves nothing about
/// this test's second claim — that an ordinary call on the same connection
/// afterwards finds the stream whole. No one-literal edit was found that leaves a
/// deferred call's stream half-used while still answering it, so that half is
/// unwatched and is recorded as such rather than assumed.
#[tokio::test]
async fn a_peer_cannot_tell_a_deferred_method_from_an_ordinary_one() {
    let node = local_node().await;
    let caller = local_node().await;
    let book = orders::actor(Book::default());
    let _server = Server::builder(&node)
        .service(book.service(Allowlist::anyone()).build())
        .spawn()
        .expect("serves");
    let client = orders::Client::connect(&caller, node.endpoint().addr())
        .await
        .expect("dials");

    assert_eq!(client.quote(21).await.expect("answers"), 42);
    // The same client, the same connection, an ordinary method after a deferred
    // one: nothing about the stream was left half-used.
    assert_eq!(client.parked().await.expect("answers"), 0);
}
