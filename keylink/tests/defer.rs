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
    Server, Stop, TurnFut, TurnInfo,
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
/// `self.0.answer(Answered::Ran(value));` with `drop((self, value));` — a park that
/// takes the channel away and never gives it back. Watched: this fails at the
/// answer, "the actor is running: MailboxClosed", which is what a caller sees when
/// the channel dies unanswered. It prints rather than hangs because the wait is
/// bounded by the sender being dropped, not by a timeout.
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
/// `self.0.answer(Answered::Ran(value));` replaced by `drop((self, value));` —
/// watched here as "running: MailboxClosed" at the first order. That fails both
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
/// keylink that is `TurnLost::Gone`; through a generated `Handle` it is
/// `MailboxClosed`, which is the same fact under this path's one error type.
///
/// **The timeout is the assertion.** A hang is not a failure to a test harness, so
/// the wait is bounded and the bound is what goes red. `local.rs`'s own unit test
/// asserts the `TurnLost::Gone` underneath, where the variant is visible.
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

    book.stop().await;

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
/// future, so there is nothing left to answer with: the caller is told the actor is
/// gone instead of being handed the panic the way `ask` hands one on every other
/// kind. The actor survives it, which is the half that keeps the trade worth making.
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
        answer.is_err(),
        "a handler that panicked before answering owes its caller a refusal: got {answer:?}"
    );

    // The half that matters: one bad message cost one message.
    assert_eq!(book.fill(7).await.expect("still running"), 0);
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
/// `Gone` and not `Faulted`, and that is not a near miss: there is one `Panic`,
/// it is the original payload rather than a message, and it belongs to the turn's
/// own caller. `local.rs`'s
/// `a_released_reply_is_gone_when_the_turn_that_released_it_panicked` asserts the
/// variant, which a `Handle` folds away.
///
/// Made red by `scripts/break-once.sh keylink/src/local.rs` on `Deferred::send`'s
/// `Verdict::Faulted => drop(reply),` → `Verdict::Faulted =>
/// reply.answer(Answered::Ran(value)),`. Watched: `a panicking turn's answer
/// crossed anyway: Ok(15)`.
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
        answer.is_err(),
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
/// another task must not be gated by the turn that spawned it: MailboxClosed`.
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
/// Remote(NoSuchActor { id: "Orders" })`.
///
/// **That error named the wrong fault, and it was a finding rather than a detail.**
/// The actor exists, was reached, and ran the turn; a served reply that died
/// unanswered was reported to the peer as though the service had never been mounted.
/// The test went red either way — the finding was about what an operator reads.
/// **Fixed:** `deliver_to_local` now splits the dead-channel arm three ways, and the
/// same break watched under it prints `Remote(Failed("dropped: the reply was taken
/// out of the turn and never answered"))`. `a_live_actor_does_not_tell_a_peer_no_such_actor`
/// reaches the same rendering through the public API with nothing broken at all.
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

/// A **live** actor must not be reported to a peer as absent.
///
/// The served path had one `Err(_)` arm for a dead reply channel and rendered it
/// [`RemoteError::NoSuchActor`] — *there is nothing here, stop asking*. `#[defer]`
/// added a cause that channel can die from **while the actor is running**: a handler
/// takes `reply.later()` and drops the `Deferred`. That is sanctioned rather than
/// misuse — `Deferred`'s own `#[must_use]` says "dropping a deferred reply answers
/// the caller `Gone`; that is sometimes right, so say so with `drop`" — so the
/// answer a peer reads for it has to be true.
///
/// No supervision, no panic, no `stop()`. The actor answers the very next call on
/// the same connection, which is the half `NoSuchActor` denies.
///
/// Made red by `scripts/break-once.sh keylink/src/local.rs` on the arm itself,
/// `lost`'s `Standing::Running => RemoteError::Failed(..)` →
/// `Standing::Running => RemoteError::NoSuchActor { id: id.to_owned() }`, which
/// collapses the split back into the pre-fix single answer. Watched:
/// `a live actor must not be reported as absent: Remote(NoSuchActor { id: "Orders" })`.
#[tokio::test]
async fn a_live_actor_does_not_tell_a_peer_no_such_actor() {
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
        keylink::Error::Remote(RemoteError::Failed(why)) => assert!(
            why.starts_with("dropped: "),
            "a lost reply is its own answer and names itself: {why}"
        ),
        other => panic!("a live actor must not be reported as absent: {other:?}"),
    }

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
/// Made red by `scripts/break-once.sh keylink/src/local.rs` on the default
/// `Inbox::ending`'s `Standing::Gone` → `Standing::Running`. Watched: `a stopped
/// actor is gone, not merely unanswering: Remote(Failed("dropped: the reply left
/// its turn and was never answered"))`.
#[tokio::test]
async fn a_stopped_actor_still_tells_a_peer_no_such_actor() {
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
    book.stop().await;

    let lost = n0_future::time::timeout(Duration::from_secs(10), placing)
        .await
        .expect("a peer must not hang on a reply its actor took to the grave")
        .expect("the caller is not itself panicking")
        .expect_err("the state died holding it");
    assert!(
        matches!(&lost, keylink::Error::Remote(RemoteError::NoSuchActor { id }) if id == "Orders"),
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
/// Made red by `scripts/break-once.sh keylink/src/local.rs` on `LocalActor::abort`'s
/// `self.fate.settle(Standing::Gone);` → `self.fate.settle(Standing::Running);`.
/// Watched: `an aborted actor is gone, and its own loop will never say so:
/// Remote(Failed("dropped: the reply left its turn and was never answered"))`.
#[tokio::test]
async fn an_aborted_actor_tells_a_peer_no_such_actor() {
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
        matches!(&lost, keylink::Error::Remote(RemoteError::NoSuchActor { id }) if id == "Orders"),
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
/// The twin of `a_live_actor_does_not_tell_a_peer_no_such_actor` one path over,
/// and the pair is the point: `deliver_to_local` and `deliver_to_actor` render
/// through one function now, so a condition cannot be answered two ways.
///
/// Made red by `scripts/break-once.sh keylink/src/actor/remote.rs` on the arm
/// itself, `.map_err(|_| lost(route.fate.standing(), actor.as_str()))` →
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
        keylink::Error::Remote(RemoteError::Failed(why)) => assert!(
            why.starts_with("dropped: "),
            "a lost reply is its own answer and names itself: {why}"
        ),
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
/// gone, and its own loop will never say so: Remote(Failed("dropped: the reply
/// left its turn and was never answered"))`.
#[tokio::test]
async fn an_aborted_keyed_actor_tells_a_peer_no_such_actor() {
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
        matches!(&lost, keylink::Error::Remote(RemoteError::NoSuchActor { id }) if id == "obra/4471"),
        "an aborted keyed actor is gone, and its own loop will never say so: {lost:?}"
    );
}

/// And the third door out of a registry: a `shutdown` whose budget expires.
///
/// A peer waiting on a parked reply holds a clone of the actor's mailbox sender
/// for as long as it waits, so the loop never sees its senders go and the drain
/// always runs out its budget — the actor is destroyed rather than drained. That
/// is `drain`'s abort, a different line from `System::abort` above, and it needs
/// the same settle for the same reason.
///
/// Made red by `scripts/break-once.sh keylink/src/actor/registry.rs` on
/// `Mailbox`'s `Drop`, `self.fate.settle(Standing::Gone);` →
/// `self.fate.settle(Standing::Running);` — the floor, for the twin above's
/// reason: on an *unsupervised* keyed actor it drops before the state and is what
/// answers, so breaking `drain`'s own settle leaves this green. `drain`'s line is
/// held by
/// `registry::tests::an_expired_shutdown_budget_settles_the_served_answer_before_the_abort`.
/// Watched: `an actor destroyed by an expired budget is gone:
/// Remote(Failed("dropped: the reply left its turn and was never answered"))`.
#[tokio::test]
async fn a_keyed_actor_destroyed_by_an_expired_budget_tells_a_peer_no_such_actor() {
    let nodes = [local_node().await, local_node().await];
    let (system, client, _server) = keyed(&nodes).await;

    let placing = {
        let client = client.clone();
        tokio::spawn(async move { orders::at(&client, book_id()).place(3).await })
    };
    keyed_parked(&client, 1).await;
    assert!(
        matches!(
            system.shutdown(&book_id(), Duration::from_millis(50)).await,
            Some(Ended::Vanished)
        ),
        "the waiting peer holds a sender, so nothing drains and the budget runs out"
    );

    let lost = n0_future::time::timeout(Duration::from_secs(10), placing)
        .await
        .expect("a peer must not hang on a reply its actor was killed holding")
        .expect("the caller is not itself panicking")
        .expect_err("the state was destroyed holding it");
    assert!(
        matches!(&lost, keylink::Error::Remote(RemoteError::NoSuchActor { id }) if id == "obra/4471"),
        "an actor destroyed by an expired budget is gone: {lost:?}"
    );
}

/// **The other half of the `dropped: ` arm, which had no served test at all.**
///
/// `a_deferred_handler_that_panics_costs_its_caller_the_answer_and_not_the_actor`
/// is a **local** call: it asserts `is_err()` on a `MailboxClosed` and never
/// reaches `deliver_to_local`, so nothing anywhere asserted what a *peer* reads
/// for this. The arm's own comment concedes the case — "a handler that panics
/// after taking `reply.later()` arrives here too" — and the sentence it rendered
/// said "the reply was taken out of the turn and never answered", which names the
/// sanctioned drop and is false of this one.
///
/// So the sentence names no cause now, and this is what pins that it covers both:
/// a handler that panicked **without** consuming its reply reads exactly what a
/// handler that dropped one deliberately reads. The panic message is gone, and
/// that is the honest outcome rather than a gap — the `Reply` died in the unwind,
/// so there is no channel left to render a `Panic` down.
///
/// Made red by `scripts/break-once.sh keylink/src/local.rs` on `lost`'s
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
        keylink::Error::Remote(RemoteError::Failed(why)) => assert!(
            why.starts_with(RemoteError::DROPPED),
            "a panicking deferred handler loses the reply, and that is what the \
             peer must read: {why:?}"
        ),
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
/// as "answers: Remote(NoSuchActor { id: "Orders" })" at the **first** assertion.
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
