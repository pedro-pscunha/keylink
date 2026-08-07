//! `#[defer]`: a reply that outlives the turn that received it.
//!
//! Wave 2c. The wire does not change — a deferred method is an ordinary `rpc` to
//! the caller, which blocks on its stream exactly as before — so everything here is
//! about who holds the reply channel and when.
//!
//! Four claims, and each of the last three is a case a reader would otherwise have
//! to guess at:
//!
//! - a parked reply is answered from a **later turn**, and the value reaches the
//!   original caller;
//! - an answer given **inside** the turn is still gated by the observer chain, so a
//!   `#[defer]` method does not quietly cost a durable actor its commit;
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
    Allowlist, Caller, Deferred, Ended, Observer, Replied, Reply, Server, Stop, TurnFut, TurnInfo,
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

// ─────────────────────────────────────────────────────── and over the wire

/// The served path, on two real nodes, which is the only one where a deferred value
/// has to become the service's own `Response` on the way out.
///
/// A peer calls `place`, the turn parks its reply, the turn ends, and a **local**
/// `fill` answers it. So the value crosses `Reply::wrapping`'s boxed conversion,
/// installed by a generated arm and invoked from a turn that ran later — which is
/// the whole of what the served half of `#[defer]` is.
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

/// And the ordinary shape over the wire: answered inside the turn, so the peer sees
/// nothing different from any other `rpc`.
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
