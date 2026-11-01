//! An actor's second input, from outside the crate.
//!
//! A `feed` is a locally wired stream, selected against the mailbox inside the
//! same loop. What is pinned here is the five things that makes true, each of
//! which is a decision that could plausibly have gone the other way:
//!
//! | the decision | the test |
//! |---|---|
//! | a feed item reaches `&mut self`, interleaved with mailbox turns | `an_actor_takes_feed_items_interleaved_with_its_mailbox` |
//! | a feed item is a turn: the chain runs, `TurnInfo::method` names the feed | `a_feed_item_runs_the_observer_chain_and_the_turn_names_the_feed` |
//! | a feed item is committed by a durable observer, exactly as a turn is | `a_feed_item_is_committed_by_a_durable_observer` |
//! | a panic in a feed handler ends the actor | `a_panic_in_a_feed_handler_ends_the_actor` |
//! | a feed that ends, ends the actor | `a_feed_that_ends_ends_the_actor_and_names_it` |
//! | a feed does not consume a mailbox permit | `a_full_mailbox_still_admits_feed_items` |
//! | neither input starves the other | `neither_input_starves_the_other` |
//! | an observer attached to a **keyed** actor runs | `an_observer_attached_to_a_keyed_actor_runs` |
//!
//! # Not one assertion in this file reads a clock
//!
//! The brief for this wave asked for `tokio::time::pause`, on the ground that a
//! wall-clock assertion is the thing to avoid. A paused clock is one way to get
//! there and it is not the strongest one available here, so this file does
//! something better and says so rather than reaching for the named tool: **it
//! gates the loop.**
//!
//! Every test that cares about ordering holds the actor inside one turn — a
//! `hold` handler that awaits a channel — loads both inputs while it is held, and
//! only then releases it. At that point the arrival order is fully determined by
//! the loop's own fairness rule, so the assertions are exact sequences rather than
//! statistical ones. There is no clock to pause, because nothing waits for time to
//! pass.
//!
//! The one place a clock would still be needed is a *negative* claim about
//! throughput ("the mailbox is served at least this often per second"), and no
//! test here makes one: the claim is about order, and order is checkable.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use keylink::prelude::*;
use std::sync::{Arc, Mutex};

use keylink::{Caller, Ended, Observer, Stream, TurnFut, TurnInfo, TurnOutcome};
#[cfg(feature = "persist-file")]
use keylink::{Persist, Whole};
#[cfg(feature = "persist-file")]
use keylink_persist::file::Files;
#[cfg(feature = "persist-file")]
use keylink_persist::{Durable, RestoreError, StateId};
use tokio::sync::{mpsc, oneshot};

keylink::service! {
    service Book;

    /// Holds the loop inside one turn until the test releases it, which is what
    /// makes every ordering assertion below exact instead of statistical.
    rpc Hold() -> u64;
    /// A message that is queued by the time its `await` returns, so a test can
    /// load the mailbox deterministically. An `rpc` would wait for the turn, and
    /// the turn cannot run while the loop is held.
    tell Note(line: u64) -> ();
    /// The feed under test.
    feed Fills(fill: u64);
    /// A second one, so the rotation among feeds is exercised and so
    /// `Ended::Feed` has a wrong name available to report.
    feed Quotes(quote: u64);
}

/// A fill this book cannot price, which is how a feed handler is made to panic
/// without a second declaration.
const POISON: u64 = 666;

/// What the actor did, in the order it did it.
///
/// Outside the state on purpose: several tests here end the actor, and an actor
/// that ended has dropped its state. A `Vec` shared with the test is the only way
/// to read the order *after* the ending it produced.
type Log = Arc<Mutex<Vec<Event>>>;

/// One thing the actor did. A single enum rather than three counters, because
/// every ordering claim in this file is about which of these came between which.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Event {
    Held,
    Note(u64),
    Fill(u64),
    Quote(u64),
}

/// The obvious code, with no feed concern in sight beyond the handler's own
/// shape: no `Caller`, no return.
struct Book {
    log: Log,
    /// Sent the moment the loop enters `hold`, so a test knows the actor is
    /// blocked inside a turn rather than merely soon to be.
    entered: Option<oneshot::Sender<()>>,
    /// Awaited inside `hold`. Dropping the sender releases it.
    gate: Option<oneshot::Receiver<()>>,
}

impl Book {
    fn open(log: &Log) -> (Self, Gate) {
        let (entered, entered_rx) = oneshot::channel();
        let (release, gate) = oneshot::channel();
        (
            Self {
                log: log.clone(),
                entered: Some(entered),
                gate: Some(gate),
            },
            Gate {
                entered: entered_rx,
                release,
            },
        )
    }
}

/// The two ends of the hold, kept together so a test cannot release a loop it
/// never waited for.
struct Gate {
    entered: oneshot::Receiver<()>,
    release: oneshot::Sender<()>,
}

impl Gate {
    /// Block until the actor is inside `hold`, and hand back the release.
    async fn held(self) -> oneshot::Sender<()> {
        self.entered.await.expect("the actor enters `hold`");
        self.release
    }
}

impl book::Actor for Book {
    async fn hold(&mut self, _cx: Caller<'_>) -> u64 {
        if let Some(entered) = self.entered.take() {
            let _ = entered.send(());
        }
        if let Some(gate) = self.gate.take() {
            let _ = gate.await;
        }
        self.log.lock().unwrap().push(Event::Held);
        0
    }

    async fn note(&mut self, _cx: Caller<'_>, line: u64) {
        self.log.lock().unwrap().push(Event::Note(line));
    }

    // No `Caller` and no return. That this is the signature the trait demands is
    // itself an assertion: adding either is `E0053`.
    async fn on_fills(&mut self, fill: u64) {
        assert_ne!(fill, POISON, "a fill this book cannot price");
        self.log.lock().unwrap().push(Event::Fill(fill));
    }

    async fn on_quotes(&mut self, quote: u64) {
        self.log.lock().unwrap().push(Event::Quote(quote));
    }
}

/// A feed backed by a channel, so a test decides exactly what is queued and when
/// the feed ends.
///
/// `poll_fn` over `Receiver::poll_recv` rather than `tokio_stream`, which is not a
/// dependency of this crate — and it is also the shape a real caller reaches for
/// least often, so it is worth pinning that a plain `Stream` is all a feed wants:
/// nothing here names a keylink type.
fn feed(mut items: mpsc::Receiver<u64>) -> impl Stream<Item = u64> + Send + 'static {
    n0_future::stream::poll_fn(move |cx| items.poll_recv(cx))
}

/// A feed that never yields and never ends, for the tests that only wire one.
///
/// Deliberately **not** an empty stream: an empty one ends immediately, which
/// would end the actor as `Ended::Feed`. That is the correct behaviour and it is
/// exactly why a quiet feed has to be spelled differently from a finished one.
fn quiet() -> impl Stream<Item = u64> + Send + 'static {
    n0_future::stream::pending()
}

// ───────────────────────────────────────────────────────────── the observers

/// Reports every turn it saw down a channel, so a test can await the report
/// rather than sleep until it has probably happened.
struct Reporting {
    to: mpsc::UnboundedSender<(&'static str, bool)>,
}

impl<A> Observer<A> for Reporting {
    fn name(&self) -> &'static str {
        "reporting"
    }

    fn after_turn<'a>(&'a mut self, _state: &'a A, turn: TurnInfo<'a>) -> TurnFut<'a> {
        let seen = (
            turn.method,
            matches!(turn.outcome, TurnOutcome::Panicked { .. }),
        );
        let to = self.to.clone();
        Box::pin(async move {
            let _ = to.send(seen);
            Ok(())
        })
    }
}

// ───────────────────────────────────────────────────── an actor and its inputs

/// One actor, its log, and the far end of both feeds.
struct Wired {
    book: book::Handle<Book>,
    log: Log,
    fills: mpsc::Sender<u64>,
    quotes: mpsc::Sender<u64>,
}

/// Start a fed actor and hand back everything a test needs to drive it.
///
/// The `Gate` comes back separately so that awaiting it is a step a test has to
/// take: an ordering assertion made before the actor is inside `hold` proves
/// nothing.
fn wired(observers: Vec<Box<dyn Observer<Book>>>) -> (Wired, Gate) {
    let log: Log = Arc::new(Mutex::new(Vec::new()));
    let (state, gate) = Book::open(&log);
    let (fills, fill_rx) = mpsc::channel(16);
    let (quotes, quote_rx) = mpsc::channel(16);
    let book = observers
        .into_iter()
        .fold(book::setup(state), |setup, observer| {
            setup.observe(observer)
        })
        .feeds(book::Feeds::new(feed(fill_rx), feed(quote_rx)))
        .actor();
    (
        Wired {
            book,
            log,
            fills,
            quotes,
        },
        gate,
    )
}

/// Start the `hold` turn and wait until the actor is inside it.
///
/// The handle is cloned into a task because `hold` cannot return until the test
/// releases it, and the test is what does the releasing.
async fn hold(
    book: &book::Handle<Book>,
    gate: Gate,
) -> (tokio::task::JoinHandle<()>, oneshot::Sender<()>) {
    let held = {
        let book = book.clone();
        tokio::spawn(async move {
            let _ = book.hold().await;
        })
    };
    (held, gate.held().await)
}

// ─────────────────────────────────────────────────────────────────── the tests

/// **The wave's first criterion**: a feed item reaches `&mut self`, and it is
/// interleaved with mailbox turns rather than batched before or after them.
///
/// The sequence is exact, and it is exact because the loop's fairness rule is
/// exact: it alternates which input gets first refusal. `Held` is turn one, so the
/// feeds have first refusal on turn two, the mailbox on turn three, and so on.
/// A biased loop — mailbox always first — would put `Note` at position two and
/// every fill after it, which is the assertion that fails.
///
/// `Fills` is then dropped, so the actor ends on its own once everything queued
/// has run: no `stop()`, which would race the queued fills for the loop's
/// attention and make this test's order undecidable.
///
/// `Quotes` is deliberately kept alive. Dropping **both** ends the actor at
/// whichever the rotation reaches first, which is correct behaviour and cost this
/// test its last two fills when it was first written — a finished feed outranks a
/// queued item on a different feed, because the ending is not a turn that can wait
/// its place in a queue.
///
/// Made red by `scripts/break-once.sh`, replacing the loop's
/// `mail_first = !mail_first;` with `mail_first = true;` — the alternation deleted
/// and the mailbox given first refusal every time. Watched: left
/// `[Held, Note(7), Fill(1), Fill(2), Fill(3)]` against right
/// `[Held, Fill(1), Note(7), Fill(2), Fill(3)]`, which is the batching this test
/// names.
#[tokio::test]
async fn an_actor_takes_feed_items_interleaved_with_its_mailbox() {
    let (wired, gate) = wired(Vec::new());
    let (held, release) = hold(&wired.book, gate).await;

    // Both inputs loaded while the loop cannot look at either.
    for fill in [1, 2, 3] {
        wired.fills.send(fill).await.expect("the feed accepts");
    }
    let _ = wired.book.note(7).await.expect("running");
    drop(wired.fills);

    let _ = release.send(());
    held.await.expect("no panic");

    let ended = wired.book.ended().await;
    assert!(
        matches!(&ended, Ended::Feed { name } if *name == "Fills"),
        "got {ended:?}"
    );

    assert_eq!(
        *wired.log.lock().unwrap(),
        vec![
            Event::Held,
            Event::Fill(1),
            Event::Note(7),
            Event::Fill(2),
            Event::Fill(3),
        ],
        "a feed item must interleave with mailbox turns rather than being batched \
         before or after them"
    );
}

/// **A feed item is a turn**: the observer chain runs for one, and
/// `TurnInfo::method` names the *feed* — `"Fills"`, the declared spelling, not
/// `"on_fills"`.
///
/// Wave 2b predicted this field would need no widening for feeds, on the ground
/// that it names whichever declaration produced the turn. This is that prediction
/// executed.
///
/// Made red by `scripts/break-once.sh`, replacing the generated feed arm's
/// `::core::stringify!($feed)` — the argument `feed_turn` reports as
/// `TurnInfo::method` — with the literal `"on_fills"`, which is the handler's
/// spelling rather than the declaration's and is the exact mistake this pins.
/// Watched: left `Some(("on_fills", false))` against right `Some(("Fills", false))`.
#[tokio::test]
async fn a_feed_item_runs_the_observer_chain_and_the_turn_names_the_feed() {
    let (seen, mut reports) = mpsc::unbounded_channel();
    let (wired, gate) = wired(vec![Box::new(Reporting { to: seen })]);
    let (held, release) = hold(&wired.book, gate).await;

    wired.fills.send(1).await.expect("the feed accepts");
    wired.quotes.send(9).await.expect("the feed accepts");
    let _ = release.send(());
    held.await.expect("no panic");

    assert_eq!(reports.recv().await, Some(("Hold", false)));
    // Fills first, because the feeds get first refusal on turn two and `Fills` is
    // declared first — the rotation starts where the last item came from.
    assert_eq!(reports.recv().await, Some(("Fills", false)));
    assert_eq!(reports.recv().await, Some(("Quotes", false)));
}

/// **A panic inside a feed handler ends the actor**, as `Ended::Panicked`.
///
/// Deliberately unlike an `rpc`, whose panic costs one message and not the actor.
/// The difference is that an `rpc`'s fault is *answered* — it crosses the reply
/// channel and `ask` resumes it in the caller — and a feed has neither channel nor
/// caller, so surviving would mean dropping the fault silently and carrying on
/// with a state the handler may have left half applied.
///
/// The observer sees `TurnOutcome::Panicked` first, which is what a durability
/// adapter needs in order to refuse the write.
///
/// Made red by `scripts/break-once.sh`, replacing `feed_turn`'s `end.or(faulted)`
/// with `end` — the fault computed and then dropped, which is the silent-survival
/// defect this exists to catch. Watched: "got Feed { name: "Fills" } — a feed's
/// fault has nobody to be answered to, so it must end the actor rather than be
/// dropped". The prose above predicted exactly that reading, and it is why the feed
/// is dropped one item later: the break prints a wrong answer instead of hanging.
#[tokio::test]
async fn a_panic_in_a_feed_handler_ends_the_actor() {
    let (seen, mut reports) = mpsc::unbounded_channel();
    let (wired, gate) = wired(vec![Box::new(Reporting { to: seen })]);
    let (held, release) = hold(&wired.book, gate).await;

    wired.fills.send(POISON).await.expect("the feed accepts");
    // Dropped so the feed *also* ends, one item later. That is what makes the
    // assertion below fail rather than hang if the fault ever stops ending the
    // actor: an actor that survived the panic would go on to meet the feed's end
    // and report `Ended::Feed`, which is a wrong answer this test can print. A red
    // test that hangs proves the same thing and reports nothing.
    drop(wired.fills);
    let _ = release.send(());
    held.await.expect("no panic");

    let ended = wired.book.ended().await;
    assert!(
        matches!(&ended, Ended::Panicked { message } if message.contains("cannot price")),
        "got {ended:?} — a feed's fault has nobody to be answered to, so it must end \
         the actor rather than be dropped"
    );

    assert_eq!(reports.recv().await, Some(("Hold", false)));
    assert_eq!(
        reports.recv().await,
        Some(("Fills", true)),
        "the observer must see the fault, which is what lets it refuse the write"
    );
}

/// **A feed that ends, ends the actor**, and `Ended::Feed` names which one.
///
/// The failure this prevents is the one that has no symptom: a market-data
/// subscription that quietly stopped, an actor that goes on answering from state
/// frozen at the last item it saw, and every read of it stale but well formed.
///
/// The name matters as much as the stop. Two feeds are wired and only `Quotes` is
/// ended, so a loop that reported whichever feed it happened to be looking at
/// would name `Fills` half the time.
///
/// Red-tested by making the loop treat a finished feed as `Ended::Stopped`:
/// `scripts/break-once.sh keylink/src/turns.rs 'reason = Ended::Feed { name };' \
/// 'reason = Ended::Stopped; let _ = name;' a_feed_that_ends_ends_the_actor_and_names_it`.
#[tokio::test]
async fn a_feed_that_ends_ends_the_actor_and_names_it() {
    let (wired, gate) = wired(Vec::new());
    let (held, release) = hold(&wired.book, gate).await;

    // The one that ends is the one that must be named, and it is not the first
    // declared.
    drop(wired.quotes);
    let _ = release.send(());
    held.await.expect("no panic");

    let ended = wired.book.ended().await;
    assert!(
        matches!(&ended, Ended::Feed { name } if *name == "Quotes"),
        "got {ended:?}"
    );
    assert!(
        !wired.book.is_running(),
        "the actor must stop rather than serve state frozen at the feed's last item"
    );
}

/// **A feed does not consume a mailbox permit.** The mailbox is full — one message
/// in its single slot, a second sender parked on a permit only the loop can free —
/// and a feed item is handled **ahead of** the message that is already queued.
///
/// This is what keeps the depth of one honest. If a feed item were a *message* it
/// would compete for that single slot, so wiring a busy feed would starve every
/// caller and the mailbox constant would silently stop meaning what it says. It is a
/// different `select` arm instead, which is a statement about the loop rather than
/// about the channel's capacity.
///
/// # What is asserted, and what a test in this harness cannot assert
///
/// The strongest claim would be about a *continuously* saturated mailbox, and that
/// state cannot be built here: at the default local mailbox depth of 1, on a
/// single-threaded
/// test runtime,
/// the loop takes the slot's message, runs the turn and polls again without yielding,
/// so the parked sender has not been rescheduled and the mailbox is deterministically
/// **empty** at the next poll. A test that tried would be asserting the scheduler.
///
/// The first version of this test asserted the weaker "a fill came before the second
/// note", which is true of a *biased* loop as well — it was watched to pass with the
/// loop's fairness deleted, and proved nothing. What distinguishes the two is the
/// exact position of the fill: with the mailbox provably full at release, a fair loop
/// hands the feed the turn **before** the queued `Note(1)`, and a mailbox-first loop
/// does not.
///
/// The `no permit` property itself is structural rather than behavioural: a feed item
/// is a `Feed`, there is no `Mail` variant for one, and `Feeds` never names an
/// `mpsc`. This test is that property's behavioural witness, not its proof.
///
/// Red-tested with the loop's alternation deleted — `mail_first = true` — which puts
/// `Note(1)` first.
#[tokio::test]
async fn a_full_mailbox_still_admits_feed_items() {
    let (wired, gate) = wired(Vec::new());
    let (held, release) = hold(&wired.book, gate).await;

    // One in the slot.
    let _ = wired.book.note(1).await.expect("running");
    // And one parked on a permit that only the loop can free.
    let parked = {
        let book = wired.book.clone();
        tokio::spawn(async move { book.note(2).await })
    };

    for fill in [10, 11] {
        wired.fills.send(fill).await.expect("the feed accepts");
    }
    assert!(
        !parked.is_finished(),
        "the mailbox must still be full when the feed items are loaded, or this test \
         proves nothing about permits"
    );

    let _ = release.send(());
    held.await.expect("no panic");
    let _ = parked.await.expect("no panic").expect("running");

    drop(wired.fills);
    let _ = wired.book.ended().await;

    let log = wired.log.lock().unwrap().clone();
    assert_eq!(
        log.get(..3),
        Some(&[Event::Held, Event::Fill(10), Event::Note(1)][..]),
        "a feed item must be handled while the mailbox is full and ahead of the \
         message already queued in it, which is what having no permit to wait for \
         looks like from outside: {log:?}"
    );
}

/// **Neither input starves the other**, quantitatively, with no clock.
///
/// Sixteen feed items and one message are loaded while the loop is held, so at
/// release both inputs are ready and the order is decided entirely by the loop's
/// fairness rule. Both directions fall out of one exact prefix:
///
/// - the message is served after **exactly one** of the sixteen queued items, so a
///   saturated feed does not starve the mailbox;
/// - and the first feed item is served **before** the message that was queued
///   first, so the mailbox does not preempt a waiting feed.
///
/// The two matter for different reasons and a fix for one does not imply the other:
/// a starved feed is a **correctness** problem, because the state it feeds is served
/// as fresh while it is stale, and a starved mailbox is a latency problem. Neither
/// wins by default and there is deliberately no knob to make one.
///
/// Asserting a *position* rather than a rate is what removes the clock. An earlier
/// version asserted `note <= 2`, which a mailbox-first loop also satisfies — it puts
/// the note at 1 — and it was watched to pass with the fairness deleted. `== 2` is
/// the assertion that discriminates.
///
/// Red-tested with `mail_first = true`.
#[tokio::test]
async fn neither_input_starves_the_other() {
    let (wired, gate) = wired(Vec::new());
    let (held, release) = hold(&wired.book, gate).await;

    for fill in 1..=16 {
        wired.fills.send(fill).await.expect("the feed accepts");
    }
    let _ = wired.book.note(7).await.expect("running");
    drop(wired.fills);
    let _ = release.send(());
    held.await.expect("no panic");
    let _ = wired.book.ended().await;

    let log = wired.log.lock().unwrap().clone();
    assert_eq!(
        log.get(..4),
        Some(&[Event::Held, Event::Fill(1), Event::Note(7), Event::Fill(2)][..]),
        "sixteen queued feed items and one queued message must alternate: {log:?}"
    );
    assert_eq!(
        log.len(),
        18,
        "everything queued must run: one hold, one note, sixteen fills — {log:?}"
    );
}

// ───────────────────────────────────────── the keyed half, and the durable one

/// **An observer attached to a keyed actor runs.** This is the gap Wave 1a
/// declared out of scope and Wave 1b left at "one line": the keyed loop handed the
/// dispatch an empty hook slice.
///
/// It needs no node and no `RegistryClient`, and that is the interesting part rather
/// than a shortcut: a keyed actor with a feed is driven entirely from inside this
/// process, so the thing being tested is the *loop*, with no transport in the way
/// to explain a pass. The mailbox half of the same loop is `Turn::run` at the same
/// call site — `keylink/src/turns.rs`'s `dispatch`, which `rg` shows is the only
/// one — so an observer that sees a keyed feed turn is an observer on the keyed
/// loop.
#[tokio::test]
async fn an_observer_attached_to_a_keyed_actor_runs() {
    let (seen, mut reports) = mpsc::unbounded_channel();
    let log: Log = Arc::new(Mutex::new(Vec::new()));
    let (state, _gate) = Book::open(&log);
    let (fills, fill_rx) = mpsc::channel(16);

    let system = book::Registry::builder(keylink::Eviction::Never)
        .capacity(4)
        .build();
    system
        .mount(
            keylink::ActorId::new("books/one").expect("valid"),
            book::setup(state)
                .observe(Reporting { to: seen })
                .feeds(book::Feeds::new(feed(fill_rx), quiet()))
                .mountable(),
        )
        .expect("mounts");

    fills.send(4).await.expect("the feed accepts");

    assert_eq!(
        reports.recv().await,
        Some(("Fills", false)),
        "a keyed actor's observers must run: this is RFC-06 §5.3's limit, removed"
    );
    assert_eq!(*log.lock().unwrap(), vec![Event::Fill(4)]);
}

#[tokio::test]
async fn a_keyed_system_ending_reports_a_finished_feed() {
    let log: Log = Arc::new(Mutex::new(Vec::new()));
    let actor_id = keylink::ActorId::new("books/finished").expect("valid");
    let (fills, fill_rx) = mpsc::channel(1);
    drop(fills);
    let system = book::Registry::builder(keylink::Eviction::Never).build();
    system
        .mount(
            actor_id.clone(),
            book::setup(Book {
                log,
                entered: None,
                gate: None,
            })
            .feeds(book::Feeds::new(feed(fill_rx), quiet()))
            .mountable(),
        )
        .expect("mounts");
    let ending = system.ended(&actor_id).expect("the actor is mounted");

    assert!(matches!(ending.await, keylink::Ended::Feed { name } if name == "Fills"));
    assert!(system.ended(&actor_id).is_none());
}

#[tokio::test]
async fn a_local_keyed_ref_queues_a_tell() {
    let log: Log = Arc::new(Mutex::new(Vec::new()));
    let actor_id = keylink::ActorId::new("books/local").expect("valid");
    let system = book::Registry::builder(keylink::Eviction::Never).build();
    system
        .mount(
            actor_id.clone(),
            book::setup(Book {
                log: log.clone(),
                entered: None,
                gate: None,
            })
            .feeds(book::Feeds::new(quiet(), quiet()))
            .mountable(),
        )
        .expect("mounts");

    let _ = book::at_local(&system, actor_id.clone())
        .note(9)
        .await
        .expect("the tell enters the keyed mailbox");
    system
        .stop(&actor_id, std::time::Duration::from_secs(1))
        .await
        .expect("the actor drains the queued tell");
    assert_eq!(*log.lock().unwrap(), vec![Event::Note(9)]);
}

// A second, smaller service for the durability half: a plain `Vec<u64>` state
// with nothing in it but what a feed put there, so what the store holds is
// exactly the claim.
keylink::service! {
    service Tape;

    rpc Marks() -> Vec<u64>;
    feed Marked(mark: u64);
}

#[derive(Default)]
struct Tape {
    marks: Vec<u64>,
}

impl tape::Actor for Tape {
    async fn marks(&mut self, _cx: Caller<'_>) -> Vec<u64> {
        self.marks.clone()
    }

    async fn on_marked(&mut self, mark: u64) {
        self.marks.push(mark);
    }
}

/// Eight bytes per mark, little-endian. The encoding is the state's decision and
/// the port never looks inside — `tests/durable.rs` says the same of its ledger.
#[cfg(feature = "persist-file")]
impl Durable for Tape {
    const VERSION: u32 = 1;

    fn snapshot(&self) -> Vec<u8> {
        self.marks.iter().flat_map(|n| n.to_le_bytes()).collect()
    }

    fn restore(bytes: &[u8]) -> Result<Self, RestoreError> {
        if !bytes.len().is_multiple_of(8) {
            return Err(RestoreError::Malformed(format!(
                "a tape is whole u64s, and this is {} bytes",
                bytes.len()
            )));
        }
        Ok(Self {
            marks: bytes
                .as_chunks::<8>()
                .0
                .iter()
                .map(|c| u64::from_le_bytes(*c))
                .collect(),
        })
    }

    fn migrate(_from: u32, _bytes: &[u8]) -> Result<Option<Vec<u8>>, RestoreError> {
        Ok(None)
    }
}

/// A scratch directory that removes itself.
#[cfg(feature = "persist-file")]
struct Scratch(std::path::PathBuf);

#[cfg(feature = "persist-file")]
impl Scratch {
    fn new(name: &str) -> Self {
        let path =
            std::env::temp_dir().join(format!("keylink-feeds-{name}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&path);
        Self(path)
    }
}

#[cfg(feature = "persist-file")]
impl Drop for Scratch {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

/// **A feed turn is stored on exactly the same rule as a mailbox turn.**
///
/// Wave 5's criterion, and the reason it is here rather than in
/// `tests/persist.rs`: this file owns the second input, and the claim is about
/// what that input is. Nothing in this test calls a method that changes the
/// state — every mark on the tape arrived on the feed — and every one of them is
/// in the file afterwards. So the file is a witness that the observer chain ran
/// *for a feed item* and that its write was taken as this turn's commit.
///
/// The adapter is `keylink::Persist` and not a hand-rolled one. It used to be a
/// `Saver` written in this file, which was the right thing while Wave 5 had not
/// happened: it was a *prototype* of the adapter, and keeping it beside the real
/// one would be two implementations of one rule, differing wherever nobody
/// looked. Wave 5 deleted it, and this test is the one that would notice.
///
/// The real store and the real file, per the house rule: no mock of the ledger.
///
/// Made red by having `feed_turn` skip the observer chain — the reload then comes
/// back empty, because nothing ever wrote.
#[cfg(feature = "persist-file")]
#[tokio::test]
async fn a_feed_turn_is_stored_on_the_same_rule_as_a_mailbox_turn() {
    let scratch = Scratch::new("committed");
    let files = Files::open(&scratch.0).await.expect("opens");
    let id = StateId::new("tapes/one").expect("valid");

    let (marks, mark_rx) = mpsc::channel(4);
    // Loaded and closed BEFORE the actor exists, so the feed yields both items and
    // then ends. That is what makes this test clock-free and race-free at once: the
    // actor's whole life is two feed turns and an ending, in that order, and
    // `ended()` is the wait.
    //
    // The first draft called `tape.marks()` to wait instead, and it lost the race —
    // the mailbox has first refusal on turn one, so the read ran before either feed
    // item and came back empty. Recorded because it is the fairness rule working
    // rather than a flake.
    marks.send(11).await.expect("the feed accepts");
    marks.send(22).await.expect("the feed accepts");
    drop(marks);
    {
        let opened = Persist::<Tape, _, Whole>::open(&files, &id)
            .await
            .expect("claims and loads");
        assert!(!opened.was_stored(), "nothing has been stored yet");
        let (state, saver) = opened.start_from(Tape::default);

        let tape = tape::setup(state)
            .observe(saver)
            .feeds(tape::Feeds::new(feed(mark_rx)))
            .actor();

        let ended = tape.ended().await;
        assert!(
            matches!(&ended, Ended::Feed { name } if *name == "Marked"),
            "got {ended:?}"
        );
    }

    // The claim is released with the actor, so a new "process" can open it.
    let opened = Persist::<Tape, _, Whole>::open(&files, &id)
        .await
        .expect("the claim was released with the actor");
    assert!(opened.was_stored(), "something was stored");
    let (reloaded, _saver) = opened.start_from(Tape::default);
    assert_eq!(
        reloaded.marks,
        vec![11, 22],
        "a feed item must be committed by the observer chain exactly as a turn is"
    );
}
