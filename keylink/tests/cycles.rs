//! A cycle of blocking sends, at N = 2 — the case the crate documents at N = 1.
//!
//! `macros.rs` documents the self-call deadlock thoroughly: "An actor must not
//! call itself. Any method, including `tell`." What it does not say is that the
//! count of actors does not matter. These tests establish that it does not, and
//! they establish the mechanism, because at N = 2 the mechanism is **not** the one
//! the self-call section names.
//!
//! The self-call section's `tell` case is about the *permit*: the mailbox holds one
//! message, the actor released that permit when it took the message it is running,
//! so a self `tell` succeeds only if nobody else took the permit first. That is
//! load-dependent, which is what makes it dangerous.
//!
//! At N = 2 nothing is load-dependent. Both sends *would* succeed on an idle pair,
//! and both callers would then block on an **answer** that only a loop can
//! produce. So A → B → A used to hang on the first attempt, every time, with no
//! contention anywhere — a harder failure than the one the crate already warns
//! about, and a quieter one.
//!
//! # It is refused now, and the depth is not what does it
//!
//! Item M closed the ring: every local message carries the chain of turns it was
//! sent from, and a send to an actor already on that chain is refused with
//! [`Unanswered::Reentrant`] rather than queued. So the tests below assert a
//! **refusal** where they used to assert a hang, and the mechanism is identity
//! rather than the permit — measured, because at depth 32 every deadlock guard in
//! this file still fired.
//!
//! What the depth buys is therefore much smaller than this header used to claim,
//! and it is worth keeping exact: any depth ≥ 1 is silent for one queued message,
//! and `mpsc::channel(0)` panics, so no depth *would* have complained about the
//! send. What 1 buys is that **exactly one** message fits — nothing about
//! deadlock, and the first test below is the only thing in this file that depends
//! on it.
//!
//! The order of these tests is the argument:
//!
//! - a turn runs with its own permit already free, so exactly one message fits
//!   behind it — the one depth fact this file rests on;
//! - A → B → A is refused at the closing edge, and both actors stay live;
//! - `abort` recovers the *other* actor and destroys the one it names — wedged by
//!   a turn that never returns, which is the shape no refusal can reach;
//! - a read from inside a turn is the same refusal through
//!   [`Handle::with_state`], and it is the SELF half of the pair the ring test is
//!   the RING half of;
//! - parking the reply and leaving the turn breaks the cycle — a spawned task
//!   starts a fresh chain, which is why the crate's documented remedy still works
//!   — and parking it without leaving the turn does not, which is the distinction
//!   a reader needs;
//! - and the cycle `compose` already breaks: the same publish from inside a turn
//!   hangs under [`Overflow::BlockPublisher`] and completes under
//!   [`Overflow::DropSubscriber`], so that policy is a safety property and not a
//!   preference about throughput. A `Topic` is not a `LocalActor`, so no chain
//!   reaches it and that hang is a stated residual rather than an oversight.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use keylink::prelude::*;
use std::num::{NonZeroU32, NonZeroUsize};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use keylink::compose::{Overflow, Subscription, Topic};
use keylink::{Backpressure, Caller, Ended, Intensity, Replied, Reply};
use tokio::sync::Notify;

keylink::service! {
    service Ring;

    /// Hop to the wired peer `hops` times, and answer with the depth reached.
    /// `hops` of 0 answers without calling anyone, so it is the only arm that
    /// can terminate a chain.
    rpc Hop(hops: u64) -> u64;

    /// Answers immediately with the `bump` count. A liveness probe: only a loop
    /// that is polling can produce this answer, so a timeout on it means the
    /// actor is inside a turn.
    rpc Ping() -> u64;

    /// Reads this actor's own state, from inside its own turn, through the
    /// public read seam.
    rpc SelfRead() -> u64;

    /// `tell`s itself twice from inside its own turn, and reports which of
    /// [`TELL_THEN_REFUSED`]'s four outcomes happened. Both halves of the rule
    /// in one turn: the first send takes the permit the loop released, the
    /// second finds the mailbox full.
    rpc SelfTellTwice() -> u64;

    /// Calls `stop` on itself from inside its own turn. A `stop` takes a permit
    /// like any message and then waits for a loop that cannot exit while this
    /// turn is running, so it is the refuse-always half at its clearest.
    rpc SelfStop() -> u64;

    /// Holds the turn open until the test releases the gate. Nothing else here
    /// can pin a turn without also sleeping.
    rpc Wait() -> u64;

    /// A message with no reply. `tell` is what makes the permit observable: it
    /// resolves when the **send** completes, so it separates "the mailbox took
    /// it" from "the loop ran it".
    tell Bump() -> ();

    /// Parks its reply, leaves the turn, and hops from a spawned task.
    #[defer]
    rpc ParkedHop(hops: u64) -> u64;

    /// Parks its reply and then hops **without** leaving the turn. The control
    /// for the one above: it isolates leaving the turn from parking the reply.
    #[defer]
    rpc ParkedBlockingHop(hops: u64) -> u64;
}

/// One state, two instances. Two actors of one service is still two actors, and
/// it costs a reader one type instead of two.
struct Peer {
    /// The peer this actor hops to. An `Arc<OnceLock<_>>` because each handle
    /// needs the other one, so neither can be built first — and because a
    /// `Peer` holding a `Handle<Peer>` inline would have no size.
    other: Arc<OnceLock<ring::Handle<Peer>>>,
    /// This actor's own handle, for the self-call probe.
    me: Arc<OnceLock<ring::Handle<Peer>>>,
    /// Bumped on entry to `wait`, so a test can learn the turn has started by
    /// looking rather than by sleeping.
    entered: Arc<AtomicUsize>,
    /// Released by the test, so `wait` holds its turn for as long as the test
    /// needs and not for a wall-clock guess.
    gate: Arc<Notify>,
    /// How many `tell`s this actor has run. Answered by `ping`, so a test can
    /// tell a send that landed in the mailbox from one the loop got to.
    bumps: u64,
}

/// What a hop answers when the edge it tried to close was **refused as
/// re-entrant**: the actor it called is already on the chain of turns behind it.
const REFUSED: u64 = u64::MAX;

/// What a hop answers when the actor it called was **gone** — aborted out from
/// under it, which is the recovery path this file also measures.
///
/// A second sentinel and not one shared with [`REFUSED`], because the whole point
/// of the error gaining a second variant is that a caller can tell the two apart:
/// gone means stop calling, and refused means the actor is alive and running the
/// turn you called from.
const GONE: u64 = u64::MAX - 1;

/// What a call answers for a loss **this file installs nothing to produce**: an
/// [`Observer`](keylink::Observer) refusing a turn that ran, a queueing budget
/// expiring before one started, a live actor throwing a parked reply away, or an
/// incarnation dying with one held.
///
/// A third sentinel and not [`GONE`], for [`GONE`]'s own reason. Every one of them
/// leaves an actor a caller can still reach, so answering *stop calling* about any
/// is exactly the collapse [`Unanswered`] was widened to remove — and a test that
/// re-made the fold here would stay green while hiding it.
///
/// Each is refuted by this file rather than by the crate: no `Peer` is built with
/// an observer, a local origin carries no queueing budget, both deferred handlers
/// **send** what they park, and nothing here panics, so the one supervised `Peer`
/// never restarts. An assertion that ever reads this is reporting a change in what
/// `ask` maps rather than anything about the ring.
const SURPRISE: u64 = u64::MAX - 2;

/// The right answer from `self_tell_twice`: the first self-`tell` landed in the
/// permit the loop released when it took the message it is running, and the
/// second was refused because there was no permit left.
///
/// **Both halves of the rule, observed in one turn.** A `tell` waits for a permit
/// and nothing else, so refusing it into a free slot would delete a working
/// pattern — the message runs after the turn returns. Refusing it into a full one
/// replaces an intermittent hang with an intermittent error the caller can see.
const TELL_THEN_REFUSED: u64 = 1;

/// Both self-`tell`s landed: either the depth moved, or the refuse-when-full half
/// is not firing.
const BOTH_TELLS_LANDED: u64 = 2;

/// The first self-`tell` was refused: the refuse-**always** half leaked into
/// `tell`, which deletes the pattern the rule promises to keep.
const FIRST_TELL_REFUSED: u64 = 3;

/// Neither shape — a `NotQueued::Closed`, which would mean the actor died
/// mid-turn. It is the whole of what the wildcard arm below can still reach: a
/// `tell` answers two states, and the other one is matched by name twice.
const TELL_SURPRISE: u64 = 4;

impl ring::Actor for Peer {
    async fn hop(&mut self, _cx: Caller<'_>, hops: u64) -> u64 {
        if hops == 0 {
            return 0;
        }
        let other = self.other.get().expect("wired").clone();
        answered(other.hop(hops - 1).await)
    }

    async fn ping(&mut self, _cx: Caller<'_>) -> u64 {
        self.bumps
    }

    async fn self_read(&mut self, _cx: Caller<'_>) -> u64 {
        let me = self.me.get().expect("wired").clone();
        // A read runs no observers, starts no turn, and parks no reply, so the
        // six losses below it are unreachable through this door several times
        // over — named rather than wildcarded, because a wildcard here would also
        // swallow whatever the next loss turned out to be. They are `Withheld`
        // variants now rather than `Unanswered` ones, and `Panicked` joins them:
        // a read resumes a panic in its caller, so it cannot arrive as a value
        // either.
        match me.with_state(|state: &Peer| state.bumps).await {
            Ok(bumps) => bumps,
            Err(keylink::Unanswered::Reentrant) => REFUSED,
            Err(keylink::Unanswered::Turn(keylink::Withheld::Gone)) => GONE,
            Err(keylink::Unanswered::Turn(
                keylink::Withheld::Panicked { .. }
                | keylink::Withheld::Refused { .. }
                | keylink::Withheld::ObserverPanicked { .. }
                | keylink::Withheld::Expired
                | keylink::Withheld::Dropped
                | keylink::Withheld::Restarting { .. },
            )) => SURPRISE,
            // These actors park, so a shed cannot happen. Named rather than
            // wildcarded, for the same reason the six above are.
            Err(keylink::Unanswered::Full) => SURPRISE,
        }
    }

    async fn self_tell_twice(&mut self, _cx: Caller<'_>) -> u64 {
        let me = self.me.get().expect("wired").clone();
        let first = me.bump().await;
        let second = me.bump().await;
        match (first, second) {
            (Ok(()), Err(keylink::NotQueued::Reentrant)) => TELL_THEN_REFUSED,
            (Ok(()), Ok(())) => BOTH_TELLS_LANDED,
            (Err(keylink::NotQueued::Reentrant), _) => FIRST_TELL_REFUSED,
            _ => TELL_SURPRISE,
        }
    }

    async fn self_stop(&mut self, _cx: Caller<'_>) -> u64 {
        let me = self.me.get().expect("wired").clone();
        match me.stop().await {
            Err(keylink::NotQueued::Reentrant) => REFUSED,
            Err(keylink::NotQueued::Closed) => GONE,
            // This actor parks, so a shed cannot happen.
            Err(keylink::NotQueued::Full) => SURPRISE,
            // Unreachable while the check holds, and it is what a hang would
            // become if the check were removed — this handler would never get
            // here at all, and the test's timeout is what would say so.
            Ok(()) => 0,
        }
    }

    async fn wait(&mut self, _cx: Caller<'_>) -> u64 {
        self.entered.fetch_add(1, Ordering::SeqCst);
        self.gate.notified().await;
        self.bumps
    }

    async fn bump(&mut self, _cx: Caller<'_>) {
        self.bumps += 1;
    }

    async fn parked_hop(&mut self, _cx: Caller<'_>, reply: Reply<u64>, hops: u64) -> Replied<u64> {
        let (deferred, replied) = reply.later();
        let other = self.other.get().expect("wired").clone();
        // Off the turn's task, which is the half that matters: the turn returns
        // now, so the loop is polling again by the time the peer calls back.
        tokio::spawn(async move {
            deferred.send(answered(other.hop(hops - 1).await));
        });
        replied
    }

    async fn parked_blocking_hop(
        &mut self,
        _cx: Caller<'_>,
        reply: Reply<u64>,
        hops: u64,
    ) -> Replied<u64> {
        let (deferred, replied) = reply.later();
        let other = self.other.get().expect("wired").clone();
        // Parked, and then awaited anyway. The turn has not returned, so the
        // loop is not polling, so nothing about the park has helped.
        deferred.send(answered(other.hop(hops - 1).await));
        replied
    }
}

/// One hop's answer, turned into this actor's own.
///
/// Shared by the three hopping handlers so that a sentinel is decided in **one**
/// place: two of them wrote `map_or(u64::MAX, |depth| depth + 1)`, which folds a
/// refusal and a departure into one value and then overflows on the way back
/// through the next hop.
fn answered(hop: Result<u64, keylink::Unanswered>) -> u64 {
    match hop {
        // A sentinel travels back unchanged. Adding a hop count to it would
        // rename it — and `REFUSED` is `u64::MAX`, so it would also overflow.
        Ok(depth @ (REFUSED | GONE | SURPRISE)) => depth,
        Ok(depth) => depth + 1,
        Err(keylink::Unanswered::Reentrant) => REFUSED,
        Err(keylink::Unanswered::Turn(keylink::Withheld::Gone)) => GONE,
        Err(keylink::Unanswered::Turn(
            keylink::Withheld::Panicked { .. }
            | keylink::Withheld::Refused { .. }
            | keylink::Withheld::ObserverPanicked { .. }
            | keylink::Withheld::Expired
            | keylink::Withheld::Dropped
            | keylink::Withheld::Restarting { .. },
        )) => SURPRISE,
        // These actors park, so a shed cannot happen.
        Err(keylink::Unanswered::Full) => SURPRISE,
    }
}

/// Long enough that a machine under load does not call a live actor hung, short
/// enough that a genuinely hung one costs a test run a second and not a session.
const HANG: Duration = Duration::from_millis(750);

/// For the calls that must finish. Generous, because a failure here is a real
/// failure and not a slow machine.
const SETTLE: Duration = Duration::from_secs(5);

/// Two wired actors, and the gate and counter of the first.
///
/// # The wiring is a strong reference cycle, and that is an exemption
///
/// Each `Peer` holds an `Arc<OnceLock<Handle>>` for its peer **and** for itself,
/// so the `mpsc::Sender`s outlive every handle a test holds. These actors
/// therefore cannot be ended by dropping handles — only `abort`, `stop` or
/// runtime teardown ends them, and two tests below deliberately leave a live
/// actor to teardown.
///
/// Named rather than left silent, because this file warns about leaked busy
/// actors a few tests down and the exemption would otherwise read as an
/// oversight. It is safe here for one reason: `#[tokio::test]` builds a runtime
/// per test and drops it at the end, so nothing outlives the test that made it.
/// A cycle wired this way in a **shared** runtime would leak both actors for the
/// life of the process.
fn ring_of_two() -> (
    ring::Handle<Peer>,
    ring::Handle<Peer>,
    Arc<Notify>,
    Arc<AtomicUsize>,
) {
    let first = Arc::new(OnceLock::new());
    let second = Arc::new(OnceLock::new());
    let gate = Arc::new(Notify::new());
    let entered = Arc::new(AtomicUsize::new(0));

    let a = ring::actor(Peer {
        other: second.clone(),
        me: first.clone(),
        entered: entered.clone(),
        gate: gate.clone(),
        bumps: 0,
    });
    let b = ring::actor(Peer {
        other: first.clone(),
        me: second.clone(),
        entered: Arc::new(AtomicUsize::new(0)),
        gate: Arc::new(Notify::new()),
        bumps: 0,
    });

    first.set(a.clone()).expect("set once");
    second.set(b.clone()).expect("set once");
    (a, b, gate, entered)
}

/// Three wired actors, A → B → C → A.
///
/// **The reason this exists is one `Chain` arm and not a bigger number.** A ring
/// of two exercises `Chain::Empty → One → Deep`, and the arm that grows a `Deep`
/// chain by one more actor is never reached: the closing edge of a two-ring is
/// sent from a chain of exactly two. At N = 3 the closing edge C → A is sent from
/// `Deep([A, B, C])`, which is the first message in the tree whose chain was grown
/// from a `Deep` rather than from a `One`.
///
/// Each actor's `me` is its own handle, as in [`ring_of_two`], so the self-call
/// probes work at any position in the ring. The strong reference cycle exemption
/// that helper documents applies here identically, and with one more actor in it.
fn ring_of_three() -> (ring::Handle<Peer>, ring::Handle<Peer>, ring::Handle<Peer>) {
    let first = Arc::new(OnceLock::new());
    let second = Arc::new(OnceLock::new());
    let third = Arc::new(OnceLock::new());

    let wired = |other: &Arc<OnceLock<ring::Handle<Peer>>>,
                 me: &Arc<OnceLock<ring::Handle<Peer>>>| {
        ring::actor(Peer {
            other: other.clone(),
            me: me.clone(),
            entered: Arc::new(AtomicUsize::new(0)),
            gate: Arc::new(Notify::new()),
            bumps: 0,
        })
    };

    let a = wired(&second, &first);
    let b = wired(&third, &second);
    let c = wired(&first, &third);

    first.set(a.clone()).expect("set once");
    second.set(b.clone()).expect("set once");
    third.set(c.clone()).expect("set once");
    (a, b, c)
}

/// Wait for `a` to be inside its `wait` turn, by looking at the counter the
/// handler bumps on entry rather than by sleeping.
async fn inside_a_turn(entered: &AtomicUsize) {
    for _ in 0..10_000 {
        if entered.load(Ordering::SeqCst) > 0 {
            return;
        }
        tokio::task::yield_now().await;
    }
    panic!("the actor never entered its turn");
}

// ───────────────────────────────────────────── the permit a running turn leaves

/// The fact everything below rests on: a turn runs with its own permit already
/// released, so **exactly one** message fits behind a running turn.
///
/// This is `macros.rs`'s self-`tell` paragraph turned into an observation. The
/// first `bump` completes while the actor is wedged inside `wait` — the mailbox
/// took it — and the second does not, because `DEFAULT_LOCAL_MAILBOX_DEPTH` is 1
/// and the first one is now sitting in it. `tell` is the only method that can show
/// this: an `rpc` would block on the answer either way and could not tell the two
/// apart.
///
/// **These `bump`s come from the test's task and not from a turn**, so the
/// re-entrancy check is not what they meet — this test measures the permit, which
/// is the one thing in this file the depth still governs.
///
/// Made red by `scripts/break-once.sh`, replacing `turns.rs`'s
/// `const DEFAULT_LOCAL_MAILBOX_DEPTH: NonZeroUsize = NonZeroUsize::MIN;` with the
/// whole line
/// `const DEFAULT_LOCAL_MAILBOX_DEPTH: NonZeroUsize = match NonZeroUsize::new(2) { Some(two) => two, None => NonZeroUsize::MIN };`
/// — spelled out in full because the replacement substitutes for the *entire*
/// searched line, and written as a `match` rather than an `unwrap` because the
/// workspace denies `unwrap_used` in `keylink/src` and a break that fails to
/// compile is a red that proves nothing. Watched: the second `bump` then completes
/// too and the test fails at
/// "a second message must not fit: DEFAULT_LOCAL_MAILBOX_DEPTH is spent by the first".
/// That is the right red — it is the depth, and not the loop's release of the
/// permit, that this assertion pins.
///
/// **The message names the constant and not its value, and that is deliberate.** It
/// read "LOCAL_MAILBOX_DEPTH is 1" until item M, which put a copy of a value
/// defined in `turns.rs` into a failure message in `tests/`, where nothing checks
/// it: raise the depth and the assertion fails while its own message asserts the old
/// value as a fact. Naming the constant instead keeps the message an accurate
/// statement of what must hold, whatever the depth is, and still points the reader
/// at the one line that governs it. The value is private to `turns.rs`, so an
/// integration test cannot interpolate it and naming it is the only option here.
#[tokio::test]
async fn a_running_turn_leaves_exactly_one_free_permit_behind_it() {
    let (a, _b, gate, entered) = ring_of_two();

    let waiting = {
        let a = a.clone();
        tokio::spawn(async move { a.wait().await })
    };
    inside_a_turn(&entered).await;

    n0_future::time::timeout(SETTLE, a.bump())
        .await
        .expect("the permit the loop released when it took `wait` is free")
        .expect("the actor is running");

    assert!(
        n0_future::time::timeout(HANG, a.bump()).await.is_err(),
        "a second message must not fit: DEFAULT_LOCAL_MAILBOX_DEPTH is spent by the first"
    );

    gate.notify_one();
    n0_future::time::timeout(SETTLE, waiting)
        .await
        .expect("the gate was released")
        .expect("the caller is not itself panicking")
        .expect("the actor is running");
}

// ──────────────────────────────────────────────────────── the two-actor cycle

/// The claim: **A → B → A hangs, on an idle pair, with no contention.**
///
/// A's handler calls B, B's handler calls A, and both callers block for ever.
/// Both sends succeed — the previous test is why: each loop released its permit
/// when it took the message it is running, so B's call to A finds the mailbox
/// open. What neither call gets is an *answer*, because the only task that can
/// produce one is the task that is blocked waiting for it.
///
/// The second half of the test is the part a reader will not predict: **neither
/// actor answers anything else either**, and they are stuck two different ways.
/// A's one free permit was consumed by B's call, so a further call to A blocks in
/// `send`. B's permit is still free, so a further call to B is accepted and then
/// blocks on the answer. One deadlock, two shapes, and the crate names neither.
///
/// That asymmetry is **asserted and not merely stated**, with `tell` rather than
/// `rpc`: a `tell` resolves when the send completes, so it separates a full
/// mailbox from a busy loop. A ping cannot — it times out identically at both
/// ends, which is why the two ping assertions claim only that a loop is not
/// polling. The `tell`s therefore run **first**: a ping to B takes B's free permit
/// on its way to timing out, so the permit assertions have to happen before any
/// ping or they measure the pings instead.
///
/// Reddened by making the cycle not a cycle: `scripts/break-once.sh` on this file,
/// searching for `hop`'s early-return line — pass it **with its eight spaces of
/// indentation** — `if hops == 0 {`, and replacing it with `if hops <= 1 {`, so the
/// chain stops one hop early and never closes the second edge. Watched:
/// "A -> B -> A must hang", in 0.00s, because a chain that stops answers before the
/// timeout can start. That is the intended red, and it is why the break is on the
/// hop count rather than on the crate: the hang is the *absence* of a mechanism, so
/// no crate line can be edited to remove it.
///
/// # This test asserted a HANG until item M, and the rewrite is the item
///
/// Everything above about the mechanism still holds — both sends *would* succeed,
/// and neither caller *would* get an answer. What changed is that the closing
/// edge is now refused before it is sent, by identity rather than by the permit,
/// so the ring answers [`REFUSED`] instead of hanging. The assertion below is the
/// same claim with the outcome the crate now produces.
///
/// The asymmetry the old test measured with two `tell`s is gone with the hang: no
/// mailbox stays full, because nothing is left waiting in one. What replaces it is
/// the half a reader needs now — **both actors are still live and polling after
/// the refusal**, which is what makes this an error rather than a different
/// failure.
///
/// Made red by `scripts/break-once.sh` on `turns.rs`, replacing `Chain::holds`'s
/// `            Self::Deep(deep) => deep.iter().any(|each| each.is(who)),` with
/// `            Self::Deep(_) => false,` — a chain that can name the turn it is in
/// and none of the turns behind it. Watched: "A -> B -> A must be refused rather
/// than hang: Elapsed(())", after 5.00s.
///
/// **That break reddens TWO tests, and naming one of them was one edit away from
/// naming the wrong one.** `a_parked_reply_that_stays_in_the_turn_does_not_break_the_cycle`
/// fails under it too, at "parking the reply without leaving the turn must be
/// refused: Elapsed(())" — measured, not inferred, and unsurprising once said: that
/// test is a two-actor ring as well, differing only in where the reply is parked.
/// Neither is incidental and either would catch the defect; this one is the record
/// because it is the ring stated as a ring.
///
/// **And `a_read_from_inside_a_turn_is_refused` stays GREEN under that same
/// break** — measured, exit 0, 1 passed. That pairing is why both tests exist and
/// it is the design's own Critical finding turned into evidence: a chain that can
/// only ever hold ONE actor detects a self-call and silently misses `A → B → A`,
/// which is the deterministic hazard. The rejected design — putting the chain on
/// `Origin` — could have installed nothing but `Chain::One`, so it would have
/// shipped in exactly this state, with this test red and that one green.
///
/// # The convention, and it is narrower twice over
///
/// `break-once.sh` greps and counts **only its search literal**; the replacement is
/// never searched (`scripts/break-once.sh`: `grep -qF -- "$from"`, then
/// `assert text.count(old) == 1`). So the replacement is always safe to quote, and
/// a one-line search paired with a multi-line replacement is legal — the harness
/// never cares how many lines the edit *site* spans, only the search literal.
///
/// **And the search literal is safe to quote here too**, which is the second
/// narrowing and the one that took these records off the ungated side. A record in
/// the file it breaks does not collide with its own quote provided the search
/// carries the code's **indentation**: this quote sits unindented inside a `///`
/// line while the code carries eight spaces, so the indented literal is unique and
/// the record's copy is not a second occurrence.
///
/// Two earlier versions of this paragraph were wrong in the same direction, and both
/// were reasoned rather than measured. The first said "describe the edit", which
/// left a reader inventing replacement code that had to compile. The second said a
/// search literal "must not appear in the file it targets" — true of the **bare**
/// literal, false of the **indented** one, so it described what was quotable all
/// along. The counts are in `.mise/tasks/anchors`, which registers the indented form
/// and re-checks it on every run.
#[tokio::test]
async fn a_two_actor_cycle_is_refused_at_the_closing_edge() {
    let (a, b, _gate, _entered) = ring_of_two();

    let depth = n0_future::time::timeout(SETTLE, a.hop(2))
        .await
        .expect("A -> B -> A must be refused rather than hang")
        .expect("A is running: a refusal is not an ending");
    assert_eq!(
        depth, REFUSED,
        "the closing edge B -> A is the one refused, and the answer travels back"
    );

    // The half that makes this a remedy rather than a different failure: both
    // actors are still polling, and neither mailbox is holding anything. A `tell`
    // is used for A because it is the method that can see a full mailbox — it
    // resolves at the send — and a `ping` for B because it is the method that can
    // see a loop that is not polling.
    n0_future::time::timeout(SETTLE, a.bump())
        .await
        .expect("A's mailbox is empty: nothing was left waiting in it")
        .expect("A is running");
    n0_future::time::timeout(SETTLE, b.ping())
        .await
        .expect("B's loop is polling: its turn returned when the refusal did")
        .expect("B is running");

    // Nothing to abort — neither actor is wedged, which is the whole point — but
    // the handles are a strong reference cycle, so the runtime's teardown is what
    // ends them. See `ring_of_two`.
}

/// The recovery, and the word for it is not "recovers both".
///
/// `abort` on **one** actor frees the other: A's task dies, its queued message
/// from B is dropped, that drop closes B's reply channel, B's handler gets
/// `Withheld::Gone` and returns, and B's loop is polling again. So B answers
/// `ping` afterwards — it is a live actor with its state intact.
///
/// A is not recovered. It is destroyed, which is what `abort` documents: "the
/// state is **destroyed**". The original caller is told `Withheld::Gone`.
/// Naming this as "recovers both" would be the crate claiming a repair it does
/// not perform, so the guard asserts the asymmetry rather than the phrase.
///
/// # The wedge is a held turn and no longer a ring, and the swap is item M
///
/// This test used to wedge A by closing the A → B → A ring, which is refused now
/// and completes in milliseconds. So the wedge is built the way the crate says
/// cannot be repaired at all: `wait` holds A's turn open on a gate the test never
/// releases, which is the residual `ask` documents — *a turn can also simply await
/// something that never fires, which is neither cycle shape*. The shape it is now
/// built from is the one the refusal deliberately cannot reach: no send is
/// involved in holding A, so no chain check can see it.
///
/// **An earlier version of this note said "everything this test asserts is
/// unchanged; what changed is the one line that makes A unresponsive". That
/// claimed an invariance the rewrite does not have**, and a review caught it by
/// extracting the function body from both trees and comparing rather than reading
/// the sentence. Four assertions are genuinely identical — `b.ping()` answers,
/// `!a.is_running()`, `b.is_running()`, and `a.ended()` is `Vanished`. Three
/// things moved:
///
/// * **the blocked caller changed subject.** It was A's own `hop(2)` closing the
///   ring; it is now B calling into A. A different actor is being told.
/// * **an assertion changed shape**, `assert!(answer.is_err())` to
///   `assert_eq!(answer, GONE)`. Stronger, and kept — but changed.
/// * **an assertion was added**, that B answered because B is the one freed
///   rather than the one destroyed.
///
/// The rewrite is an improvement. "Only one line changed" is the kind of sentence
/// that stops a reviewer looking, which is the whole reason it is corrected here
/// rather than quietly softened.
///
/// Made red by `scripts/break-once.sh`, replacing `LocalActor::abort`'s
/// `self.task.abort();` with `()`. Watched: "the caller of an aborted actor is
/// told, not left hanging: Elapsed(())" — which is one assertion *earlier* than
/// the one this test is named for. Worth recording rather than tidying: with
/// nothing aborted A simply stays wedged, so the first thing to fail is the
/// caller still hanging, and B's freedom is never reached to be checked.
#[tokio::test]
async fn aborting_one_actor_of_a_cycle_frees_the_other_and_destroys_it() {
    let (a, b, _gate, entered) = ring_of_two();

    // A goes into a turn that never returns, and the gate is never notified.
    let held = {
        let a = a.clone();
        tokio::spawn(async move { a.wait().await })
    };
    inside_a_turn(&entered).await;

    // B calls A. A's loop released its permit when it took `wait`, so the send
    // lands and B then blocks on an answer A cannot produce — B is wedged behind
    // A, which is what the abort has to free.
    let mut hopping = {
        let b = b.clone();
        tokio::spawn(async move { b.hop(1).await })
    };
    assert!(
        n0_future::time::timeout(HANG, &mut hopping).await.is_err(),
        "B must be waiting on A before the abort means anything"
    );

    a.abort();
    // The task holding A's turn open is A's own, and `abort` took it. This one is
    // only the caller that was waiting on it.
    held.abort();

    let answer = n0_future::time::timeout(SETTLE, hopping)
        .await
        .expect("the caller of an aborted actor is told, not left hanging")
        .expect("the caller is not itself panicking")
        .expect("B answered: B is the one that was freed, not the one destroyed");
    assert_eq!(
        answer, GONE,
        "A was destroyed, so B's hop into it is told `Withheld::Gone`"
    );

    n0_future::time::timeout(SETTLE, b.ping())
        .await
        .expect("aborting A must free B")
        .expect("B is still running: only A was aborted");
    assert!(!a.is_running(), "A was aborted");
    assert!(b.is_running(), "B was not");

    // The other half of "abort is the recovery", and the half `ask`'s
    // documentation now leans on: a wedged actor's `ended` never resolves,
    // because only its loop fills it. Aborting the task drops the sender, and
    // that is what makes this answer at all.
    //
    // **The timeout is the whole of the finding.** The variant is near-tautological
    // — `ended`'s dropped-sender arm can only be `Vanished`, and the `Floor` writes
    // `Fate` rather than this watch — so the `matches!` below is a change-detector
    // for that arm and not evidence about `abort`.
    let ended = n0_future::time::timeout(SETTLE, a.ended())
        .await
        .expect("aborting is what makes a wedged actor's `ended` resolve");
    assert!(
        matches!(ended, Ended::Vanished),
        "an aborted task filled nothing, so the reason is `Vanished`, not {ended:?}"
    );
}

// ────────────────────────────────────────────────────── the read inside a turn

/// A read from inside a turn is the same refusal, through the public read seam.
///
/// `Handle::with_state` is `LocalActor::read`, and `read` takes a mailbox permit
/// like any other message — so on an idle actor the send used to succeed, taking
/// the free permit the running turn left, and then wait forever for an answer only
/// the loop could produce. It is `Refuse::Always` now, and the actor is on its own
/// chain, so it never reaches the permit at all.
///
/// **The permit is not what decides this, and that is the point.** The one method
/// that never touches `&mut self` is refused at any depth, because what makes it a
/// deadlock is the answer rather than the slot.
///
/// This test is the SELF-call half of the pair, and the two-actor test above is
/// the ring half. Both must be here: the design that put the chain on `Origin`
/// would have passed this one and failed that one, which is why the vehicle is the
/// private `Message` enum.
///
/// Made red by `scripts/break-once.sh`, replacing `Inbox::identity`'s
/// `Some(Actor(Arc::clone(&self.busy)))` with `None` — the `Watched` impl, which
/// is the only thing that gives a local actor an identity to be found on a chain.
/// Watched: "a read from inside a turn must be refused, not hang: Elapsed(())",
/// because without an identity the chain is empty, the send takes the free permit,
/// and the read waits for a loop that is inside the turn that asked.
#[tokio::test]
async fn a_read_from_inside_a_turn_is_refused() {
    let (a, _b, _gate, _entered) = ring_of_two();

    let bumps = n0_future::time::timeout(SETTLE, a.self_read())
        .await
        .expect("a read from inside a turn must be refused, not hang")
        .expect("A is running: a refusal is not an ending");
    assert_eq!(
        bumps, REFUSED,
        "`with_state` on your own handle is a message to yourself"
    );

    // Still polling, which is what separates a refusal from the deadlock it
    // replaces.
    n0_future::time::timeout(SETTLE, a.ping())
        .await
        .expect("A's loop is polling")
        .expect("A is running");
}

/// **A ring of three, and it is the arm two actors cannot reach.**
///
/// A → B → C → A. The closing edge C → A is the first message in this tree whose
/// chain was grown from a `Chain::Deep` rather than from a `Chain::One`: A installs
/// `One([A])`, B grows it to `Deep([A, B])` through the arm the two-actor ring
/// already covers, and C grows *that* to `Deep([A, B, C])` through the arm nothing
/// else touches. So this is a guard for one `match` arm, not a bigger version of
/// the test above.
///
/// It also answers the question a reader asks after the two-actor case — *does it
/// scale, or does it only know about the actor one hop back* — and the answer has
/// to be a test, because the code that would be wrong reads identically either way.
///
/// Made red by `scripts/break-once.sh` on `turns.rs`, replacing `Chain::with`'s
/// `                grown.extend(outer.iter().cloned());` with
/// `                let _ = &outer;` — the grow arm keeps the actor about to run
/// and drops every ancestor behind it. Watched: "A -> B -> C -> A must be refused
/// rather than hang: Elapsed(())", after 5.00s.
///
/// **And `a_two_actor_cycle_is_refused_at_the_closing_edge` stays GREEN under that
/// same break** — measured, `1 passed`, exit 0. That is the pairing this test is
/// for: a two-actor ring closes through the `One → Deep` arm, which this break
/// does not touch, so a chain that remembers exactly one hop passes that test and
/// fails this one.
///
/// **One line and not the whole arm, deliberately.** Replacing the entire
/// `Self::Deep(outer) => { .. }` block with `Self::Deep(_) => Self::One(running),`
/// was tried first and discriminates identically — same red here, same green there
/// — but a five-line search literal has five lines of `rustfmt` exposure and has to
/// be quoted whole in this record for `.mise/tasks/anchors` to check it. The
/// `extend` line alone is the smallest edit that removes the ancestors, and
/// `outer` stays used by `Vec::with_capacity` on the line above, so the break is a
/// behaviour change rather than a dead-code build failure.
#[tokio::test]
async fn a_three_actor_ring_is_refused_at_the_closing_edge() {
    let (a, b, c) = ring_of_three();

    let depth = n0_future::time::timeout(SETTLE, a.hop(3))
        .await
        .expect("A -> B -> C -> A must be refused rather than hang")
        .expect("A is running: a refusal is not an ending");
    assert_eq!(
        depth, REFUSED,
        "the closing edge C -> A is refused, and the answer travels back three hops"
    );

    // All three still polling. A ring that is refused costs the ring nothing,
    // which is the claim that makes this an error and not a different failure.
    for (name, actor) in [("A", &a), ("B", &b), ("C", &c)] {
        n0_future::time::timeout(SETTLE, actor.ping())
            .await
            .unwrap_or_else(|_| panic!("{name}'s loop is polling"))
            .unwrap_or_else(|_| panic!("{name} is running"));
    }
}

// ────────────────────────────────────────────────────────────────── the knob

/// The depth is a **default** now, and this is the door that overrides it.
///
/// The same observation as the first test in this file, one message further
/// along: at depth 2 a running turn leaves two slots behind it, so two `tell`s fit
/// and the third does not. `tell` is again the only method that can show it — an
/// `rpc` would block on the answer either way.
///
/// **It is a knob and not a moved default.** Every other actor in this file is
/// spawned through `ring::actor`, which takes 1, and every assertion about
/// depth-1 behaviour above still holds beside this one. That pairing is the whole
/// claim: the door exists, and using it changes nothing for anybody who does not.
///
/// **And the depth is not what makes a self-call safe**, which is why this test
/// does not touch the refusal: a re-entrant call is refused by identity at any
/// depth, and `a_self_tell_fits_once_and_is_refused_when_the_mailbox_is_full` is
/// the guard for the one place where depth and the rule interact.
///
/// Made red by `scripts/break-once.sh` on `turns.rs` — the knob accepted and then
/// ignored, which is the failure a caller could not see. Watched: "a second
/// message must fit at depth 2: the knob was ignored: Elapsed(())", after 5.00s,
/// because a third `tell` into a depth-1 mailbox blocks rather than answering.
///
/// **The search literal is two lines, and it has to be.** `spawn_deep` and
/// `spawn_supervised_deep` both open with
/// `        let (mail, inbox) = mpsc::channel::<Message<A, M>>(backpressure.depth().get());`,
/// so that line alone aborts the harness at "expected exactly one occurrence,
/// found 2" — measured, not predicted. The comment line above it is what makes the
/// unsupervised one unique, so the search literal is the pair
/// `        // closure it used to carry made one by accident.
///         let (mail, inbox) = mpsc::channel::<Message<A, M>>(backpressure.depth().get());`,
/// replaced by the same pair with `DEFAULT_LOCAL_BACKPRESSURE.depth().get()` in
/// place of `backpressure.depth().get()`. The two-line anchor is registered in
/// `.mise/tasks/anchors`, so a `rustfmt` reflow between those lines goes to count 0
/// rather than quietly breaking the wrong function.
#[tokio::test]
async fn a_deeper_mailbox_takes_one_more_message_behind_a_running_turn() {
    let me = Arc::new(OnceLock::new());
    let gate = Arc::new(Notify::new());
    let entered = Arc::new(AtomicUsize::new(0));

    let a = ring::setup(Peer {
        other: me.clone(),
        me: me.clone(),
        entered: entered.clone(),
        gate: gate.clone(),
        bumps: 0,
    })
    .feeds(ring::Feeds::new())
    .backpressure(Backpressure::Park(
        NonZeroUsize::new(2).expect("two is not zero"),
    ))
    .actor();
    me.set(a.clone()).expect("set once");

    let waiting = {
        let a = a.clone();
        tokio::spawn(async move { a.wait().await })
    };
    inside_a_turn(&entered).await;

    n0_future::time::timeout(SETTLE, a.bump())
        .await
        .expect("the permit the loop released when it took `wait` is free")
        .expect("the actor is running");
    n0_future::time::timeout(SETTLE, a.bump())
        .await
        .expect("a second message must fit at depth 2: the knob was ignored")
        .expect("the actor is running");

    assert!(
        n0_future::time::timeout(HANG, a.bump()).await.is_err(),
        "and a third must not: the depth asked for is 2, not unbounded"
    );

    gate.notify_one();
    n0_future::time::timeout(SETTLE, waiting)
        .await
        .expect("the gate was released")
        .expect("the caller is not itself panicking")
        .expect("the actor is running");
}

/// The same knob through the **supervised** builder.
///
/// The test above uses `setup(..).backpressure(..).actor()`. This test uses
/// `restarting(..).backpressure(..).actor()`, which is the generated route to
/// `LocalActor::spawn_supervised_deep`. `Handle` keeps its `inner` field private,
/// so a `service!` user cannot bypass this forwarding path. That makes the path
/// worth a direct test.
///
/// # One half is asserted and the other is carried by the signature
///
/// The door's body is one line, so what can go wrong in it is narrow. It can
/// **lose the depth** — by forwarding to `spawn_supervised`, or by passing a
/// constant — and that is what the assertions below see.
///
/// It cannot lose the **supervision**: `spawn_deep` takes `(state, observers,
/// feeds, backpressure)` and this takes `(factory, limit, backpressure)`, so a
/// factory cannot be
/// handed to the unsupervised door and the mutation does not compile. Stated
/// rather than asserted, because a compile error and a caught defect leave the
/// harness by the same exit and a break that cannot build proves nothing about
/// this test.
///
/// # Why it is here rather than in `tests/supervision.rs`
///
/// The claim is the knob's, not supervision's: two `tell`s fit behind a running
/// turn at depth 2 and a third does not, exactly as above. `Wait` is what pins a
/// turn open without sleeping, and it lives in this file. Nothing faults, so the
/// supervisor never restarts anything and `SURPRISE`'s note above — that no `Peer`
/// panics here — still holds.
///
/// Made red by `scripts/break-once.sh keylink/src/macros.rs` on the door itself,
/// `$crate::LocalActor::spawn_supervised_deep(factory, limit, policy)`
/// replaced by `$crate::LocalActor::spawn_supervised(factory, limit)` —
/// the policy accepted and dropped one line later, which is the failure a caller
/// cannot see. **Hand the harness the whole line with its twenty spaces of
/// indentation**, which is what `F1` registers. Watched:
/// `a second message must fit at depth 2: the supervised door dropped it:
/// Elapsed(())`, after 5.00s, because the second `tell` into a depth-1 mailbox
/// blocks rather than answering.
///
/// The break is in `macros.rs` and not in `turns.rs` on purpose. The channel line
/// inside `spawn_supervised_deep` is shared word for word with `spawn_deep` — the
/// record above measures that collision — so breaking there would test the path
/// both doors share. The gap this closes is the **door**, and the door is one
/// line in the macro.
#[tokio::test]
async fn the_restarting_builder_takes_the_depth_it_was_given() {
    let me = Arc::new(OnceLock::new());
    let other = Arc::new(OnceLock::new());
    let gate = Arc::new(Notify::new());
    let entered = Arc::new(AtomicUsize::new(0));

    let a = {
        let me = me.clone();
        let other = other.clone();
        let gate = gate.clone();
        let entered = entered.clone();
        ring::restarting(
            move || {
                let me = me.clone();
                let other = other.clone();
                let gate = gate.clone();
                let entered = entered.clone();
                async move {
                    Ok((
                        Peer {
                            other,
                            me,
                            entered,
                            gate,
                            bumps: 0,
                        },
                        Vec::new(),
                        (),
                    ))
                }
            },
            Intensity::new(
                NonZeroU32::new(3).expect("a budget of at least one"),
                Duration::from_secs(10),
            )
            .expect("a recovery span whose backoff is not zero"),
        )
        .backpressure(Backpressure::Park(
            NonZeroUsize::new(2).expect("two is not zero"),
        ))
        .actor()
    };
    me.set(a.clone()).expect("set once");

    let waiting = {
        let a = a.clone();
        tokio::spawn(async move { a.wait().await })
    };
    inside_a_turn(&entered).await;

    n0_future::time::timeout(SETTLE, a.bump())
        .await
        .expect("the permit the loop released when it took `wait` is free")
        .expect("the actor is running");
    n0_future::time::timeout(SETTLE, a.bump())
        .await
        .expect("a second message must fit at depth 2: the supervised door dropped it")
        .expect("the actor is running");

    assert!(
        n0_future::time::timeout(HANG, a.bump()).await.is_err(),
        "and a third must not: the depth asked for is 2, not unbounded"
    );

    gate.notify_one();
    n0_future::time::timeout(SETTLE, waiting)
        .await
        .expect("the gate was released")
        .expect("the caller is not itself panicking")
        .expect("the actor is running");
}

// ──────────────────────────────────────────── the two halves, and the two paths

/// **The rule has two halves and this is both of them, in one turn.**
///
/// `ask`, `read` and `stop` are refused whether or not the mailbox has room,
/// because the hang is on the answer and a free slot does not save it. A `tell`
/// waits for a permit and nothing else, so refusing it into a free slot would
/// delete a working pattern — the message runs after the turn returns. So the
/// first self-`tell` must land and the second must be refused.
///
/// The two are asserted together because a check that cannot tell them apart
/// passes half of this and fails the other half whichever way it errs. `send`
/// receives byte-identical arguments from `notify` and from the ask path, so what
/// separates them is the parameter naming the half and nothing else.
///
/// Made red twice, and the two reds are opposite:
///
/// - `scripts/break-once.sh` on `turns.rs`, replacing `notify`'s
///   `            Refuse::WhenFull,` with `            Refuse::Always,`. Watched:
///   "the first self-tell takes the permit the loop released: 3", the code for
///   [`FIRST_TELL_REFUSED`].
/// - and replacing `send`'s
///   `                Refuse::WhenFull if self.mail.capacity() == 0 => return Err(NotQueued::Reentrant),`
///   with `                Refuse::WhenFull if false => return Err(NotQueued::Reentrant),`.
///   Watched: "a self-tell into a full mailbox must be refused, not block:
///   Elapsed(())", after 5.00s.
///
/// **The second red is a TIMEOUT and not [`BOTH_TELLS_LANDED`]**, which is what
/// this record predicted before it was run. The prediction was wrong about the
/// mechanism in a way worth keeping: with the arm off, the second `tell` does not
/// *land*, it **blocks** — the mailbox really is full, and the only task that can
/// free the permit is the one inside this turn. So `BOTH_TELLS_LANDED` names a
/// state the crate cannot reach by removing this check, and the code that would
/// report it is unreachable through this break. It stays in the enum because it is
/// what a **deeper default** would produce, which is a different edit.
///
/// # The filter, and the two columns the second anchor has left
///
/// Both runs take this test's own name —
/// `a_self_tell_fits_once_and_is_refused_when_the_mailbox_is_full` — plus
/// `--test cycles`. The harness appends the filter **positionally** as a plain
/// libtest substring. A filterset there matches nothing and exits non-zero, which
/// the banner prints as a caught defect. The filter was described in prose and not
/// written down here until a reader had to reconstruct it.
///
/// **CAUTION: the second anchor is 98 of the 100 columns `rustfmt` allows.** Two
/// columns of headroom, so a longer type name anywhere on that line reflows it and
/// the anchor rots.
///
/// Not hypothetical, and it has already happened once in the **other** direction:
/// at `ac5a6ef` this arm was a three-line block, `Unanswered::Reentrant` was
/// renamed to the shorter `NotQueued::Reentrant`, `rustfmt` collapsed the block
/// onto one line, and D11's row in `.mise/tasks/anchors` had to be re-quoted. Any
/// rename that moves this line either way does it again, and with two columns left
/// the growing direction is now the cheap one.
///
/// It is loud rather than silent when it happens: `mise run anchors` fails before
/// the harness is ever reached, and the harness would abort at "found 0". The
/// figure is measured against the line as it stands and is not a limit anyone
/// chose, so it belongs beside the anchor rather than in a rule.
#[tokio::test]
async fn a_self_tell_fits_once_and_is_refused_when_the_mailbox_is_full() {
    let (a, _b, _gate, _entered) = ring_of_two();

    let outcome = n0_future::time::timeout(SETTLE, a.self_tell_twice())
        .await
        .expect("a self-tell into a full mailbox must be refused, not block")
        .expect("A is running");
    assert_eq!(
        outcome, TELL_THEN_REFUSED,
        "the first self-tell takes the permit the loop released and the second \
         finds the mailbox full"
    );
}

/// `stop` on your own handle is the refuse-always half at its clearest: it takes
/// a permit like any message and then waits for a loop that cannot exit while
/// this turn is running.
///
/// It is also the reason `stop` gained a `Result`. With no error position the
/// refusal would be a silent no-op — an actor told to stop, not stopping, and
/// nobody told — which is the fallback this crate refuses everywhere else.
///
/// Made red by `scripts/break-once.sh` on `turns.rs`, replacing `stop`'s
/// `        match self.send(Message::Stop, Refuse::Always).await {` with
/// `        match self.send(Message::Stop, Refuse::WhenFull).await {`. Watched:
/// "a self-stop must be refused rather than wedge the actor: Elapsed(())" — the
/// timeout, because the permit is free, so the stop is queued and the handler
/// then waits for a loop that is inside it.
#[tokio::test]
async fn a_self_stop_is_refused_rather_than_wedging_the_actor() {
    let (a, _b, _gate, _entered) = ring_of_two();

    let outcome = n0_future::time::timeout(SETTLE, a.self_stop())
        .await
        .expect("a self-stop must be refused rather than wedge the actor")
        .expect("A is running");
    assert_eq!(outcome, REFUSED, "a stop is a message to yourself");

    // And the actor really is still there, which is the difference between a
    // refusal and the stop it declined to perform.
    n0_future::time::timeout(SETTLE, a.ping())
        .await
        .expect("A's loop is polling")
        .expect("A is running: a refused stop stopped nothing");
}

/// **A supervised actor refuses a self-call too, and nothing would have said so.**
///
/// The supervised path does not hand the loop the mailbox it was spawned with: it
/// hands it a `Lent`, a delegating wrapper the supervisor owns, so an incarnation
/// borrows the mailbox rather than owning it. `Lent` forwards `identity` in one
/// line, exactly as it already forwards `mark_busy` — and a delegating wrapper
/// that forgets to forward turns a safety check off with **no test failing**,
/// because the default is `None` and `None` means "this actor cannot be named".
///
/// So this test is not a variation on the one above. It is the only thing in the
/// tree that distinguishes a forwarded identity from a forgotten one, and every
/// supervised actor in production runs the path it covers.
///
/// Made red by `scripts/break-once.sh` on `turns.rs`, replacing `Lent`'s
/// `        self.inbox.identity()` with `        None`. Watched: "a supervised
/// self-read must be refused rather than hang: Elapsed(())" — the timeout, and
/// the unsupervised twin above stays green through it, which is the whole of what
/// this test adds.
#[tokio::test]
async fn a_supervised_actor_still_refuses_a_self_call() {
    let me = Arc::new(OnceLock::new());
    let other = Arc::new(OnceLock::new());

    let a = {
        let me = me.clone();
        let other = other.clone();
        ring::restarting(
            move || {
                let me = me.clone();
                let other = other.clone();
                async move {
                    Ok((
                        Peer {
                            other,
                            me,
                            entered: Arc::new(AtomicUsize::new(0)),
                            gate: Arc::new(Notify::new()),
                            bumps: 0,
                        },
                        Vec::new(),
                        (),
                    ))
                }
            },
            Intensity::new(
                NonZeroU32::new(3).expect("a budget of at least one"),
                Duration::from_secs(10),
            )
            .expect("a recovery span whose backoff is not zero"),
        )
        .actor()
    };
    me.set(a.clone()).expect("set once");
    other.set(a.clone()).expect("set once");

    let bumps = n0_future::time::timeout(SETTLE, a.self_read())
        .await
        .expect("a supervised self-read must be refused rather than hang")
        .expect("the supervised actor is running");
    assert_eq!(
        bumps, REFUSED,
        "`Lent` forwards the identity, so a supervised actor is on its own chain"
    );

    n0_future::time::timeout(SETTLE, a.ping())
        .await
        .expect("the incarnation's loop is polling")
        .expect("a refusal is not an ending, and did not spend a restart");
}

// ─────────────────────────────────────────────────────────── the cycle-breaker

/// The claim: parking the reply and **leaving the turn** breaks the cycle.
///
/// A's handler parks its reply, hands the hop to a spawned task and returns. The
/// loop is polling again immediately, so when B calls back, A takes the message
/// and answers it. The full A → B → A chain then completes and the *original*
/// caller gets the depth, from a turn that ended long before the answer existed.
///
/// The mechanism is worth stating exactly, because it is not the permit: the
/// permit was free in the hanging case too. What `Deferred` changes is that the
/// loop goes back to **polling**, so the peer's message is taken and run.
///
/// **Reddened, but not on its distinguishing claim**, and `defer.rs` already has
/// the vocabulary for that: `scripts/break-once.sh` replacing `Deferred::send`'s
/// `Verdict::Committed => reply.answer(Answered::Ran(value)),` with
/// `Verdict::Committed => drop((reply, value)),`. Watched: "the actor is running:
/// Dropped". That proves the parked answer is load-bearing and proves
/// nothing about the *cycle*.
///
/// **What the break reaches is one arm, and naming the arm is the whole
/// statement.** `Deferred::send` is the only caller of `parking::park` outside
/// `parking.rs`'s own unit tests, so this substitution discards the value of every
/// parked reply whose turn then COMMITS — which is every guard needing a parked
/// answer to arrive, wherever it lives. It leaves `Verdict`'s other two arms
/// untouched: a reply the observer chain REFUSES and a reply lost to a FAULT are
/// each resolved by their own arm, so a guard asserting one of those losses still
/// sees it. And a reply simply dropped, as `defer.rs`'s `abandon` drops it, never
/// calls `send` and so never reaches `park` at all.
///
/// **That qualifier does not weaken the "every", because a count of callers was
/// never what licensed it.** `parking.rs`'s unit tests call `park` four times, each
/// handing it a bare `Parked` closure of their own — a `Box<dyn FnOnce(Verdict)>`
/// that owns no `Reply`, which is why that file constructs none. They never execute
/// the line this break substitutes. What licenses "every" is narrower and does not
/// move when a fifth such test is written: `Deferred::send` is the only place a
/// REPLY is ever parked, so every parked reply's answer runs the arm above.
///
/// **This sentence used to say the break reddens every `#[defer]` test in the
/// tree, and that is the shape to avoid.** It reads as a population, and which
/// population is left to the reader: on "every test in a file that writes
/// `#[defer]`" it is simply false, because `tests/protocol.rs` declares a
/// `#[defer] rpc` that no test drives at all. On "every test needing a parked
/// answer" it is about right — but then it is a claim over an OPEN set, and the
/// next such test anybody writes silently joins it, which is the rot
/// `.mise/tasks/anchors` warns about for its own row count. Counting the attribute
/// does not rescue it either: the deferred method `tests/inspect.rs` drives is
/// declared in `src/console.rs`, so no count taken over the test files reaches it.
/// The arm has none of these problems. It is one line of `turns.rs`, and the
/// anchor registered for this record IS that line.
///
/// **One row does not cover that line, though, and this record is not its only
/// reader.** `tests/persist.rs::a_deferred_reply_that_is_answered_is_a_stored_reply`
/// breaks the SAME line with a different replacement — `Verdict::Committed =>
/// drop(value),` rather than the `drop((reply, value))` above — and watches a
/// different sentence. That record is unregistered, so the registry carries one row
/// for a line two records rely on, and a row names one `lives_in` and cannot say so.
/// Anyone re-quoting this anchor must fix `persist.rs` too. The registry holds D3
/// unsplit on purpose and says it is next, so this paragraph is the reader's only
/// warning until that happens.
///
/// The break that would have isolated the cycle does not exist. `Reply::later`'s
/// `Replied(Settled::Later)` → `Replied(Settled::Now)` was tried first and does
/// not compile — `Settled::Now` is `Now(Reply<T>, T)`, so the arm needs the two
/// values `later` exists precisely to not have. What isolates the distinguishing
/// claim here is not a break but the test below, which differs from this one in
/// the `tokio::spawn` alone and hangs.
#[tokio::test]
async fn a_parked_reply_that_leaves_the_turn_breaks_the_cycle() {
    let (a, b, _gate, _entered) = ring_of_two();

    let hopping = {
        let a = a.clone();
        tokio::spawn(async move { a.parked_hop(2).await })
    };

    let depth = n0_future::time::timeout(SETTLE, hopping)
        .await
        .expect("a parked reply must break the cycle rather than hang")
        .expect("the caller is not itself panicking")
        .expect("the actor is running");
    assert_eq!(depth, 2, "A -> B -> A ran to the end and came back");

    // Both are live: nothing was aborted and nothing is wedged.
    n0_future::time::timeout(SETTLE, a.ping())
        .await
        .expect("A is polling")
        .expect("A is running");
    n0_future::time::timeout(SETTLE, b.ping())
        .await
        .expect("B is polling")
        .expect("B is running");
}

/// The control, and the reason the sentence has to say "leaves the turn".
///
/// This handler parks its reply exactly as the one above does, and then awaits
/// the hop **without returning**. The ring is still a ring, so the closing edge
/// is refused and the chain answers [`REFUSED`] rather than the depth. So
/// `Reply::later` is not the cycle-breaker by itself — returning from the turn is,
/// and `later` is what makes returning possible while a caller is still owed an
/// answer.
///
/// **It asserted a hang until item M**, and the pair still says the same thing
/// with a different second half: the two tests differ in one line, the
/// `tokio::spawn` around the hop, and they disagree about the answer. The spawned
/// one reaches depth 2; this one is refused.
///
/// Guarded by construction rather than by a break, for that reason: reddening this
/// one means deleting that difference, which is the other test.
#[tokio::test]
async fn a_parked_reply_that_stays_in_the_turn_does_not_break_the_cycle() {
    let (a, _b, _gate, _entered) = ring_of_two();

    let depth = n0_future::time::timeout(SETTLE, a.parked_blocking_hop(2))
        .await
        .expect("parking the reply without leaving the turn must be refused")
        .expect("A is running");
    assert_eq!(
        depth, REFUSED,
        "the ring closed on A while A's turn was still on the stack"
    );
}

// ─────────────────────────────────────────── the cycle a policy already breaks

keylink::service! {
    service Room;

    /// Publish `times` items from inside a turn, to a topic this actor
    /// subscribes to and is therefore not reading. Answers with how many
    /// publishes dropped a subscriber.
    rpc Shout(times: u64) -> u64;
}

/// An actor subscribed to its own topic, holding the [`Subscription`] in its
/// state and **never polling it** — which is precisely what an actor inside a
/// turn is.
struct Chatter {
    topic: Topic<u64>,
    _feed: Subscription<u64>,
}

impl room::Actor for Chatter {
    async fn shout(&mut self, _cx: Caller<'_>, times: u64) -> u64 {
        let mut dropped = 0;
        for item in 0..times {
            dropped += self.topic.publish(item).await.dropped as u64;
        }
        dropped
    }
}

fn chatter(overflow: Overflow) -> room::Handle<Chatter> {
    let topic = Topic::new(overflow);
    let feed = topic.subscribe();
    room::actor(Chatter { topic, _feed: feed })
}

/// `Overflow::DropSubscriber` is not a style preference. It is what makes a
/// publish safe from inside a turn, and this is the pair that says so.
///
/// `topic.rs` already states it — "`Overflow::DropSubscriber` never awaits — it
/// completes without yielding, which is worth knowing for a caller that
/// publishes from inside a turn" — and states it as prose. This is the guard.
///
/// The policy is constructed with explicit depth one, so the first item fits and
/// the second finds the subscriber full. This policy unsubscribes the reader, and
/// the publish returns. Nothing awaits, so no edge of the cycle can block.
///
/// Made red by `scripts/break-once.sh`, replacing `Topic::publish`'s
/// `Overflow::DropSubscriber(_) => self.publish_dropping(item),` with
/// `Overflow::DropSubscriber(_) => self.publish_blocking(item).await,` — the policy
/// swapped for the one that awaits. Watched: "a dropping publish must not hang:
/// nothing in it awaits: Elapsed(())".
#[tokio::test]
async fn publishing_into_your_own_topic_completes_under_drop_subscriber() {
    let room = chatter(Overflow::DropSubscriber(NonZeroUsize::MIN));

    let dropped = n0_future::time::timeout(SETTLE, room.shout(2))
        .await
        .expect("a dropping publish must not hang: nothing in it awaits")
        .expect("the actor is running");
    assert_eq!(
        dropped, 1,
        "at depth one, the first item fits and the second unsubscribes the reader"
    );

    // And the actor is still taking messages, which is the half that makes this
    // a remedy rather than merely a different failure.
    n0_future::time::timeout(SETTLE, room.shout(1))
        .await
        .expect("still polling")
        .expect("still running");
}

/// The other half of the pair, and the one the crate already documents: under
/// `Overflow::BlockPublisher` the same handler hangs.
///
/// Same actor, same topic, same two publishes. The only difference is the
/// policy, which is what makes the policy the safety property rather than a
/// preference about throughput.
///
/// Made red by `scripts/break-once.sh`, replacing `Topic::publish`'s
/// `Overflow::BlockPublisher(_) => self.publish_blocking(item).await,` with
/// `Overflow::BlockPublisher(_) => self.publish_dropping(item),`. Watched: "a
/// blocking publish into your own topic must hang".
#[tokio::test]
async fn publishing_into_your_own_topic_hangs_under_block_publisher() {
    let room = chatter(Overflow::BlockPublisher(NonZeroUsize::MIN));

    let mut shouting = {
        let room = room.clone();
        tokio::spawn(async move { room.shout(2).await })
    };
    assert!(
        n0_future::time::timeout(HANG, &mut shouting).await.is_err(),
        "a blocking publish into your own topic must hang"
    );

    room.abort();
}
