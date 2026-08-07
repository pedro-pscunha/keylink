//! Which actor, and for how long. Never what a message is made of.
//!
//! A keyed map of tasks with a capacity and an eviction clock is worth having
//! without a network, and this half is arranged so that it stays that way:
//! everything is generic over `M`, the thing a mailbox carries, and nothing
//! decodes it, encodes it, or knows that a peer exists. `remote.rs` depends on
//! this file and serves it. This file does not depend back.
//!
//! # What in this file still points the wrong way
//!
//! RFC-04 §16.8 prices the crate split at six items and is the list of record.
//! An earlier list of three was wrong in three ways, which is what a bill kept
//! in two places costs. **Two of the four items that lived here are now gone,**
//! not because the split happened but because the wave that made every actor
//! system per-service removed the reason for them:
//!
//! - ~~four type-parameter defaults — `System<M = Mail>` and its three
//!   relatives~~ — gone. A default existed so that `keylink::System` written
//!   with no parameter went on meaning what it always meant. There is no such
//!   spelling any more: a system is `inventory::System`, a generated alias that
//!   applies `M` itself, so there is nothing left for a default to rescue.
//! - ~~`impl SystemBuilder<Mail>`, for one associated constant~~ — gone with
//!   them. Its own doc called it a compatibility shim; `DEFAULT_CAPACITY` now
//!   sits on the generic impl where the cap always belonged, and every call site
//!   reaches it through a fully applied alias.
//! - three intra-doc links to `RemoteError`, a `reply.rs` type. Prose rather
//!   than a code path, but an intra-doc link across a crate boundary still
//!   needs a Cargo dependency to resolve.
//! - the seam itself: `RouteError` and `route` are `pub(super)` here, and
//!   across a crate boundary a within-module courtesy has to become a designed
//!   public interface.
//!
//! `rg 'super::|crate::' registry.rs` is the check that keeps that list honest.
//! Nothing in this file can name a type from the transport half except through
//! an import or a qualified path, so a new one cannot arrive without adding a
//! line to that output. `rg 'Mail'` is NOT that check and never was — it cannot
//! see `RemoteError` at all, which is how three links to a wire type got in
//! under a header claiming nothing else was there.
//!
//! **This file no longer names the transport half at all.** There is no `use
//! super::remote::..` line in it, which is a stronger statement than any list:
//! the tests below run at a witness type with no derives, and they are the only
//! instantiation this file knows about.
//!
//! Two names from the rest of the crate appear in that output, and neither is a
//! wire type: [`ActorId`], which is what a registry is keyed by and which would
//! move with it, and [`Ended`], which is what [`System::shutdown`] reports.
//! **`Ended` is a new item on §16.8's bill**, and it is put here rather than left
//! to be counted later: `local.rs` names it too, so a split would have to give
//! both halves somewhere to see it from. That is a smaller cost than a second
//! lifecycle vocabulary, which is what returning anything else would have been.

use std::collections::HashMap;
use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, PoisonError, RwLock, Weak};
use std::time::Duration;

use n0_future::task::JoinHandle;
use n0_future::time::Instant;
use tokio::sync::mpsc;

use crate::{ActorId, Ended};

/// Messages that may wait ahead of the one being handled.
///
/// One, deliberately, and there is no overflow policy because there is no
/// overflow to have a policy about.
///
/// A message occupies a QUIC stream from the moment it arrives until its reply
/// is written, including the time it spends here. `max_concurrent_bidi_streams`
/// is 100, so a peer cannot have more than 100 messages outstanding against this
/// actor whatever this number is. The queue is bounded by the transport, and
/// the 101st `open_bi` blocks ON THE CALLING MACHINE — which is where you want
/// a slow actor to be felt.
///
/// Raising this does not raise the ceiling. It moves messages out of the
/// transport, where the peer feels them, into a buffer where nobody does, and
/// it adds that much latency to every message behind a slow one.
///
/// Before changing this, read RFC-04 section 6. If the transport ever stops
/// bounding in-flight calls, this whole argument dies and a real policy is
/// needed.
const MAILBOX: usize = 1;

// ──────────────────────────────────────────────────────────────── the mailbox

/// The receiving half of one actor's mailbox.
///
/// # Why the slot is a `Box<M>` and not an `M`
///
/// Boxing here is not a micro-optimisation, and it is a decision about the
/// registry rather than about any particular message type — which is why the
/// argument lives on this alias, where the `Box` is written, rather than on
/// whatever `M` happens to be this month.
///
/// A `tokio::sync::mpsc` channel allocates its first block of 32 slots eagerly,
/// so a mailbox costs `32 * size_of::<M>()` whatever its capacity is, and the
/// capacity is 1. At ten thousand idle actors, measured on this machine against
/// a service with one `u64` argument — the smallest case there is:
///
/// | slot type  | per idle actor | ten thousand |
/// |------------|----------------|--------------|
/// | `Box<M>`   | 1,159 bytes    | 11.6 MB      |
/// | `M`        | 8,327 bytes    | 83.3 MB      |
///
/// The 7,168 byte difference is the block: that `M` is 232 bytes, so 32 of them
/// is 7,424, against 256 for 32 pointers. The cost of boxing is one allocation
/// per message, on a path that already allocates.
///
/// # The box here is half of the decision, and the other half is in `local.rs`
///
/// The sentence that used to sit here — that the pressure "gets worse as `M`
/// grows, and `M` is the caller's to grow" — was true and was about the wrong
/// allocation. A slot is a pointer, so this block is 256 bytes whatever `M` is;
/// but the **task** spawned beside it ran a future that inlined the whole turn's
/// state machine, and *that* was sized to the service's own message. Measured
/// then: 1,686 bytes per idle actor at a 232 byte `M` and 4,662 at a 1,248 byte
/// one, so boxing the slot bought back one of every three bytes and left the
/// other two.
///
/// The turn's future is boxed at its own loop's await now, for the same reason
/// written one layer in, and the two together are what make an idle actor's cost
/// keylink's rather than the service author's. Re-measured at the same pair of
/// services: **1,159 bytes at 232 and 1,146 at 1,248** — flat, where it used to
/// be 2.9 bytes of permanent cost per byte of message.
/// `tests/actor_memory.rs` asserts that flatness rather than describing it, which
/// is what turns this paragraph from a claim into a guard.
///
/// Only the boxed row above is measured; the unboxed row is that number plus the
/// block difference, because the test never builds ten thousand unboxed mailboxes
/// — it would allocate 83 MB to measure a subtraction. Saying which row is
/// checked matters here: every figure in this table except the boxed one was
/// wrong for months, under a sentence claiming the whole thing was measured.
#[doc(hidden)]
pub type Mailbox<M> = mpsc::Receiver<Box<M>>;

/// An actor ready to run: its loop and the sender that reaches it.
///
/// Produced by the generated `spawn` and handed to [`System::mount`] or
/// returned from a factory. Opaque on purpose: the registry owns the pairing of
/// a loop with the sender that reaches it, and nothing else may take them apart.
///
/// It used to carry a third thing, a schema hash, because one node-wide system
/// held actors of unrelated services and something had to tell them apart. A
/// system is per-service now, so the hash has nothing left to discriminate and
/// the comparison it fed is unrepresentable rather than refused.
///
/// `M` is the message the mailbox carries. See [`System`] for what a registry is
/// allowed to know about it, which is nothing.
///
/// # This holds a future, and it used to hold a `JoinHandle`
///
/// [`mount`] spawned the task and this struct carried the handle, which meant
/// **the actor was already running before anybody had accepted it**. Every path
/// that refused one therefore dropped a `JoinHandle` — and a dropped
/// `JoinHandle` *detaches*, it does not cancel — so both of [`System::mount`]'s
/// refusals left a live, unreachable task behind: [`MountError`] hands nothing
/// back, and the registry never had it. A durable actor refused for a taken id
/// went on holding its exclusive claim with no handle anywhere in the process
/// able to stop it.
///
/// So the loop is carried unstarted and [`System::mount`] is the one place it
/// starts, *after* the two refusals. A refused mount now drops a future that
/// has never been polled: no task, no state moved onto one, nothing to detach.
/// That is the same guarantee the lazy path already had — the cap is checked
/// before the factory is called — written once for both.
///
/// The cost is one `Box` per mount, on a path that already allocates a channel,
/// and it is transient: nothing the registry *keeps* is boxed by this, so the
/// measured per-idle-actor cost above is untouched.
pub struct Mounted<M> {
    mail: mpsc::Sender<Box<M>>,
    /// Polled for the first time by [`Registry::entry`]. Boxed because the
    /// closure's future type is the caller's, and this struct is named in
    /// public signatures where a second parameter for it would be noise.
    run: Pin<Box<dyn Future<Output = ()> + Send + 'static>>,
}

/// Hand-written for the same reason as [`System`]'s, and it was derived by
/// mistake: `derive(Debug)` writes `impl<M: Debug> Debug`, which would make
/// printing a handle depend on a message type nobody prints. The sender does not
/// need it — its own `Debug` is unconditional — and the loop has none to borrow:
/// a `dyn Future` cannot be printed, which is what `finish_non_exhaustive` says
/// out loud rather than by inventing a placeholder for it.
impl<M> fmt::Debug for Mounted<M> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Mounted")
            .field("mail", &self.mail)
            .finish_non_exhaustive()
    }
}

/// Build one actor's mailbox and hold its loop until a registry accepts it.
///
/// Called by the generated `spawn`, which supplies the loop.
///
/// **Nothing is spawned here.** The loop runs from [`System::mount`], or from the
/// insert a lazy spawn makes, which is what makes a refused mount cost no task —
/// see [`Mounted`] for the defect that argument comes from.
///
/// # The mailbox must be named
///
/// A function's type parameter cannot carry a default — only a type's can — and
/// there are no defaults left to inherit anyway, so `M` is inferred and the only
/// thing here to infer it from is the closure's own argument. An unannotated
/// closure is `error[E0282]: type annotations needed`, pointed at the parameter
/// — or at the binding, as `Mounted<_>`, if the handle is bound before it is
/// used:
///
/// ```compile_fail
/// keylink::mount(|mut inbox| async move { while inbox.recv().await.is_some() {} });
/// ```
///
/// That hazard is unchanged by the per-service rewrite and it is worth being
/// exact about why, because the wave that removed the type defaults was expected
/// to remove this too. It did not: `mount` is generic in a *function* parameter,
/// a function generic cannot carry a default, and a generated alias applies `M`
/// in type position where this signature has none. The generated `spawn`
/// annotates the closure with its own `Mailbox` alias every time, which is why
/// the break reaches no caller.
#[doc(hidden)]
pub fn mount<M, F>(run: impl FnOnce(Mailbox<M>) -> F) -> Mounted<M>
where
    F: Future<Output = ()> + Send + 'static,
{
    let (mail, inbox) = mpsc::channel(MAILBOX);
    Mounted {
        mail,
        run: Box::pin(run(inbox)),
    }
}

// ─────────────────────────────────────────────────────────────── the registry

/// What to do with an actor nobody has addressed for a while.
///
/// There is deliberately no default. An actor system that only grows is a leak,
/// and an eviction policy is a decision about *your* state — whether dropping it
/// loses anything — so it is typed out at the call site rather than inherited.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Eviction {
    /// Shut down an actor that has not been addressed for a while.
    ///
    /// Eviction is [`System::shutdown`], not [`System::abort`]: queued messages
    /// drain and the state is dropped after them, never mid-turn — **for as long
    /// as `shutdown_budget` allows.** Past it the actor is destroyed where it
    /// stands, exactly as `abort` would.
    ///
    /// That last clause is the whole of item 4b. This doc used to read "Eviction
    /// is `System::stop`, not `System::abort`" with nothing after it, and the
    /// sweeper it described **never awaited at all**: it dropped the entry inside
    /// a `retain` under the write lock, which dropped the `JoinHandle`, which
    /// detached the task. An actor wedged in a turn was neither drained nor
    /// stopped — it was set loose, still owning its state and still holding
    /// whatever that state held, with nothing left in the map to reach it.
    Idle {
        /// How long an actor may go unaddressed before it is evicted.
        ///
        /// Measured from the last message routed to it, not from its mount.
        /// Spending all of it is spending it: idle for exactly this long is
        /// evicted.
        after: Duration,
        /// How long an evicted actor gets to drain before it is destroyed.
        ///
        /// The same budget [`System::shutdown`] takes, applied by the sweeper
        /// instead of by a caller — so read that method for what a budget buys
        /// and what expiry costs.
        ///
        /// **There is no relationship between this and `after`, and picking one
        /// from the other is a mistake.** `after` is a staleness threshold and is
        /// usually seconds or minutes; this is how long the longest turn plus the
        /// queue behind it needs, and is a property of the service's handlers.
        /// A budget shorter than a turn turns eviction into `abort` for every
        /// actor that happens to be busy when its clock runs out.
        shutdown_budget: Duration,
    },
    /// Keep every actor until it is stopped by hand.
    ///
    /// Correct when actors are mounted from a fixed configuration. A leak when
    /// they are spawned lazily from the network, which is why
    /// [`SystemBuilder::capacity`] still applies.
    Never,
}

/// Builds an actor's state on first message. See [`SystemBuilder::spawn_with`],
/// which takes the closure directly — this alias is how the system stores it,
/// not something a caller ever names.
type Factory<M> = Arc<dyn Fn(&ActorId) -> Option<Mounted<M>> + Send + Sync>;

/// Why an actor could not be mounted.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
#[non_exhaustive]
pub enum MountError {
    /// An actor already holds that id. Refused rather than replaced: replacing
    /// would drop live state that somebody is still addressing.
    #[error("an actor is already mounted as {id}")]
    Occupied {
        /// The id that is taken.
        id: ActorId,
    },
    /// The registry is full.
    #[error("the actor system is at its capacity of {capacity} live actors")]
    AtCapacity {
        /// The cap that was reached.
        capacity: usize,
    },
}

/// Why a message could not reach an actor.
///
/// Two conditions, and neither of them is one of [`MountError`]'s: an id that
/// is taken cannot stop a message, and an id that is empty cannot stop a mount.
/// One enum over both would hand each path an arm the other can never produce,
/// so the shared vocabulary is the *sentence* rather than the type — the
/// capacity refusal reads the same whether a cap was met by hand or by a lazy
/// spawn, and `the_capacity_refusal_reads_the_same_by_hand_and_lazily` is what
/// keeps the two in step.
///
/// Transport-free on purpose. The registry decides what happened; the wire
/// decides what a peer is told about it, in the [`From`] impl `remote.rs`
/// writes.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub(super) enum RouteError {
    /// Nothing is mounted under that id, and no factory built one either.
    ///
    /// No peer ever reads this sentence: `remote.rs` maps this arm to
    /// `RemoteError::NoSuchActor`, which carries the one that goes on the wire.
    /// It used to be that sentence word for word, with nothing keeping the two
    /// in step — an invitation to edit a string here in the belief a caller
    /// would see it. It says what the registry decided instead, which is the
    /// only thing this half can be right about, and it exists at all because
    /// `thiserror` wants a display on every variant.
    #[error("nothing is mounted as {id} and no factory built one")]
    NoSuchActor {
        /// The id that was addressed.
        id: ActorId,
    },
    /// The registry is full, so the factory was never called.
    ///
    /// This sentence *is* rendered — `remote.rs` puts it inside
    /// `RemoteError::Failed` — and `remote.rs`'s
    /// `the_capacity_refusal_reads_the_same_by_hand_and_lazily` pins the words.
    #[error("the actor system is at its capacity of {capacity} live actors")]
    AtCapacity {
        /// The cap that was reached.
        capacity: usize,
    },
}

struct Entry<M> {
    mail: mpsc::Sender<Box<M>>,
    task: JoinHandle<()>,
    /// Milliseconds since the registry's epoch, as of the last message routed
    /// here. An atomic so that touching an actor stays on the read lock.
    last_seen: AtomicU64,
}

struct Registry<M> {
    entries: RwLock<HashMap<ActorId, Entry<M>>>,
    factory: Option<Factory<M>>,
    capacity: usize,
    eviction: Eviction,
    /// `n0_future::time::Instant`, which is neither of the obvious two.
    ///
    /// On native it IS `tokio::time::Instant`, so `tokio::time::pause` and
    /// `advance` still drive it — which is what makes the eviction policy
    /// provable rather than sampled by a sleep long enough to usually work. The
    /// paused-clock tests in this file's `tests` module are the only proof that
    /// this indirection did not quietly break that.
    ///
    /// In a browser it is `web_time::Instant`, reading `performance.now()`.
    /// `std::time::Instant::now()` — which `tokio::time::Instant` resolves to
    /// off a test runtime — **panics** on `wasm32-unknown-unknown`, where std
    /// dispatches to an unsupported platform stub. This field is built in
    /// `SystemBuilder::build`, so before this change every actor system panicked
    /// in a tab. RFC-03 never caught it because this module postdates the RFC.
    epoch: Instant,
}

impl<M> Registry<M> {
    fn now(&self) -> u64 {
        u64::try_from(self.epoch.elapsed().as_millis()).unwrap_or(u64::MAX)
    }

    fn read(&self) -> std::sync::RwLockReadGuard<'_, HashMap<ActorId, Entry<M>>> {
        // A panic under the lock must cost one message, not wedge the registry
        // for the life of the process. The same call the demo's handler makes.
        self.entries.read().unwrap_or_else(PoisonError::into_inner)
    }

    fn write(&self) -> std::sync::RwLockWriteGuard<'_, HashMap<ActorId, Entry<M>>> {
        self.entries.write().unwrap_or_else(PoisonError::into_inner)
    }

    /// Find the actor, spawning it if the system was given a factory.
    fn route(&self, id: &ActorId) -> std::result::Result<mpsc::Sender<Box<M>>, RouteError> {
        if let Some(found) = self.touch(&self.read(), id) {
            return Ok(found);
        }
        let Some(factory) = self.factory.as_ref() else {
            return Err(RouteError::NoSuchActor { id: id.clone() });
        };

        let mut entries = self.write();
        // Another message to the same new id may have won the race between the
        // two locks. One id is one actor, so the loser adopts the winner's.
        if let Some(found) = self.touch(&entries, id) {
            return Ok(found);
        }
        if entries.len() >= self.capacity {
            return Err(RouteError::AtCapacity {
                capacity: self.capacity,
            });
        }
        let Some(mounted) = factory(id) else {
            return Err(RouteError::NoSuchActor { id: id.clone() });
        };
        let found = mounted.mail.clone();
        entries.insert(id.clone(), self.entry(mounted));
        Ok(found)
    }

    fn touch(
        &self,
        entries: &HashMap<ActorId, Entry<M>>,
        id: &ActorId,
    ) -> Option<mpsc::Sender<Box<M>>> {
        let entry = entries.get(id)?;
        entry.last_seen.store(self.now(), Ordering::Relaxed);
        Some(entry.mail.clone())
    }

    /// Start the loop and pair the task with the sender that reaches it.
    ///
    /// **The only place an actor's task is spawned**, which is what lets
    /// [`Mounted`] be refused for free.
    ///
    /// # Panics
    ///
    /// Panics when called outside a tokio runtime, like every other spawn.
    fn entry(&self, mounted: Mounted<M>) -> Entry<M> {
        Entry {
            mail: mounted.mail,
            task: n0_future::task::spawn(mounted.run),
            last_seen: AtomicU64::new(self.now()),
        }
    }

    /// Evict every actor that has been idle past `after`, and destroy the ones
    /// that will not drain inside `shutdown_budget`.
    ///
    /// # Why the expired entries come out from under the lock first
    ///
    /// This was a `retain` over the write guard, which is where its defect lived:
    /// tearing an actor down needs an await, and `retain` takes a synchronous
    /// closure, so the only thing it *could* do was drop the entry — dropping the
    /// `JoinHandle` and detaching the task. Awaiting inside `retain` is not the
    /// repair either, because it would hold the write guard across the await and
    /// stall every route, mount and shutdown in the system behind one wedged
    /// actor.
    ///
    /// So the lock is used for the one thing it is needed for — deciding who is
    /// expired and taking them out of the map — and released before anything is
    /// awaited. This is [`Registry::route`]'s discipline read backwards: `route`
    /// holds the guard across the user's `factory` call and is documented as such;
    /// nothing here may hold it across an await.
    ///
    /// # Concurrently, and that is not an optimisation
    ///
    /// A hundred expired actors that all refuse to drain must cost one budget, not
    /// a hundred. Serially, a full sweep would take `entries * budget` and the
    /// next tick would not start until it finished, so a single wedged actor would
    /// push every other actor's eviction past its own deadline — a queue that
    /// grows under exactly the condition it exists to clear.
    async fn sweep(&self) {
        let Eviction::Idle {
            after,
            shutdown_budget,
        } = self.eviction
        else {
            return;
        };
        let after = u64::try_from(after.as_millis()).unwrap_or(u64::MAX);
        let now = self.now();
        // `extract_if` and not `retain`, which is the whole change: it hands the
        // removed entries back instead of dropping them, so the expired ones can
        // be torn down after the guard is gone. The predicate is the old
        // `retain`'s, inverted — `>=`, so an actor idle for exactly its budget is
        // evicted.
        let expired: Vec<Entry<M>> = self
            .write()
            .extract_if(|_, entry| {
                now.saturating_sub(entry.last_seen.load(Ordering::Relaxed)) >= after
            })
            .map(|(_, entry)| entry)
            .collect();
        // The `Ended` of an actor nobody asked about is nobody's answer. It is
        // computed anyway because `drain` is the one teardown and a second,
        // quieter one is how the two would drift.
        n0_future::join_all(
            expired
                .into_iter()
                .map(|entry| drain(entry, shutdown_budget)),
        )
        .await;
    }
}

/// Close the mailbox, wait out `budget`, and destroy what is still running.
///
/// The one teardown in this file: [`System::shutdown`] and [`Registry::sweep`]
/// both go through here, so an eviction and a shutdown cannot mean different
/// things. [`System::abort`] does not, because it never drains — that is a
/// different operation rather than this one with a budget of zero.
///
/// # The handle must outlive the timeout, and that is the whole defect
///
/// `n0_future::time::timeout(budget, task)` **consumes** the handle, so when the
/// budget expires the timeout's future drops it — and a dropped `JoinHandle`
/// detaches its task on both targets. Verified against `n0-future 0.3.2`: the
/// native branch re-exports tokio's, whose documented behaviour is to detach,
/// and the wasm branch has no `impl Drop for JoinHandle` at all — abort-on-drop
/// lives in a separate `AbortOnDropHandle`. So the actor would go on running,
/// still owning `&mut A` and whatever `A` holds, with the entry already out of
/// the map and no handle anywhere able to reach it.
///
/// `pin!` puts the handle on this future's own stack and hands the timeout a
/// `&mut` to it, so expiry leaves the handle right here to be aborted. Stack
/// pinning rather than `Box::pin` because a teardown should not allocate, and
/// `&mut task` rather than either would rest on `JoinHandle: Unpin`, which is
/// documented for tokio's and merely true of n0-future's wasm shim.
///
/// # What it returns, and the one thing it cannot know
///
/// - the task returned → [`Ended::Stopped`];
/// - the task's join failed → [`Ended::Vanished`], which is what a panicked or
///   already-aborted task is;
/// - the budget expired → the task is aborted, joined, and reported
///   [`Ended::Vanished`], because that is what `abort` produces everywhere else
///   in the crate.
///
/// The `Ended` is **synthesised from the join**, not read from the actor, and
/// [`System::shutdown`] carries the argument for that and states its residual.
async fn drain<M>(entry: Entry<M>, budget: Duration) -> Ended {
    let Entry { mail, task, .. } = entry;
    // Dropping the registry's sender is what ends the loop. The task lives while
    // any other sender does, which is what lets a message already in flight
    // finish rather than be cancelled.
    drop(mail);
    let mut task = std::pin::pin!(task);
    match n0_future::time::timeout(budget, &mut task).await {
        Ok(Ok(())) => Ended::Stopped,
        Ok(Err(_join)) => Ended::Vanished,
        Err(_expired) => {
            // Before the second await, never after: an `abort` that has been
            // requested lands whether or not anybody is still waiting for it,
            // which is the asymmetry that made `abort` correct and `stop` not.
            task.abort();
            let _ = task.await;
            Ended::Vanished
        }
    }
}

/// A registry of live actors: which ones exist, and for how long.
///
/// Cheap to clone: every clone names the same registry.
///
/// `M` is what a mailbox carries, and it is a parameter because everything here
/// — the map, the cap, the eviction clock, the lifecycle — is about *which*
/// actor and never about what a message is made of. Naming a message type in
/// these fields would put a QUIC stream and a postcard payload inside the one
/// half of this module that has no business knowing either, and no import list
/// would show it.
///
/// **There is no default, and there used to be.** One system per node held
/// actors of unrelated services, so `keylink::System` written bare had to mean
/// something and a type-parameter default was the only way to say it. A system
/// is per-service now: `M` is that service's own mailbox type, `inventory::Mail`,
/// and a caller writes the generated `inventory::System` alias rather than this
/// type. That is not tidying — a per-service registry **physically cannot hold**
/// an actor of another service, so mounting the wrong one is `E0308` at the call
/// site instead of a schema comparison at run time on the serving node.
///
/// `System<YourOwnType>` still compiles, and there is still no way to deliver a
/// message to anything in it: `route` is `pub(super)` and [`Mounted`] keeps its
/// sender to itself. What you get is a map you can mount into, count, stop and
/// evict, and never send to. The tests in this file are the exception that
/// proves it — they are inside the crate, so they can call `route`.
///
/// # `error[E0282]: type annotations needed`
///
/// This is where somebody who has just met that error is looking. `M` has to
/// come from somewhere, and a `let` that binds what
/// [`build`](SystemBuilder::build) returns without naming its type gives it
/// nowhere to come from:
///
/// ```compile_fail
/// let system = keylink::System::builder(keylink::Eviction::Never).build();
/// assert!(system.is_empty());
/// ```
///
/// The compiler points at `build`'s own `M: Send + 'static` bound, from the
/// `.build()` line alone: that bound is what demands `M` be known, so the
/// binding fails by itself and the second line never gets a chance to matter.
/// The second line is what such a system goes on to be used for, and it is why
/// nothing downstream rescues the binding either — [`contains`](Self::contains),
/// [`len`](Self::len), [`is_empty`](Self::is_empty),
/// [`evict_idle`](Self::evict_idle), [`shutdown`](Self::shutdown) and
/// [`abort`](Self::abort) never mention `M`, so none of them pins it.
///
/// `compile_fail` says only that it failed, and writing `compile_fail,E0282`
/// would not pin the code — **rustdoc ignores the error code entirely**, so a
/// doctest tagged with one error and failing with another reports as passing.
/// That the failure is the one described is checked by lifting the tag and
/// reading the error, which is a thing a person does, not the harness.
///
/// Name the service's own system and it compiles, which is what every real call
/// site does anyway:
///
/// ```ignore
/// let system: inventory::System = inventory::System::builder(Eviction::Never).build();
/// assert!(system.is_empty());
/// ```
///
/// Most code never meets it, because `mount(id, inventory::spawn(..))` pins `M`
/// through [`Mounted`] without anybody trying.
pub struct System<M> {
    inner: Arc<Registry<M>>,
}

/// Hand-written, because the derive would bound the impl on `M: Clone` — and a
/// mailbox message, which owns a reply channel and a stream, is not. A clone
/// here is a clone of the `Arc` and never of a message.
///
/// The derive does not fail where it is written, either: `self.clone()` in
/// `dispatch` would quietly resolve to `Clone` on the *reference* and surface
/// as a lifetime error inside the dispatch closure.
impl<M> Clone for System<M> {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl<M> System<M> {
    /// Configure a system. The eviction policy is an argument because there is
    /// no safe default for it.
    #[must_use]
    pub fn builder(eviction: Eviction) -> SystemBuilder<M> {
        SystemBuilder {
            capacity: DEFAULT_CAPACITY,
            eviction,
            factory: None,
        }
    }

    /// Mount an actor under `id`, starting it.
    ///
    /// Takes effect on the next message. Nothing is announced, no lock outside
    /// this registry is taken, and no connection is disturbed.
    ///
    /// **A refusal costs no task.** Both refusals below return before the actor's
    /// loop is ever polled, so the [`Mounted`] this drops is an unstarted future
    /// rather than a running task — which is the whole reason [`Mounted`] holds a
    /// future. [`MountError`] hands nothing back, so anything already spawned by
    /// the time a refusal is decided would be unreachable by construction.
    ///
    /// # Panics
    ///
    /// Panics when called outside a tokio runtime, like every other spawn. It
    /// starts the actor's task, and until this wave the generated `spawn` did —
    /// so the panic moved from where an actor is built to where it starts, which
    /// is the same call stack in every caller in this repository.
    ///
    /// # Errors
    ///
    /// [`MountError::Occupied`] when the id is taken, and
    /// [`MountError::AtCapacity`] when the registry is full.
    pub fn mount(&self, id: ActorId, actor: Mounted<M>) -> std::result::Result<(), MountError> {
        let mut entries = self.inner.write();
        if entries.contains_key(&id) {
            return Err(MountError::Occupied { id });
        }
        if entries.len() >= self.inner.capacity {
            return Err(MountError::AtCapacity {
                capacity: self.inner.capacity,
            });
        }
        entries.insert(id, self.inner.entry(actor));
        Ok(())
    }

    /// Find the actor `id` addresses, spawning it if this system has a factory.
    ///
    /// **The whole of the seam**, and an operation rather than a handle on
    /// purpose. With `inner` reachable from `remote.rs`, the transport half held
    /// the entire [`Registry`] and every method on it, so nothing but manners
    /// stopped the next change from reaching the map, the cap or the eviction
    /// clock from the side that has a network in it. One `pub(super)` function
    /// is a boundary that can be read in full, and adding a second is a visible
    /// act rather than a use of something already in hand.
    ///
    /// # Errors
    ///
    /// [`RouteError`], which `remote.rs` translates into what a peer is told.
    pub(super) fn route(
        &self,
        id: &ActorId,
    ) -> std::result::Result<mpsc::Sender<Box<M>>, RouteError> {
        self.inner.route(id)
    }

    /// Stop routing to `id`, give the actor `budget` to finish what it has, and
    /// destroy it if it does not.
    ///
    /// The queued messages are handled and answered; the state is dropped after
    /// them. Routing stops the instant this returns from the registry, so a
    /// later message gets
    /// [`RemoteError::NoSuchActor`](crate::RemoteError::NoSuchActor) on a live
    /// connection while every other actor on it carries on.
    ///
    /// `None` when nothing was mounted under that id. Otherwise
    /// [`Ended::Stopped`] if the actor drained inside its budget, and
    /// [`Ended::Vanished`] if it did not and was destroyed, or if its task had
    /// already failed.
    ///
    /// # The budget is a parameter, and its absence was a live defect
    ///
    /// This method replaces a `stop` whose own rustdoc read *"Deliberately takes
    /// no deadline: `tokio::time::timeout` composes, and a parameter here would
    /// duplicate it."* It does not compose, and that sentence is the reason to
    /// look: wrap the old `stop` in a timeout and let the timeout fire, and the
    /// dropped future drops the `Entry`, which drops the `JoinHandle`, which
    /// **detaches** the task. The entry had already left the map — it is removed
    /// before the await — so what a timed-out `stop` produced was a running actor
    /// that still owned its state, still held whatever that state held, and could
    /// no longer be addressed, stopped or aborted by anyone.
    ///
    /// [`LocalActor::stop`](crate::LocalActor::stop) carries the identical
    /// sentence and there it is **true**, which is why it keeps it: a
    /// `LocalActor` holds `task: Arc<JoinHandle<()>>` and waits on a separate
    /// `ended` channel, so a dropped `stop()` future costs the *wait* and leaves
    /// `abort()` reachable through the handle the caller still has. One sentence,
    /// true of a handle that survives its own await and false of an entry that
    /// does not.
    ///
    /// So the budget is not optional and there is no deadline-free spelling to
    /// fall back to — the [`Eviction`]/`Retention`/`Allowlist` rule, that a
    /// decision with no safe default is typed out at the call site. [`Self::abort`]
    /// survives alongside this because it never drains, which makes it a different
    /// operation rather than this one with a budget of zero.
    ///
    /// # The `Ended` is synthesised here, and what that costs
    ///
    /// A registry entry has no `ended` channel — the keyed loop computes an
    /// [`Ended`] and drops it, because a keyed actor has no `ended()` for anyone
    /// to await — so this reports what the *join* said: the task returned, or it
    /// did not.
    ///
    /// **That is not the whole truth, and the gap is new.** The argument this
    /// design inherited was that a synthesised `Ended` "cannot lie", because
    /// `Stopped` and `Vanished` were the only two endings a keyed actor had. Wave
    /// 3 gives a keyed actor observers, and with observers the loop can break on
    /// its own with [`Ended::Panicked`] or [`Ended::Observer`] — an ending this
    /// call did not cause and cannot see, because the loop returns normally
    /// either way and the join says `Ok(())` for both. So:
    ///
    /// - [`Ended::Vanished`] is exact. It means the task did not return, and only
    ///   an abort — this call's, or an earlier one — produces that.
    /// - [`Ended::Stopped`] is exact about **this call**: routing has stopped and
    ///   the task is finished. It does not claim to be *why* the loop exited. An
    ///   actor that ended on an observer's refusal a second earlier is reported
    ///   `Stopped` here.
    ///
    /// Adding `ended: watch::Receiver<Option<Ended>>` to that entry would close
    /// that gap and is deliberately not done, for a reason that survives Wave 3:
    /// it is a **second** determination of a fact this call already has. For the
    /// two answers a shutdown owes a caller — did it drain, did we have to kill
    /// it — the join is authoritative and a channel adds nothing, and in the one
    /// case they could disagree the channel is *worse*: an aborted loop never
    /// fills it, so the receiver reports `Vanished` from a dropped sender, which
    /// is the join's answer arrived by a longer route. The residual is a question
    /// about the **loop**, which is the half that knows: the keyed loop already
    /// computes the true `Ended` and throws it away. Whoever wants it should give
    /// the loop somewhere to put it, not have the registry guess a second time.
    pub async fn shutdown(&self, id: &ActorId, budget: Duration) -> Option<Ended> {
        let entry = self.inner.write().remove(id)?;
        Some(drain(entry, budget).await)
    }

    /// Stop routing to `id` and cancel what the actor is doing where it stands.
    ///
    /// The state is **destroyed**: the task owns it, `abort` drops that task's
    /// future at its next await point, and unflushed work goes with it. That is
    /// the difference from [`Self::shutdown`], and it is why the two are separate
    /// methods rather than one with a budget of zero: this one does not drain, so
    /// there is no budget to name.
    ///
    /// `abort()` is called **before** the await, which is why this survived the
    /// wave that deleted `stop`. A caller who wraps this in a timeout and lets it
    /// fire loses the wait and not the cancellation — the request has already
    /// landed on the task.
    ///
    /// Returns whether an actor was mounted under that id.
    pub async fn abort(&self, id: &ActorId) -> bool {
        let Some(entry) = self.inner.write().remove(id) else {
            return false;
        };
        entry.task.abort();
        let _ = entry.task.await;
        true
    }

    /// Whether an actor is mounted under `id`.
    #[must_use]
    pub fn contains(&self, id: &ActorId) -> bool {
        self.inner.read().contains_key(id)
    }

    /// How many actors are live.
    #[must_use]
    pub fn len(&self) -> usize {
        self.inner.read().len()
    }

    /// Whether no actor is mounted.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Evict every actor that has been idle past [`Eviction::Idle::after`], and
    /// destroy the ones that do not drain inside
    /// [`shutdown_budget`](Eviction::Idle::shutdown_budget).
    ///
    /// Called on a timer by the system itself. Public so that a program with its
    /// own clock, or a test, can make it happen instead of waiting for one.
    ///
    /// Awaiting this awaits the teardown of everything it evicted, so it returns
    /// only once each of those actors has either drained or been destroyed. It was
    /// synchronous, and could not be anything else while eviction meant dropping
    /// an entry and detaching its task; a budget is a thing to wait out, and a
    /// caller who cannot wait can spawn this.
    pub async fn evict_idle(&self) {
        self.inner.sweep().await;
    }
}

impl<M> fmt::Debug for System<M> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("System")
            .field("live", &self.len())
            .field("capacity", &self.inner.capacity)
            .field("eviction", &self.inner.eviction)
            .field("lazy_spawn", &self.inner.factory.is_some())
            .finish()
    }
}

/// Assembles a [`System`]: its eviction policy, its cap, and how it spawns.
///
/// `M` is the system's message type, exactly as on [`System`], and it has no
/// default for the same reason.
pub struct SystemBuilder<M> {
    capacity: usize,
    eviction: Eviction,
    factory: Option<Factory<M>>,
}

/// The cap, as the generic half spells it. [`SystemBuilder::DEFAULT_CAPACITY`]
/// is the public name and carries the reasoning for the number.
const DEFAULT_CAPACITY: usize = 10_000;

impl<M> SystemBuilder<M> {
    /// The cap a system has when none is named.
    ///
    /// Ten thousand live actors was the number the design was costed against,
    /// so it is the number a system holds before it starts refusing.
    ///
    /// This lived on `impl SystemBuilder<Mail>` for one release, under a doc
    /// comment calling itself a compatibility shim: a type-parameter default
    /// applies in type position and not in expression position, so a constant on
    /// the generic impl left `M` uninferrable at every call site that named it.
    /// The defaults are gone and a caller reaches this through a generated,
    /// fully applied alias — `inventory::SystemBuilder::DEFAULT_CAPACITY` — so
    /// the constant is back where the cap belongs, on every instantiation at
    /// once.
    pub const DEFAULT_CAPACITY: usize = self::DEFAULT_CAPACITY;

    /// Build an actor on the first message addressed to an unmounted id.
    ///
    /// Without this, a message to an unmounted id is
    /// [`RemoteError::NoSuchActor`](crate::RemoteError::NoSuchActor) and the
    /// registry only ever grows by hand. With it, the registry grows from the
    /// network — which is why [`Self::capacity`] and the eviction policy are not
    /// optional decoration. Return `None` for an id this system will not serve.
    ///
    /// Runs under the registry's write lock, so it must not block. Building
    /// state and spawning a task is what it is for; reading a database is not.
    #[must_use]
    pub fn spawn_with<F>(mut self, factory: F) -> Self
    where
        F: Fn(&ActorId) -> Option<Mounted<M>> + Send + Sync + 'static,
    {
        self.factory = Some(Arc::new(factory));
        self
    }

    /// The most actors this system will hold at once.
    ///
    /// A mount past it is [`MountError::AtCapacity`]; a lazy spawn past it is
    /// [`RemoteError::Failed`](crate::RemoteError::Failed) naming the cap. This
    /// is the bound that keeps a lazily spawning system from being a
    /// memory-exhaustion target, so raising it is a statement about how much
    /// memory the process may spend.
    #[must_use]
    pub fn capacity(mut self, actors: usize) -> Self {
        self.capacity = actors;
        self
    }

    /// Start the system, and the sweeper that enforces its eviction policy.
    ///
    /// # Panics
    ///
    /// Panics when [`Eviction::Idle`] is set outside a tokio runtime, because
    /// the sweeper is a task.
    #[must_use]
    pub fn build(self) -> System<M>
    where
        M: Send + 'static,
    {
        let system = System {
            inner: Arc::new(Registry {
                entries: RwLock::new(HashMap::new()),
                factory: self.factory,
                capacity: self.capacity,
                eviction: self.eviction,
                epoch: Instant::now(),
            }),
        };
        if let Eviction::Idle { after, .. } = self.eviction {
            spawn_sweeper(&system.inner, after);
        }
        system
    }
}

impl<M> fmt::Debug for SystemBuilder<M> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SystemBuilder")
            .field("capacity", &self.capacity)
            .field("eviction", &self.eviction)
            .field("lazy_spawn", &self.factory.is_some())
            .finish()
    }
}

/// Sweep on a timer, and stop when the last [`System`] handle goes away.
///
/// Holds a `Weak`, so the sweeper never keeps a registry alive. It outlives the
/// last handle by at most one tick, which nobody can observe.
///
/// `after` and not the shutdown budget sets the tick: the tick is about how
/// promptly a stale actor is noticed, and the budget is about how long its
/// teardown may take once it has been. A sweep that runs long because something
/// would not drain delays the next tick, and it delays it by one budget rather
/// than by one per wedged actor — see [`Registry::sweep`].
fn spawn_sweeper<M: Send + 'static>(registry: &Arc<Registry<M>>, after: Duration) {
    let registry = Arc::downgrade(registry);
    // Half the budget, so an actor is evicted between one and one and a half
    // budgets after its last message rather than up to two.
    let tick = (after / 2).max(Duration::from_millis(1));
    n0_future::task::spawn(async move {
        loop {
            n0_future::time::sleep(tick).await;
            let Some(registry) = Weak::upgrade(&registry) else {
                return;
            };
            registry.sweep().await;
        }
    });
}

#[cfg(test)]
mod tests {
    //! Every test here runs at `M = Bare`, and that instantiation is the
    //! assertion.
    //!
    //! The header above claims this half places no requirement on what a mailbox
    //! carries. Run against a real per-service message type, that claim is an
    //! argument rather than a proof, because a real one satisfies any bound one
    //! of these types might quietly grow. So the witness is a type that
    //! satisfies as little as a witness can, and `mount`, `route`, `shutdown`
    //! and `build` all appear here at it: a bound on any of the four stops this
    //! binary from compiling. `shutdown` and `sweep` both await a task inside a
    //! generic `impl<M>`, which is the shape a `M: Send` bound arrives under —
    //! and it must not, because `build`'s existing `M: Send + 'static` is the
    //! only one this file is allowed to want.
    //!
    //! **`()` was the witness first, and it was the wrong one.** It is not
    //! "`Send + 'static` and nothing else" — it is `Serialize`,
    //! `DeserializeOwned`, `Schema`, `Clone`, `Copy`, `Default` and `Debug` as
    //! well, so it satisfies the likeliest accidental bound of all, the one
    //! encoding needs. Measured, not argued: with `M: Debug` on `build`, the
    //! whole crate passed — 18 `test result: ok` lines, 0 failures.
    //!
    //! What the integration-test binaries cover is worth being exact about,
    //! because it is most of the real cover: a per-service mailbox message is
    //! not `Serialize`, so a serde bound on `build` fails `tests/api.rs`,
    //! `tests/actors.rs`, `tests/blobs.rs` and `tests/actor_memory.rs` with no
    //! help from here — also measured. What they cannot catch is a bound a real
    //! message happens to satisfy, and `Debug` is one, because
    //! `remote.rs` writes one by hand. That gap is what a witness with no
    //! derives at all closes.
    //!
    //! Nothing here needs a message to *be* anything, which is the same point
    //! from the other side: mounting, the cap, eviction, routing and the
    //! lifecycle are all decisions about **which** actor. The tests that need a
    //! real payload live in `remote.rs`, where a payload exists.

    #![allow(clippy::expect_used, clippy::unwrap_used)]

    use std::sync::atomic::AtomicUsize;

    use super::*;

    /// The witness: `Send + 'static`, which [`SystemBuilder::build`] requires
    /// and no message type can do without, and not one thing more. No derives,
    /// deliberately — every trait added here is a bound this file would stop
    /// noticing.
    struct Bare;

    /// An actor that does nothing at all, for testing the registry rather than
    /// any message it carries.
    fn idler() -> Mounted<Bare> {
        mount(|mut inbox: Mailbox<Bare>| async move { while inbox.recv().await.is_some() {} })
    }

    /// A policy for the tests that are about `after` and not about the budget.
    ///
    /// The budget is a minute because every actor these tests evict is an
    /// [`idler`], which drains the instant its sender is dropped. A number small
    /// enough to expire would make each of them a test of the teardown as well as
    /// of the clock, and the teardown has its own tests over an actor that will
    /// not drain — in `tests/actors.rs`, where there is a claim to watch be
    /// released.
    fn idle_after(after: Duration) -> Eviction {
        Eviction::Idle {
            after,
            shutdown_budget: Duration::from_secs(60),
        }
    }

    /// Printing a handle must not depend on what its mailbox carries, which a
    /// `derive(Debug)` on [`Mounted`] would make it do — and nothing else would
    /// notice, because nothing else here prints one. `Bare` is not `Debug`, so
    /// this line is the notice.
    #[tokio::test]
    async fn a_mounted_actor_prints_without_help_from_its_message_type() {
        assert!(format!("{:?}", idler()).starts_with("Mounted"));
    }

    /// Eviction is by last message, not by mount time, or a busy actor would be
    /// evicted out from under its callers.
    ///
    /// Time is paused, so this proves the policy instead of sampling it. The
    /// same test against the wall clock WAS flaky, and failed: it needed the
    /// background sweeper not to fire between two touches, which is a race no
    /// test can win under load, and losing it evicted the actor the test was
    /// about to address.
    #[tokio::test(start_paused = true)]
    async fn eviction_is_by_last_message_and_not_by_mount_time() {
        let system = System::builder(idle_after(Duration::from_secs(60))).build();
        let busy = ActorId::new("busy").expect("valid");
        let idle = ActorId::new("idle").expect("valid");
        system.mount(busy.clone(), idler()).expect("free");
        system.mount(idle.clone(), idler()).expect("free");

        // Both are now older than the budget.
        tokio::time::advance(Duration::from_secs(90)).await;
        // Routing is what marks an actor as used.
        system.route(&busy).expect("mounted");
        system.evict_idle().await;

        assert!(system.contains(&busy), "the addressed actor must survive");
        assert!(!system.contains(&idle), "the idle actor must be gone");
    }

    /// The other half: an actor inside its budget survives however often the
    /// sweeper runs.
    #[tokio::test(start_paused = true)]
    async fn sweeping_does_not_evict_an_actor_inside_its_budget() {
        let system = System::builder(idle_after(Duration::from_secs(60))).build();
        let fresh = ActorId::new("fresh").expect("valid");
        system.mount(fresh.clone(), idler()).expect("free");

        tokio::time::advance(Duration::from_secs(59)).await;
        system.evict_idle().await;

        assert!(system.contains(&fresh));
    }

    /// The boundary itself, which is the only place `< budget` and `<= budget`
    /// disagree — 59 s and 90 s of a 60 s budget cannot tell them apart, and one
    /// keystroke is all that separates "idle for its whole budget" from "idle
    /// for a budget and a tick". A paused clock lands on it exactly, for free.
    #[tokio::test(start_paused = true)]
    async fn an_actor_idle_for_exactly_its_budget_is_evicted() {
        let system = System::builder(idle_after(Duration::from_secs(60))).build();
        let borderline = ActorId::new("borderline").expect("valid");
        system.mount(borderline.clone(), idler()).expect("free");

        tokio::time::advance(Duration::from_secs(60)).await;
        system.evict_idle().await;

        assert!(
            !system.contains(&borderline),
            "the budget is how long an actor may be idle, so spending all of it is spending it"
        );
    }

    /// `Eviction::Never` means never, however long nobody calls.
    #[tokio::test(start_paused = true)]
    async fn eviction_never_keeps_an_actor_nobody_addresses() {
        let system = System::builder(Eviction::Never).build();
        let forgotten = ActorId::new("forgotten").expect("valid");
        system.mount(forgotten.clone(), idler()).expect("free");

        tokio::time::advance(Duration::from_secs(60 * 60 * 24)).await;
        system.evict_idle().await;

        assert!(system.contains(&forgotten));
    }

    #[test]
    fn a_system_without_a_factory_never_grows_by_itself() {
        let system: System<Bare> = System::builder(Eviction::Never).build();
        let error = system
            .route(&ActorId::new("nobody").expect("valid"))
            .expect_err("nothing is mounted and nothing builds it");
        assert!(
            matches!(error, RouteError::NoSuchActor { .. }),
            "got {error:?}"
        );
        assert!(system.is_empty());
    }

    /// The cap is registry policy, so it refuses on both paths to an entry and
    /// it refuses without knowing what a message is: by hand through
    /// [`System::mount`], and from the factory through [`System::route`].
    ///
    /// The two refusals come from different enums, which is why both are here.
    /// `remote.rs`'s `the_capacity_refusal_reads_the_same_by_hand_and_lazily` is
    /// what keeps their sentences in step.
    ///
    /// The counter is the third assertion, and it is about memory rather than
    /// about a message: the cap exists so that a peer naming ten thousand ids
    /// this system has never heard of cannot make it build ten thousand actors.
    /// Refusing *after* calling the factory would satisfy both `assert_eq!`s
    /// above while doing exactly the work the cap is there to prevent — and
    /// doing it under the write lock, where it stalls every other route and
    /// mount rather than merely wasting a task.
    #[tokio::test]
    async fn a_full_registry_refuses_by_hand_and_lazily() {
        let built = Arc::new(AtomicUsize::new(0));
        let counter = Arc::clone(&built);
        let system = System::builder(Eviction::Never)
            .capacity(1)
            .spawn_with(move |_| {
                counter.fetch_add(1, Ordering::SeqCst);
                Some(idler())
            })
            .build();
        system
            .mount(ActorId::new("first").expect("valid"), idler())
            .expect("the cap is one and nothing is mounted");

        let by_hand = system
            .mount(ActorId::new("second").expect("valid"), idler())
            .expect_err("the cap is one and one is mounted");
        assert_eq!(by_hand, MountError::AtCapacity { capacity: 1 });

        let lazily = system
            .route(&ActorId::new("third").expect("valid"))
            .expect_err("the factory is never called past the cap");
        assert_eq!(lazily, RouteError::AtCapacity { capacity: 1 });
        assert_eq!(
            built.load(Ordering::SeqCst),
            0,
            "a refused message must cost no state and no task"
        );
    }

    /// An id that is taken is [`MountError::Occupied`] even when the registry is
    /// also full, because the two say different things to a caller: `Occupied`
    /// means this id is somebody else's, `AtCapacity` means the system is out of
    /// room. Checking the cap first would tell a caller re-mounting a live id to
    /// go and make room, which would not help them once they had.
    #[tokio::test]
    async fn a_taken_id_is_occupied_even_when_the_registry_is_full() {
        let system = System::builder(Eviction::Never).capacity(1).build();
        let taken = ActorId::new("taken").expect("valid");
        system.mount(taken.clone(), idler()).expect("free");

        let again = system
            .mount(taken.clone(), idler())
            .expect_err("the id is taken");

        assert_eq!(again, MountError::Occupied { id: taken });
    }

    /// The two lifecycle methods, at a message type with no wire behind it.
    ///
    /// They are the likeliest place for a transport bound to arrive by accident,
    /// because both await a task and one of them drops a sender — and neither
    /// needs to know what that sender carries. Here it carries a `Bare`.
    ///
    /// What each one does to state in flight is `remote.rs`'s and
    /// `tests/actors.rs`'s to prove, over a real actor with real state. This one
    /// only asks that unmounting works at all when `M` is nothing — and that
    /// `shutdown`'s `None` still says "nothing was mounted", which is the whole
    /// of what its predecessor's `false` said.
    #[tokio::test]
    async fn shutdown_and_abort_unmount_whatever_the_mailbox_carries() {
        let system = System::builder(Eviction::Never).build();
        let budget = Duration::from_secs(60);
        let drained = ActorId::new("drained").expect("valid");
        let cancelled = ActorId::new("cancelled").expect("valid");
        system.mount(drained.clone(), idler()).expect("free");
        system.mount(cancelled.clone(), idler()).expect("free");
        assert_eq!(system.len(), 2);

        assert!(matches!(
            system.shutdown(&drained, budget).await,
            Some(Ended::Stopped)
        ));
        assert!(system.abort(&cancelled).await);
        assert!(system.is_empty());

        assert!(
            system.shutdown(&drained, budget).await.is_none(),
            "gone stays gone"
        );
        assert!(!system.abort(&cancelled).await);
    }

    /// The refusal that used to cost a task, at the witness type.
    ///
    /// `Mounted` holds an unstarted future, so a refused mount drops a loop that
    /// has never been polled. The guard is the loop's own capture: a `Started`
    /// that increments when the loop first runs, which for a refused actor must
    /// stay at zero however long the test waits.
    ///
    /// `tests/actors.rs` holds the other half of this over a real exclusive claim,
    /// which is the consequence a counter cannot show. This one is here because
    /// this is where the two refusals are written, and because it needs nothing of
    /// `M`.
    #[tokio::test]
    async fn neither_refusal_starts_the_actor_it_refused() {
        let started = Arc::new(AtomicUsize::new(0));
        let watcher = |started: &Arc<AtomicUsize>| {
            let started = Arc::clone(started);
            mount(|mut inbox: Mailbox<Bare>| async move {
                started.fetch_add(1, Ordering::SeqCst);
                while inbox.recv().await.is_some() {}
            })
        };

        let system = System::builder(Eviction::Never).capacity(1).build();
        let taken = ActorId::new("taken").expect("valid");
        system.mount(taken.clone(), idler()).expect("free");

        let occupied = system
            .mount(taken, watcher(&started))
            .expect_err("the id is taken");
        assert!(matches!(occupied, MountError::Occupied { .. }));
        let full = system
            .mount(ActorId::new("second").expect("valid"), watcher(&started))
            .expect_err("the cap is one");
        assert!(matches!(full, MountError::AtCapacity { .. }));

        // Both refused actors are dropped by now. A detached task would have been
        // scheduled and would run at the first opportunity, so give it several.
        for _ in 0..8 {
            tokio::task::yield_now().await;
        }
        assert_eq!(
            started.load(Ordering::SeqCst),
            0,
            "a refused mount must start no task: a MountError hands nothing back, \
             so anything running by now is unreachable forever"
        );
    }
}
