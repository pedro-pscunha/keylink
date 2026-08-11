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
//! - ~~four type-parameter defaults — `Registry<M = Mail>` and its three
//!   relatives~~ — gone. A default existed so that `keylink::Registry` written
//!   with no parameter went on meaning what it always meant. There is no such
//!   spelling any more: a system is `inventory::Registry`, a generated alias that
//!   applies `M` itself, so there is nothing left for a default to rescue.
//! - ~~`impl RegistryBuilder<Mail>`, for one associated constant~~ — gone with
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
//! move with it, and [`Ended`], which is what [`Registry::stop`] reports.
//! **`Ended` is a new item on §16.8's bill**, and it is put here rather than left
//! to be counted later: `turns.rs` names it too, so a split would have to give
//! both halves somewhere to see it from. That is a smaller cost than a second
//! lifecycle vocabulary, which is what returning anything else would have been.

use std::collections::hash_map;
use std::collections::HashMap;
use std::fmt;
use std::future::Future;
use std::num::NonZeroUsize;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, PoisonError, RwLock, Weak};
use std::time::Duration;

use n0_future::task::{AbortOnDropHandle, JoinHandle};
use n0_future::time::Instant;
use tokio::sync::{mpsc, watch};

use crate::turns::{current, Actor, Chain, Fate, Standing};
use crate::{ActorId, Ended, Origin, Reply, Serving, Unanswered, Withheld};

/// Messages that may wait ahead of the one being handled, when nobody says.
///
/// One, which is where this started as a constant and where it stays as a
/// default: at one, every caller feels the actor's own pace immediately, and a
/// default that hides an actor's pace from the caller who is waiting on it is the
/// wrong one to inherit.
///
/// **What the number means, and every consequence of moving it, is on
/// [`RegistryBuilder::mailbox_depth`]** — one place, because this used to be two
/// constants with two different arguments and only one of them was about the
/// registry. There is no `unbounded`, here or there; that is stated at the
/// builder because that is where somebody would go looking for it.
const DEFAULT_MAILBOX_DEPTH: usize = 1;

/// Whether a turn is running right now: one bit, shared by an actor's loop and
/// its registry entry.
///
/// An `Arc<AtomicBool>` behind a name, so that the pair cannot be assembled from
/// two unrelated flags and so the invariant has one place to be written. The
/// invariant is: **`turns.rs`'s `run_loop` sets it on the way into a turn and
/// clears it on the way out, for both of an actor's inputs, and nothing else
/// writes it.** [`Registry::status`] reads it.
///
/// The loop and not [`Mailbox::recv`], which is where the design this implements
/// first put it, and the reason is a feed. A feed item is a turn and it never
/// touches the mailbox, so a flag maintained inside the receive reads *false*
/// through every feed turn — and after Wave 3 the receive is a synchronous poll
/// that the loop may skip entirely on any iteration where the feed was ready
/// first, which leaves it stale rather than merely wrong. The loop is the one
/// place that sees both inputs, and it is keylink's own code rather than
/// generated code, which is the other half of what the design asked for.
///
/// `Relaxed` on both ends, for [`Entry::last_seen`]'s reason: this orders nothing
/// but itself, and a status read is a diagnostic that may be one turn stale in
/// exchange for costing an actor's hot path nothing.
#[derive(Clone, Default)]
pub(crate) struct Busy(Arc<AtomicBool>);

impl Busy {
    /// Called by the loop, on the way into and out of every turn.
    pub(crate) fn set(&self, running: bool) {
        self.0.store(running, Ordering::Relaxed);
    }

    fn running(&self) -> bool {
        self.0.load(Ordering::Relaxed)
    }

    pub(crate) fn identity(&self) -> Actor {
        Actor::new(Arc::clone(&self.0))
    }
}

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
/// so a mailbox costs `32 * size_of::<M>()` whatever its capacity is — which is
/// why [`RegistryBuilder::mailbox_depth`] is free to move below 32 and measured to
/// be. At ten thousand idle actors, measured on this machine against a service
/// with one `u64` argument — the smallest case there is:
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
/// # The box here is half of the decision, and the other half is in `turns.rs`
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
///
/// # A struct, and it was a `pub type` alias
///
/// It carries two things now, and the second one is why it stopped being an
/// alias: the slots, and the [`Busy`] flag [`Registry::status`] reads. The flag has
/// to travel from the registry — which creates the channel — to the loop, which
/// is the only thing that knows when a turn starts. Widening [`mount`]'s closure
/// to take a second argument would have carried it just as well and put the
/// pairing in **generated** code, one expansion per `service!` in the world, so
/// that two sites could drift about which of them was authoritative. Through the
/// mailbox it costs the generated code nothing: `mount(|inbox: Mailbox| ..)` is
/// the same line it was.
///
/// # `recv` here does NOT touch that flag, and a hand-written loop is why
///
/// [`mount`] is `#[doc(hidden)] pub`, so a loop can be written by hand — the
/// tests in this file, in `tests/api.rs` and in `tests/actors.rs` do exactly that,
/// three of them to build an actor that will not drain. Such a loop never reports a
/// turn, so
/// [`ActorStatus::busy`] stays `false` for it. That is the honest answer rather
/// than a flag `recv` sets: `recv` cannot see when the turn it handed out ended,
/// so a flag set here would read `true` for an idle actor from its first message
/// until its last. Every actor a `service!` produces runs `turns.rs`'s one loop,
/// which is where the flag is maintained.
///
/// # It carries a second cell now, for the same reason and a different question
///
/// [`Fate`] rides here beside [`Busy`], by the identical argument: the registry
/// makes it, the loop is the only thing that knows what to write in it, and the
/// alternative — widening [`mount`]'s closure — puts the pairing in **generated**
/// code, one expansion per `service!` in the world. Through the mailbox it costs
/// generated code nothing: `mount(|inbox: Mailbox| ..)` is the same line it was,
/// and `run_keyed` takes the cell off the mailbox it was already handed.
///
/// The two are not folded into one allocation, and the reason is that they are
/// read by different halves of the crate. A
/// [`LocalActor`](crate::LocalActor) carries both — the fate for a served
/// dispatch, and its own busy cell for
/// [`is_busy`](crate::LocalActor::is_busy) — but it carries them **on the
/// handle**, because a local actor has no registry entry to hang either on.
/// Serving it does not give it one: `Handle::service` clones the same
/// `LocalActor` and adds a door, so what decides where these live is the actor's
/// kind and never whether anybody serves it. This
/// sentence used to say a `LocalActor` "deliberately has no `Busy`"; that was
/// true of the type in this file and stopped being true of the concept when the
/// local twin shipped, so it says which is which now.
#[doc(hidden)]
pub struct Mailbox<M> {
    slots: mpsc::Receiver<(Box<M>, Chain)>,
    busy: Busy,
    fate: Fate,
}

/// The floor under a **hand-written** loop, and nothing more than a floor.
///
/// [`mount`] is `#[doc(hidden)] pub`, so a loop can be written by hand, and such
/// a loop never calls the `Inbox::ending` hook where every loop keylink generates
/// settles its own fate. Without this, a hand-written actor that returned would
/// leave its cell reading `Running` for ever, and a peer whose reply died with it
/// would be told the actor is still there.
///
/// **What it does not claim.** A `Drop` runs its body before its fields, so this
/// store lands before the receiver closes and before whatever is queued behind it
/// is dropped — that much is exact. Where it sits relative to a *hand-written*
/// loop's own state is not knowable from here: that state is whatever the
/// closure captured, and the closure decides the order. The loops keylink
/// generates do not rely on this at all; they settle inside [`run_loop`], where
/// the ordering is stated and holds.
///
/// **Where it is load-bearing, corrected.** This said the one generated case was
/// a supervised keyed actor whose *factory* gives up — which is wrong twice:
/// `supervise` settles `Gone` explicitly on that path, so this is redundant
/// there, and the case it *does* answer is an **aborted unsupervised keyed**
/// actor, where `run_loop` owns the mailbox as a parameter and drops it before
/// the state. `defer::an_aborted_keyed_actor_tells_a_peer_the_actor_is_gone` and its
/// expired-budget twin both record that breaking this line is what reddens them,
/// while breaking the teardown door's own settle leaves them green.
///
/// [`run_loop`]: crate::run_keyed
impl<M> Drop for Mailbox<M> {
    fn drop(&mut self) {
        self.fate.settle(Standing::Gone);
    }
}

impl<M> Mailbox<M> {
    /// The next message, or `None` once the registry has let go of the sender.
    ///
    /// `#[doc(hidden)] pub`, like [`mount`] itself, and for the same reason: a
    /// hand-written loop outside this crate — every one of them a test — needs to
    /// take its messages somehow.
    #[doc(hidden)]
    pub async fn recv(&mut self) -> Option<Box<M>> {
        self.slots.recv().await.map(|(mail, _chain)| mail)
    }

    /// The same receive as a poll, which is what the one loop wants: it selects
    /// against a feed without building a future over either input. The chain is
    /// kept on this crate-visible path so keyed loops participate in the same
    /// re-entrancy check as local actors; hand-written loops intentionally keep
    /// the old `recv` surface and discard it.
    pub(crate) fn poll_recv(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<(Box<M>, Chain)>> {
        self.slots.poll_recv(cx)
    }

    pub(crate) fn identity(&self) -> Actor {
        self.busy.identity()
    }

    /// Report a turn starting or ending. See [`Busy`] for whose job this is.
    pub(crate) fn mark_busy(&self, running: bool) {
        self.busy.set(running);
    }

    /// The cell this actor's loop settles on its way out, and a served dispatch
    /// reads when a reply dies. See [`Fate`].
    pub(crate) fn fate(&self) -> Fate {
        self.fate.clone()
    }

    /// Refuse further messages while letting the buffer drain. See
    /// `turns.rs`'s `Inbox::close`, which owns the argument: only a supervisor
    /// calls this, on the path where there will be no next incarnation.
    pub(crate) fn close(&mut self) {
        self.slots.close();
    }
}

/// An actor ready to run: its loop and the sender that reaches it.
///
/// Produced by the generated `spawn` and handed to [`Registry::mount`] or
/// returned from a factory. Opaque on purpose: the registry owns the pairing of
/// a loop with the sender that reaches it, and nothing else may take them apart.
///
/// It used to carry a third thing, a schema hash, because one node-wide system
/// held actors of unrelated services and something had to tell them apart. A
/// system is per-service now, so the hash has nothing left to discriminate and
/// the comparison it fed is unrepresentable rather than refused.
///
/// `M` is the message the mailbox carries. See [`Registry`] for what a registry is
/// allowed to know about it, which is nothing.
///
/// # This holds a future, and it used to hold a `JoinHandle`
///
/// [`mount`] spawned the task and this struct carried the handle, which meant
/// **the actor was already running before anybody had accepted it**. Every path
/// that refused one therefore dropped a `JoinHandle` — and a dropped
/// `JoinHandle` *detaches*, it does not cancel — so both of [`Registry::mount`]'s
/// refusals left a live, unreachable task behind: [`MountError`] hands nothing
/// back, and the registry never had it. A durable actor refused for a taken id
/// went on holding its exclusive claim with no handle anywhere in the process
/// able to stop it.
///
/// So the loop is carried unstarted and [`Registry::mount`] is the one place it
/// starts, *after* the two refusals. A refused mount now drops a future that
/// has never been polled: no task, no state moved onto one, nothing to detach.
/// That is the same guarantee the lazy path already had — the cap is checked
/// before the factory is called — written once for both.
///
/// The cost is one `Box` per mount, on a path that already allocates a channel,
/// and it is transient: nothing the registry *keeps* is boxed by this, so the
/// measured per-idle-actor cost above is untouched.
///
/// # It holds the loop's *builder*, and it used to hold the mailbox too
///
/// [`mount`] made the channel, so the depth was fixed before any registry saw the
/// actor — which is exactly why the depth could only ever be a constant. A knob
/// on [`RegistryBuilder`] would have applied to nothing: the closure has already
/// taken its receiver by then, and a lazily spawned actor's channel is made
/// inside `Shared::route`, where no caller is present to name anything.
///
/// So this carries `FnOnce(Mailbox<M>) -> impl Future` and the **registry** makes
/// the channel, in `Shared::entry`, with its own depth. Every existing call
/// site is unchanged — `spawn(state)` still hands back one of these and
/// [`Registry::mount`] still takes it — and depth becomes a property of the system
/// an actor is mounted in rather than of the line that built it. That is the
/// right home for it twice over: it is the same decision for every actor a system
/// serves, and a system is the only thing a lazy factory's actors have in common.
///
/// The second `Box` is the cost, one per mount, transient like the first.
///
/// `#[must_use]` on the **type** as well as on the generated `spawn`, because an
/// attribute on one function covers one function. Dropping one of these drops an
/// actor's state, and it can be produced by [`mount`], by a factory, or by any
/// helper a service author writes over either — measured: a local
/// `fn helper() -> Mountable<_>` called as a statement warns with the attribute
/// here and is silent without it. Bind-then-shadow is *not* what this buys;
/// `unused_variables` already has that one.
#[must_use = "an unmounted actor is dropped, and the state it was built around \
              goes with it"]
pub struct Mountable<M> {
    backpressure: Option<crate::Backpressure>,
    /// Called once, by [`Shared::entry`], with the mailbox that registry made.
    /// Boxed twice because both the closure and the future it returns are the
    /// caller's own types, and this struct is named in public signatures where two
    /// more parameters would be noise.
    run: Loop<M>,
}

/// One actor's loop, before it has a mailbox to run against.
///
/// An alias because the type is written twice — here and in [`mount`] — and a
/// nine-line type spelled twice is a place for the two to disagree.
///
/// # It resolves to an [`Ended`], and that return type is the fourth way out
///
/// A registry entry left the map at three sites — [`Registry::stop`],
/// [`Registry::abort`] and [`Shared::sweep`] — and every one of them is
/// *somebody asking*. The fourth way out has no asker: a feed that ends, a
/// handler that panics, an observer that refuses, all end the loop where it
/// stands. The task finished, the receiver dropped, and the [`Entry`] survived
/// forever: [`Registry::status`] listed the actor with `busy: false` while a real
/// call to it answered
/// [`NoSuchActor`](crate::RemoteError::NoSuchActor), and no [`Lifecycle`] was
/// ever told. Under [`Eviction::Never`] that was permanent.
///
/// `Output = Ended` is the repair, and it is a *type* rather than a call because
/// the reason was the part that kept getting lost — `run_keyed` computed the
/// true [`Ended`] and dropped it on the floor. A loop that cannot return without
/// naming its ending cannot drop it, and [`Shared::entry`] is the one place
/// that ending is turned into a removal and a report.
type Loop<M> = Box<dyn FnOnce(Mailbox<M>) -> Pin<Box<dyn Future<Output = Ended> + Send>> + Send>;

/// Hand-written for the same reason as [`Registry`]'s, and it was derived by
/// mistake: `derive(Debug)` writes `impl<M: Debug> Debug`, which would make
/// printing a handle depend on a message type nobody prints. There is nothing
/// left inside to borrow a `Debug` from — a `dyn FnOnce` cannot be printed, and
/// the sender this used to show belongs to the registry now — which is what
/// `finish_non_exhaustive` says out loud rather than by inventing a placeholder.
impl<M> fmt::Debug for Mountable<M> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Mountable").finish_non_exhaustive()
    }
}

/// Hold one actor's loop until a registry accepts it and gives it a mailbox.
///
/// Called by the generated `spawn`, which supplies the loop.
///
/// **Nothing is spawned here, and nothing is allocated for a mailbox either.**
/// The loop runs from [`Registry::mount`], or from the insert a lazy spawn makes,
/// which is what makes a refused mount cost no task — see [`Mountable`] for the
/// defect that argument comes from, and for why the channel is the registry's to
/// make.
///
/// # The mailbox must be named
///
/// A function's type parameter cannot carry a default — only a type's can — and
/// there are no defaults left to inherit anyway, so `M` is inferred and the only
/// thing here to infer it from is the closure's own argument. An unannotated
/// closure is `error[E0282]: type annotations needed`, pointed at the parameter
/// — or at the binding, as `Mountable<_>`, if the handle is bound before it is
/// used:
///
/// ```compile_fail
/// keylink::mount(|mut inbox| async move {
///     while inbox.recv().await.is_some() {}
///     keylink::Ended::Stopped
/// });
/// ```
///
/// That hazard is unchanged by the per-service rewrite and it is worth being
/// exact about why, because the wave that removed the type defaults was expected
/// to remove this too. It did not: `mount` is generic in a *function* parameter,
/// a function generic cannot carry a default, and a generated alias applies `M`
/// in type position where this signature has none. The generated `spawn`
/// annotates the closure with its own `Mailbox` alias every time, which is why
/// the break reaches no caller.
///
/// # The loop must say why it stopped
///
/// `F::Output` is [`Ended`] and not `()`, so a hand-written loop cannot fall off
/// its own end without naming an ending. [`Loop`] carries the defect that
/// demands it. A loop that simply ran out of messages returns
/// [`Ended::Stopped`], which is the same value [`drain`] synthesises from a task
/// that returned.
#[doc(hidden)]
pub fn mount<M, F>(run: impl FnOnce(Mailbox<M>) -> F + Send + 'static) -> Mountable<M>
where
    F: Future<Output = Ended> + Send + 'static,
{
    Mountable {
        backpressure: None,
        run: Box::new(move |inbox| Box::pin(run(inbox))),
    }
}

/// Hold an actor loop with its own mailbox policy until a registry mounts it.
#[doc(hidden)]
pub fn mount_with_backpressure<M, F>(
    run: impl FnOnce(Mailbox<M>) -> F + Send + 'static,
    backpressure: crate::Backpressure,
) -> Mountable<M>
where
    F: Future<Output = Ended> + Send + 'static,
{
    Mountable {
        backpressure: Some(backpressure),
        run: Box::new(move |inbox| Box::pin(run(inbox))),
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
    /// Eviction is [`Registry::stop`], not [`Registry::abort`]: queued messages
    /// drain and the state is dropped after them, never mid-turn — **for as long
    /// as `shutdown_budget` allows.** Past it the actor is destroyed where it
    /// stands, exactly as `abort` would.
    ///
    /// That last clause is the whole of item 4b. This doc used to read "Eviction
    /// is `Registry::stop`, not `Registry::abort`" with nothing after it, and the
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
        /// The same budget [`Registry::stop`] takes, applied by the sweeper
        /// instead of by a caller — so read that method for what a budget buys
        /// and what expiry costs.
        ///
        /// **There is no relationship between this and `after`, and picking one
        /// from the other is a mistake.** `after` is a staleness threshold and is
        /// usually seconds or minutes; this is how long the longest turn plus the
        /// queue behind it needs, and is a property of the service's handlers.
        /// A budget shorter than a turn turns eviction into `abort` for every
        /// actor that happens to be busy when its clock runs out.
        ///
        /// # The one relationship it *does* have, and it is peer-visible
        ///
        /// This is also **the longest an evicted actor's exclusive resources stay
        /// held after its id has left the map.** The sweeper takes the entry out
        /// under the write guard and only then waits out this budget, so for up to
        /// one budget the id is free and the state behind it is not.
        ///
        /// With a lazy factory — [`RegistryBuilder::spawn_with`] — that window is
        /// something a peer can hit. The next message to that id routes, finds
        /// nothing, calls the factory, and the actor it builds goes for a resource
        /// its predecessor has not let go of yet: a
        /// `keylink_persist::Error::Busy` for a durable actor, or whatever else
        /// "exclusive" means to that state. The refusal is **bounded** by this
        /// number and it is not a bug in the eviction — it is the cost of draining
        /// rather than detaching, and the old sweeper hid it by holding those
        /// resources *forever* on a task nobody could reach.
        ///
        /// Two consequences worth stating. An id being free is not a promise that
        /// its state is: a service whose actors own exclusive resources should
        /// expect `Busy` from a re-spawn and answer it rather than treat it as
        /// impossible. And the other end of the dial is worse, because it is
        /// **silent**: a budget below the longest turn destroys that turn where it
        /// stands, and nothing anywhere says so — the `Ended` the sweeper computes
        /// is nobody's answer, an aborted task's observers never run, and the
        /// caller was a peer whose stream is severed rather than answered. That
        /// gap is known and Wave 6 owns it, where `Registry::snapshot` and the
        /// console are. Until then this number is the only control over it.
        shutdown_budget: Duration,
    },
    /// Keep every actor until it is stopped by hand.
    ///
    /// Correct when actors are mounted from a fixed configuration. A leak when
    /// they are spawned lazily from the network, which is why
    /// [`RegistryBuilder::capacity`] still applies.
    Never,
}

/// Builds an actor's state on first message. See [`RegistryBuilder::spawn_with`],
/// which takes the closure directly — this alias is how the system stores it,
/// not something a caller ever names.
type Factory<M> = Arc<dyn Fn(&ActorId) -> Option<Mountable<M>> + Send + Sync>;

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
    /// This sentence *is* rendered — `remote.rs` widens it onto
    /// `RemoteError::AtCapacity`, which carries the same cap and writes the same
    /// words — and `remote.rs`'s
    /// `the_capacity_refusal_reads_the_same_by_hand_and_lazily` pins them.
    #[error("the actor system is at its capacity of {capacity} live actors")]
    AtCapacity {
        /// The cap that was reached.
        capacity: usize,
    },
}

/// What a local keyed call can learn without crossing the wire.
///
/// Routing refusals stay separate from a turn that ran: there is no remote
/// transport error to report, and folding `NoSuchActor` or `AtCapacity` into a
/// `Withheld` would claim that a handler had already started. Once a message is
/// queued, the existing [`Unanswered`] vocabulary carries the turn's outcome.
/// Keyed local delivery parks on the bounded mailbox until a slot opens; this
/// surface has no queue-timeout parameter and therefore reports no `Full` case.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum LocalError {
    /// No actor is mounted under this id and no lazy factory built one.
    #[error("nothing is mounted as {id} and no factory built one")]
    NoSuchActor {
        /// The id that was addressed.
        id: ActorId,
    },
    /// The registry cannot admit another live actor.
    #[error("the actor system is at its capacity of {capacity} live actors")]
    AtCapacity {
        /// The configured live-actor cap.
        capacity: usize,
    },
    /// The opted-in shedding mailbox was full, so the message was not queued.
    #[error("the actor mailbox is full")]
    MailboxFull,
    /// The actor's mailbox closed before this message could be queued.
    #[error("the actor is no longer running")]
    Closed,
    /// The call would wait on the actor's own turn.
    #[error("this call was made from inside the target actor's own turn: it would deadlock")]
    Reentrant,
    /// The message ran, but its reply was not handed back.
    #[error("{0}")]
    Turn(Withheld),
}

impl From<RouteError> for LocalError {
    fn from(error: RouteError) -> Self {
        match error {
            RouteError::NoSuchActor { id } => Self::NoSuchActor { id },
            RouteError::AtCapacity { capacity } => Self::AtCapacity { capacity },
        }
    }
}

fn local_error(error: Unanswered) -> LocalError {
    match error {
        Unanswered::Turn(withheld) => LocalError::Turn(withheld),
        Unanswered::Reentrant => LocalError::Reentrant,
        // Keyed local delivery uses `send`, so a full mailbox parks the caller
        // until a slot opens. `asked` runs after that send and cannot produce the
        // local handle's `Full` refusal.
        Unanswered::Full => unreachable!("keyed local delivery cannot shed a queued message"),
    }
}

struct Entry<M> {
    mail: mpsc::Sender<(Box<M>, Chain)>,
    shed: bool,
    task: JoinHandle<()>,
    /// Identity of this mounted incarnation, distinct from its reusable id.
    /// The task carries a clone so a late natural ending cannot reap a newer
    /// actor remounted under the same id.
    identity: Arc<()>,
    /// Milliseconds since the registry's epoch, as of the last message routed
    /// here. An atomic so that touching an actor stays on the read lock.
    last_seen: AtomicU64,
    /// Whether this actor is inside a turn. Written by its loop, read by
    /// [`Registry::status`] — see [`Busy`], which owns the invariant.
    busy: Busy,
    /// What a peer whose reply died is told. Written by this actor's loop, read
    /// by `deliver_keyed` — see [`Fate`], which owns that invariant, and
    /// [`Route`], which is how it leaves the registry.
    fate: Fate,
    /// The ending reported by the actor's loop, shared with local waiters.
    ended: watch::Sender<Option<Ended>>,
}

impl<M> Entry<M> {
    /// The two facts this entry can state about a turn, folded once.
    ///
    /// One body rather than two because [`ActorStatus`] and [`Serving`] both
    /// carry this pair, and two computations of one pair is exactly the drift
    /// [`Serving`] exists to make impossible: [`Registry::status`]'s row for an
    /// actor and [`Registry::serving`]'s answer about the same actor read the same
    /// lines, so they cannot disagree.
    ///
    /// Takes no lock and touches no clock, so a caller already holding the read
    /// guard can call it per entry, and asking does **not** reset
    /// [`ActorStatus::idle_for`] — a diagnostic that kept an actor alive against
    /// [`Eviction::Idle`] would be a policy, not a reading.
    fn serving(&self) -> Serving {
        Serving {
            busy: self.busy.running(),
            // `saturating_sub` cannot fire: permits outstanding are bounded
            // by the buffer they came from. It is written this way because a
            // diagnostic must not be the thing that panics, and because the
            // alternative is an `expect` on a path with nothing to say.
            // `try_from` and not `as`, the same call `now()` makes: the
            // widening cannot fail on any target keylink builds for, and a
            // diagnostic must not be the thing that panics if one ever
            // appears.
            buffered: u64::try_from(
                self.mail
                    .max_capacity()
                    .saturating_sub(self.mail.capacity()),
            )
            .unwrap_or(u64::MAX),
        }
    }
}

/// Where a message goes, and what its caller is told if the answer never comes.
///
/// [`Shared::route`]'s answer, and a struct rather than the bare sender it used
/// to be. The pair travels together because they are read together: a dispatch
/// sends on the first, and reads the second in the one case where the send
/// succeeded and no answer followed. Looking the fate up again at that point
/// would take a second read lock and could find a *different* actor — an
/// eviction and a lazy re-spawn under the same id — so the answer would be about
/// somebody else's incarnation.
pub(super) struct Route<M> {
    /// One end of the actor's mailbox, cloned out of the entry.
    pub(super) mail: mpsc::Sender<(Box<M>, Chain)>,
    /// Whether a full mailbox refuses immediately.
    pub(super) shed: bool,
    /// The actor identity used by local re-entrancy checks.
    pub(super) actor: Actor,
    /// The cell that actor's own loop settles before it drops anything.
    pub(super) fate: Fate,
}

/// Hand-written for [`Registry`]'s reason: the derive would write `impl<M: Debug>`,
/// and `M` is a mailbox message that owns a reply channel and a stream. It exists
/// at all because `Result::expect_err` wants the `Ok` side printable, which is
/// what this file's own routing tests do with one.
impl<M> fmt::Debug for Route<M> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Route")
            .field("fate", &self.fate)
            .finish_non_exhaustive()
    }
}

struct Shared<M> {
    entries: RwLock<HashMap<ActorId, Entry<M>>>,
    factory: Option<Factory<M>>,
    /// Where mounts and endings are reported, in the order they were installed.
    /// Empty is the default and costs one length check per mount and per
    /// teardown — never per message, because routing to a live actor is not a
    /// lifecycle event. `Vec::new` does not allocate, so a system nobody watches
    /// carries a pointer and two words and asks the allocator for nothing. See
    /// [`Lifecycle`].
    lifecycle: Vec<Arc<dyn Lifecycle>>,
    capacity: usize,
    /// How many messages may wait ahead of the one being handled, for every
    /// actor in this system. [`RegistryBuilder::mailbox_depth`] is where it means
    /// something.
    mailbox_depth: usize,
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
    /// `RegistryBuilder::build`, so before this change every actor system panicked
    /// in a tab. RFC-03 never caught it because this module postdates the RFC.
    epoch: Instant,
}

impl<M> Shared<M> {
    fn now(&self) -> u64 {
        u64::try_from(self.epoch.elapsed().as_millis()).unwrap_or(u64::MAX)
    }

    /// The two report sites, so that "is anybody listening" is asked in one
    /// place rather than at each of the five call sites.
    ///
    /// One report goes to every installed [`Lifecycle`], in install order, on the
    /// reporting task. They run one after another because both trait methods are
    /// synchronous and neither may block — see [`Lifecycle`] — so the whole fan is
    /// as long as the slowest member and the trait already forbids that being
    /// long. Nothing is spawned per report: a task per ending would make the
    /// order a race and would let a diagnostic outlive the thing it describes.
    ///
    /// **Each call is caught, so one bad sink costs one report.** That is
    /// `keyed.rs`'s "one bad report should cost one report" arriving at the
    /// fan-out that feeds it, and it became necessary when this field became a
    /// list: with one sink a panic deprived that sink and unwound into whoever
    /// was reporting, and with several it deprived every sink installed after it
    /// as well. [`Lifecycle`] carries the whole argument, including why the
    /// unwind was a failure policy too. Nothing is reported about the catch:
    /// there is no channel here to report it on, and the panic hook has already
    /// printed at the fault.
    fn report_mounted(&self, id: &ActorId, identity: &Arc<()>) {
        let incarnation = Arc::as_ptr(identity) as usize;
        for watching in &self.lifecycle {
            let _ = crate::panic::catching(|| watching.__mounted(id, incarnation));
        }
    }

    fn report_ended(&self, id: &ActorId, identity: &Arc<()>, why: &Ended) {
        let incarnation = Arc::as_ptr(identity) as usize;
        for watching in &self.lifecycle {
            let _ = crate::panic::catching(|| watching.__ended(id, incarnation, why));
        }
    }

    /// An actor ended itself. Take it out of the map, and report why.
    ///
    /// **The fourth way out**, and the only one with nobody to answer to: a feed
    /// that ended, a handler that panicked, an observer that refused, a
    /// supervisor that gave up. [`Loop`] carries what leaving that entry behind
    /// cost. Called from the actor's own task, by the wrapper
    /// [`Shared::entry`] spawns, with the [`Ended`] the loop returned — which
    /// is the *true* reason rather than [`drain`]'s synthesis from a join.
    ///
    /// # It reports only if the entry was still ours, and that is the whole interlock
    ///
    /// [`Registry::stop`], [`Registry::abort`] and [`Shared::sweep`] all take
    /// the entry out **before** they await the task, and each reports its own
    /// ending afterwards. The id may be remounted during that await, so absence
    /// is not the interlock: the task's opaque identity must still match the
    /// current entry. A late old task therefore neither removes nor reports
    /// through its replacement. When nobody asked for teardown, this is the only
    /// site that can report at all.
    ///
    /// The removed [`Entry`] holds the `JoinHandle` of the task running this
    /// line. Dropping it detaches a task that is finishing anyway, which is the
    /// one case where a detach costs nothing.
    fn reap(&self, id: &ActorId, identity: &Arc<()>, why: &Ended) {
        // A teardown removes its entry before awaiting the task, so the id may
        // already belong to a newer incarnation. Only that task's identity may
        // reap the slot; a late old task must not remove or report through the
        // replacement.
        let ours = {
            let mut entries = self.write();
            let ours = entries
                .get(id)
                .is_some_and(|current| Arc::ptr_eq(&current.identity, identity));
            if ours {
                entries.remove(id);
            }
            ours
        };
        // The guard dies with the statement, so `report_ended` runs without it:
        // a `Lifecycle` may ask this same system what is running, exactly as at
        // `Registry::mount`'s report site.
        if ours {
            self.report_ended(id, identity, why);
        }
    }

    /// The id to announce a mount under, or `None` when nobody is listening.
    ///
    /// The clone is the whole reason this is a method: a mount reports **after**
    /// the write guard is released, so the key has to be copied out from under
    /// it — and a registry with no [`Lifecycle`] must not pay for a `String`
    /// clone on its mount path to produce a value nothing reads.
    ///
    /// **The list did not change that, and this line is where it would have.**
    /// The predicate used to be "the `Option` is `Some`" and is now "the list is
    /// not empty" — one non-empty check either way, and **one** clone for any
    /// number of listeners rather than one per listener, because
    /// [`report_mounted`](Self::report_mounted) hands the same `&ActorId` to each.
    /// Zero listeners still allocate nothing on the mount path.
    fn announcing(&self, id: &ActorId, identity: &Arc<()>) -> Option<(ActorId, Arc<()>)> {
        (!self.lifecycle.is_empty()).then(|| (id.clone(), identity.clone()))
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
    ///
    /// `self: &Arc<Self>` because a lazy spawn reaches [`Shared::entry`], and
    /// the task that starts there holds a [`Weak`] back to this
    /// registry so it can reap its own entry. There is no other reason.
    fn route(self: &Arc<Self>, id: &ActorId) -> std::result::Result<Route<M>, RouteError>
    where
        M: Send + 'static,
    {
        if let Some(found) = self.touch(&self.read(), id) {
            return Ok(found);
        }
        let Some(factory) = self.factory.as_ref() else {
            return Err(RouteError::NoSuchActor { id: id.clone() });
        };

        let mut entries = self.write();
        // Read before the entry is taken, because a `VacantEntry` borrows the map
        // and `len()` cannot be asked while one is held. The count is the one
        // before this insert either way, which is what the cap is about.
        let live = entries.len();
        // The mount is announced after the guard below is released, so the arm
        // that performs one hands the id out here rather than reporting in place.
        // A lazy spawn is the mount NO caller sees: there is no `mount()` call to
        // report from, which is half of why `Lifecycle` exists at all.
        let (found, announce) = match entries.entry(id.clone()) {
            hash_map::Entry::Occupied(taken) => {
                // Another message to the same new id won the race between the two
                // locks. One id is one actor, so the loser adopts the winner's.
                // `touch` inlined, because it wants a `&HashMap` this arm has
                // handed to the entry. Not a mount: the winner announced it.
                let entry = taken.into_mut();
                entry.last_seen.store(self.now(), Ordering::Relaxed);
                (
                    Route {
                        mail: entry.mail.clone(),
                        shed: entry.shed,
                        actor: entry.busy.identity(),
                        fate: entry.fate.clone(),
                    },
                    None,
                )
            }
            hash_map::Entry::Vacant(free) => {
                if live >= self.capacity {
                    return Err(RouteError::AtCapacity {
                        capacity: self.capacity,
                    });
                }
                let Some(mounted) = factory(id) else {
                    return Err(RouteError::NoSuchActor { id: id.clone() });
                };
                // The sender comes out of the entry rather than out of the
                // `Mountable`, because the mailbox is made here now — see
                // [`Mountable`] for why the depth could not otherwise be a knob.
                let entry = self.entry(free.key(), mounted);
                let found = Route {
                    mail: entry.mail.clone(),
                    shed: entry.shed,
                    actor: entry.busy.identity(),
                    fate: entry.fate.clone(),
                };
                let announce = self.announcing(free.key(), &entry.identity);
                free.insert(entry);
                (found, announce)
            }
        };
        drop(entries);
        if let Some((id, identity)) = announce {
            self.report_mounted(&id, &identity);
        }
        Ok(found)
    }

    /// One row per live actor. [`Registry::status`] is the door and carries what
    /// every field does and does not claim; this is the body, here because
    /// [`Registry::weak_status`] needs it reachable from a
    /// [`Weak`] that has no `Registry` around it.
    fn status(&self) -> Vec<ActorStatus> {
        let now = self.now();
        self.read()
            .iter()
            .map(|(id, entry)| {
                // The two turn facts come from `Entry::serving`, so this row and
                // the `Serving` a peer reads for the same actor cannot disagree.
                let Serving { busy, buffered } = entry.serving();
                ActorStatus {
                    id: id.clone(),
                    idle_for: Duration::from_millis(
                        now.saturating_sub(entry.last_seen.load(Ordering::Relaxed)),
                    ),
                    busy,
                    buffered,
                }
            })
            .collect()
    }

    fn touch(&self, entries: &HashMap<ActorId, Entry<M>>, id: &ActorId) -> Option<Route<M>> {
        let entry = entries.get(id)?;
        entry.last_seen.store(self.now(), Ordering::Relaxed);
        Some(Route {
            mail: entry.mail.clone(),
            shed: entry.shed,
            actor: entry.busy.identity(),
            fate: entry.fate.clone(),
        })
    }

    /// Make the mailbox, start the loop, and pair the task with the sender that
    /// reaches it.
    ///
    /// **The only place an actor's task is spawned**, which is what lets
    /// [`Mountable`] be refused for free — and **the only place a mailbox is
    /// created**, which is what lets [`RegistryBuilder::mailbox_depth`] reach a
    /// lazily spawned actor as well as a hand-mounted one.
    ///
    /// # Every caller inserts through a `VacantEntry`, and that is structural
    ///
    /// What this returns owns a `JoinHandle`, so displacing a live entry with it
    /// would detach the actor that was there — the defect this wave exists to
    /// remove, in the one shape a type cannot catch: `HashMap::insert` hands the
    /// displaced value back as an `Option` that a caller may discard, and
    /// [`Registry::mount`] and [`Shared::route`] both did discard it. Both were
    /// safe, each by a presence check a few lines up under the same write guard —
    /// an argument that has to be re-made every time either function is edited.
    ///
    /// They go through `HashMap::entry` instead. A `VacantEntry::insert` returns
    /// no predecessor because it has none, so the safe path is the only one that
    /// type-checks and the argument does not have to be re-made. It also drops
    /// the second lookup each of them was doing.
    ///
    /// # The task is wrapped, and the wrapper is what closes the fourth way out
    ///
    /// The loop resolves to an [`Ended`] — see [`Loop`] — and the wrapper hands
    /// it to [`Shared::reap`], which is the only site that can unmount an
    /// actor **nobody asked to unmount**. A [`Weak`] and not an
    /// `Arc`, and not for tidiness: the registry owns the [`Entry`], the entry
    /// owns the `JoinHandle`, and a strong reference here would close that ring.
    /// A program that dropped every [`Registry`] clone would keep the registry —
    /// and therefore every actor's own sender — alive off its actors' tasks, so
    /// `Taken::Mail(None)` would become unreachable and nothing would ever end.
    ///
    /// A `None` from `upgrade` is the honest case rather than an error: the last
    /// [`Registry`] is gone, so there is no map to take the entry out of and
    /// nobody left holding a [`Lifecycle`] to tell.
    ///
    /// # Panics
    ///
    /// Panics when called outside a tokio runtime, like every other spawn.
    fn entry(self: &Arc<Self>, id: &ActorId, mounted: Mountable<M>) -> Entry<M>
    where
        M: Send + 'static,
    {
        let (mailbox_depth, shed) = match mounted.backpressure {
            Some(crate::Backpressure::Park(depth)) => (depth.get(), false),
            Some(crate::Backpressure::Shed(depth)) => (depth.get(), true),
            None => (self.mailbox_depth, false),
        };
        let (mail, slots) = mpsc::channel(mailbox_depth);
        let busy = Busy::default();
        let fate = Fate::default();
        let (ended, _) = watch::channel(None);
        let ended_for_task = ended.clone();
        let identity = Arc::new(());
        let identity_for_task = identity.clone();
        let inbox = Mailbox {
            slots,
            busy: busy.clone(),
            fate: fate.clone(),
        };
        // Built here rather than inside the task, so the loop is still a future
        // that has never been polled at the moment the spawn takes it — which is
        // what [`Mountable`] is for and what a refused mount rests on.
        let running = (mounted.run)(inbox);
        let registry = Arc::downgrade(self);
        // One `ActorId` clone per live actor, held for its life. The map already
        // holds one, and this is the copy the reaping task addresses it by —
        // unconditional, unlike `announcing`'s, because removing the entry is
        // not something only a `Lifecycle` wants.
        let id = id.clone();
        Entry {
            mail,
            shed,
            task: n0_future::task::spawn(async move {
                let why = running.await;
                ended_for_task.send_if_modified(|slot| {
                    if slot.is_none() {
                        *slot = Some(why.clone());
                        true
                    } else {
                        false
                    }
                });
                if let Some(registry) = registry.upgrade() {
                    registry.reap(&id, &identity_for_task, &why);
                }
            }),
            identity,
            last_seen: AtomicU64::new(self.now()),
            busy,
            fate,
            ended,
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
    /// awaited. This is [`Shared::route`]'s discipline read backwards: `route`
    /// holds the guard across the user's `factory` call and is documented as such;
    /// nothing here may hold it across an await.
    ///
    /// **On native that discipline is compiler-enforced and not merely argued.**
    /// This future is spawned by [`spawn_sweeper`] through
    /// `n0_future::task::spawn`, which is `tokio::spawn` and requires `Send`;
    /// `std::sync::RwLockWriteGuard` is `!Send`; so a guard alive across the
    /// `join_all` below makes this future `!Send` and the spawn stops compiling.
    /// The paragraph above says why the shape is right, and the spawn is what
    /// keeps it that way.
    ///
    /// # Concurrently, and that is not an optimisation
    ///
    /// A hundred expired actors that all refuse to drain must cost one budget, not
    /// a hundred. Serially, a full sweep would take `entries * budget` and the
    /// next tick would not start until it finished, so a single wedged actor would
    /// push every other actor's eviction past its own deadline — a queue that
    /// grows under exactly the condition it exists to clear.
    ///
    /// # A gap that Wave 6 closed, and the residual it left
    ///
    /// This paragraph read *"the [`Ended`] each teardown computes is dropped
    /// here, deliberately"*, and named Wave 6 as the wave that would own it. It
    /// does not any more: the `join_all` below hands each teardown's `Ended` to
    /// the system's `Lifecycle`, and
    /// `an_eviction_that_destroys_a_busy_actor_is_no_longer_silent` is the guard.
    /// A system with no [`Lifecycle`] computes the same `Ended` and drops it
    /// exactly as before, which is the only case the old sentence still describes.
    ///
    /// **The residual is the observers, and it is real.** An aborted task's
    /// `finish` never runs, so a `shutdown_budget` set below the longest turn
    /// destroys that turn with no observer told and no caller answered — the
    /// caller of an evicted actor is a peer whose stream is severed rather than
    /// answered. What is no longer true is *no signal anywhere*: the ending
    /// reaches a `Lifecycle`, and through [`Events`](crate::Events) a console.
    /// The mitigation for the rest is the budget itself: pick it from the longest
    /// turn plus the queue behind it, as [`Eviction::Idle::shutdown_budget`] says.
    async fn sweep(self: &Arc<Self>) {
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
        let expired: Vec<(ActorId, Entry<M>)> = self
            .write()
            .extract_if(|_, entry| {
                now.saturating_sub(entry.last_seen.load(Ordering::Relaxed)) >= after
            })
            .collect();
        // The id travels with the entry now, and it is the whole of the fix: an
        // eviction's `Ended` used to be computed here and dropped, because there
        // was nowhere to put it and nobody to hand it to. There is one place now,
        // and it is optional — a system with no `Lifecycle` computes the same
        // `Ended` and drops it exactly as before.
        n0_future::join_all(expired.into_iter().map(|(id, entry)| {
            let registry = Arc::clone(self);
            async move {
                let _ = drain(registry, id, entry, shutdown_budget).await;
            }
        }))
        .await;
    }
}

/// Close the mailbox, wait out `budget`, and destroy what is still running.
///
/// The one teardown in this file: [`Registry::stop`] and [`Shared::sweep`]
/// both go through here, so an eviction and a shutdown cannot mean different
/// things. [`Registry::abort`] does not, because it never drains — that is a
/// different operation rather than this one with a budget of zero.
///
/// # The handle must outlive the timeout, and must not outlive this future
///
/// There are two ways to let go of a `JoinHandle` here, and a dropped one
/// **detaches** its task on both targets rather than cancelling it. Verified
/// against `n0-future 0.3.2`: the native branch re-exports tokio's, whose
/// documented behaviour is to detach, and the wasm branch has no `impl Drop for
/// JoinHandle` at all — abort-on-drop lives in a separate `AbortOnDropHandle`.
/// Either way of letting go leaves the actor running, still owning `&mut A` and
/// whatever `A` holds, with the entry already out of the map and no handle
/// anywhere able to reach it.
///
/// The first way is **expiry**. `n0_future::time::timeout(budget, task)`
/// **consumes** the handle, so when the budget runs out the timeout's own future
/// drops it. `pin!` puts the handle on this future's stack and hands the timeout
/// a `&mut` to it instead, so expiry leaves the handle right here to be aborted.
/// Stack pinning rather than `Box::pin` because a teardown should not allocate,
/// and rather than a bare `&mut task` because that rests on the wrapper being
/// `Unpin`, which nothing promises: natively it is `tokio_util`'s plain struct
/// around a `JoinHandle`, and on wasm it is n0-future's own
/// `#[pin_project(PinnedDrop)]` shim, whose `Unpin` is derived from its pinned
/// field. Both are `Unpin` today — measured on both targets, not assumed — so
/// `pin!` is not what makes this compile. It is what stops a change to either of
/// those two types from turning this line into a detach again, and it costs
/// nothing.
///
/// The second way is **cancellation**, and it is the same defect one level up.
/// This future is awaited by [`Registry::stop`] and by [`Shared::sweep`],
/// and a caller may drop either: `tokio::select!`, a `race`, a `timeout` around
/// the whole call, a spawned sweep whose handle goes away. Dropping the
/// *outer* future drops this frame, and this frame is where the handle lives —
/// so with a bare `JoinHandle` a cancelled `shutdown` reproduced the deleted
/// `stop` exactly: entry gone from the map, task detached, and
/// [`Registry::abort`] answering `false` for an actor that is still running.
///
/// So the handle is an [`AbortOnDropHandle`], and letting go of it — by expiry,
/// by cancellation, or by a panic unwinding this frame — aborts instead of
/// detaching. A dropped call loses the *wait*; it can no longer lose the
/// *actor*.
///
/// # What it returns, and the one thing it cannot know
///
/// - the task returned → [`Ended::Stopped`];
/// - the task's join failed → [`Ended::Vanished`], which is what an aborted or
///   panicked task is;
/// - the budget expired → the task is aborted, joined, and reported
///   [`Ended::Vanished`]. A natural ending already published before cancellation
///   remains authoritative for direct observation.
///
/// The `Ended` is **synthesised from the join**, not read from the actor, and
/// [`Registry::stop`] carries the argument for that and states its residual.
struct TeardownGuard<M> {
    registry: Arc<Shared<M>>,
    id: ActorId,
    ended: watch::Sender<Option<Ended>>,
    identity: Arc<()>,
    armed: bool,
}

impl<M> TeardownGuard<M> {
    fn complete(&mut self, why: &Ended) {
        self.ended.send_if_modified(|slot| {
            if slot.is_none() {
                *slot = Some(why.clone());
                true
            } else {
                false
            }
        });
        self.registry.report_ended(&self.id, &self.identity, why);
        self.armed = false;
    }
}

impl<M> Drop for TeardownGuard<M> {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        // Cancellation has already caused `AbortOnDropHandle` to abort the
        // actor. Preserve a natural ending that won the race; otherwise make
        // the lost teardown explicit to both direct waiters and Lifecycle.
        let published = { self.ended.borrow().clone() };
        let why = published.clone().unwrap_or(Ended::Vanished);
        if published.is_none() {
            self.ended.send_replace(Some(why.clone()));
        }
        self.registry.report_ended(&self.id, &self.identity, &why);
    }
}

async fn drain<M>(
    registry: Arc<Shared<M>>,
    id: ActorId,
    entry: Entry<M>,
    budget: Duration,
) -> Ended {
    let Entry {
        mail,
        task,
        fate,
        ended,
        identity,
        ..
    } = entry;
    let mut report = TeardownGuard {
        registry,
        id,
        ended,
        identity,
        armed: true,
    };
    // Dropping the registry's sender is what ends the loop. The task lives while
    // any other sender does, which is what lets a message already in flight
    // finish rather than be cancelled.
    //
    // Which makes this a constraint on every dispatch, and one was breaking it: a
    // caller that keeps its [`Route`] alive across the wait for the *answer* holds
    // this open for as long as it waits, and a parked `#[defer]` reply never
    // answers. `deliver_keyed` did exactly that — for no reason of its own,
    // only because the `map_err` at the bottom read the other field — so a keyed
    // actor holding one could not be stopped gracefully at all: every budget was
    // spent in full and the actor was aborted. It clones the [`Fate`] out and
    // drops the sender the moment the send lands now, and
    // `defer::a_keyed_actor_holding_a_parked_reply_drains_inside_its_budget` is
    // what says so.
    drop(mail);
    let mut task = std::pin::pin!(AbortOnDropHandle::new(task));
    let why = match n0_future::time::timeout(budget, &mut task).await {
        Ok(Ok(())) => Ended::Stopped,
        Ok(Err(_join)) => Ended::Vanished,
        Err(_expired) => {
            // Settled before the abort: an aborted loop never reaches
            // `Inbox::ending`, so nothing inside it decides what this destroyed.
            // The runtime drops the actor's future only after it has seen the
            // abort, so every reply killed by this teardown dies with `Gone`
            // already published.
            //
            // **What this still covers, narrowly**: a **hand-written** `mount`
            // loop, which has no `run_loop` and therefore no floor. For every
            // loop keylink generates, `run_loop`'s own floor is strictly earlier
            // in the same drop and this is a belt.
            // `an_expired_shutdown_budget_settles_the_served_answer_before_the_abort`
            // is the guard, and it drives a hand-written loop for exactly that
            // reason. The cross-reference here used to point at
            // `LocalActor::abort`, whose settle was deleted in the same wave that
            // added the floor — precisely because no break of it could be made
            // red.
            fate.settle(Standing::Gone);
            // Before the second await, never after: an `abort` that has been
            // requested lands whether or not anybody is still waiting for it,
            // which is the asymmetry that made `abort` correct and `stop` not.
            task.abort();
            let _ = task.await;
            Ended::Vanished
        }
    };
    // A generated loop publishes its own ending before the registry reaps it.
    // A shutdown budget that aborts a task has no such turn, so publish the
    // synthesized result only when the loop did not already report one. The
    // guard also reports here, making cancellation of the outer future safe.
    report.complete(&why);
    why
}

// ─────────────────────────────────────────────────────────── what is running

/// What one mounted actor is doing, as of the moment [`Registry::status`] looked.
///
/// Stale the instant it is returned — an actor takes its next message on another
/// task — so it answers "what was going on" and never "what is guaranteed now".
///
/// `#[non_exhaustive]`: the registry builds these and a caller reads them, so a
/// field arriving later must not be a breaking change. What is deliberately
/// **not** here is as much of the design as what is:
///
/// - **no `incarnation`.** Nothing in the crate counts one — Wave 2 looked and
///   added no counter — and inventing one to fill a field is how an observational
///   number becomes a fact people route on. It belongs to the wave that makes
///   re-incarnation a thing that happens.
/// - **no `queued`.** The total backlog is not observable, and the field that
///   claimed it is refused by name. [`Self::buffered`] is the part that can be
///   measured, said as a floor.
///
/// # It crosses the wire, and that was a decision about one ALPN
///
/// The three wire derives are here so that a console on another machine reads
/// the same four fields a local caller does, rather than a parallel struct that
/// would have to be kept in step by hand. The service that carries it —
/// [`inspect`](crate::inspect) — is **new**, so its ALPN is derived for the
/// first time from this shape and there is no hash to move. Nothing that
/// existed before this wave carries an `ActorStatus`: the keyed and singleton
/// ALPNs hash a service's own request and response types, and
/// `keylink/tests/protocol.rs` pins them.
///
/// The cost is that this struct is now part of a wire contract. A fifth field
/// is still additive in Rust — `#[non_exhaustive]` says so — and is **not**
/// additive on the wire: it moves `keylink/Inspect`'s hash, and an old console
/// is refused at the handshake rather than misparsing. That is the same trade
/// [`derive_alpn`](crate::derive_alpn) makes everywhere, and it is the reason
/// the two refused fields above are refused rather than reserved.
#[derive(
    Debug, Clone, serde::Serialize, serde::Deserialize, postcard_schema::Schema, PartialEq, Eq,
)]
#[non_exhaustive]
pub struct ActorStatus {
    /// The id this actor is mounted under.
    pub id: ActorId,
    /// How long since a message was last routed to this actor.
    ///
    /// The same clock and the same reading [`Eviction::Idle::after`] is compared
    /// against — `n0_future::time`, so `tokio::time::pause` drives it natively and
    /// a browser reads `performance.now()`. Never `std::time::Instant`, which
    /// panics on `wasm32-unknown-unknown`.
    ///
    /// It is *last routed*, not *last handled*: a message sitting in the mailbox
    /// has already reset this. That is what eviction means by idle, and a second
    /// definition here would be a second answer to one question.
    pub idle_for: Duration,
    /// Whether a turn is running right now.
    ///
    /// True from the moment the loop takes a message or a feed item until that
    /// turn's handler returns, including across every `await` inside it. A turn
    /// that is parked on I/O is a turn that is running.
    ///
    /// `false` for an actor whose loop was written by hand around
    /// [`mount`] rather than by `service!` — see
    /// [`Mailbox`]. Every actor a service declares runs the
    /// generated loop.
    pub busy: bool,
    /// Messages sitting in the mailbox.
    ///
    /// **A floor on the real backlog, not the backlog.** This is exactly
    /// `max_capacity() - capacity()`, which is exact for what the channel is
    /// *holding* — and callers parked waiting for a permit are not observable at
    /// all, because `tokio`'s `Semaphore` does not expose its waiters. Four
    /// hundred senders can be queued behind a `buffered` of 3.
    ///
    /// So: 0 or 1 at the default depth of one, at most
    /// [`RegistryBuilder::mailbox_depth`] at any other, and never a number to
    /// compute a queue-length alert from. What it is good for is the question it
    /// answers exactly — is this actor's mailbox full, and therefore is anybody
    /// who sends to it going to block.
    ///
    /// # `u64` and not `usize`, and the reason is the browser
    ///
    /// The count comes from a `usize` and is widened here, because **`usize` has
    /// no wire schema and that is deliberate**: `postcard_schema` implements
    /// `Schema` for every fixed-width integer and for neither pointer-sized one,
    /// since a type whose width is the *compiler's* choice cannot be half of a
    /// contract between two machines. keylink builds for `wasm32-unknown-unknown`
    /// and for 64-bit natives, so a console in a tab reading a server's
    /// `ActorStatus` is exactly the pair that would disagree.
    ///
    /// The widening is lossless on every target keylink supports — `usize` is at
    /// most 64 bits on all of them — and the field is bounded by
    /// [`RegistryBuilder::mailbox_depth`] anyway, which is a small number.
    pub buffered: u64,
}

/// Where a [`Registry`] reports an actor arriving and an actor leaving.
///
/// Two methods, because a registry knows exactly two things about an actor's
/// life that nothing else in the process can see. What happens *in between* is a
/// turn, and a turn is [`Observer`](crate::Observer)'s — this trait deliberately
/// has no third method for one, so there is one place to learn that a handler
/// ran and it is not here.
///
/// # The ending is the reason this exists, and it closes a stated gap
///
/// [`Self::mounted`] could be reported by whoever called [`Registry::mount`]. Two
/// mounts cannot: a lazily spawned actor is built inside
/// [`RegistryBuilder::spawn_with`]'s factory with no caller present, and an actor
/// **evicted** by the sweeper is torn down on a timer with no caller present
/// either. The second one was a documented hole — `Shared::sweep` computed an
/// [`Ended`] for every actor it destroyed and dropped it, so a
/// [`shutdown_budget`](Eviction::Idle::shutdown_budget) set below the longest
/// turn destroyed that turn with no log, no counter, no observer and no caller,
/// because the caller of an evicted actor is a peer whose stream is severed
/// rather than answered. This is where that `Ended` now goes.
///
/// # Both methods are synchronous, and must not block
///
/// They are called on the task that mounted, shut down, aborted or swept — the
/// sweeper's own task for an eviction — and a report that waits would make a
/// diagnostic able to stall the thing it describes. That is
/// [`Subscribers`](crate::Subscribers)' argument about
/// `Connection::closed` one layer down: an observability path that can hold its
/// subject open is a leak wearing a diagnostic's clothes.
///
/// So an implementation records and returns. keylink's own — [`Events`] — does
/// exactly that: a bounded channel, `try_send`, and a count of what it could not
/// fit, published as [`NodeStatus::dropped`]. Losing a diagnostic loudly beats
/// blocking a teardown quietly.
///
/// Neither call holds the registry's lock. [`Registry::mount`] releases the write
/// guard before it reports, so an implementation may call back into the same
/// system — [`Registry::status`], [`Registry::len`] — without deadlocking.
///
/// # A system holds as many of these as were installed
///
/// [`RegistryBuilder::lifecycle`] appends. Every implementation installed on a
/// system sees every mount and every ending, in the order the calls were made,
/// on the one task that mounted or tore down.
///
/// # One that panics costs its own report and nothing else
///
/// The registry catches each call. **The list is what made that necessary.**
/// With a single sink a panic deprived that sink and unwound into whoever was
/// reporting; with several it deprived every sink installed *after* it too, and
/// one of those can be `compose::Endings` — whose missed `ended` leaves every
/// waiter it holds pending for the life of the system, so a `Group` watching a
/// keyed member is never decided on and its `ended()` never resolves. A lost
/// console line and a wedged group are not the same loss, and only one of them
/// is visible.
///
/// **This doc used to argue the other way**, on the grounds that a
/// `catch_unwind` here would be inventing a failure policy for caller code the
/// registry cannot name. What that missed is that unwinding is a policy too, and
/// nobody chose it: the caller it reaches is `Registry::mount`, `Registry::stop`,
/// `Registry::abort` or the sweeper's own task, none of which asked to die for a
/// diagnostic. Everywhere else this crate does name the policy and it is always
/// the same one — a panicking turn, observer, group policy or `retire` each
/// costs one thing and not the run.
///
/// The panic is still loud where it happened. Nothing here installs a panic
/// hook, so `std`'s default one prints at the fault before the unwind is caught.
///
/// [`Events`]: crate::Events
/// [`NodeStatus::dropped`]: crate::NodeStatus::dropped
pub trait Lifecycle: Send + Sync + 'static {
    /// An actor is now mounted under `id` and its task is running.
    ///
    /// Reported after the registry's write guard is released, so the actor is
    /// already addressable and may already have taken a turn.
    fn mounted(&self, id: &ActorId);

    /// Registry-only form carrying one mounted incarnation's opaque identity.
    #[doc(hidden)]
    fn __mounted(&self, id: &ActorId, _incarnation: usize) {
        self.mounted(id);
    }

    /// The actor that was mounted under `id` is gone, and `why`.
    ///
    /// A natural self-ending carries the [`Ended`] returned by the actor loop.
    /// Shutdown and eviction carry the teardown result synthesized from their
    /// join; abort carries a natural value that won the cancellation race or
    /// [`Ended::Vanished`] otherwise. Cancellation of the outer shutdown future
    /// also reports `Vanished`, so a lifecycle waiter is never abandoned.
    fn ended(&self, id: &ActorId, why: &Ended);

    /// Registry-only form pairing an ending with the incarnation mounted above.
    #[doc(hidden)]
    fn __ended(&self, id: &ActorId, _incarnation: usize, why: &Ended) {
        self.ended(id, why);
    }
}

/// A registry of live actors: which ones exist, and for how long.
///
/// Cheap to clone: every clone names the same registry.
///
/// # Awaiting a keyed actor's ending
///
/// A local actor hands its caller `ended()`. A keyed actor is addressed by id,
/// so its system owns the observation door: [`Registry::ended`] returns an
/// optional future that subscribes to the entry's watch channel without creating
/// an actor. `None` means the id is not mounted at the instant of the read.
/// Subscribe after `mount` and before a teardown can remove the entry.
///
/// The channel is shared by the actor loop and the registry. The loop publishes
/// its actual ending before `Shared::reap` removes the entry; `shutdown` and
/// `abort` publish a synthesized `Ended` when they destroy an actor before its
/// loop can report one. [`Lifecycle::ended`] still reports every removal, and
/// `compose::Endings` remains useful when a composition wants to fan those
/// callbacks into a group-level awaitable.
///
/// `compose::Endings` is backticks and not a link, for the reason
/// [`RegistryBuilder::lifecycle`] gives at length: `compose` is built on top of this
/// module, and a link from here would point the seam at the layer above it.
///
/// `M` is what a mailbox carries, and it is a parameter because everything here
/// — the map, the cap, the eviction clock, the lifecycle — is about *which*
/// actor and never about what a message is made of. Naming a message type in
/// these fields would put a QUIC stream and a postcard payload inside the one
/// half of this module that has no business knowing either, and no import list
/// would show it.
///
/// **There is no default, and there used to be.** One system per node held
/// actors of unrelated services, so `keylink::Registry` written bare had to mean
/// something and a type-parameter default was the only way to say it. A system
/// is per-service now: `M` is that service's own mailbox type, `inventory::Mail`,
/// and a caller writes the generated `inventory::Registry` alias rather than this
/// type. That is not tidying — a per-service registry **physically cannot hold**
/// an actor of another service, so mounting the wrong one is `E0308` at the call
/// site instead of a schema comparison at run time on the serving node.
///
/// `Registry<YourOwnType>` still compiles, and there is still no way to deliver a
/// message to anything in it: `route` is `pub(super)` and [`Mountable`] keeps its
/// sender to itself. What you get is a map you can mount into, count, stop and
/// evict, and never send to. The tests in this file are the exception that
/// proves it — they are inside the crate, so they can call `route`.
///
/// # `error[E0282]: type annotations needed`
///
/// This is where somebody who has just met that error is looking. `M` has to
/// come from somewhere, and a `let` that binds what
/// [`build`](RegistryBuilder::build) returns without naming its type gives it
/// nowhere to come from:
///
/// ```compile_fail
/// let system = keylink::Registry::builder(keylink::Eviction::Never).build();
/// assert!(system.is_empty());
/// ```
///
/// The compiler points at `build`'s own `M: Send + 'static` bound, from the
/// `.build()` line alone: that bound is what demands `M` be known, so the
/// binding fails by itself and the second line never gets a chance to matter.
/// The second line is what such a system goes on to be used for, and it is why
/// nothing downstream rescues the binding either — [`contains`](Self::contains),
/// [`len`](Self::len), [`is_empty`](Self::is_empty),
/// [`evict_idle`](Self::evict_idle), [`shutdown`](Self::stop) and
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
/// let system: inventory::Registry = inventory::Registry::builder(Eviction::Never).build();
/// assert!(system.is_empty());
/// ```
///
/// Most code never meets it, because `mount(id, inventory::spawn(..))` pins `M`
/// through [`Mountable`] without anybody trying.
pub struct Registry<M> {
    inner: Arc<Shared<M>>,
}

/// Hand-written, because the derive would bound the impl on `M: Clone` — and a
/// mailbox message, which owns a reply channel and a stream, is not. A clone
/// here is a clone of the `Arc` and never of a message.
///
/// The derive does not fail where it is written, either: `self.clone()` in
/// `dispatch` would quietly resolve to `Clone` on the *reference* and surface
/// as a lifetime error inside the dispatch closure.
impl<M> Clone for Registry<M> {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl<M> Registry<M> {
    /// Configure a system. The eviction policy is an argument because there is
    /// no safe default for it.
    #[must_use]
    pub fn builder(eviction: Eviction) -> RegistryBuilder<M> {
        RegistryBuilder {
            capacity: DEFAULT_CAPACITY,
            mailbox_depth: DEFAULT_MAILBOX_DEPTH,
            eviction,
            factory: None,
            lifecycle: Vec::new(),
        }
    }

    /// Dispatch a typed local keyed RPC through this system's registry.
    ///
    /// `service!` calls this hidden seam from its generated local reference. It
    /// is public because the generated code lives in the caller's crate.
    #[doc(hidden)]
    pub async fn __local_ask<T>(
        &self,
        actor: &ActorId,
        mail: impl FnOnce(Origin, Reply<T>) -> M,
    ) -> Result<T, LocalError>
    where
        M: Send + 'static,
        T: Send + 'static,
    {
        let route = self.route(actor).map_err(LocalError::from)?;
        let chain = current();
        if chain.holds(&route.actor) {
            return Err(LocalError::Reentrant);
        }
        let fate = route.fate.clone();
        let (reply, answer) = Reply::channel();
        let message = (Box::new(mail(Origin::local(), reply)), chain);
        if route.shed {
            match route.mail.try_send(message) {
                Ok(()) => {}
                Err(mpsc::error::TrySendError::Full(_)) => return Err(LocalError::MailboxFull),
                Err(mpsc::error::TrySendError::Closed(_)) => return Err(LocalError::Closed),
            }
        } else {
            route
                .mail
                .send(message)
                .await
                .map_err(|_| LocalError::Closed)?;
        }
        drop(route);
        crate::turns::asked(answer, &fate)
            .await
            .map_err(local_error)
    }

    /// Dispatch a typed local keyed tell through this system's registry.
    #[doc(hidden)]
    pub async fn __local_notify<T>(
        &self,
        actor: &ActorId,
        mail: impl FnOnce(Origin, Reply<T>) -> M,
    ) -> Result<(), LocalError>
    where
        M: Send + 'static,
        T: Send + 'static,
    {
        let route = self.route(actor).map_err(LocalError::from)?;
        let chain = current();
        let reentrant = chain.holds(&route.actor);
        let (reply, answer) = Reply::channel();
        drop(answer);
        let message = (Box::new(mail(Origin::local(), reply)), chain);
        if reentrant {
            // Checking `capacity()` before an awaited `send` is a race: another
            // sender can take the last slot between the two, leaving this actor
            // waiting on its own turn. `try_send` makes the test and enqueue one
            // operation while preserving a non-blocking self-notify when room
            // exists.
            return match route.mail.try_send(message) {
                Ok(()) => Ok(()),
                Err(mpsc::error::TrySendError::Full(_)) => Err(LocalError::Reentrant),
                Err(mpsc::error::TrySendError::Closed(_)) => Err(LocalError::Closed),
            };
        }
        if route.shed {
            return match route.mail.try_send(message) {
                Ok(()) => Ok(()),
                Err(mpsc::error::TrySendError::Full(_)) => Err(LocalError::MailboxFull),
                Err(mpsc::error::TrySendError::Closed(_)) => Err(LocalError::Closed),
            };
        }
        let sent = route.mail.send(message).await;
        drop(route);
        sent.map_err(|_| LocalError::Closed)
    }

    /// Mount an actor under `id`, starting it.
    ///
    /// Takes effect on the next message. Nothing is announced, no lock outside
    /// this registry is taken, and no connection is disturbed.
    ///
    /// **A refusal costs no task.** Both refusals below return before the actor's
    /// loop is ever polled, so the [`Mountable`] this drops is an unstarted future
    /// rather than a running task — which is the whole reason [`Mountable`] holds a
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
    pub fn mount(&self, id: ActorId, actor: Mountable<M>) -> std::result::Result<(), MountError>
    where
        M: Send + 'static,
    {
        let mut entries = self.inner.write();
        // See `Shared::route`: a `VacantEntry` borrows the map, so the count is
        // taken first. It is the count before this insert, which is the one the
        // cap is about.
        let live = entries.len();
        let announce = match entries.entry(id) {
            // The refusal clones the key it was given back out of the map, which
            // is the one allocation this shape costs and it is on the cold path.
            hash_map::Entry::Occupied(taken) => {
                return Err(MountError::Occupied {
                    id: taken.key().clone(),
                })
            }
            hash_map::Entry::Vacant(free) => {
                if live >= self.inner.capacity {
                    return Err(MountError::AtCapacity {
                        capacity: self.inner.capacity,
                    });
                }
                let entry = self.inner.entry(free.key(), actor);
                let announce = self.inner.announcing(free.key(), &entry.identity);
                free.insert(entry);
                announce
            }
        };
        // After the guard, never under it: a `Lifecycle` may ask this same system
        // what is running, and an `RwLock` write guard held across that call is a
        // deadlock the implementor could not have seen coming. A refusal above
        // returns before this and announces nothing — there is no actor.
        drop(entries);
        if let Some((id, identity)) = announce {
            self.inner.report_mounted(&id, &identity);
        }
        Ok(())
    }

    /// Find the actor `id` addresses, spawning it if this system has a factory.
    ///
    /// **The whole of the seam**, and an operation rather than a handle on
    /// purpose. With `inner` reachable from `remote.rs`, the transport half held
    /// the entire [`Shared`] and every method on it, so nothing but manners
    /// stopped the next change from reaching the map, the cap or the eviction
    /// clock from the side that has a network in it. One `pub(super)` function
    /// is a boundary that can be read in full, and adding a second is a visible
    /// act rather than a use of something already in hand.
    ///
    /// # Errors
    ///
    /// [`RouteError`], which `remote.rs` translates into what a peer is told.
    pub(super) fn route(&self, id: &ActorId) -> std::result::Result<Route<M>, RouteError>
    where
        M: Send + 'static,
    {
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
    /// # Cancel-safe: dropping this loses the answer, never the actor
    ///
    /// The id leaves the map before the first await, so a caller who drops this
    /// future — `tokio::select!`, a `race`, a `timeout` around the call — has
    /// already unmounted the actor and cannot be handed it back. What the drop
    /// must not do is *detach* it, and until this wave a dropped `JoinHandle`
    /// did exactly that: the deleted `stop`'s whole defect, reachable through
    /// cancellation instead of through a budget.
    ///
    /// So the teardown holds an abort-on-drop handle, and dropping this future
    /// aborts the actor exactly as an expired budget would. A
    /// dropped call therefore loses the [`Ended`] and the *wait*, and destroys
    /// the actor rather than draining it, which is the same trade a budget of
    /// zero would be. [`Self::abort`] is the deliberate spelling of that.
    ///
    /// # The exclusive resources outlive the id, by up to one budget
    ///
    /// The entry is gone and the actor is not, until it drains or the budget
    /// expires. Anything exclusive the state owns — a
    /// `keylink_persist::Claim`, a file lock, a device — is still held
    /// throughout. So a lazily spawned system can serve the same id again
    /// straight away and the actor it builds can be refused for as long as this
    /// budget lasts. [`Eviction::Idle::shutdown_budget`] carries that in full,
    /// because a sweeper applies it to every expired actor at once and a caller
    /// applies it to one.
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
    /// # The `Ended` returned here answers a different question from `Registry::ended`
    ///
    /// This method reports the fate of **this shutdown operation**: `Stopped` when
    /// the task drained inside `budget`, and `Vanished` when the budget expired or
    /// the task had already failed and had to be aborted. It does not claim to
    /// explain why a loop that finished normally ended.
    ///
    /// The registry entry also carries a watch channel. The generated loop publishes
    /// its actual [`Ended`] value before the registry reaps it, and
    /// [`Registry::ended`] lets a caller that subscribed before removal await that
    /// value. If `shutdown` has to abort the task, `drain` publishes the synthesized
    /// `Vanished` value only when the loop did not publish one first.
    ///
    /// These are complementary observations, not two guesses about one value:
    /// `shutdown` says whether this caller's drain succeeded, while `ended` says
    /// why the actor loop reported that it was over. A shutdown caused by an
    /// observer's refusal can therefore return `Stopped` while a pre-existing
    /// `ended` subscriber receives `Ended::Observer`.
    ///
    pub async fn stop(&self, id: &ActorId, budget: Duration) -> Option<Ended> {
        let entry = self.inner.write().remove(id)?;
        let ended = drain(Arc::clone(&self.inner), id.clone(), entry, budget).await;
        Some(ended)
    }

    /// Stop routing to `id` and cancel what the actor is doing where it stands.
    ///
    /// The state is **destroyed**: the task owns it, `abort` drops that task's
    /// future at its next await point, and unflushed work goes with it. That is
    /// the difference from [`Self::stop`], and it is why the two are separate
    /// methods rather than one with a budget of zero: this one does not drain, so
    /// there is no budget to name.
    ///
    /// `abort()` is called **before** the await, which is why this survived the
    /// wave that deleted `stop`. A caller who wraps this in a timeout and lets it
    /// fire loses the wait and not the cancellation — the request has already
    /// landed on the task. The teardown guard also settles a direct
    /// [`Registry::ended`] future and every [`Lifecycle`] callback as
    /// [`Ended::Vanished`] when that outer wait is cancelled; a natural ending
    /// already published by the loop remains authoritative.
    ///
    /// Returns whether an actor was mounted under that id.
    pub async fn abort(&self, id: &ActorId) -> bool {
        let Some(entry) = self.inner.write().remove(id) else {
            return false;
        };
        let mut report = TeardownGuard {
            registry: Arc::clone(&self.inner),
            id: id.clone(),
            ended: entry.ended.clone(),
            identity: entry.identity.clone(),
            armed: true,
        };
        // Before the abort and before the await, for `drain`'s reason: an
        // aborted loop never reaches `Inbox::ending`, so a peer holding a parked
        // reply this destroys must read *gone* rather than *the actor is still
        // there*.
        //
        // **Narrowly**, like `drain`'s: what this covers that nothing else does
        // is a **hand-written** `mount` loop, which has no `run_loop` and so no
        // floor. `abort_settles_the_served_answer_before_it_destroys_the_actor`
        // drives one for that reason.
        let ending = entry.ended.subscribe();
        entry.fate.settle(Standing::Gone);
        entry.task.abort();
        let _ = entry.task.await;
        // Publish the synthetic result only after the task has joined. If the
        // loop won the race before cancellation, its natural `Feed`/`Panicked`/
        // `Observer` value is already in the channel and remains authoritative.
        // Delaying the synthetic send keeps a direct `Registry::ended` waiter and
        // the lifecycle callback on the same final value.
        let published = { ending.borrow().clone() };
        let why = published.unwrap_or(Ended::Vanished);
        report.complete(&why);
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

    /// What every mounted actor is doing, as of now.
    ///
    /// One [`ActorStatus`] per live actor, in no particular order — the map is a
    /// `HashMap` and sorting a diagnostic nobody asked to be sorted is a cost
    /// every caller pays for one caller's benefit.
    ///
    /// Read under the read lock, so it does not stop an actor being addressed
    /// while it runs; it does briefly stop one being mounted or unmounted. Each
    /// row is sampled as the iterator reaches it, so a list of ten is ten readings
    /// and not one instant — which is the honest shape for a fact about ten tasks
    /// on other threads, and why [`ActorStatus`] says so out loud rather than
    /// pretending to a snapshot.
    ///
    /// # Stale before you read it, and eviction is the reason to care
    ///
    /// Every row here is a fact about another task, so it is out of date the
    /// instant it is returned. That is true of any diagnostic. What makes it
    /// **actionable** staleness rather than the ordinary kind is
    /// [`Eviction::Idle`]: a sweeper runs on a timer, on its own task, and it
    /// removes actors this list has already named. So an [`ActorId`] read out of
    /// this `Vec` may be unmounted before the next line of the caller's own code,
    /// with nothing in between — no message, no error, no call the caller made.
    ///
    /// The consequence a caller has to hold: **this list is evidence, never a
    /// precondition.** Never `status().iter().find(..)` and then act as if that
    /// actor is there. Address it and handle
    /// [`RemoteError::NoSuchActor`](crate::RemoteError::NoSuchActor), or mount it
    /// under [`Eviction::Never`] where nothing sweeps. The other half of the same
    /// clock is [`ActorStatus::idle_for`], which is the exact reading
    /// [`Eviction::Idle::after`] is compared against — so a row whose `idle_for`
    /// is near the policy is a row whose actor is about to go.
    ///
    /// This method is named for a reading and not for a snapshot, and it is the
    /// only one: there is no `snapshot()` beside it. A second name would promise
    /// the one property this cannot have — that every row was taken at one
    /// instant — while doing exactly what this does.
    ///
    /// `contains`, `len` and `is_empty` answer three questions this answers as
    /// well, and they stay: they take one lock and allocate nothing, which is what
    /// a hot path wants.
    #[must_use]
    pub fn status(&self) -> Vec<ActorStatus> {
        self.inner.status()
    }

    /// What the actor under `id` is doing, as of now.
    ///
    /// The two turn facts [`Self::status`] reports, for one actor instead of
    /// every actor, and read from the same lines — so a row of that list and this
    /// answer for the same id cannot disagree. [`Serving`] carries what the pair
    /// does and does not claim, and the singleton twin is
    /// [`LocalActor::serving`](crate::LocalActor::serving).
    ///
    /// # `None` is the liveness answer, and it is exact here
    ///
    /// Nothing is mounted under that id. **This is a non-inserting reader, and
    /// that is the whole point of it not going through routing.** Routing takes
    /// the write lock and inserts from the factory a system was built with, so on
    /// a [`RegistryBuilder::spawn_with`] system a status question asked that way
    /// would *create* the actor it was asked about. This takes the read lock the
    /// way [`Self::contains`] does and answers about what is already there:
    /// asking what an actor is doing must not be how one comes into existence.
    ///
    /// It does not move [`ActorStatus::idle_for`] either, so watching an actor
    /// does not stop [`Eviction::Idle`] sweeping it.
    ///
    /// # Evidence, never a precondition
    ///
    /// [`Self::status`]'s own warning applies unchanged to one row of it: the
    /// actor can be evicted, stopped or spawned between this answer and the next
    /// line, with no message and no error in between. Address it and handle
    /// [`RemoteError::NoSuchActor`](crate::RemoteError::NoSuchActor); do not read
    /// `Some` here and act as though the actor is still there.
    #[must_use]
    pub fn serving(&self, id: &ActorId) -> Option<Serving> {
        self.inner.read().get(id).map(Entry::serving)
    }

    /// Await the ending of the actor currently mounted under `id`.
    ///
    /// The future subscribes before the actor is removed from the registry, so
    /// shutdown, eviction, natural loop endings and aborts all report through the
    /// same [`Ended`] value. `None` means the id is not live at this instant; like
    /// [`Self::serving`], this is a non-inserting observation.
    #[must_use]
    pub fn ended(&self, id: &ActorId) -> Option<impl Future<Output = Ended> + Send + 'static> {
        let mut ended = self.inner.read().get(id)?.ended.subscribe();
        Some(async move {
            loop {
                let value = ended.borrow().clone();
                if let Some(value) = value {
                    return value;
                }
                if ended.changed().await.is_err() {
                    return Ended::Vanished;
                }
            }
        })
    }

    /// [`Self::status`], read through a handle that does **not** keep this
    /// system alive.
    ///
    /// `pub(crate)`, and the one caller is [`Inspector`](crate::Inspector),
    /// which erases a `Registry<M>` to a closure so that one console can answer
    /// for a system of any service. Erasing it through a `Registry` clone made
    /// that closure an *owner*: a program that dropped its own system went on
    /// running every actor in it, because the inspect actor was holding the
    /// registry — and [`spawn_sweeper`] is already `Weak` for exactly this
    /// reason, so the strong reference here was the odd one out.
    ///
    /// `None` is the system being gone, which a caller must be able to tell
    /// from a system with nothing in it. Returning an empty `Vec` for both would
    /// report a shutting-down node as an idle one.
    ///
    /// An `impl Trait` return rather than the `Weak` itself, because
    /// [`Shared`] is this module's and is not going to stop being.
    pub(crate) fn weak_status(
        &self,
    ) -> impl Fn() -> Option<Vec<ActorStatus>> + Send + Sync + 'static
    where
        M: Send + 'static,
    {
        let weak = Arc::downgrade(&self.inner);
        move || Weak::upgrade(&weak).map(|registry| registry.status())
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
    /// an entry and detaching its task; a budget is a thing to wait out.
    ///
    /// # Cancel-safe, which is what makes "spawn it" a real escape hatch
    ///
    /// A caller who cannot wait can spawn this, or wrap it in a `timeout`, or put
    /// it in a `select!`. Every one of those can drop the future mid-sweep, and
    /// this method takes **no budget parameter**, so a caller who needs a bound
    /// has no other spelling — one dropped sweep would have detached *every*
    /// expired actor at once, because they are torn down concurrently.
    ///
    /// It does not: the entries are already out of the map when the first await
    /// happens, and each teardown holds an abort-on-drop handle, so dropping this
    /// future destroys those actors instead of setting them loose. The cost of
    /// cancelling is that actors which would have drained are aborted mid-turn
    /// — the same cost an under-sized
    /// [`shutdown_budget`](Eviction::Idle::shutdown_budget) has, and just as
    /// silent. Prefer letting a sweep finish.
    pub async fn evict_idle(&self) {
        self.inner.sweep().await;
    }
}

impl<M> fmt::Debug for Registry<M> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Registry")
            .field("live", &self.len())
            .field("capacity", &self.inner.capacity)
            .field("mailbox_depth", &self.inner.mailbox_depth)
            .field("eviction", &self.inner.eviction)
            .field("lazy_spawn", &self.inner.factory.is_some())
            .finish()
    }
}

/// Assembles a [`Registry`]: its eviction policy, its cap, and how it spawns.
///
/// `M` is the system's message type, exactly as on [`Registry`], and it has no
/// default for the same reason.
pub struct RegistryBuilder<M> {
    capacity: usize,
    mailbox_depth: usize,
    eviction: Eviction,
    factory: Option<Factory<M>>,
    /// Every sink [`RegistryBuilder::lifecycle`] was given, in call order. A `Vec`
    /// and not an `Option`, because that method appends — see it for why one
    /// slot was wrong.
    lifecycle: Vec<Arc<dyn Lifecycle>>,
}

/// The cap, as the generic half spells it. [`RegistryBuilder::DEFAULT_CAPACITY`]
/// is the public name and carries the reasoning for the number.
const DEFAULT_CAPACITY: usize = 10_000;

impl<M> RegistryBuilder<M> {
    /// The cap a system has when none is named.
    ///
    /// Ten thousand live actors was the number the design was costed against,
    /// so it is the number a system holds before it starts refusing.
    ///
    /// This lived on `impl RegistryBuilder<Mail>` for one release, under a doc
    /// comment calling itself a compatibility shim: a type-parameter default
    /// applies in type position and not in expression position, so a constant on
    /// the generic impl left `M` uninferrable at every call site that named it.
    /// The defaults are gone and a caller reaches this through a generated,
    /// fully applied alias — `inventory::RegistryBuilder::DEFAULT_CAPACITY` — so
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
        F: Fn(&ActorId) -> Option<Mountable<M>> + Send + Sync + 'static,
    {
        self.factory = Some(Arc::new(factory));
        self
    }

    /// The most actors this system will hold at once.
    ///
    /// A mount past it is [`MountError::AtCapacity`]; a lazy spawn past it is
    /// [`RemoteError::AtCapacity`](crate::RemoteError::AtCapacity) naming the
    /// cap. This
    /// is the bound that keeps a lazily spawning system from being a
    /// memory-exhaustion target, so raising it is a statement about how much
    /// memory the process may spend.
    #[must_use]
    pub fn capacity(mut self, actors: usize) -> Self {
        self.capacity = actors;
        self
    }

    /// Report every mount and every ending to `lifecycle`.
    ///
    /// Off when nobody says, which is the one arrangement that does not decide
    /// anything for a caller who did not ask: a system with no sink computes the
    /// same [`Ended`] on the same teardown path and drops it, exactly as every
    /// system did before this method existed.
    ///
    /// [`Events`](crate::Events) is the one keylink ships, and it is what feeds
    /// [`Inspector`](crate::Inspector) — but the parameter is the trait, so a
    /// program that already has a log or a metric sink can implement two methods
    /// and skip the console entirely.
    ///
    /// Read [`Lifecycle`] before implementing one: both methods are synchronous
    /// and must not block, because a mount and an eviction wait for them.
    ///
    /// # Additive, and the single slot it replaced was a trap
    ///
    /// **Calling this twice installs both.** It used to overwrite, and that is
    /// the defect rather than a simplification worth keeping: the sink is the
    /// crate's own console on one side and a library type on the other, so the
    /// two calls are written by two different people. A program with
    /// `.lifecycle(events)` for [`Inspector`](crate::Inspector) that later adds
    /// `.lifecycle(endings)` for `compose::Endings` lost its console silently —
    /// no error, no warning, no method returning what it displaced. The failure
    /// surfaced as a console that had stopped showing mounts, a long way from
    /// the builder line that caused it.
    ///
    /// The alternative was to keep one slot and return the displaced sink, which
    /// is `compose::Names::register`'s answer to the same shape. It is right
    /// there and wrong here: a name is *one* actor, so displacing is what
    /// registering a second one means, while a report is a broadcast and two
    /// watchers of one system are not in competition. Nothing about a mount is
    /// consumed by being reported.
    ///
    /// **Both `compose::` names above are backticks on purpose, not links
    /// waiting to be written.** `compose` is built on top of this module, so an
    /// intra-doc link from here would point the seam at the layer above it —
    /// the cost the module header names at the top of this file. Do not "fix"
    /// them.
    ///
    /// **Order is install order and it is observable**, because the reports run
    /// one after another on the reporting task. Do not build on it: it says
    /// which of two consoles prints first and nothing about which of two
    /// implementations *sees* an actor, since neither can change what the other
    /// is told.
    #[must_use]
    pub fn lifecycle(mut self, lifecycle: impl Lifecycle) -> Self {
        self.lifecycle.push(Arc::new(lifecycle));
        self
    }

    /// How many messages may wait ahead of the one an actor is handling.
    ///
    /// **One when nobody says.** This is a tuning decision rather than a
    /// correctness one, which is why it has a default at all while
    /// [`Eviction`]/`Retention`/`Allowlist` do not: a wrong depth is slower or
    /// fatter, and both are visible and recoverable. A wrong eviction policy loses
    /// state and a wrong allowlist admits a stranger.
    ///
    /// # What the number means — and this doc names no recommended one
    ///
    /// **A deeper mailbox drains the whole queue faster. It does not move
    /// backpressure off the caller, and this paragraph used to say it did.** Every
    /// keyed call *through a generated client* waits for the actor it is addressing,
    /// at every depth, and that is a property of the client surface rather than of
    /// the timings below: `__keylink_actor_client_method!` in
    /// `keylink/src/macros.rs` generates **no** method for a `tell` on a keyed
    /// `ActorRef`, and its own comment gives the reason — four keyed refusals are
    /// decided after the bytes reach the wire and are reported on the reply. So a
    /// caller holding a generated keyed client awaits a reply or it does not call,
    /// and a message the mailbox has *accepted* still leaves its caller waiting.
    /// `keylink/tests/mailbox_latency.rs` asserts exactly that, at three slots with
    /// four callers.
    ///
    /// **A hand-rolled peer is outside that bound, and the same macro arm says so in
    /// its own comment**: "a keyed `tell` really would work — `RpcClient::tell` writes
    /// a `WireRequest::Call` on a unidirectional stream and `serve_one_tell` walks the
    /// same dispatch a call does". That is
    /// [`RpcClient::tell`](crate::RpcClient::tell), generic over any `Serialize`
    /// request. What is absent is the generated *method*, not the wire path. So a peer
    /// that dials a service's `registry_alpn()` and writes an `Addressed` request by hand
    /// reaches this mailbox without awaiting anything, and the sender on the mailbox
    /// is the serving task rather than the peer — the peer returned when the bytes
    /// went out.
    ///
    /// **That does not bring the deleted sentence back. It takes the caller out of the
    /// picture altogether.** Such a peer returns at every depth and never feels this
    /// queue at all, so the staircase the deleted prose described — the first `N`
    /// callers going on and the `N + 1`th feeling the whole queue — is unreachable on
    /// that path too, and for a blunter reason. The sentence was wrong for both kinds
    /// of caller, for two different reasons. Finding the `tell` is a reason to read
    /// this paragraph, not to restore that one.
    ///
    /// What depth buys is the hop **between** turns. At one slot every turn but the
    /// first waits for a parked sender to be woken and rescheduled before the loop
    /// can receive; at `N` the next `N` messages are already in the channel. That
    /// takes work off the critical path of every queued caller, the last one
    /// included — so the gain arrives as the whole burst finishing sooner, not as
    /// `N` callers escaping the queue.
    ///
    /// | 32 concurrent callers, one actor | depth 1 | depth 32 | runs |
    /// |---|---|---|---|
    /// | drain, per queued turn, the release included | 18.8 – 20.5 µs | 1.3 – 1.9 µs | 4 |
    /// | slowest caller's own wait | 717 – 779 µs | 165 – 234 µs | 4 |
    /// | fastest caller's own wait | 378 – 453 µs | 155 – 222 µs | 4 |
    /// | that spread, within one run | 1.7 – 1.9× | 1.03 – 1.07× | 4 |
    /// | control: one call at a time | 151 – 168 µs/call | 137 – 410 µs/call | 4 |
    ///
    /// Apple M5 Max (`Mac17,6`), macOS 27.0.0 `arm64`, release profile, 2026-08-08
    /// (-03:00), at a load average between 13.95 and 18.69 with two sibling
    /// compilers running. `keylink/tests/mailbox_latency.rs`'s `#[ignore]`d
    /// measurement produced these and prints its own load average and sccache hit
    /// rate beside them, because a figure taken on that machine is not evidence
    /// without them.
    ///
    /// **Read the ratios, not the absolutes**, and the control row is what says why:
    /// it must not depend on depth, and across four runs it did not, apart from one
    /// 410 µs reading under load against a 137 – 168 µs band. Every figure includes
    /// a QUIC stream setup, a postcard encode and a round trip, because
    /// `Shared::route` is `pub(super)` and a test reaches a keyed mailbox only
    /// through a served client. What survives that noise is the shape: the drain is
    /// 10.8 to 15 times apart within a run, and the burst's own spread narrows from
    /// 70 – 90% at one slot to 3 – 7% at 32. A burst of 32 into 32 slots arrives
    /// almost together, which is what a staircase of `N` fast callers and one slow
    /// one does not look like.
    ///
    /// The debug profile agrees in direction over four more runs, at 6.8 to 9.8
    /// times for the same drain ratio. That rules out one thing and states nothing
    /// else: the gap is not an artifact of the optimization level. It does not
    /// separate a task wakeup from a permit acquisition, or from any other per-turn
    /// constant a debug build inflates. What the gap *is* comes from the mechanism
    /// paragraph above, which argues it from the code.
    ///
    /// Nothing here recommends a value, deliberately. A number in a doc gets
    /// copied into services it was never measured against; the trade above is the
    /// thing to reason from. Two questions decide it: does anything downstream act
    /// on how long this actor took, and how long is the longest turn.
    ///
    /// # A KEYED mailbox BLOCKS the sender. It never drops and it never errors
    ///
    /// There is no overflow policy on this path because there is no overflow: past
    /// the last slot, `send` parks the sending task until the loop frees a permit.
    /// This builder has no policy argument at all, so a keyed actor parks and there
    /// is nothing to choose.
    ///
    /// **This heading read *a full mailbox* and the paragraph said *nothing in this
    /// crate calls `try_send`*, and both outgrew their scope.**
    /// [`Backpressure::Shed`](crate::Backpressure::Shed) is a `try_send`, on the
    /// **local** doors — [`LocalActor::spawn_deep`](crate::LocalActor::spawn_deep)
    /// and its supervised twin — where a full mailbox answers
    /// [`NotQueued::Full`](crate::NotQueued::Full) and loses the caller's
    /// arguments. That is opt-in, it is not this door, and a sentence claiming the
    /// whole crate from a knob that governs one path is the drift worth naming
    /// rather than quietly narrowing.
    ///
    /// What did **not** change is the part that was refused rather than deferred:
    /// there is still **no `unbounded` spelling — permanently**, on either path. An
    /// unbounded mailbox converts backpressure into memory growth: a latency
    /// problem becomes an OOM, at a moment nobody chose, in a process that had been
    /// reporting healthy. A shed is not that — it bounds the mailbox exactly as
    /// parking does and differs only in what it tells a sender that arrives past
    /// the last slot. `keylink/src/representability.rs` holds the probes that say
    /// unbounded cannot be asked for, on both paths.
    ///
    /// One caller-visible exception, and it is opt-in and does not queue: a
    /// [`QueueTimeout`](crate::QueueTimeout) layer turns the wait into
    /// [`RemoteError::TimedOut`](crate::RemoteError::TimedOut) for a peer, and a
    /// message refused that way provably never entered the mailbox — the send
    /// future is dropped before a permit arrives. That is a deadline a service
    /// asked for, not the mailbox dropping anything.
    ///
    /// # Below 32 there is no memory trade at all, and it is measured
    ///
    /// `tokio::sync::mpsc` allocates its first block of 32 slots eagerly, so an
    /// idle mailbox costs the same at depth 1 as at depth 32 — and, measured,
    /// the same at 1,024, because further blocks are allocated when messages
    /// actually arrive rather than when capacity is declared. So below 32 the knob's
    /// **memory** cost is zero.
    ///
    /// **Two measurements, in two files, and this heading covers one of them.** This
    /// sentence used to read "below 32 the knob buys latency for free" and cite the
    /// paragraph below for all of it — but that test measures BYTES, so the word
    /// "latency" was sourced by nothing. The bytes are here; the microseconds are in
    /// `keylink/tests/mailbox_latency.rs` and are tabled under "What the number
    /// means" above. Neither citation covers the other.
    ///
    /// `tests/actor_memory.rs`'s depth phase is that BYTE measurement, printing the
    /// per-actor cost at each depth. It runs at that file's `ACTORS_PER_DEPTH` —
    /// **two thousand** idle actors per depth — and that constant's own doc argues
    /// the count: phase 5 compares three systems against each other rather than
    /// against a budget, so one block of slots, 256 bytes per actor, is unmissable
    /// at that size. It is measured and not asserted from the tokio source because
    /// this crate has been wrong about this allocator's shape before — the table on
    /// [`Mailbox`] carried three wrong figures under a sentence
    /// claiming they were measured.
    ///
    /// **This line said ten thousand, and that is the file's other constant.**
    /// `ACTORS` is 10,000 and drives phases 1 to 4; the depth phase is phase 5 and
    /// never touches it. A wrong figure in a doc pointing at a test reads as
    /// sourced, which is how this one propagated: issue #13 quoted it from here.
    ///
    /// What is **not** measured, and follows from the same block size: past 32
    /// queued messages a real queue does allocate again. The trade in the heading
    /// is about a mailbox that is *full*, not about one that is *deep*.
    ///
    /// # The shutdown budget's meaning moves with this, and under-draining is a defect
    ///
    /// [`Registry::stop`] and [`Eviction::Idle::shutdown_budget`] give an actor
    /// one budget to finish **everything it has**: at depth 1 that is at most one
    /// queued turn plus the one in flight, and at depth `N` it is up to `N + 1`. A
    /// budget chosen against the default therefore silently under-drains at any
    /// deeper setting, and an expired budget does not wait — it destroys the actor
    /// where it stands, which is the detach defect the budget exists to remove
    /// arriving by the other door.
    ///
    /// So raising this raises the budget you owe, by up to `N` turns. Nothing can
    /// check that for you: the budget is a `Duration` and the queue is a count.
    ///
    /// # What you still cannot see, and it is the part that hurts
    ///
    /// The wait list behind the last slot is unbounded and **unobservable**.
    /// [`ActorStatus::buffered`] reports what the channel is holding, exactly, and
    /// there is no number anywhere for the senders parked behind it, because
    /// `Semaphore` does not expose its waiters. A deeper mailbox does not shorten
    /// that list; it moves the first `N` entries of it somewhere countable.
    ///
    /// # Why `NonZeroUsize`
    ///
    /// `mpsc::channel(0)` panics, and after this wave the channel is created per
    /// actor in `Shared::entry` — so a zero would panic on the first *mount*,
    /// or worse, inside a lazy factory on a serving node, one message after the
    /// builder that caused it returned. The type moves that from a runtime panic
    /// nobody can test for to an argument that cannot be written. Saturating a
    /// zero up to one would be the silent fallback this crate refuses everywhere
    /// else.
    ///
    /// # It is a property of the system, not of the actor
    ///
    /// Every actor in this system gets this depth, including one a lazy
    /// [`spawn_with`](Self::spawn_with) factory builds — the factory is a `Fn` with
    /// no argument for it and no caller present to supply one. Per-actor depths
    /// would be a second construction path for a mailbox, and there is one.
    #[must_use]
    pub fn mailbox_depth(mut self, depth: NonZeroUsize) -> Self {
        self.mailbox_depth = depth.get();
        self
    }

    /// Start the system, and the sweeper that enforces its eviction policy.
    ///
    /// # Panics
    ///
    /// Panics when [`Eviction::Idle`] is set outside a tokio runtime, because
    /// the sweeper is a task.
    #[must_use]
    pub fn build(self) -> Registry<M>
    where
        M: Send + 'static,
    {
        let system = Registry {
            inner: Arc::new(Shared {
                entries: RwLock::new(HashMap::new()),
                factory: self.factory,
                lifecycle: self.lifecycle,
                capacity: self.capacity,
                mailbox_depth: self.mailbox_depth,
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

impl<M> fmt::Debug for RegistryBuilder<M> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RegistryBuilder")
            .field("capacity", &self.capacity)
            .field("mailbox_depth", &self.mailbox_depth)
            .field("eviction", &self.eviction)
            .field("lazy_spawn", &self.factory.is_some())
            // A count and not a bool, because "installed one" and "installed
            // three" are now different states and a bool would report them the
            // same.
            .field("lifecycle", &self.lifecycle.len())
            .finish()
    }
}

/// Sweep on a timer, and stop when the last [`Registry`] handle goes away.
///
/// Holds a `Weak`, so the sweeper never keeps a registry alive. It outlives the
/// last handle by at most one tick, which nobody can observe.
///
/// `after` and not the shutdown budget sets the tick: the tick is about how
/// promptly a stale actor is noticed, and the budget is about how long its
/// teardown may take once it has been. A sweep that runs long because something
/// would not drain delays the next tick, and it delays it by one budget rather
/// than by one per wedged actor — see [`Shared::sweep`].
fn spawn_sweeper<M: Send + 'static>(registry: &Arc<Shared<M>>, after: Duration) {
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

    use tokio::sync::oneshot;

    use super::*;

    /// The witness: `Send + 'static`, which [`RegistryBuilder::build`] requires
    /// and no message type can do without, and not one thing more. No derives,
    /// deliberately — every trait added here is a bound this file would stop
    /// noticing.
    struct Bare;

    /// An actor that does nothing at all, for testing the registry rather than
    /// any message it carries.
    fn idler() -> Mountable<Bare> {
        mount(|mut inbox: Mailbox<Bare>| async move {
            while inbox.recv().await.is_some() {}
            Ended::Stopped
        })
    }

    /// An actor that ends **itself**, on a signal no registry gave it.
    ///
    /// The fourth way out, in the smallest shape there is: nothing calls
    /// `shutdown`, nothing calls `abort`, no sweeper runs, and the loop returns
    /// anyway. `Ended::Feed` is the reason a real one most often ends this way —
    /// a subscription that ran out — and any of the four would do here.
    fn self_ending(go: oneshot::Receiver<()>) -> Mountable<Bare> {
        mount(|inbox: Mailbox<Bare>| async move {
            // Held, so the mailbox stays open and the entry stays routable right
            // up to the ending: an actor that had already dropped its receiver
            // would be a weaker case.
            let _inbox = inbox;
            let _ = go.await;
            Ended::Feed { name: "ticks" }
        })
    }

    /// An actor that takes nothing until its gate opens, then drains everything
    /// and counts what it took.
    ///
    /// The device every depth test needs: while it is closed, the mailbox fills
    /// deterministically, because the one thing that could empty it is not running
    /// yet. Nothing here reads a clock.
    fn gated(gate: oneshot::Receiver<()>, taken: &Arc<AtomicUsize>) -> Mountable<Bare> {
        let taken = Arc::clone(taken);
        mount(|mut inbox: Mailbox<Bare>| async move {
            let _ = gate.await;
            while inbox.recv().await.is_some() {
                taken.fetch_add(1, Ordering::SeqCst);
            }
            Ended::Stopped
        })
    }

    /// One turn, for the tests that size a shutdown budget in turns.
    const TURN: Duration = Duration::from_millis(100);

    /// An actor whose every turn takes exactly one [`TURN`].
    ///
    /// `n0_future::time::sleep`, so a paused clock drives it and the budget
    /// arithmetic below is exact rather than sampled.
    fn slow(handled: &Arc<AtomicUsize>) -> Mountable<Bare> {
        let handled = Arc::clone(handled);
        mount(|mut inbox: Mailbox<Bare>| async move {
            while inbox.recv().await.is_some() {
                n0_future::time::sleep(TURN).await;
                handled.fetch_add(1, Ordering::SeqCst);
            }
            Ended::Stopped
        })
    }

    /// An actor that enters a turn and never leaves it.
    ///
    /// The shape a shutdown budget destroys. `finished` counts turns that ran to
    /// the end, so a test can say "this work was thrown away" rather than "the
    /// actor is gone", which is true of a clean drain too.
    fn wedged(
        entered: oneshot::Sender<()>,
        hold: oneshot::Receiver<()>,
        finished: &Arc<AtomicUsize>,
    ) -> Mountable<Bare> {
        let finished = Arc::clone(finished);
        mount(|mut inbox: Mailbox<Bare>| async move {
            if inbox.recv().await.is_some() {
                let _ = entered.send(());
                let _ = hold.await;
                finished.fetch_add(1, Ordering::SeqCst);
            }
            Ended::Stopped
        })
    }

    /// A depth, for a test that means one.
    fn depth(slots: usize) -> NonZeroUsize {
        NonZeroUsize::new(slots).expect("a test never asks for a zero-slot mailbox")
    }

    /// Let every spawned task run until it parks, without letting the clock move:
    /// a ready task keeps a paused runtime from auto-advancing to the next timer.
    async fn settle() {
        for _ in 0..8 {
            tokio::task::yield_now().await;
        }
    }

    /// The one row a single-actor test is about.
    fn only(system: &Registry<Bare>) -> ActorStatus {
        let mut rows = system.status();
        assert_eq!(rows.len(), 1, "these tests mount exactly one actor");
        rows.remove(0)
    }

    /// A [`Lifecycle`] that remembers, so a test can assert on what a registry
    /// said rather than on whether it said anything.
    ///
    /// Rendered to strings on the way in, deliberately: the assertions below are
    /// then a list compared to a list, in order, which catches a report that is
    /// duplicated or out of sequence — neither of which a set of counters would.
    #[derive(Clone, Default)]
    struct Reported(Arc<std::sync::Mutex<Vec<String>>>);

    impl Reported {
        fn seen(&self) -> Vec<String> {
            self.0.lock().unwrap().clone()
        }
    }

    impl Lifecycle for Reported {
        fn mounted(&self, id: &ActorId) {
            self.0.lock().unwrap().push(format!("mounted {id}"));
        }

        fn ended(&self, id: &ActorId, why: &Ended) {
            self.0.lock().unwrap().push(format!("ended {id} {why:?}"));
        }
    }

    /// **Every way an actor arrives is reported, and every way it leaves.**
    ///
    /// Four of the five report sites in one sequence, in order. The two that
    /// carry the design are the ones with no caller to report from: a **lazy
    /// spawn** happens inside a factory that no caller can see, and it is why
    /// this is a port on the registry rather than a line at each `mount` call
    /// site. The other two arms are the ones that must **not** report: a refused
    /// mount announces nothing because there is no actor, and routing to an
    /// actor already there is not a second mount.
    ///
    /// Red-tested twice, once per direction: announcing from `Registry::mount`'s
    /// `MountError::Occupied` arm puts a fifth line in the list below (a mount
    /// that never happened), and making `Shared::route`'s vacant arm announce
    /// nothing removes the second (a mount nobody would have heard about).
    ///
    /// A third break was tried and **retracted**, because it proved the test was
    /// blind rather than sharp: announcing from `Shared::route`'s *Occupied*
    /// arm left this green. That arm is only reached when two messages to one new
    /// id race between the read lock and the write lock — the second `route` here
    /// finds the actor on `touch`'s fast path and never reaches the match at all.
    ///
    /// **That arm cannot be reached by a test from outside this module, and the
    /// lock order is why.** Reaching it needs the loser to miss `touch` *before*
    /// the winner takes the write lock. The obvious way to arrange that is a
    /// factory that blocks, since the factory runs under the write lock — but a
    /// winner holding the write lock also blocks the loser's `self.read()`, so by
    /// the time the loser can call `touch` the entry is in and it takes the fast
    /// path. Every ordering a caller can impose from outside collapses to the
    /// fast path; the arm needs a seam *between* `touch` and `self.write()`,
    /// which production code does not have and should not grow for a test. A
    /// probabilistic version — thirty routes and hope — would exercise the arm
    /// sometimes and pass either way, which is the vacuous shape this file spends
    /// its length objecting to.
    ///
    /// What holds it instead: the arm is four lines, it clones a sender and
    /// stamps `last_seen`, and it is the only arm that does not announce. If it
    /// ever needs a real guard, the seam to add is a `#[cfg(test)]` hook after
    /// `touch` — recorded here so the next reader does not re-derive why the
    /// straightforward test does not work.
    ///
    /// So the "a second route is not a second mount" claim below is carried by
    /// the fast path, and the race arm is covered by argument rather than by this
    /// test.
    #[tokio::test]
    async fn every_way_an_actor_arrives_and_leaves_is_reported() {
        let reported = Reported::default();
        let system: Registry<Bare> = Registry::builder(Eviction::Never)
            .lifecycle(reported.clone())
            .spawn_with(|_| Some(idler()))
            .build();

        let by_hand = ActorId::new("by-hand").expect("valid");
        let lazily = ActorId::new("lazy").expect("valid");

        system.mount(by_hand.clone(), idler()).expect("free");
        assert!(
            system.mount(by_hand.clone(), idler()).is_err(),
            "the id is taken, and a refusal is not a mount"
        );
        system.route(&lazily).expect("the factory builds one");
        system
            .route(&lazily)
            .expect("and the second message finds it");

        assert!(matches!(
            system.stop(&by_hand, TURN).await,
            Some(Ended::Stopped)
        ));
        assert!(system.abort(&lazily).await);

        assert_eq!(
            reported.seen(),
            [
                "mounted by-hand",
                "mounted lazy",
                "ended by-hand Stopped",
                "ended lazy Vanished",
            ],
            "two mounts and two endings, in order, with nothing said about the \
             refusal or the second route"
        );
    }

    /// **An actor that ends itself leaves the registry, and says why.**
    ///
    /// The fourth way out, which had no site. Nothing here calls `shutdown`,
    /// nothing calls `abort`, and `Eviction::Never` means no sweeper runs — so
    /// every one of the three sites that used to remove an entry is *absent by
    /// construction*, and the loop returning is the whole of what happens.
    ///
    /// Three assertions, because the defect had three faces on one node at one
    /// instant and a test of any one of them alone would have missed the others:
    ///
    /// 1. `contains` is false — the entry is gone, so `Registry::status` and the
    ///    console's actor list cannot report an actor a call would refuse;
    /// 2. the id is **free again**, which is what a `mount` succeeding proves. A
    ///    stale entry made a self-ended id permanently unmountable under
    ///    `Eviction::Never`;
    /// 3. the ending is reported, **with the reason the loop had** — `Feed`, not
    ///    `Stopped` and not `Vanished`. That is the half `drain`'s synthesis
    ///    from a join could never have produced: a loop that returns normally
    ///    joins `Ok(())` whatever ended it.
    ///
    /// Red-tested with `scripts/break-once.sh` by replacing `Shared::reap`'s
    /// body with `let _ = (id, why);`, which is the code as it stood before this
    /// fix. Watched to fail on assertion 1, printing
    /// `a self-ended actor must not still be mounted`.
    #[tokio::test]
    async fn an_actor_that_ends_itself_is_unmounted_and_reported() {
        let reported = Reported::default();
        let system: Registry<Bare> = Registry::builder(Eviction::Never)
            .lifecycle(reported.clone())
            .build();

        let (end_it, go) = oneshot::channel();
        let alone = ActorId::new("alone").expect("valid");
        system
            .mount(alone.clone(), self_ending(go))
            .expect("the id is free");
        assert!(system.contains(&alone), "mounted, before it ends itself");

        end_it.send(()).expect("the actor is waiting on this");
        // The reap happens on the actor's own task, so the assertions below need
        // it to have been scheduled. `yield_now` is enough and a clock is not:
        // nothing in this test is timed.
        for _ in 0..8 {
            if !reported.seen().is_empty() && reported.seen().len() == 2 {
                break;
            }
            tokio::task::yield_now().await;
        }

        assert!(
            !system.contains(&alone),
            "a self-ended actor must not still be mounted"
        );
        assert!(
            system.is_empty(),
            "and `Registry::status` reads the same map: {:?}",
            system.status()
        );
        system
            .mount(alone.clone(), idler())
            .expect("the id a self-ended actor held must be free again");
        assert_eq!(
            reported.seen(),
            [
                "mounted alone",
                "ended alone Feed { name: \"ticks\" }",
                "mounted alone",
            ],
            "the loop's own reason, reported once, and the id reusable after it"
        );
    }

    /// **A shutdown that races the actor's own ending reports it exactly once.**
    ///
    /// The interlock `Shared::reap` rests on, in the shape that would break it:
    /// both the caller and the loop have a reason in hand at the same moment.
    /// `shutdown` takes the entry out **before** it awaits, so the reap that runs
    /// during that await finds nothing and says nothing — and the caller's answer
    /// and the `Lifecycle`'s are still the same single value, which is the
    /// property `Registry::stop` already promised.
    ///
    /// Red-tested by dropping the `is_some()` interlock from `reap` — reporting
    /// unconditionally — which puts `ended raced Feed { name: "ticks" }` in the
    /// list beside `ended raced Stopped`.
    #[tokio::test]
    async fn an_ending_the_loop_and_a_shutdown_both_saw_is_reported_once() {
        let reported = Reported::default();
        let system: Registry<Bare> = Registry::builder(Eviction::Never)
            .lifecycle(reported.clone())
            .build();

        let (end_it, go) = oneshot::channel();
        let raced = ActorId::new("raced").expect("valid");
        system
            .mount(raced.clone(), self_ending(go))
            .expect("the id is free");

        // Released and then shut down in the same breath: the loop is runnable
        // and `shutdown` removes the entry before its first await, so which of
        // the two gets there first is a race and the assertion is that it does
        // not matter.
        end_it.send(()).expect("the actor is waiting on this");
        let ended = system
            .stop(&raced, TURN)
            .await
            .expect("it was mounted when the call was made");
        assert!(
            matches!(ended, Ended::Stopped),
            "the loop had already returned, so the join is clean: got {ended:?}"
        );

        for _ in 0..8 {
            tokio::task::yield_now().await;
        }
        assert_eq!(
            reported.seen(),
            ["mounted raced", "ended raced Stopped"],
            "one mount, one ending, and never both determinations of it"
        );
    }

    /// **An eviction that destroys a busy actor is no longer silent**, which is
    /// the gap `Shared::sweep`'s own documentation said Wave 6 owned.
    ///
    /// A `shutdown_budget` below the longest turn destroys that turn where it
    /// stands, and until this wave **nothing anywhere said so**: the `Ended` the
    /// sweeper computed was dropped, an aborted task's observers never run, and
    /// the caller of an evicted actor is a peer whose stream is severed rather
    /// than answered. The actor here is mid-turn with a budget of half a turn,
    /// so the destruction is exact rather than sampled — the clock is paused and
    /// nothing rests on which of two timers fires first.
    ///
    /// `handled == 0` is what makes this a test of the *silent* case: an actor
    /// that had finished its turn would report the same `Vanished` for a much
    /// less interesting reason.
    ///
    /// The actor is wedged on a channel rather than on a timer, deliberately:
    /// the eviction clock is paused and advanced in this test, so a turn that
    /// ended because *that* advance reached its `sleep` would be a different
    /// test — and it is the one that was written first, where advancing 60 s to
    /// trigger the eviction also completed the 100 ms turn it was meant to
    /// destroy.
    ///
    /// Red-tested by dropping the `report_ended` call from `sweep`, which leaves
    /// only the mount reported.
    #[tokio::test(start_paused = true)]
    async fn an_eviction_that_destroys_a_busy_actor_is_no_longer_silent() {
        let reported = Reported::default();
        let finished = Arc::new(AtomicUsize::new(0));
        let system: Registry<Bare> = Registry::builder(Eviction::Idle {
            after: Duration::from_secs(60),
            shutdown_budget: TURN,
        })
        .lifecycle(reported.clone())
        .build();

        let (entered, started) = oneshot::channel();
        // Held for the whole test, so the turn below is parked rather than
        // resumed by a dropped sender.
        let (_hold, held) = oneshot::channel::<()>();

        let one = ActorId::new("one").expect("valid");
        system
            .mount(one.clone(), wedged(entered, held, &finished))
            .expect("free");
        system
            .route(&one)
            .expect("mounted")
            .mail
            .send((Box::new(Bare), Chain::Empty))
            .await
            .expect("the one slot is free");
        started.await.expect("the actor enters its turn");

        tokio::time::advance(Duration::from_secs(60)).await;
        system.evict_idle().await;
        // The sweeper's own tick lands at the same instant this call does, so
        // which task took the entry is a race and which reported it is not:
        // whoever drained it reports once, and `sleep` on a paused runtime is
        // what lets that drain's budget expire. `advance` plus `yield_now` would
        // not — a ready task keeps the runtime from reaching the timer.
        for _ in 0..100 {
            if reported.seen().len() == 2 {
                break;
            }
            tokio::time::sleep(TURN).await;
        }

        assert!(
            !system.contains(&one),
            "sixty seconds of a sixty second policy"
        );
        assert_eq!(
            finished.load(Ordering::SeqCst),
            0,
            "the turn was destroyed mid-flight, which is the case that was silent"
        );
        assert_eq!(
            reported.seen(),
            ["mounted one", "ended one Vanished"],
            "and the ending the sweeper used to drop is now somebody's answer"
        );
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
    /// `derive(Debug)` on [`Mountable`] would make it do — and nothing else would
    /// notice, because nothing else here prints one. `Bare` is not `Debug`, so
    /// this line is the notice.
    #[tokio::test]
    async fn a_mounted_actor_prints_without_help_from_its_message_type() {
        assert!(format!("{:?}", idler()).starts_with("Mountable"));
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
        let system = Registry::builder(idle_after(Duration::from_secs(60))).build();
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
        let system = Registry::builder(idle_after(Duration::from_secs(60))).build();
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
        let system = Registry::builder(idle_after(Duration::from_secs(60))).build();
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
        let system = Registry::builder(Eviction::Never).build();
        let forgotten = ActorId::new("forgotten").expect("valid");
        system.mount(forgotten.clone(), idler()).expect("free");

        tokio::time::advance(Duration::from_secs(60 * 60 * 24)).await;
        system.evict_idle().await;

        assert!(system.contains(&forgotten));
    }

    #[test]
    fn a_system_without_a_factory_never_grows_by_itself() {
        let system: Registry<Bare> = Registry::builder(Eviction::Never).build();
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
    /// [`Registry::mount`], and from the factory through [`Registry::route`].
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
        let system = Registry::builder(Eviction::Never)
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
        let system = Registry::builder(Eviction::Never).capacity(1).build();
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
        let system = Registry::builder(Eviction::Never).build();
        let budget = Duration::from_secs(60);
        let drained = ActorId::new("drained").expect("valid");
        let cancelled = ActorId::new("cancelled").expect("valid");
        system.mount(drained.clone(), idler()).expect("free");
        system.mount(cancelled.clone(), idler()).expect("free");
        assert_eq!(system.len(), 2);

        assert!(matches!(
            system.stop(&drained, budget).await,
            Some(Ended::Stopped)
        ));
        assert!(system.abort(&cancelled).await);
        assert!(system.is_empty());

        assert!(
            system.stop(&drained, budget).await.is_none(),
            "gone stays gone"
        );
        assert!(!system.abort(&cancelled).await);
    }

    /// A task ending after its id was reused must not reap the replacement.
    ///
    /// Shutdown removes the old entry before it awaits, deliberately allowing a
    /// new incarnation to mount while the old one drains. The old task therefore
    /// needs entry identity as well as the public id when it reaps itself.
    #[tokio::test]
    async fn a_late_old_ending_does_not_reap_a_remounted_incarnation() {
        let reports = Reported::default();
        let system: Registry<Bare> = Registry::builder(Eviction::Never)
            .lifecycle(reports.clone())
            .build();
        let id = ActorId::new("reused").expect("valid");
        let (finish_old, old_finishes) = oneshot::channel();
        system
            .mount(id.clone(), self_ending(old_finishes))
            .expect("the old incarnation mounts");

        let shutting_down = {
            let system = system.clone();
            let id = id.clone();
            tokio::spawn(async move { system.stop(&id, Duration::from_secs(60)).await })
        };
        settle().await;
        assert!(!system.contains(&id), "shutdown unmounts before awaiting");

        let (finish_new, new_finishes) = oneshot::channel();
        system
            .mount(id.clone(), self_ending(new_finishes))
            .expect("the replacement mounts while the old actor drains");
        let new_ending = system.ended(&id).expect("the replacement is mounted");

        finish_old.send(()).expect("the old actor may finish");
        assert!(matches!(
            shutting_down.await.expect("shutdown task joins"),
            Some(Ended::Stopped)
        ));
        assert!(
            system.contains(&id),
            "the old task's late reap must leave the replacement mounted"
        );

        finish_new.send(()).expect("the replacement may finish");
        assert!(matches!(new_ending.await, Ended::Feed { name: "ticks" }));
        settle().await;
        assert!(!system.contains(&id));
        assert_eq!(
            reports.seen(),
            vec![
                "mounted reused",
                "mounted reused",
                "ended reused Stopped",
                r#"ended reused Feed { name: "ticks" }"#,
            ]
        );
    }

    /// Cancelling shutdown still settles both observation doors.
    #[tokio::test]
    async fn a_cancelled_shutdown_reports_vanished_to_direct_and_lifecycle_waiters() {
        let reports = Reported::default();
        let system: Registry<Bare> = Registry::builder(Eviction::Never)
            .lifecycle(reports.clone())
            .build();
        let id = ActorId::new("cancelled-shutdown").expect("valid");
        let (_never_finish, waits_forever) = oneshot::channel();
        system
            .mount(id.clone(), self_ending(waits_forever))
            .expect("the actor mounts");
        let ending = system.ended(&id).expect("the actor is mounted");

        let shutdown = {
            let system = system.clone();
            let id = id.clone();
            tokio::spawn(async move { system.stop(&id, Duration::from_secs(60)).await })
        };
        settle().await;
        assert!(!system.contains(&id), "shutdown has taken the entry");
        shutdown.abort();
        assert!(shutdown
            .await
            .expect_err("the outer shutdown is cancelled")
            .is_cancelled());

        assert!(matches!(ending.await, Ended::Vanished));
        assert_eq!(
            reports.seen(),
            vec![
                "mounted cancelled-shutdown",
                "ended cancelled-shutdown Vanished",
            ]
        );
    }

    /// Cancelling the wait around `abort` cannot abandon lifecycle observers.
    ///
    /// The actor deliberately stays inside one poll after cancellation is
    /// requested. That holds the join pending long enough to cancel the outer
    /// future without a clock or a scheduling guess.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_cancelled_abort_reports_vanished_to_direct_and_lifecycle_waiters() {
        let reports = Reported::default();
        let system: Registry<Bare> = Registry::builder(Eviction::Never)
            .lifecycle(reports.clone())
            .build();
        let id = ActorId::new("cancelled-abort").expect("valid");
        let release = Arc::new(AtomicBool::new(false));
        let released = Arc::clone(&release);
        let (entered, entering) = oneshot::channel();
        let (finished, finishing) = oneshot::channel();
        system
            .mount(
                id.clone(),
                mount(|mut inbox: Mailbox<Bare>| async move {
                    let _ = inbox.recv().await;
                    let _ = entered.send(());
                    while !released.load(Ordering::Acquire) {
                        std::thread::yield_now();
                    }
                    let _ = finished.send(());
                    Ended::Stopped
                }),
            )
            .expect("the actor mounts");
        let ending = system.ended(&id).expect("the actor is mounted");
        system
            .route(&id)
            .expect("the actor is routed")
            .mail
            .send((Box::new(Bare), Chain::default()))
            .await
            .expect("the actor receives one turn");
        entering.await.expect("the actor entered its turn");

        let aborting = {
            let system = system.clone();
            let id = id.clone();
            tokio::spawn(async move { system.abort(&id).await })
        };
        while system.contains(&id) {
            tokio::task::yield_now().await;
        }
        aborting.abort();
        let cancellation = aborting.await;

        let mut ending = Box::pin(ending);
        let waker = std::task::Waker::noop();
        let mut task_cx = std::task::Context::from_waker(waker);
        let direct = ending.as_mut().poll(&mut task_cx);
        let during = reports.seen();

        // Release the deliberately non-cooperative poll before any assertion can
        // panic, so a red run still leaves no spinning task behind.
        release.store(true, Ordering::Release);
        let _ = finishing.await;
        settle().await;
        let after = reports.seen();

        assert!(cancellation
            .expect_err("the outer abort is cancelled")
            .is_cancelled());
        assert!(matches!(direct, std::task::Poll::Ready(Ended::Vanished)));
        assert_eq!(
            during,
            vec!["mounted cancelled-abort", "ended cancelled-abort Vanished",]
        );
        assert_eq!(after, during, "the late actor task must not report twice");
    }

    /// Abort publishes one final reason to both direct and callback observers.
    #[tokio::test]
    async fn abort_reports_the_same_final_ending_to_both_observation_doors() {
        let reports = Reported::default();
        let system: Registry<Bare> = Registry::builder(Eviction::Never)
            .lifecycle(reports.clone())
            .build();
        let id = ActorId::new("aborted-ending").expect("valid");
        system.mount(id.clone(), idler()).expect("the actor mounts");
        let ending = system.ended(&id).expect("the actor is mounted");

        assert!(system.abort(&id).await);
        assert!(matches!(ending.await, Ended::Vanished));
        assert_eq!(
            reports.seen(),
            vec!["mounted aborted-ending", "ended aborted-ending Vanished"]
        );
    }

    /// The refusal that used to cost a task, at the witness type.
    ///
    /// `Mountable` holds an unstarted future, so a refused mount drops a loop that
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
                Ended::Stopped
            })
        };

        let system = Registry::builder(Eviction::Never).capacity(1).build();
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

    // ───────────────────────────────────────────────────── the mailbox's depth

    /// **A full mailbox blocks the sender.** It does not drop the message and it
    /// does not hand back an error.
    ///
    /// Those are the three things an overflow policy can do, and this watches all
    /// three rather than asserting the one: the send has **not returned** while the
    /// mailbox is full, so it is not a refusal; it returns **`Ok`** once a slot
    /// frees, so nothing was dropped; and the actor counts **both** messages, so
    /// nothing was lost between the two.
    ///
    /// Red-tested by moving [`DEFAULT_MAILBOX_DEPTH`] from one to two, through
    /// `scripts/break-once.sh` on that constant's own line: with a second slot the
    /// second send completes at once and the parked assertion fires. Watched.
    ///
    /// The break is described rather than pasted, and every red-test note in this
    /// module is, for a mechanical reason worth knowing: `break-once.sh` requires
    /// its literal to occur **exactly once** in the file it edits, and a doc
    /// comment quoting the line it breaks is a second occurrence. Pasted commands
    /// live in the files whose code is elsewhere.
    #[tokio::test]
    async fn a_full_mailbox_blocks_the_sender_and_never_loses_the_message() {
        let (open, gate) = oneshot::channel();
        let taken = Arc::new(AtomicUsize::new(0));
        let system = Registry::builder(Eviction::Never).build();
        let held = ActorId::new("held").expect("valid");
        system
            .mount(held.clone(), gated(gate, &taken))
            .expect("free");

        let mail = system.route(&held).expect("mounted").mail;
        assert_eq!(only(&system).buffered, 0, "nothing is waiting yet");
        mail.send((Box::new(Bare), Chain::Empty))
            .await
            .expect("the one slot is free");

        // Nowhere for a second message to go: one slot, one message in it, and the
        // loop is not taking anything until the gate opens.
        let (sent, mut second) = oneshot::channel();
        let parked = mail.clone();
        n0_future::task::spawn(async move {
            let _ = sent.send(parked.send((Box::new(Bare), Chain::Empty)).await.is_ok());
        });
        settle().await;

        assert!(
            matches!(second.try_recv(), Err(oneshot::error::TryRecvError::Empty)),
            "a send into a full mailbox must park. A return here is an overflow \
             policy — a refusal or a drop — and there is deliberately none"
        );
        let full = only(&system);
        assert_eq!(
            full.buffered, 1,
            "the default depth is one slot, and it is full"
        );
        assert!(!full.busy, "the loop is at its gate: no turn has started");
        assert_eq!(taken.load(Ordering::SeqCst), 0);

        let _ = open.send(());
        assert!(
            second.await.expect("the sending task reports"),
            "the parked send must complete, and complete successfully: a full \
             mailbox delays a message, and never refuses or drops one"
        );

        // Both messages, counted by the actor itself. `drop(mail)` first, because
        // the loop ends when every sender is gone and this test holds one.
        drop(mail);
        assert!(matches!(
            system.stop(&held, Duration::from_secs(60)).await,
            Some(Ended::Stopped)
        ));
        assert_eq!(
            taken.load(Ordering::SeqCst),
            2,
            "the blocked message was delivered, not dropped"
        );
    }

    /// **`buffered` is exact for what the mailbox holds, and counts nobody parked
    /// behind it.**
    ///
    /// Both halves at once, at a depth where the two can differ: three messages in
    /// three slots is a `buffered` of 3, and a fourth sender parked on a permit
    /// leaves it at 3. That is the floor the field's documentation claims, observed
    /// — and the reason the field is not called `queued`, which at this moment
    /// would be four.
    ///
    /// Red-tested by deleting the subtraction in [`Registry::status`], so that
    /// `buffered` reports the channel's capacity instead of its contents. Watched:
    /// this test, `a_full_mailbox_blocks_the_sender_and_never_loses_the_message` and
    /// `status_reports_every_actor_with_the_idle_for_eviction_reads` all fail.
    ///
    /// **The first version of this test passed that break**, which is why the
    /// zero-and-then-one-at-a-time readings above exist: it asserted only "3 with
    /// three messages queued", and a field reporting the depth says 3 too. A guard
    /// that cannot tell the measurement from the constant is not a guard, and this
    /// one was watched to be vacuous before it was watched to fail.
    #[tokio::test]
    async fn buffered_is_exact_at_depth_and_counts_no_parked_sender() {
        let (open, gate) = oneshot::channel();
        let taken = Arc::new(AtomicUsize::new(0));
        let system = Registry::builder(Eviction::Never)
            .mailbox_depth(depth(3))
            .build();
        let held = ActorId::new("held").expect("valid");
        system
            .mount(held.clone(), gated(gate, &taken))
            .expect("free");

        let mail = system.route(&held).expect("mounted").mail;
        // Counted up one at a time, and from zero, because that is what makes this
        // a count rather than a restatement of the depth. A `buffered` that simply
        // reported `max_capacity()` would satisfy "3 at three messages" — measured:
        // deleting the subtraction in `status` left this test passing until these
        // intermediate readings were added.
        assert_eq!(
            only(&system).buffered,
            0,
            "an untouched mailbox holds nothing, whatever its depth"
        );
        for expected in 1..=3 {
            mail.send((Box::new(Bare), Chain::Empty))
                .await
                .expect("three slots, three messages");
            assert_eq!(
                only(&system).buffered,
                expected,
                "each message is counted as it lands"
            );
        }

        let (sent, mut fourth) = oneshot::channel();
        let parked = mail.clone();
        n0_future::task::spawn(async move {
            let _ = sent.send(parked.send((Box::new(Bare), Chain::Empty)).await.is_ok());
        });
        settle().await;
        assert!(
            matches!(fourth.try_recv(), Err(oneshot::error::TryRecvError::Empty)),
            "the fourth sender is parked on a permit"
        );
        assert_eq!(
            only(&system).buffered,
            3,
            "and it is invisible: `buffered` is a FLOOR on the backlog, because \
             tokio's Semaphore does not expose its waiters"
        );

        let _ = open.send(());
        assert!(fourth.await.expect("the sending task reports"));
        settle().await;
        assert_eq!(
            only(&system).buffered,
            0,
            "and it falls again as the loop drains: a count, in both directions"
        );
        drop(mail);
        assert!(matches!(
            system.stop(&held, Duration::from_secs(60)).await,
            Some(Ended::Stopped)
        ));
        assert_eq!(taken.load(Ordering::SeqCst), 4);
    }

    /// **`status` reports every mounted actor, on the clock eviction reads.**
    ///
    /// The two numbers are made to agree at the boundary rather than merely both
    /// exist: at 59 s of a 60 s policy `idle_for` says 59 and the sweep leaves the
    /// actor alone; one second later the sweep takes it. An `idle_for` computed off
    /// a second clock — `std::time`, which panics in a browser, or a per-status
    /// `Instant` — would drift from the decision and this is where it would show.
    ///
    /// Routing is what resets it, which is what `idle_for` means: last *addressed*,
    /// not last handled. A message still sitting in the mailbox has already reset
    /// it, exactly as it has for eviction.
    #[tokio::test(start_paused = true)]
    async fn status_reports_every_actor_with_the_idle_for_eviction_reads() {
        let system = Registry::builder(idle_after(Duration::from_secs(60))).build();
        let one = ActorId::new("one").expect("valid");
        let two = ActorId::new("two").expect("valid");
        system.mount(one.clone(), idler()).expect("free");
        system.mount(two.clone(), idler()).expect("free");

        tokio::time::advance(Duration::from_secs(59)).await;
        let rows = system.status();
        assert_eq!(rows.len(), 2, "every mounted actor, not a sample");
        for row in &rows {
            assert_eq!(row.idle_for, Duration::from_secs(59));
            assert!(!row.busy, "an idler is never in a turn");
            assert_eq!(row.buffered, 0, "and nothing is waiting for it");
        }
        system.evict_idle().await;
        assert_eq!(
            system.len(),
            2,
            "59 s of a 60 s policy is not idle enough, which is the same number \
             `status` just reported"
        );

        system.route(&one).expect("mounted");
        let addressed = system
            .status()
            .into_iter()
            .find(|row| row.id == one)
            .expect("still mounted");
        assert_eq!(
            addressed.idle_for,
            Duration::ZERO,
            "routing resets it, because routing is what eviction counts from"
        );

        tokio::time::advance(Duration::from_secs(1)).await;
        system.evict_idle().await;
        assert!(system.contains(&one), "one second idle, of sixty");
        assert!(
            !system.contains(&two),
            "and the actor `status` reported at 59 s is the one the next second took"
        );
    }

    /// **The shutdown budget's meaning is depth-dependent**, and a budget sized
    /// against the default silently under-drains at any deeper one.
    ///
    /// The same budget, the same actor, the same four-turn arithmetic, twice: at
    /// depth 1 there is one queued turn plus one in flight and the budget covers
    /// both, so the actor drains and reports [`Ended::Stopped`]. At depth 3 there
    /// are three queued plus one in flight, the budget covers two of the four, and
    /// the actor is **destroyed** with two turns' work thrown away — which is the
    /// detach defect the budget exists to remove, arriving through the other door.
    ///
    /// Both numbers are exact rather than sampled: the clock is paused, a turn is
    /// one `TURN`, and the budget is two and a half of them, so no assertion rests
    /// on which of two simultaneous timers a runtime fires first.
    ///
    /// Red-tested by making [`Shared::entry`] build every channel at
    /// [`DEFAULT_MAILBOX_DEPTH`] instead of the system's depth — the one literal in
    /// its `mpsc::channel` argument, which is the whole of what the knob does.
    /// Watched: `Some(Stopped)` where this asserts `Vanished`, because a deep system
    /// whose mailboxes are shallow drains like a shallow one.
    #[tokio::test(start_paused = true)]
    async fn a_budget_sized_for_the_default_depth_under_drains_at_a_deeper_one() {
        /// Queue everything one mailbox of `slots` can hold, plus the one in
        /// flight, and shut down with `budget`.
        async fn drain_at(slots: usize, budget: Duration) -> (Option<Ended>, usize) {
            let handled = Arc::new(AtomicUsize::new(0));
            let system = Registry::builder(Eviction::Never)
                .mailbox_depth(depth(slots))
                .build();
            let id = ActorId::new("slow").expect("valid");
            system.mount(id.clone(), slow(&handled)).expect("free");

            let mail = system.route(&id).expect("mounted").mail;
            for _ in 0..=slots {
                mail.send((Box::new(Bare), Chain::Empty))
                    .await
                    .expect("a slot, or the loop");
            }
            // The registry's is not the only sender while this test holds one, and
            // a loop whose channel never closes cannot drain by any budget.
            drop(mail);
            let ended = system.stop(&id, budget).await;
            (ended, handled.load(Ordering::SeqCst))
        }

        let budget = TURN * 5 / 2;

        let (shallow, at_one) = drain_at(1, budget).await;
        assert!(
            matches!(shallow, Some(Ended::Stopped)),
            "two turns inside two and a half: {shallow:?}"
        );
        assert_eq!(at_one, 2, "one queued plus one in flight, both answered");

        let (deep, at_three) = drain_at(3, budget).await;
        assert!(
            matches!(deep, Some(Ended::Vanished)),
            "four turns do not fit in two and a half, so the actor is destroyed \
             rather than drained: {deep:?}"
        );
        assert_eq!(
            at_three, 2,
            "the same budget bought the same two turns — and at this depth two of \
             the four queued turns went with the actor"
        );
    }

    /// The depth is the **system's**, so it reaches an actor a factory built as
    /// well as one a caller mounted, and the default is one.
    ///
    /// The lazy half is why the knob is here rather than on the generated `spawn`:
    /// a factory is a `Fn(&ActorId) -> Option<Mountable<M>>` with no argument for a
    /// depth and no caller present to supply one, so a per-`spawn` depth would have
    /// covered every actor except the ones a serving node builds by itself.
    #[tokio::test]
    async fn the_mailbox_depth_is_the_systems_and_defaults_to_one() {
        let by_default = Registry::builder(Eviction::Never).build();
        let plain = ActorId::new("plain").expect("valid");
        by_default.mount(plain.clone(), idler()).expect("free");
        assert_eq!(
            by_default
                .route(&plain)
                .expect("mounted")
                .mail
                .max_capacity(),
            1,
            "the documented default: one message may wait ahead of the one being \
             handled"
        );

        let deeper = Registry::builder(Eviction::Never)
            .mailbox_depth(depth(4))
            .spawn_with(|_| Some(idler()))
            .build();
        let by_hand = ActorId::new("by-hand").expect("valid");
        deeper.mount(by_hand.clone(), idler()).expect("free");
        assert_eq!(
            deeper.route(&by_hand).expect("mounted").mail.max_capacity(),
            4
        );
        assert_eq!(
            deeper
                .route(&ActorId::new("lazily").expect("valid"))
                .expect("the factory builds it")
                .mail
                .max_capacity(),
            4,
            "including an actor nobody mounted by hand"
        );
    }

    // ─────────────────────── what a destroyed actor tells a waiting peer

    /// Reads its own actor's [`Standing`] as the task that holds it is destroyed.
    ///
    /// The stand-in for a `Deferred` parked in a real actor's state: both die
    /// exactly when the task's future is dropped, so what this records is what a
    /// peer woken by that death can read. It has to be an in-drop observer,
    /// because from outside the task there is nothing to see — the whole teardown
    /// runs before any other task is polled, so a served call always reads the
    /// final value and every ordering here would look correct.
    struct Recorder {
        fate: Fate,
        seen: Arc<std::sync::Mutex<Vec<Standing>>>,
    }

    impl Drop for Recorder {
        fn drop(&mut self) {
            self.seen
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .push(self.fate.standing());
        }
    }

    /// The incarnation: it holds the [`Recorder`] and never returns.
    ///
    /// A **body local** of [`lending`], which is the whole point — see there.
    async fn incarnation(recorder: Recorder, entered: oneshot::Sender<()>) {
        let _recorder = recorder;
        let _ = entered.send(());
        std::future::pending::<()>().await;
    }

    /// Holds the mailbox as a **parameter** and awaits the incarnation as a body
    /// local, so the mailbox outlives the recorder by the language's rule rather
    /// than by generator layout.
    ///
    /// Parameters drop after a body's locals, which is the same rule `run_loop`'s
    /// own floor rests on. So a destroyed task drops the recorder — and, in the
    /// real thing, the incarnation's state and every reply parked in it — while
    /// `Mailbox` is still very much alive. That is
    /// [`supervise`](crate::run_keyed_supervised)'s shape: the receiver belongs
    /// to the supervisor's frame and outlives every incarnation.
    ///
    /// **Two earlier shapes were both weaker, and the second one only looked
    /// stronger.** One future holding both made "the recorder drops first" a
    /// claim about generator drop order — the reasoning `turns.rs` refuses as *a
    /// fact about rustc rather than about this crate*. Splitting them into two
    /// `let`s in one block did not fix it: siblings in one scope drop in reverse
    /// declaration order, which is the same category of guarantee. Only the
    /// parameter/body-local split is structural, and if it ever flipped,
    /// `Mailbox`'s floor would write `Gone` before the recorder looked and both
    /// tests below would pass with the settle they are about deleted.
    async fn lending(inbox: Mailbox<Bare>, recorder: Recorder, entered: oneshot::Sender<()>) {
        incarnation(recorder, entered).await;
        // Unreachable: the incarnation never returns. Written so that `inbox` is
        // read here and cannot be mistaken for an unused parameter.
        drop(inbox);
    }

    /// A loop that will not return, holding a [`Recorder`] on its own cell.
    fn recording(
        entered: oneshot::Sender<()>,
        seen: &Arc<std::sync::Mutex<Vec<Standing>>>,
    ) -> Mountable<Bare> {
        let seen = Arc::clone(seen);
        mount(move |inbox: Mailbox<Bare>| async move {
            let recorder = Recorder {
                fate: inbox.fate(),
                seen,
            };
            lending(inbox, recorder, entered).await;
            Ended::Stopped
        })
    }

    /// [`Registry::abort`] settles the served answer **before** it destroys the
    /// task.
    ///
    /// An aborted loop never reaches `Inbox::ending`, so nothing inside it can say
    /// what was destroyed. `Mailbox`'s `Drop` is a floor and it is *too late* on
    /// the path that matters: a supervised keyed actor's mailbox belongs to the
    /// supervisor's frame, which outlives the incarnation whose state — and whose
    /// parked replies — the destruction kills first. So the publish is on this
    /// door, before the request lands on the runtime.
    ///
    /// Made red by `scripts/break-once.sh keylink/src/actor/registry.rs` on
    /// `Registry::abort`'s `entry.fate.settle(Standing::Gone);` →
    /// `entry.fate.settle(Standing::Running);`. Watched: `an aborted actor is
    /// gone before it is destroyed: [Running]`.
    #[tokio::test]
    async fn abort_settles_the_served_answer_before_it_destroys_the_actor() {
        let seen: Arc<std::sync::Mutex<Vec<Standing>>> = Arc::default();
        let (entered, started) = oneshot::channel();
        let system: Registry<Bare> = Registry::builder(Eviction::Never).build();
        let doomed = ActorId::new("doomed").expect("valid");
        system
            .mount(doomed.clone(), recording(entered, &seen))
            .expect("free");
        started.await.expect("the loop reaches its wait");

        assert!(system.abort(&doomed).await, "it was mounted");

        let seen = seen.lock().unwrap_or_else(PoisonError::into_inner).clone();
        assert!(
            matches!(seen.as_slice(), [Standing::Gone]),
            "an aborted actor is gone before it is destroyed: {seen:?}"
        );
    }

    /// And the other teardown door: a [`Registry::stop`] whose budget expires
    /// ends in an abort, and that abort needs the same publish.
    ///
    /// A separate line in [`drain`] rather than a share of the one above, because
    /// `abort` never drains and `drain` is the only place a *timed-out* teardown
    /// destroys anything.
    ///
    /// Made red by `scripts/break-once.sh keylink/src/actor/registry.rs` on
    /// `drain`'s `fate.settle(Standing::Gone);` → `fate.settle(Standing::Running);`.
    /// Watched: `an actor destroyed by an expired budget is gone before it is
    /// destroyed: [Running]`.
    #[tokio::test]
    async fn an_expired_shutdown_budget_settles_the_served_answer_before_the_abort() {
        let seen: Arc<std::sync::Mutex<Vec<Standing>>> = Arc::default();
        let (entered, started) = oneshot::channel();
        let system: Registry<Bare> = Registry::builder(Eviction::Never).build();
        let doomed = ActorId::new("doomed").expect("valid");
        system
            .mount(doomed.clone(), recording(entered, &seen))
            .expect("free");
        started.await.expect("the loop reaches its wait");

        assert!(
            matches!(
                system.stop(&doomed, Duration::from_millis(10)).await,
                Some(Ended::Vanished)
            ),
            "a loop that never returns cannot drain"
        );

        let seen = seen.lock().unwrap_or_else(PoisonError::into_inner).clone();
        assert!(
            matches!(seen.as_slice(), [Standing::Gone]),
            "an actor destroyed by an expired budget is gone before it is \
             destroyed: {seen:?}"
        );
    }
}
