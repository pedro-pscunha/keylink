//! N interchangeable actors, one routing rule, and a permit instead of a queue.
//!
//! A [`Pool`] hands back a handle and lets the caller decide what to
//! do with it. A factory takes the next step and decides *which* member, by a rule
//! the caller named once at construction — including the one a pool refuses,
//! `hash(key) % N` onto a fixed set of workers, which is the only routing this
//! crate had no spelling for at all.
//!
//! **`ignore` and not `no_run`**, in a module that watches two `compile_fail`
//! fences in both directions: the `parse` and `render` modules below are what a
//! `service!` invocation generates, and neither exists — so this fence is checked
//! by the two `service!`-built factories in `keylink/tests/compose.rs`, which
//! compile and run, rather than by rustdoc. It is the crate's convention for a
//! snippet naming generated items, and `turns.rs`, `peer.rs` and
//! `actor/registry.rs` all take it.
//!
//! ```ignore
//! // Keyed: `Factory<parse::Handle<Parser>, TenantId>`, and `dispatch_keyed` is
//! // its only sensible door.
//! let ingest = Factory::from_fn(
//!     NonZeroUsize::new(8).expect("eight"),
//!     Routing::ByKey(Shard::by_hash()),
//!     |member| parse::actor(Parser::new(member)),
//! );
//!
//! let parsed = ingest.dispatch_keyed(tenant, |worker| worker.parse(blob)).await??;
//!
//! // Non-keyed: `Factory<render::Handle<Renderer>, ()>`, and `dispatch` takes no
//! // key. The members are built with `setup(..).backpressure(..)` on purpose — `LeastLoaded` has
//! // no gradient to read at the default mailbox depth of one, where `queued` is
//! // zero or one and the mode is reading N booleans.
//! let pages = Factory::from_fn(
//!     NonZeroUsize::new(8).expect("eight"),
//!     Routing::LeastLoaded(Sample::TwoOfN),
//!     |member| {
//!         render::setup(Renderer::new(member))
//!             .feeds(render::Feeds::new())
//!             .backpressure(Backpressure::Park(NonZeroUsize::new(32).expect("thirty-two")))
//!             .actor()
//!     },
//! );
//!
//! let png = pages.dispatch(|worker| worker.render(doc)).await??;
//! ```
//!
//! # The admission decision is the whole design
//!
//! Every door hands the caller's closure a **[`Permit`]** rather
//! than a handle, and that one choice is what removes everything else. The permit
//! is the admission decision, taken before the payload moves: by the time the
//! closure runs, the slot is held, so the send it makes cannot be refused for
//! fullness and cannot wait again. A caller under
//! [`Backpressure::Shed`](crate::Backpressure) therefore loses nothing here — the
//! refusal arrives before any argument was built.
//!
//! ractor's factory is the comparison this module was designed against. Its
//! mailbox is unbounded, so most of that design — a queue depth, discard reasons,
//! TTL sweeps, a dynamic limit pushed to workers on a timer — exists to rebuild a
//! bound its channel threw away. keylink's mailbox has permits, so none of it has
//! anywhere to live:
//!
//! - **No queue of its own.** Work waiting in a factory's queue is work no member
//!   has admitted, which means the caller was told "accepted" by something that
//!   cannot run it. [`Pool`] carries that argument at length and it
//!   governs here unchanged.
//! - **No depth, no drop policy, no TTL.** Queue depth is the thing a permit
//!   already is, and a TTL on a queue that does not exist is a clock for nothing.
//!   **A factory caller's deadline is the caller's own, and it is
//!   `n0_future::time::timeout` around the dispatch.**
//!   [`QueueTimeout`](crate::QueueTimeout) is *not* it: that is a
//!   [`Middleware`](crate::Middleware) on the **served** path, and both doors here
//!   are local — [`Permit::ask`](crate::Permit::ask) builds an
//!   [`Origin`](crate::Origin) that carries no
//!   [`Queueing`](crate::Queueing), so neither of its checks can fire for a
//!   dispatch. That includes a dispatch parked on the wedged member
//!   [`ByKey`](Routing::ByKey) describes below, which is exactly the case a reader
//!   would reach for it in.
//!
//!   Wrapping the dispatch is safe rather than merely available: a cancelled
//!   [`reserve`](Reserving::reserve) is taken out of the wait list and any permit
//!   already assigned to it is returned, which is the same property
//!   [`FirstAvailable`](Routing::FirstAvailable) rests on when it drops N−1
//!   racers. A timeout therefore abandons the wait and leaks no slot.
//! - **No background task and no clock.** Nothing here ticks, sweeps or times
//!   anything out, so a factory adds no task to the members it holds.
//! - **Nothing to hand back on a refusal.** The factory never owns a payload:
//!   the closure builds the message, and it only runs once a slot is in hand.
//!
//! What is left is a value over N handles and a rule.
//!
//! # A factory of its own costs a `Box<[H]>` and an `AtomicUsize`
//!
//! [`from_fn`](Factory::from_fn) runs the caller's factory `count` times on the
//! caller's own task, exactly as [`Pool::from_fn`](super::Pool::from_fn) does, so
//! a factory of actors is `count` spawns and it panics outside a runtime for the
//! reason every spawn does. **That is the members' cost and not the factory's**:
//! it holds no task, no timer and no lock of its own, and the sentence above is
//! about what this type adds rather than about what the caller asked it to build.
//! Two clones share one cursor, for [`Pool`]'s reason: two cursors
//! would each start at member zero, and a service handing a clone to every
//! connection would send every first request to the same actor.
//!
//! # What a factory is not
//!
//! - **Not a supervisor.** [`Pool`]'s refusal, unchanged and for the
//!   same reason: build members with the generated
//!   `restarting(factory, limit).actor()`, and a supervised member's mailbox
//!   outlives its incarnations, so routing keeps
//!   working across a restart without this module learning what one is. Nothing
//!   here restarts a member, kills a stuck one, or takes one out of the rotation.
//! - **Not a way to make one actor concurrent.** Eight members are eight states.
//!   If the state cannot be split, a factory is eight wrong answers instead of one
//!   slow one — and [`Routing::ByKey`] is the mode that makes the split explicit,
//!   because a key is the name of the shard the caller believes in.
//! - **Not resizable.** The member count is a [`NonZeroUsize`] taken once, and the
//!   members are a `Box<[H]>` built once. [`Routing`] has no `Sticky` mode for
//!   exactly this reason: a mode whose only distinguishing input is a resize is,
//!   at a fixed member count, the same function as [`ByKey`](Routing::ByKey). It
//!   is refused here and re-opens exactly when a resize ships, at which point the
//!   serialization story is written against the resize that makes it necessary
//!   rather than in advance of it.
//! - **Not a `Calls` implementor.** `service!` generates that trait per service,
//!   so it is named here as text rather than linked, for the reason this module's
//!   parent names two private constants that way. No generated `rpc` signature
//!   has room for a key, so [`ByKey`](Routing::ByKey) is unwritable through it;
//!   and the mode is a runtime field, so one impl would be right for three
//!   variants and wrong for the fourth — which is the illegal state [`Routing`]
//!   exists to remove, put back one level out. A caller who wants a
//!   surface-agnostic function keeps writing it against `Calls` and passes one
//!   member's handle.
//!
//! [`NonZeroUsize`]: std::num::NonZeroUsize

use std::collections::hash_map::DefaultHasher;
use std::fmt;
use std::future::Future;
use std::hash::{Hash, Hasher};
use std::num::NonZeroUsize;

use n0_future::{FuturesUnordered, StreamExt};

use super::{Member, Pool};
use crate::{LocalActor, NotQueued, Permit, Serving};

/// What a [`Factory`] needs from a member, and nothing more.
///
/// Two methods, because four routing modes need exactly two facts: a slot, and a
/// load reading for the one mode that chooses by load. There is no `try_reserve`
/// here and that is deliberate — no mode calls one, and a method on a trait that
/// nothing calls is a second way to do a thing.
///
/// Implemented for [`LocalActor`] and, by `service!`, for every generated
/// `Handle`. **This is the one line `service!` generates because `compose`
/// exists**, and `compose`'s own module doc used to claim there were none.
///
/// A caller can implement it for a handle of their own, which is what makes a
/// factory over a hand-written mailbox possible without this module knowing about
/// one.
pub trait Reserving {
    /// The permit this member hands out.
    ///
    /// Generic over the borrow because a permit borrows the handle it came from —
    /// [`LocalActor::reserve_owned`] is the `'static` half and a factory does not
    /// need it, since the closure spends the slot before
    /// [`dispatch`](Factory::dispatch) returns.
    type Permit<'a>
    where
        Self: 'a;

    /// Wait for a slot in this member's mailbox and hold it.
    ///
    /// `+ Send` is stated rather than inferred, for the reason
    /// `service!`'s own generated methods state it: an `async fn` in a trait does
    /// not promise it, and a caller whose future has to cross a `tokio::spawn`
    /// would find every door on this type unusable without it.
    fn reserve(&self) -> impl Future<Output = Result<Self::Permit<'_>, NotQueued>> + Send;

    /// [`busy`](LocalActor::busy) and [`queued`](LocalActor::queued),
    /// folded, and read only by [`Routing::LeastLoaded`].
    ///
    /// A factory's members are N readings from **one** side, which is the
    /// comparison [`Serving::queued`] licenses. Comparing one of these against a
    /// keyed actor's is the one it forbids, and nothing here does.
    fn status(&self) -> Serving;
}

impl<A, M> Reserving for LocalActor<A, M>
where
    A: Send + 'static,
    M: Send + 'static,
{
    type Permit<'a>
        = Permit<'a, A, M>
    where
        Self: 'a;

    fn reserve(&self) -> impl Future<Output = Result<Self::Permit<'_>, NotQueued>> + Send {
        LocalActor::reserve(self)
    }

    fn status(&self) -> Serving {
        self.serving()
    }
}

/// How many members [`Routing::LeastLoaded`] reads before it chooses.
///
/// Constructed by a caller and matched only inside this crate, which is
/// [`Routing`]'s own argument for the attribute below.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum Sample {
    /// Read every member. Exact, and N relaxed loads per dispatch.
    All,
    /// Read two and keep the better.
    ///
    /// **The two come from the cursor, not from a random draw**, and the
    /// difference is worth stating because the name is borrowed from the
    /// power-of-two-choices result, which assumes independent uniform draws.
    /// keylink has no random number generator and this mode is not worth adding
    /// one for. What the cursor buys is the property the mode was actually wanted
    /// for: concurrent dispatchers get *disjoint* pairs rather than independent
    /// ones, so they cannot all read the same member as least loaded and then all
    /// queue behind it.
    TwoOfN,
}

/// `hash(key) % N`, and which hash.
///
/// A function pointer and not a [`BuildHasher`](std::hash::BuildHasher): a shard
/// function has to be deterministic to be worth anything, so it has no state to
/// build, and a hasher type parameter would cascade through [`Routing`] and
/// [`Factory`] to buy a generality nobody asked for.
///
/// # The default hash is not stable across releases, and that is a caller's problem to know about
///
/// [`by_hash`](Self::by_hash) uses `std`'s `DefaultHasher`, whose algorithm the
/// standard library explicitly does not promise across Rust versions. Within one
/// binary it is stable, which is all that "the same tenant lands on the same
/// worker" needs while the process runs. A caller who warms a per-worker cache
/// from disk, or who shards the same keyspace from two binaries, needs a hash that
/// survives a rebuild and passes one to [`by`](Self::by).
pub struct Shard<K> {
    /// Contravariant in `K`, which is why no `PhantomData` appears here and why a
    /// `Shard` is `Send`, `Sync` and `Copy` whatever `K` is.
    of: fn(&K) -> u64,
}

impl<K: Hash> Shard<K> {
    /// The key's own [`Hash`], through `std`'s `DefaultHasher`.
    ///
    /// Read the caveat on the type before warming anything from disk with it.
    #[must_use]
    pub fn by_hash() -> Self {
        Self { of: hash_of::<K> }
    }
}

impl<K> Shard<K> {
    /// A hash the caller owns, for when the default's instability matters.
    ///
    /// The only requirement is determinism: two calls with equal keys must return
    /// equal numbers, or `ByKey` stops being affinity and becomes an expensive
    /// round-robin.
    #[must_use]
    pub fn by(of: fn(&K) -> u64) -> Self {
        Self { of }
    }
}

/// `std`'s own default, in a function so that [`Shard`] can hold a pointer to it.
fn hash_of<K: Hash>(key: &K) -> u64 {
    let mut hasher = DefaultHasher::new();
    key.hash(&mut hasher);
    hasher.finish()
}

/// By hand, because `#[derive]` would demand `K: Clone` for a field that is a
/// function pointer and copies regardless.
impl<K> Clone for Shard<K> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<K> Copy for Shard<K> {}

impl<K> fmt::Debug for Shard<K> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Shard").finish_non_exhaustive()
    }
}

/// How a [`Factory`] chooses which member handles a dispatch.
///
/// One enum and not five booleans and a closure, for the reason every state
/// machine in this crate is one: *round-robin and by-key at once* is not a state
/// that exists, and five booleans can spell it.
///
/// # `#[non_exhaustive]`, where [`Overflow`](super::Overflow) refuses it
///
/// The two conclusions come from one rule — **the difference is who matches.** A
/// caller constructs an `Overflow` *and matches one*, so sealing it would cost
/// every call site a wildcard arm. A caller constructs a `Routing` and never
/// matches one: the factory matches it, inside this crate. So the attribute costs
/// a caller nothing here and makes a fifth mode additive.
///
/// A doctest, because it compiles as its own crate and a probe in
/// `representability.rs` cannot: in there the attribute is not in force.
///
/// ```compile_fail
/// // Fails with `E0004`: a match on an enum marked `#[non_exhaustive]` needs a
/// // wildcard arm outside the crate that declared it, and this one has none.
/// //
/// // The code is named HERE and deliberately NOT on the fence. rustdoc does not
/// // check the code after `compile_fail`, so an annotated code asserts a
/// // verification it does not perform — the same rule `TurnInfo`'s literal guard
/// // and `Reply::channel`'s privacy guard are written under.
/// //
/// // What stops this passing for the WRONG reason — a renamed variant, a moved
/// // type, a changed payload — is the array. Every variant is CONSTRUCTED there
/// // before it is matched, in code that has to compile, so the only thing left
/// // to fail is the attribute. An earlier draft of this type shipped WITHOUT the
/// // attribute while this section argued for it, and nothing noticed: nothing
/// // downstream matches a `Routing`, so nothing would.
/// use keylink::compose::{Routing, Sample, Shard};
///
/// let modes: [Routing<u64>; 4] = [
///     Routing::RoundRobin,
///     Routing::FirstAvailable,
///     Routing::LeastLoaded(Sample::All),
///     Routing::ByKey(Shard::by_hash()),
/// ];
///
/// for mode in modes {
///     match mode {
///         Routing::RoundRobin => {}
///         Routing::FirstAvailable => {}
///         Routing::LeastLoaded(_) => {}
///         Routing::ByKey(_) => {}
///     }
/// }
/// ```
///
/// Watched both ways, which is what a fence needs and an error code on it does
/// not give: with the annotation removed the compiler printed
/// `error[E0004]: non-exhaustive patterns: `_` not covered`, and the note
/// *"`Routing<u64>` is marked as non-exhaustive, so a wildcard `_` is necessary
/// to match exhaustively"* — so the attribute is what refused it and not a typo.
/// With a `_ => {}` arm added it printed
/// *"Test compiled successfully, but it's marked `compile_fail`"*, which is the
/// direction that fires the day somebody deletes the attribute.
///
/// **That argument holds on one premise, and the premise is a refusal.** There is
/// no `Factory::routing()` accessor and no other door returning one of these,
/// deliberately: an accessor hands a caller a value to match, which turns the next
/// variant from additive into breaking and inverts this section without touching
/// it. A caller who wants to know how a factory routes knows already — it passed
/// the `Routing` in.
///
/// # There is no `Sticky`
///
/// *A worker already holding this key keeps it, even after a resize* needs an
/// in-flight key→member map with counts, which would be the only shared mutable
/// state here. It buys a behaviour difference that cannot occur: a factory has no
/// resize, and a supervised member's mailbox outlives its incarnations, so a
/// restart does not move membership. At a fixed member count `Sticky` and
/// [`ByKey`](Self::ByKey) are the same function.
#[non_exhaustive]
pub enum Routing<K = ()> {
    /// A cursor, one step per dispatch.
    ///
    /// What [`Pool::pick`](super::Pool::pick) does, and it inherits that method's
    /// limitation exactly: *n* dispatches reach *n* different members, and none of
    /// them is chosen for being idle. Over a member that has ended it costs 1/N of
    /// the traffic and a retry lands elsewhere.
    RoundRobin,
    /// Race a reservation across every member; the first slot to open wins.
    ///
    /// The only mode that chooses by what the mailboxes are actually doing rather
    /// than by a number read a moment earlier, and the only one whose cost is
    /// worth a paragraph.
    ///
    /// # Sound, and not FIFO across dispatchers
    ///
    /// Each dispatch enqueues N waiters and cancels N−1. tokio's `reserve` is
    /// **not** documented cancel-safe — cancelling one loses its place in the
    /// queue — so nothing here orders two dispatchers against each other. What
    /// tokio does guarantee is the narrower thing a racer needs: capacity is
    /// conserved. A cancelled waiter is taken out of the wait list and any permit
    /// already assigned to it is returned, so racing N and dropping N−1 leaks no
    /// slot and wedges no mailbox.
    ///
    /// **No bound on the spread of per-dispatcher wait time is claimed here**,
    /// because none has been measured. The claim is soundness and nothing else.
    ///
    /// A member that has ended answers at once, so it always wins a race it should
    /// lose. This mode therefore skips refusals and takes the first member that
    /// actually admits, and only reports a refusal when every member has made one.
    FirstAvailable,
    /// Fewest messages outstanding, read from [`Reserving::status`].
    ///
    /// # It has no gradient to read at the default depth
    ///
    /// A local actor's mailbox depth defaults to **one** — `turns.rs`'s private
    /// `DEFAULT_LOCAL_MAILBOX_DEPTH`, and the keyed side's `DEFAULT_MAILBOX_DEPTH`
    /// in `registry.rs` is the same — so
    /// [`queued`](crate::LocalActor::queued) is 0 or 1 and this mode reads N
    /// booleans. [`Sample::All`] then pays N relaxed loads to learn what
    /// [`busy`](crate::LocalActor::busy) alone already says.
    ///
    /// The mode is worth its place over members built with the generated
    /// `setup(..).backpressure(..)` or [`LocalActor::spawn_deep`], and any example demonstrating
    /// it is built that way for that reason. Under the default the honest advice
    /// is [`FirstAvailable`](Self::FirstAvailable), which needs no gradient.
    LeastLoaded(Sample),
    /// `hash(key) % N`. The same key reaches the same member, for as long as the
    /// factory lives.
    ///
    /// The mode a [`Pool`] refuses and the reason this type exists.
    /// [`Registry`](crate::Registry) and [`Names`](super::Names) both give a key its
    /// own actor; this gives a key a *share* of a fixed set, which is what a
    /// worker holding one expensive thing per shard needs.
    ///
    /// # Affinity is not ordering
    ///
    /// Two dispatches with the same key reach the same actor. They are **not**
    /// guaranteed to be handled in the order they were sent: a serial mailbox
    /// gives serialization, not sequencing, and two tasks racing for one member's
    /// permit are ordered by nothing. Await the first reply, or carry a sequence
    /// number.
    ///
    /// # What a dead member costs, and it is not a hang
    ///
    /// A dead member **fails fast**. A supervisor that gives up closes the mailbox
    /// before it reports, and an unsupervised loop drops the receiver on its way
    /// out, so a dispatch onto one answers [`NotQueued::Closed`] rather than
    /// waiting.
    ///
    /// What survives is asymmetric, and it is the price of the affinity rather
    /// than a defect in it. Under [`RoundRobin`](Self::RoundRobin) a dead member
    /// costs 1/N of the *traffic* and a retry lands elsewhere. Under `ByKey` it
    /// costs 1/N of the *keyspace, permanently*: every retry for that key hashes
    /// to the same dead member. Supervise the members, which is the answer this
    /// module does not implement because the member already has one.
    ///
    /// A **wedged** member is the same shape with a worse answer. It closes
    /// nothing, so its keys park on the permit until the caller's own deadline —
    /// and where [`LeastLoaded`](Self::LeastLoaded) and
    /// [`FirstAvailable`](Self::FirstAvailable) route around it, `ByKey` routes
    /// into it for ever. Re-opening this needs a factory that learns a member's
    /// ending, and re-sharding on that signal is the resize this design refuses.
    ByKey(Shard<K>),
}

/// By hand for [`Shard`]'s reason: `#[derive]` would demand `K: Clone`.
impl<K> Clone for Routing<K> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<K> Copy for Routing<K> {}

impl<K> fmt::Debug for Routing<K> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::RoundRobin => f.write_str("RoundRobin"),
            Self::FirstAvailable => f.write_str("FirstAvailable"),
            Self::LeastLoaded(sample) => f.debug_tuple("LeastLoaded").field(sample).finish(),
            Self::ByKey(shard) => f.debug_tuple("ByKey").field(shard).finish(),
        }
    }
}

/// N interchangeable actors and the rule that picks one.
///
/// Cheap to clone, and every clone shares the rotation.
///
/// `K` is the key type and defaults to `()`, so a factory that does not route by
/// key needs no turbofish and has no key to pass.
pub struct Factory<H, K = ()> {
    /// The members and the cursor. A [`Pool`] and not a second copy
    /// of one: `RoundRobin` *is* [`Pool::pick`](super::Pool::pick), the `Arc` that
    /// makes two clones share one cursor is already written there, and a factory
    /// that grew its own `Box<[H]>` beside it would be two answers to one
    /// question.
    members: Pool<H>,
    routing: Routing<K>,
}

impl<H, K> Factory<H, K> {
    /// Build `count` members, numbering each, and route them by `routing`.
    ///
    /// Named for [`Pool::from_fn`](super::Pool::from_fn), which it wraps and whose
    /// arguments it keeps: this is that constructor with the routing rule in the
    /// middle. It takes a factory rather than a `Vec` for the same reason — a
    /// factory's members are interchangeable *by definition*, so they are produced
    /// the same way, and a heterogeneous `Vec` would be a name registry wearing a
    /// factory's clothes.
    ///
    /// **There is no builder**, and the absence is the design. Rev. A had one to
    /// separate a direct factory from a queued one; the queued mode is refused, so
    /// what is left is a builder with a single mandatory setter, which is a
    /// constructor with extra steps and one more state — *built without a routing
    /// rule* — that nothing should be able to spell.
    ///
    /// # Panics
    ///
    /// Whatever the factory panics with. It runs `count` times, here, on the
    /// caller's task — so a factory of actors is `count` spawns before this
    /// returns, and it panics outside a runtime like every other spawn.
    #[must_use]
    pub fn from_fn(
        count: NonZeroUsize,
        routing: Routing<K>,
        spawn: impl FnMut(Member) -> H,
    ) -> Self {
        Self {
            members: Pool::from_fn(count, spawn),
            routing,
        }
    }

    /// Every member, in order, for the operations a factory does not wrap.
    ///
    /// Stopping is the first of them, and [`Pool::members`](super::Pool::members)
    /// carries the argument: stopping is a method on whatever `H` is, and a
    /// factory that knew how to stop its members would need a trait describing
    /// every handle in the crate.
    ///
    /// How many there are is `members().len()`. There is no `size()` beside it:
    /// the only arithmetic that needed [`Pool::size`](super::Pool::size)'s
    /// `NonZeroUsize` was the `% N`, and the factory now does that itself.
    #[must_use]
    pub fn members(&self) -> &[H] {
        self.members.members()
    }
}

impl<H: Reserving> Factory<H, ()> {
    /// Reserve a slot on whichever member the rule chooses, and spend it.
    ///
    /// The door for [`RoundRobin`](Routing::RoundRobin),
    /// [`FirstAvailable`](Routing::FirstAvailable) and
    /// [`LeastLoaded`](Routing::LeastLoaded), which take no key — so this takes
    /// none either, rather than demanding one it would discard.
    ///
    /// `job` is handed the permit and returns whatever the method on it returns,
    /// so an `rpc` gives **two** results and they mean different things: the outer
    /// one says whether a slot was ever obtained, and the inner one says what
    /// became of the turn. `dispatch(..).await??` is the common spelling.
    ///
    /// # Errors
    ///
    /// [`NotQueued`], from the chosen member's own
    /// [`reserve`](Reserving::reserve). Never [`NotQueued::Full`]: a reservation
    /// waits for the slot rather than shedding, which is why a caller under
    /// [`Backpressure::Shed`](crate::Backpressure) loses nothing through this
    /// door.
    pub async fn dispatch<'a, J, F, T>(&'a self, job: J) -> Result<T, NotQueued>
    where
        J: FnOnce(H::Permit<'a>) -> F,
        F: Future<Output = T>,
    {
        // A `Factory<H, ()>` under `ByKey` hashes this, which is the degenerate
        // state the two-door split leaves reachable: every dispatch hashes the
        // same value onto the same member, so an eight-member factory serves every
        // call from one of them.
        //
        // **A caller reaches it without meaning to, and this comment used to say
        // otherwise.** It read *a caller who wrote `Routing::<()>::ByKey(..)`
        // deliberately gets the routing they asked for*, and no turbofish is
        // needed: `from_fn` puts no bound on `K`, `dispatch` is what fixes it at
        // `()`, and `()` implements `Hash` — so
        // `Factory::from_fn(n, Routing::ByKey(Shard::by_hash()), spawn)` followed
        // by `.dispatch(..)` compiles, infers, and collapses N to 1 in silence.
        //
        // It is not `unreachable!` because the collapse is a routing decision and
        // not an invariant: the member it picks admits the call and runs it. What
        // would remove the state is a `Key` bound that `()` does not satisfy —
        // `docs/proposals/2026-08-10-compose-factory.md` §11 records that trade as
        // open, and it stays open here: with no negative impls, such a trait needs
        // a hand-written impl for every key type a caller has and a `where` clause
        // on every keyed door, which is a cascade paid by every correct caller to
        // delete one wrong one.
        let permit = self.admit(&()).await?;
        Ok(job(permit).await)
    }
}

impl<H: Reserving, K> Factory<H, K> {
    /// Reserve a slot on the member this key belongs to, and spend it.
    ///
    /// [`ByKey`](Routing::ByKey)'s door. Under any other mode the key is read by
    /// nothing, which is not an error — a caller may legitimately hold one factory
    /// type and change the rule.
    ///
    /// **No `Hash` bound, and none is missing.** The hash lives in the
    /// [`Shard`], so [`Shard::by_hash`] is where `K: Hash` is required and
    /// [`Shard::by`] needs nothing at all. There is no `Eq` bound either: nothing
    /// here holds a map, so no two keys are ever compared.
    ///
    /// # Errors
    ///
    /// [`dispatch`](Factory::dispatch)'s.
    pub async fn dispatch_keyed<'a, J, F, T>(&'a self, key: K, job: J) -> Result<T, NotQueued>
    where
        J: FnOnce(H::Permit<'a>) -> F,
        F: Future<Output = T>,
    {
        let permit = self.admit(&key).await?;
        Ok(job(permit).await)
    }

    /// The routing rule, applied. One place, so the two doors cannot disagree
    /// about what a mode means.
    async fn admit(&self, key: &K) -> Result<H::Permit<'_>, NotQueued> {
        match &self.routing {
            Routing::RoundRobin => self.members.pick().1.reserve().await,
            Routing::ByKey(shard) => self.shard(*shard, key).reserve().await,
            Routing::LeastLoaded(sample) => self.least_loaded(*sample).reserve().await,
            Routing::FirstAvailable => self.first_available().await,
        }
    }

    /// `hash(key) % N`.
    fn shard(&self, shard: Shard<K>, key: &K) -> &H {
        let size = self.members.size().get();
        // Written as conversions rather than casts for the reason
        // `LocalActor::queued` gives: an impossible case is named instead of
        // truncated. `usize` is 64 bits or narrower on every target this crate
        // builds for, so the widening holds; and the remainder is below `size`,
        // which came from a `usize`, so the narrowing does too — which is also
        // what puts the index below in range, since a pool holds exactly `size`
        // members.
        let wide = u64::try_from(size).unwrap_or(u64::MAX);
        let at = usize::try_from((shard.of)(key) % wide).unwrap_or(0);
        &self.members.members()[at]
    }

    /// The member with the fewest messages outstanding, out of however many
    /// [`Sample`] says to read.
    ///
    /// The cursor's own pick is the seed under both, which is what stops a tie
    /// among equally idle members always naming member zero: the comparison below
    /// is strict, so a tie keeps the seed and the seed rotates.
    fn least_loaded(&self, sample: Sample) -> &H {
        let seed = self.members.pick().1;
        let seeded = (load(seed), seed);
        let (_, best) = match sample {
            Sample::All => self.members.members().iter().fold(seeded, keep_smaller),
            Sample::TwoOfN => keep_smaller(seeded, self.members.pick().1),
        };
        best
    }

    /// Race a reservation across every member and keep the first slot that opens.
    ///
    /// Dropping the losers is the cancellation [`Routing::FirstAvailable`]
    /// documents: tokio takes each one out of the wait list and returns any permit
    /// already assigned to it, so nothing leaks.
    ///
    /// **Refusals are skipped rather than raced against.** A member whose mailbox
    /// is closed answers immediately, so a race that took the first *answer* would
    /// hand back `Closed` from one dead member while N−1 live ones were still
    /// waiting — which is the mode failing in exactly the case it exists for.
    async fn first_available(&self) -> Result<H::Permit<'_>, NotQueued> {
        let mut racing: FuturesUnordered<_> = self
            .members
            .members()
            .iter()
            .map(Reserving::reserve)
            .collect();
        // Overwritten before it is read: a pool holds `NonZeroUsize` members and
        // each answers exactly once, so the loop below runs at least one arm.
        let mut refused = NotQueued::Closed;
        while let Some(answer) = racing.next().await {
            match answer {
                Ok(permit) => return Ok(permit),
                // `Reentrant` wins over `Closed` when the refusals disagree, for
                // `LocalActor::send`'s own reason: the deadlock is the more
                // specific fact and the caller fixes it differently.
                Err(why) if refused != NotQueued::Reentrant => refused = why,
                Err(_) => {}
            }
        }
        Err(refused)
    }
}

/// The two facts a [`Serving`] carries, ordered so that the smaller pair is the
/// better member: fewer queued first, then idle before busy.
fn load(of: &impl Reserving) -> (u64, bool) {
    let serving = of.status();
    (serving.queued, serving.busy)
}

/// Keep whichever of two members is carrying less, reading each member's load
/// once.
///
/// A free function taking the load beside the member so that a fold over N does
/// N + 1 reads rather than 2N — the seed's reading is carried, not re-taken.
fn keep_smaller<'a, H: Reserving>(best: ((u64, bool), &'a H), next: &'a H) -> ((u64, bool), &'a H) {
    let next_load = load(next);
    if next_load < best.0 {
        (next_load, next)
    } else {
        best
    }
}

/// By hand, because `#[derive(Clone)]` would demand `H: Clone` — and the members
/// are not cloned. Only the [`Pool`]'s `Arc` is, which is what makes
/// the cursor shared.
impl<H, K> Clone for Factory<H, K> {
    fn clone(&self) -> Self {
        Self {
            members: self.members.clone(),
            routing: self.routing,
        }
    }
}

impl<H, K> fmt::Debug for Factory<H, K> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Factory")
            .field("members", &self.members)
            .field("routing", &self.routing)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::expect_used, clippy::unwrap_used)]

    use tokio::sync::{mpsc, oneshot};

    use super::*;
    use crate::{turn, Backpressure, Ended, Hooks, Origin, Reply, Turn};

    /// A member that can say which one it is.
    struct Worker {
        me: u64,
        /// Every turn this member handled, so a test can learn which member ran a
        /// dispatch **without awaiting its reply**.
        ///
        /// That is not a convenience. The member a wrong routing decision would
        /// choose is, in every test below, one that is deliberately inside a turn
        /// — so awaiting the reply turns "the wrong member was chosen" into a
        /// hang, which a harness cannot tell from a slow machine and which no
        /// assertion message ever names. Spending the permit on a `notify` and
        /// reading this channel after the held turn is released makes the same
        /// defect a failed `assert` with both numbers in it. Measured: with the
        /// reply awaited instead, the same break reported `Elapsed(())` from a
        /// five-second bound and named no member at all.
        ran: mpsc::UnboundedSender<u64>,
    }

    /// A mailbox by hand, in the shape `service!` generates one.
    ///
    /// **Hand-written on purpose**, for the reason `turns.rs`'s own probe is: the
    /// routing rule lives in this file, so a probe only the macro could build
    /// would prove that the macro and the routing agree rather than that the
    /// routing holds. The generated handle reaches these same doors through
    /// [`Reserving`], and `macros.rs` type-checks that path.
    enum Mail {
        /// Report this member's own number, so the factory's claim about who it
        /// chose is checked against the actor that ran rather than taken on trust
        /// — [`Pool`](super::super::Pool)'s device, and the reason a cursor-only
        /// assertion is not enough.
        Whoami { origin: Origin, reply: Reply<u64> },
        /// Enter, say so, and wait to be let go, so that "this member is busy" is
        /// a state the test constructs rather than one it waits for and hopes to
        /// catch.
        Hold {
            origin: Origin,
            entered: oneshot::Sender<()>,
            release: oneshot::Receiver<()>,
            reply: Reply<u64>,
        },
        /// Occupy a mailbox slot and report nothing.
        ///
        /// The filler [`Routing::LeastLoaded`]'s depth test queues behind a held
        /// turn. It must not reach `ran`, or the dispatch under test could not be
        /// told apart from the filler ahead of it.
        Quiet { origin: Origin, reply: Reply<()> },
    }

    impl Turn<Worker> for Mail {
        async fn run(self, state: &mut Worker, hooks: &mut Hooks<Worker>) -> Option<Ended> {
            match self {
                Mail::Whoami { origin, reply } => {
                    turn(
                        state,
                        hooks,
                        origin,
                        "Whoami",
                        reply,
                        move |_from, state| {
                            Box::pin(async move {
                                let _ = state.ran.send(state.me);
                                state.me
                            })
                        },
                    )
                    .await
                }
                Mail::Hold {
                    origin,
                    entered,
                    release,
                    reply,
                } => {
                    turn(state, hooks, origin, "Hold", reply, move |_from, state| {
                        Box::pin(async move {
                            let _ = entered.send(());
                            let _ = release.await;
                            state.me
                        })
                    })
                    .await
                }
                Mail::Quiet { origin, reply } => {
                    turn(
                        state,
                        hooks,
                        origin,
                        "Quiet",
                        reply,
                        move |_from, _state| Box::pin(async move {}),
                    )
                    .await
                }
            }
        }
    }

    type Members = LocalActor<Worker, Mail>;

    /// Every turn any member of one factory ran, in the order they ran.
    type Handled = mpsc::UnboundedReceiver<u64>;

    /// `count` members, numbered from zero, at the default mailbox depth.
    fn factory<K>(count: usize, routing: Routing<K>) -> (Factory<Members, K>, Handled) {
        deep(count, routing, Backpressure::Park(NonZeroUsize::MIN))
    }

    /// The same, with the mailbox policy named — the depth is the control for
    /// [`Routing::LeastLoaded`], which has no gradient to read at one.
    fn deep<K>(
        count: usize,
        routing: Routing<K>,
        mailbox: Backpressure,
    ) -> (Factory<Members, K>, Handled) {
        let (ran, handled) = mpsc::unbounded_channel();
        let factory = Factory::from_fn(
            NonZeroUsize::new(count).expect("a non-zero member count"),
            routing,
            |member| {
                LocalActor::spawn_deep(
                    Worker {
                        me: u64::try_from(member.index()).expect("a factory smaller than u64::MAX"),
                        ran: ran.clone(),
                    },
                    Vec::new(),
                    (),
                    mailbox,
                )
            },
        );
        (factory, handled)
    }

    /// Ask the chosen member which one it is, and wait for the answer.
    ///
    /// For the tests where no member is held: nothing here can queue behind a
    /// turn that never ends, so awaiting the reply costs nothing and asserts the
    /// round trip as well as the choice.
    async fn who(factory: &Factory<Members, ()>) -> u64 {
        factory
            .dispatch(|permit| permit.ask(|origin, reply| Mail::Whoami { origin, reply }))
            .await
            .expect("a slot on some member")
            .expect("the member answers")
    }

    /// The same, keyed.
    async fn who_for(factory: &Factory<Members, u64>, key: u64) -> u64 {
        factory
            .dispatch_keyed(key, |permit| {
                permit.ask(|origin, reply| Mail::Whoami { origin, reply })
            })
            .await
            .expect("a slot on some member")
            .expect("the member answers")
    }

    /// Dispatch and do **not** wait for the turn.
    ///
    /// The permit is spent on a `notify`, so this returns as soon as a member has
    /// been chosen and has admitted the message. Which member ran it is read from
    /// [`Handled`] afterwards, for the reason [`Worker::ran`] carries.
    async fn dispatched(factory: &Factory<Members, ()>) {
        factory
            .dispatch(|permit| async move {
                let _ = permit.notify(|origin, reply| Mail::Whoami { origin, reply });
            })
            .await
            .expect("a slot on some member");
    }

    /// Put one member inside a turn and hand back the release, so every assertion
    /// around "busy" is ordered rather than raced.
    async fn hold(member: &Members) -> oneshot::Sender<()> {
        let (entered, started) = oneshot::channel();
        let (release, gate) = oneshot::channel();
        let _ = member
            .notify(move |origin, reply| Mail::Hold {
                origin,
                entered,
                release: gate,
                reply,
            })
            .await
            .expect("the member is running");
        started.await.expect("the member enters the turn");
        release
    }

    /// The signature the whole design rests on: a dispatch future that holds its
    /// borrowed permit **across an await point** is `Send`.
    ///
    /// [`Reserving::reserve`] states `+ Send` on the reservation itself, and that
    /// is a different claim: it constrains the future a member hands back, not the
    /// one [`dispatch`](Factory::dispatch) builds around it. `dispatch` is an
    /// inherent `async fn`, so its future's auto-traits are inferred from its body
    /// and nothing in the signature records them. That inference is what §5.2 of
    /// the proposal chose the borrowed permit on, and it was measured on a scratch
    /// probe that was then deleted — a probe deleted is a measurement, not a
    /// guard, so it is re-asserted here.
    ///
    /// **Watched, because a `Send` assertion is satisfied by a future that holds
    /// nothing.** With `let not_send = std::rc::Rc::new(());` live across the same
    /// block the compiler printed *"future returned by `dispatch` is not `Send`"*,
    /// *"`Rc<()>` which is not `Send`"* and, naming what refused it,
    /// *"required by a bound in `only_if_send`"*. So the bound is load-bearing and
    /// this assertion can fail.
    ///
    /// **What `yield_now` adds is narrower than it looks, and the first version of
    /// this comment overstated it.** It claimed that without the yield nothing is
    /// held across a suspension point and the test passes vacuously. Measured:
    /// with the yield deleted and the `Rc` kept, the refusal is identical and the
    /// compiler names `ask`'s own `.await` instead — `ask` consumes the permit, so
    /// the permit is inside the awaited future either way. The yield buys one
    /// thing: the permit is a **local of this block** live across a suspension,
    /// which is the shape the deleted probe measured. It is not what makes the
    /// assertion able to fail.
    ///
    /// **`'static` is the property the borrowed form does not have, and it is not
    /// the one this asserts.** A borrowed permit cannot cross `tokio::spawn`; it
    /// does not need to, because this future is awaited in place by the caller.
    /// [`LocalActor::reserve_owned`] is the `'static` half and §4 kept it for
    /// callers who spawn.
    #[tokio::test]
    async fn a_dispatch_future_holding_its_permit_across_an_await_is_send() {
        fn only_if_send<F: Send>(future: F) -> F {
            future
        }

        let (factory, _handled) = factory(2, Routing::RoundRobin);

        let dispatched = factory.dispatch(|permit| async move {
            tokio::task::yield_now().await;
            permit
                .ask(|origin, reply| Mail::Whoami { origin, reply })
                .await
        });

        let me = only_if_send(dispatched)
            .await
            .expect("a slot on some member")
            .expect("the member answers");

        assert_eq!(me, 0, "the future ran, so the permit really was held");
    }

    // ── B IS THIS WAVE'S LETTER, AND EVERY ROW BELOW IS SAME-FILE ─────────────
    //
    // `F` for factory and `R` for routing are both taken — `F1` anchors
    // `keylink/src/rpc.rs` and the `R` series runs to `R16` — so `B` is chosen by
    // the test `T`, `D` and `E` were chosen by: unused across every file that
    // holds an id-shaped token.
    //
    // **Every record here lives in the file it breaks**, so none of them may
    // quote its own search literal with the code's indentation: the registered
    // literal carries it and a `///` quote does not, which is what keeps the
    // count at 1. Do not tidy the quotes to match the code.
    //
    // Three of these literals are shared with a row whose record lives in
    // `keylink/tests/compose.rs` — the generated-handle half of the same claims —
    // so a count taken here is a count for two rows, and adding a quote of any of
    // them aborts both together.

    /// Made red by `scripts/break-once.sh keylink/src/compose/factory.rs`,
    /// replacing
    /// `Routing::RoundRobin => self.members.pick().1.reserve().await,`
    /// with
    /// `Routing::RoundRobin => { let _ = self.members.pick(); self.members.members()[0].reserve().await }`,
    /// so the cursor still turns and every dispatch lands on member zero.
    /// Watched: "the actor that answered, not the cursor that chose it", left
    /// `[0, 0, 0, 0, 0, 0, 0]` against right `[0, 1, 2, 0, 1, 2, 0]`. Registered
    /// as `B1`.
    ///
    /// The assertion is on the number each **actor** reported, not on the cursor
    /// the factory moved, which is [`Pool`](super::super::Pool)'s device: a
    /// rotation that reported one member and dispatched to another passes a
    /// cursor-only test.
    ///
    /// **The `let _ = ..pick();` is the whole of what this record demonstrates,
    /// and the substitution ran without it for one commit.** It read
    /// `self.members.members()[0].reserve().await,` alone, which deletes the only
    /// call that advances the cursor on this path — `Pool::pick` is the one place
    /// it moves, and the arm above is its only `RoundRobin` call site. So the
    /// cursor froze, the break was *cursor stopped **and** routing collapsed*, and
    /// the sentence claiming a turning cursor described a state the run never
    /// reached. A frozen cursor is caught by a cursor-only test as well, which is
    /// exactly the discrimination this record exists to exhibit. Both spellings
    /// print the same left and right; only this one distinguishes them.
    #[tokio::test]
    async fn round_robin_reaches_each_member_in_turn() {
        let (factory, _handled) = factory(3, Routing::RoundRobin);

        let mut handled = Vec::new();
        for _ in 0..7 {
            handled.push(who(&factory).await);
        }

        assert_eq!(
            handled,
            vec![0, 1, 2, 0, 1, 2, 0],
            "the actor that answered, not the cursor that chose it",
        );
    }

    /// **Two records in one block, and the block is the unit `RECORD_BLOCKS`
    /// counts** — so this pair moves that figure by one, not by two. It is
    /// written down here because the registry's ledger has an entry for exactly
    /// this shape and a reader who computes the next figure instead of reading it
    /// off the failing run gets it wrong.
    ///
    /// Both are `scripts/break-once.sh keylink/src/compose/factory.rs` on
    /// `let at = usize::try_from((shard.of)(key) % wide).unwrap_or(0);`,
    /// registered once as `B2` because a row pins a literal and this is one
    /// literal.
    ///
    /// **On the cursor**, replacing it with
    /// `let at = (usize::try_from((shard.of)(key) % wide).unwrap_or(0)
    /// + self.members.pick().0.index()) % size;` — affinity replaced by a
    /// rotation that still spreads. Watched: "one key must reach one member,
    /// every time", left `0` against right `3`.
    ///
    /// **On a constant**, replacing it with
    /// `let at = usize::try_from((shard.of)(key) % wide).unwrap_or(0) % 1;` —
    /// affinity kept and the keyspace collapsed onto one member. Watched:
    /// `64 keys over 4 members reached only [0]`.
    ///
    /// Neither break alone reddens both halves, which is why the test has two:
    /// the first assertion is satisfied by a factory that sends everything to one
    /// member, and the second by one that ignores the key entirely.
    #[tokio::test]
    async fn one_key_reaches_one_member_and_the_keyspace_still_spreads() {
        let (factory, _handled) = factory(4, Routing::ByKey(Shard::by_hash()));

        let mine = who_for(&factory, 7).await;
        for _ in 0..16 {
            assert_eq!(
                who_for(&factory, 7).await,
                mine,
                "one key must reach one member, every time",
            );
        }

        // The other half, and the one a routing that always answered member zero
        // would fail: distinct keys are not all one member's.
        let mut reached = Vec::new();
        for key in 0..64_u64 {
            reached.push(who_for(&factory, key).await);
        }
        reached.sort_unstable();
        reached.dedup();
        assert!(
            reached.len() > 1,
            "64 keys over 4 members reached only {reached:?}",
        );
    }

    /// Made red by `scripts/break-once.sh keylink/src/compose/factory.rs`,
    /// replacing `(serving.queued, serving.busy)` with
    /// `(serving.queued, false)`, so the load reading keeps the queue depth and
    /// forgets whether the member is inside a turn. Watched: "All chose the
    /// member that is inside a turn", left `0` against right `0` — an
    /// `assert_ne`, so the two numbers being equal is the failure. Registered as
    /// `B3`.
    ///
    /// Both samples are driven, because they are two code paths and not one
    /// parameter: [`Sample::All`] folds over N and [`Sample::TwoOfN`] compares the
    /// cursor's pick against the next, so a break in either fold leaves the other
    /// green.
    #[tokio::test]
    async fn least_loaded_leaves_the_member_holding_a_turn_alone() {
        for sample in [Sample::All, Sample::TwoOfN] {
            let (factory, mut handled) = factory(3, Routing::LeastLoaded(sample));
            let release = hold(&factory.members()[0]).await;

            dispatched(&factory).await;
            // Released before the answer is read, so a mode that chose member
            // zero reports member zero instead of never reporting at all.
            drop(release);

            assert_ne!(
                handled
                    .recv()
                    .await
                    .expect("the chosen member runs the turn"),
                0,
                "{sample:?} chose the member that is inside a turn",
            );
        }
    }

    /// Made red by `scripts/break-once.sh keylink/src/compose/factory.rs`,
    /// replacing `if next_load < best.0 {` with `if next_load > best.0 {`, so the
    /// mode keeps reading both facts and prefers the member carrying more.
    /// Watched: "the member with three queued was chosen over the idle one", left
    /// `1` against right `0`. Registered as `B4`.
    ///
    /// **The independence runs one way only, and it was measured rather than
    /// argued.** `B3`'s break leaves this test green — exit 0 under the same
    /// harness — because forgetting `busy` still prefers the member with less
    /// queued. This break does **not** leave `B3`'s test green: reversing the
    /// comparison reddens that one too, exit 100, since a held member's reading is
    /// the larger of the two there as well. So `B3` is the row that discriminates
    /// and this one is not, which is the reason both exist and not a reason to
    /// keep only this.
    #[tokio::test]
    async fn least_loaded_separates_two_members_by_what_is_queued() {
        // The depth is the control: at one, `queued` is 0 or 1 and this
        // assertion is about `busy` alone. At four, member one is genuinely
        // three messages behind while still admitting more, which is the
        // gradient the mode claims to read.
        let (factory, mut handled) = deep(
            2,
            Routing::LeastLoaded(Sample::All),
            Backpressure::Park(NonZeroUsize::new(4).expect("four")),
        );
        let release = hold(&factory.members()[1]).await;
        for _ in 0..3 {
            let _ = factory.members()[1]
                .notify(|origin, reply| Mail::Quiet { origin, reply })
                .await
                .expect("the member is running");
        }

        dispatched(&factory).await;
        drop(release);

        assert_eq!(
            handled
                .recv()
                .await
                .expect("the chosen member runs the turn"),
            0,
            "the member with three queued was chosen over the idle one",
        );
    }

    /// Made red by `scripts/break-once.sh keylink/src/compose/factory.rs`,
    /// replacing the two refusal arms
    /// `Err(why) if refused != NotQueued::Reentrant => refused = why,
    /// Err(_) => {}`
    /// with `Err(why) => return Err(why),`, which is the race taking the first
    /// **answer** rather than the first **slot**. Watched:
    /// `a slot on some member: Closed`. Registered as `B5`.
    ///
    /// Two members are stopped rather than one, because a mode that skipped a
    /// single refusal and then took the next answer would pass with one.
    #[tokio::test]
    async fn first_available_skips_every_member_that_has_ended() {
        let (factory, _handled) = factory(3, Routing::FirstAvailable);
        // A dead member answers its reservation at once, so it wins any race
        // that takes the first *answer* rather than the first *slot*. Two of
        // them, so that skipping once is not enough either.
        for member in &factory.members()[..2] {
            member.stop().await.expect("the member was running");
        }

        for _ in 0..4 {
            assert_eq!(
                who(&factory).await,
                2,
                "the only member still running is the only one that can answer",
            );
        }
    }

    /// Made red by `scripts/break-once.sh keylink/src/compose/factory.rs`,
    /// replacing `let mut refused = NotQueued::Closed;` with
    /// `let mut refused = NotQueued::Reentrant;`. The seed is documented as
    /// overwritten before it is read, and this shows what it costs when it is
    /// not: the arm that records a refusal is guarded on `refused` not already
    /// being `Reentrant`, so seeding it with that value makes every arm fall
    /// through and the loop reports a deadlock nobody had. Watched:
    /// `assert_eq` failed, left `Reentrant` against right `Closed`. Registered as
    /// `B6`.
    ///
    /// It leaves `B5`'s test green, because that one has a live member and never
    /// reaches the seed.
    #[tokio::test]
    async fn first_available_reports_a_refusal_only_once_every_member_has_made_one() {
        let (factory, _handled) = factory(3, Routing::FirstAvailable);
        for member in factory.members() {
            member.stop().await.expect("the member was running");
        }

        let refused = factory
            .dispatch(|permit| permit.ask(|origin, reply| Mail::Whoami { origin, reply }))
            .await
            .expect_err("every member is gone");

        assert_eq!(refused, NotQueued::Closed);
    }
}
