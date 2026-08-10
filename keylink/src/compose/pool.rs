//! N interchangeable actors behind one cursor.
//!
//! An actor serialises. That is the feature — one turn at a time against
//! `&mut self` is what makes a `Mutex` unnecessary — right up until the state is
//! *shardable* and serialisation is pure cost. A pool is for that case and only
//! that case: N actors that do the same job, none of which is the one you have to
//! reach.
//!
//! ```ignore
//! let pool = Pool::from_fn(NonZeroUsize::new(4).expect("four"), |member| {
//!     render::actor(Renderer::new(member))
//! });
//!
//! let (who, actor) = pool.pick();
//! let png = actor.render(page).await?;
//! tracing::debug!(member = who.index(), "rendered");
//! ```
//!
//! # Backpressure: the pool has none, and that is the point
//!
//! [`pick`](Pool::pick) hands back a handle. Every wait after that is the
//! ordinary mailbox permit on that one member, exactly as if the caller had held
//! the handle directly — so a pool of N actors is N mailboxes and **no queue of
//! its own**. There is nothing here to grow, and nothing here to configure.
//!
//! A pool that queued would be the interesting design and the wrong one: work
//! waiting in a pool's own queue is work no member has admitted yet, which means
//! the caller is told "accepted" by something that cannot run it. The mailbox is
//! already the admission point, and adding a second one in front turns one bound
//! into two.
//!
//! # Round-robin, and the limitation that comes with it
//!
//! The cursor advances once per [`pick`](Pool::pick), so *n* picks reach *n*
//! different members. It does **not** find an idle one: pick can hand back a
//! member that is mid-turn while a sibling is idle, and the caller then waits on
//! the busy one.
//!
//! Least-loaded would need to read how many permits each member's mailbox has
//! free, and a [`Handle`](crate::LocalActor) does not expose that — a `try_pick`
//! across N mailboxes needs a `try_reserve` the handle does not have. That is a
//! real gap and it is named here rather than hidden behind a claim of fairness:
//! this is fair in *dispatch* and not in *service time*.
//!
//! With a mailbox of one, the two coincide more often than they look like they
//! would: a member is either running a turn or has a free permit, so round-robin
//! over N members is at worst N-1 picks away from a free permit. A deeper mailbox
//! is what would separate them, because a member could then be several messages
//! behind while still admitting more.
//!
//! # What a pool is not
//!
//! - **Not a way to make one actor concurrent.** Four members are four states.
//!   If the state cannot be split, a pool is four wrong answers instead of one
//!   slow one.
//! - **Not a supervisor.** Nothing here restarts a member, notices one ending, or
//!   removes one from the rotation. [`pick`](Pool::pick) will keep handing back a
//!   handle to an actor that has stopped, and the call on it will answer
//!   [`Unanswered`](crate::Unanswered) or [`NotQueued`](crate::NotQueued), by
//!   door. **Restarting is the member's own
//!   job, and it already has one:** build members with the generated
//!   `supervised()` — or [`LocalActor::spawn_supervised`](crate::LocalActor::spawn_supervised)
//!   — and each handle survives its own faults without the pool knowing. A
//!   supervised member's mailbox outlives its incarnations, so `pick` keeps
//!   working across a restart and nothing in this module has to learn about one.
//!   That is the design rather than an omission, and it is a claim about
//!   *restarting* only: nothing in `compose` restarts anything, because a
//!   supervised member already does. One member's ending mattering to the others
//!   is a different question with its own answer one file over —
//!   [`Group`](super::Group) watches a set of actors and asks a policy the caller
//!   wrote whether the rest carry on without one, and it restarts nothing either.
//!   A pool is not that and does not grow into it: `pick` keeps no list of who is
//!   alive, and a member ending here is nobody's business but its own.
//! - **Not keyed.** If which member handles a message matters, the member is an
//!   identity and the thing wanted is [`System`](crate::System) or
//!   [`Names`](super::Names).

use std::fmt;
use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

/// Which member of a [`Pool`] this is.
///
/// A newtype and not a bare `usize`, for the reason every scalar in this crate is
/// one: the index of a member and the number of members are both `usize` and
/// mean nothing alike. There is **no public constructor**, so a `Member` a caller
/// holds was produced by a pool and is in range for it.
///
/// It is handed to the factory in [`Pool::from_fn`] as well as returned by
/// [`Pool::pick`], which is what lets an actor know its own number and report it
/// — the pool's claim about who handled a turn and the actor's own claim can then
/// be compared, rather than one being taken on trust.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Member(usize);

impl Member {
    /// The member's position in the pool, from zero.
    #[must_use]
    pub fn index(self) -> usize {
        self.0
    }
}

impl fmt::Display for Member {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "member {}", self.0)
    }
}

/// The members and the cursor, behind one [`Arc`] so that every clone of a
/// [`Pool`] shares the rotation.
///
/// Sharing it is the whole reason this indirection exists: two clones with two
/// cursors would each start at member zero, and a service handing a clone to
/// every connection would send every first request to the same actor.
#[derive(Debug)]
struct Inner<H> {
    /// Exactly `size` entries: both are built from the one `count` argument, so
    /// `size` is the authority every index below is taken modulo and no code path
    /// reads `members.len()` to rediscover it.
    members: Box<[H]>,
    size: NonZeroUsize,
    at: AtomicUsize,
}

/// N interchangeable actors behind one cursor.
///
/// Cheap to clone, and every clone shares the rotation.
pub struct Pool<H> {
    inner: Arc<Inner<H>>,
}

impl<H> Pool<H> {
    /// Build `count` members, numbering each.
    ///
    /// The only constructor, and it takes a factory rather than a `Vec` for a
    /// reason that is about the pattern and not about ergonomics: a pool's
    /// members are interchangeable *by definition*, so they are produced the same
    /// way. A constructor taking a heterogeneous `Vec` would accept members that
    /// are not interchangeable, and a pool of actors that differ is a name
    /// registry wearing a pool's clothes — [`Names`](super::Names) is that.
    ///
    /// [`NonZeroUsize`] rather than a `usize` and a `Result`: an empty pool has
    /// nothing to hand back from [`pick`](Self::pick), so it is refused by the
    /// type instead of by a runtime error nobody can act on.
    ///
    /// # Panics
    ///
    /// Whatever the factory panics with. It runs `count` times, here, on the
    /// caller's task — so a pool of actors is `count` spawns before this returns,
    /// and it panics outside a tokio runtime like every other spawn.
    #[must_use]
    pub fn from_fn(count: NonZeroUsize, spawn: impl FnMut(Member) -> H) -> Self {
        Self {
            inner: Arc::new(Inner {
                members: (0..count.get()).map(Member).map(spawn).collect(),
                size: count,
                at: AtomicUsize::new(0),
            }),
        }
    }

    /// The next member, and which one it is.
    ///
    /// Advances the cursor once, so concurrent callers get different members: the
    /// counter is `fetch_add`, not read-then-write. `Relaxed` is enough because
    /// nothing is published through it — the only requirement is that two callers
    /// get two different numbers, which `fetch_add` gives on its own.
    ///
    /// Named `pick` and not `next` so that it cannot be read as an iterator
    /// step: it takes `&self`, it never ends, and calling it twice is two
    /// dispatches rather than a traversal.
    #[must_use]
    pub fn pick(&self) -> (Member, &H) {
        // `% size` is in range and `members` has exactly `size` entries, so the
        // index below cannot be out of bounds. The wrap of `fetch_add` past
        // `usize::MAX` costs one uneven step in the rotation and nothing else.
        let at = self.inner.at.fetch_add(1, Ordering::Relaxed) % self.inner.size.get();
        (Member(at), &self.inner.members[at])
    }

    /// Every member, in order, for the operations a pool does not wrap.
    ///
    /// Stopping the pool is the first of them: there is no `stop_all` here,
    /// because stopping is a method on whatever `H` is and a pool that knew how
    /// to stop its members would need a trait describing every handle in the
    /// crate. `for actor in pool.members() { actor.stop().await }` is the whole
    /// of it, and it reads as what it does.
    #[must_use]
    pub fn members(&self) -> &[H] {
        &self.inner.members
    }

    /// How many members there are.
    ///
    /// [`NonZeroUsize`], because it is: a pool is built from one and holds that
    /// many forever. Returning it keeps the caller's arithmetic — `turns % size`
    /// in a test, a shard count in a caller — free of a zero case that cannot
    /// happen.
    #[must_use]
    pub fn size(&self) -> NonZeroUsize {
        self.inner.size
    }
}

/// By hand, because `#[derive(Clone)]` would demand `H: Clone` — and the members
/// are not cloned. Only the `Arc` is, which is what makes the cursor shared.
impl<H> Clone for Pool<H> {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl<H> fmt::Debug for Pool<H> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Pool")
            .field("size", &self.inner.size)
            .field("at", &self.inner.at.load(Ordering::Relaxed))
            .finish()
    }
}
