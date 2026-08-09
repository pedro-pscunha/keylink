//! Fate sharing: a set of actors that end together, or one the rest carry on
//! without.
//!
//! [`Pool`](super::Pool) restarts nothing, and [`Names`](super::Names) forgets a
//! name when its actor ends. Neither can say *"if `Reserve` dies, take `Charge`
//! down with it"*, and that sentence is the whole of this module. A [`Group`]
//! watches a set of actors; when one ends **for good** it asks a function the
//! caller wrote whether the group carries on without it or ends with it.
//!
//! ```ignore
//! let group = Group::new(|_who, why| match why {
//!     Ended::Stopped => Verdict::Forget,
//!     _ => Verdict::StopAll,
//! });
//!
//! let _ = group.add(ActorId::new("reserve")?, membership!(reserve));
//! let _ = group.add(ActorId::new("charge")?, membership!(charge));
//! ```
//!
//! It is **not generic over a handle type**, and the reason is not the trait
//! refusal `pool.rs` makes: the motivating example is heterogeneous. `Reserve`
//! and `Charge` are different services with different handle types, so there is
//! no one `H` to be generic over. [`Pool`](super::Pool) and
//! [`Names`](super::Names) are homogeneous by construction; a group is not, and
//! it pays four boxes per watched member for that — see the cost section
//! below, which counts them rather than rounding them to "a box".
//!
//! # A group is not an actor, and three problems stop existing rather than being solved
//!
//! Like [`Names`](super::Names) it is a plain struct: an [`Arc`] of shared state
//! and one spawned task. No mailbox, no `service!`, no ALPN, no turn. Nothing
//! here is on the wire.
//!
//! A group that were itself an actor — which is what the implementations this is
//! modelled on do — buys three problems back. A supervised group actor that
//! panics loses its list. A mailbox of one serialises ten deaths behind each
//! other. And a member holding the group's handle closes a cycle. Here there is
//! no list an incarnation owns, endings are futures rather than queue entries,
//! and nothing can call *into* a group at all.
//!
//! # Backpressure: one decision at a time, and a death allocates nothing
//!
//! | what a slow member costs | how it is bounded |
//! |---|---|
//! | a member slow to die delays the group's next **decision** | one decision at a time, in one task. N members are at most N pending endings, and a death allocates nothing — the future was built by [`add`](Group::add) |
//! | an [`add`](Group::add) made while the policy is running | the number of `add` calls in flight, drained to empty on the next wake. A staging list, not a queue that grows against a slow consumer |
//! | a member slow to **retire** delays the group's own ending | **nothing bounds it.** Teardown awaits every member, so one `retire` future that never resolves means [`ended`](Group::ended) never resolves either — not late, never |
//!
//! That third row is the one with no bound, and it is deliberately not hidden in
//! the second: the first two are about the next *decision*, and a teardown is
//! not a decision. `n0_future::time::timeout` around
//! [`ended`](Group::ended) is the escape, and it is an escape from a stated
//! problem rather than a habit.
//!
//! # A member's own restarts never reach the policy, and that is structural
//!
//! A supervised actor's incarnations run inside one task, and only that task's
//! exit publishes an ending at all —
//! [`ended`](crate::LocalActor::ended) states it and this module rests on it. So
//! there is no filter here deciding which endings are final. Every ending a
//! group is handed already is one, and the policy is asked exactly once per
//! member.
//!
//! # No cycle can be built out of keylink calls
//!
//! The policy has no await point, and a group is never *told* anything — it only
//! observes endings. There is no call from a member into a group for a cycle to
//! close through, and keylink's own synchronous API cannot open one either:
//! `blocking::Runtime::run` calls `guard()?` and answers `BlockingInAsync`, and
//! a *local* handle has no blocking method at all — the synchronous methods
//! `service!` generates belong to the blocking `Client`, which is the wire path.
//!
//! *"The policy has no await point"* is the signature saying so rather than the
//! prose promising it, and it is checked here rather than asserted. `A -> B -> A`
//! needs the policy to call something and **wait**, and an async policy is not a
//! [`Verdict`], so there is nowhere to put the wait:
//!
//! ```compile_fail
//! use keylink::compose::{Group, Verdict};
//!
//! // `Group::new` takes `FnMut(&ActorId, &Ended) -> Verdict`, so this is a type
//! // error rather than a hazard a reader has to be warned about.
//! let _ = Group::new(|_who, _why| async { Verdict::Forget });
//! ```
//!
//! `lib.rs` uses the same device for the same kind of claim — a guard that is an
//! absence has nothing to assert on, so what carries it is code that must not
//! compile.
//!
//! **The reason is written here because rustdoc cannot pin it.** A `compile_fail`
//! passes on *any* error, and the `compile_fail,E0308` form that is supposed to
//! narrow it is a **no-op on this toolchain** — measured 2026-08-08 against
//! rustc 1.97.1: the block above passes tagged `E0061` just as happily, with no
//! warning. Compiled on its own it produces exactly one error, and it is the one
//! this section claims: `error[E0308]: mismatched types — expected Verdict,
//! found async block`.
//!
//! The residual is stated rather than denied. Caller code that *blocks* inside
//! the policy stalls the group, because the table above gives it one decision at
//! a time in one task. That is a caller error with a symptom, not an
//! impossibility.
//!
//! # Keyed actors cannot be members, and that is a stated limit
//!
//! A [`Membership`] needs the member's ending, and **a keyed actor has no
//! `ended()` to ask for** — the same gap [`Names`](super::Names) names one type
//! over. [`Membership::new`] therefore takes the ending as an argument rather
//! than asking a handle for one, so it needs no change the day the keyed side
//! grows a per-actor ending channel. Today a group watches local actors.
//!
//! # Cost
//!
//! Per **group**: one spawned task, one [`Notify`], one `watch` channel, and one
//! `Mutex` around the membership list. Nothing per decision and nothing per
//! death.
//!
//! Per **watched member**: four boxes. The ending future and the retire closure
//! are boxed by [`Membership::new`] — that is what buys a group of members with
//! different handle types — and [`add`](Group::add) boxes two more: one to stamp
//! the ending with its generation, one for the panic guard around it. Two
//! further boxes exist only while a member retires: the future its closure
//! returns, and that guard's pin around it.
//!
//! # THE THING TO KNOW — a dropped [`Group`] leaves its members running
//!
//! The group holds a handle clone inside each member's `retire`, and
//! [`membership!`](crate::membership) clones rather than consuming, so the
//! caller keeps its own. A group nobody holds any more therefore stops
//! *watching* and ends nothing: whoever is awaiting [`ended`](Group::ended) is
//! answered [`GroupEnded::Dropped`] instead of waiting forever, and the actors
//! carry on.
//!
//! A caller that hands over its only handle gets the opposite, and that is
//! [`Names`](super::Names)' documented surprise arriving here through the same
//! door. [`add`](Group::add), [`remove`](Group::remove) and dropping the group
//! are three different things — stop watching one, end all, stop watching all —
//! and none of them is the other.
//!
//! # What is refused, so nobody re-derives it
//!
//! **A restart verdict, in every shape.** `RestartAll` and `RestartAfter` are
//! the OTP sibling-restart strategies, and RFC-07 §9.8 records the decision that
//! defeats them: identity here does not die with the incarnation, so there is no
//! stale reference to repair, and a group restart is both too broad and too
//! narrow to fix what is left. A single-member `Restart` goes with them for a
//! third reason — `spawn_supervised` builds a fresh mailbox, so rebuilding a
//! member through the group would hand every caller a dead handle, which is the
//! problem this crate does not otherwise have.
//!
//! **Escalation is delivered without one.** A group whose policy answers
//! [`Verdict::StopAll`] ends, and a parent group holding it as a member sees
//! that ending and decides in turn. The mapping is lossy and the caller writes
//! it, because [`Group::ended`] yields a [`GroupEnded`] and
//! [`Membership::new`] wants an [`Ended`]:
//!
//! ```ignore
//! parent.add(id, Membership::new(
//!     async move { match child.ended().await {
//!         GroupEnded::Member { reason, .. } => reason,
//!         _ => Ended::Stopped,
//!     } },
//!     move || async move { child.stop() },
//! ));
//! ```

use std::fmt;
use std::future::Future;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::pin::Pin;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError, Weak};
use std::task::Poll;

use n0_future::{FuturesUnordered, Stream, StreamExt};
use tokio::sync::{watch, Notify};

use crate::panic::{catching, guarded, message_of};
use crate::{ActorId, Ended};

/// One member's ending, boxed so that one group can hold members of different
/// services.
type Ending = Pin<Box<dyn Future<Output = Ended> + Send>>;

/// What retiring one member produces. Boxed for [`Ending`]'s reason.
type Retiring = Pin<Box<dyn Future<Output = ()> + Send>>;

/// An ending as the conductor waits for it: stamped, and naming its member.
type Stamped = Pin<Box<dyn Future<Output = Reported> + Send>>;

/// What happens to the GROUP when one member ends for good.
///
/// A member's own restarts never reach here. This is asked once, when that
/// member's ending is final.
///
/// `#[non_exhaustive]` and the cost is zero, which is worth saying because the
/// opposite reading is easy: a caller *constructs* a `Verdict` and never matches
/// one, and the attribute binds downstream matches only. The crate's own match
/// on it stays exhaustively checked, and restoring a refused variant stays
/// additive.
#[derive(Debug, Clone, Copy)]
#[non_exhaustive]
pub enum Verdict {
    /// Carry on without it. The member leaves the group.
    Forget,
    /// End the group: every other member is retired, and
    /// [`ended`](Group::ended) names this member and its reason.
    StopAll,
}

/// Why a group ended.
///
/// Every variant carries what that ending has, and nothing it does not. There is
/// no `Outcome` here and no field called `verdict`: `keylink::Outcome` is the
/// middleware chain's result type, and a second one would be a name a reader has
/// to disambiguate at every use.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum GroupEnded {
    /// The policy said [`Verdict::StopAll`].
    Member {
        /// The member the policy was asked about.
        member: ActorId,
        /// Why that member ended.
        reason: Ended,
    },
    /// The policy panicked while deciding about this member.
    ///
    /// The panic is caught and the group ends. Letting the conductor die instead
    /// would leave every member running and unwatched, which is the group-actor
    /// problem arrived at from the other side. The group ends on the first panic,
    /// so the policy is never called again.
    ///
    /// **`panic = "abort"` defeats this**, as it defeats every `catch_unwind` in
    /// the crate: there is no unwind to catch, so the process goes and no
    /// [`Group`] ends at all. Stated here rather than assumed, because
    /// `local.rs` and `panic.rs` already state it twice and a third silent copy
    /// is the drift those two exist to prevent.
    PolicyPanicked {
        /// The member the policy was asked about.
        member: ActorId,
        /// What it panicked with.
        message: String,
    },
    /// The future that was to report this member's ending panicked instead.
    ///
    /// Nothing can be asked of the policy here: a [`Verdict`] is a decision
    /// about an [`Ended`], and the value that would have carried one never
    /// arrived. The group ends rather than carry on with a member it can no
    /// longer hear, which is the loud half of the choice — the quiet half would
    /// be to forget that member and leave a caller believing it is watched.
    ///
    /// [`membership!`](crate::membership) cannot produce this: a generated
    /// `ended()` is a watch read. It is reachable only from a hand-written
    /// [`Membership::new`].
    EndingPanicked {
        /// The member whose ending was being awaited.
        member: ActorId,
        /// What the ending future panicked with.
        message: String,
    },
    /// [`Group::stop`] was called.
    Stopped,
    /// Every [`Group`] was dropped, so nothing decides any more.
    ///
    /// The members are still running. This is the group's answer to whoever was
    /// awaiting one, not a statement about the actors.
    Dropped,
}

/// The means to end a member the group has stopped watching.
///
/// [`Group::add`] hands one back when a name was taken, and [`Group::remove`]
/// whenever it removes one. The group no longer watches that member and will
/// never ask the policy about it again: its ending future was consumed when it
/// was added, and an ending has one owner.
///
/// **A separate type from [`Membership`] for exactly that reason.** A
/// `Membership` can be added and this cannot, so the two are not one type with a
/// field that is sometimes absent — a value that looks re-addable and is not
/// would leave a doc comment standing where the type should.
pub struct Retire {
    end: Box<dyn FnOnce() -> Retiring + Send>,
}

impl Retire {
    /// End that member, and wait for it.
    ///
    /// What awaiting this promises is what awaiting any ending in this crate
    /// promises, and no more: the member's task is gone. It is **not** a promise
    /// that the member's observers released what they held — a state that
    /// flushes in `Drop` needs its own signal, exactly as
    /// [`stop`](crate::LocalActor::stop) says.
    ///
    /// Dropping this instead ends nothing. The actor runs for as long as
    /// whoever else holds a handle keeps it, which for a
    /// [`membership!`](crate::membership) member is the caller.
    pub async fn retire(self) {
        (self.end)().await;
    }
}

/// By hand, because a boxed closure has no [`Debug`](fmt::Debug) to derive from,
/// and there is nothing else in here to print.
impl fmt::Debug for Retire {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Retire").finish_non_exhaustive()
    }
}

/// One actor the group watches, and the means to end it.
///
/// Two values rather than a handle, for the reason this module is not generic:
/// members are heterogeneous, so what a group can hold is the two capabilities
/// it needs, as values.
pub struct Membership {
    /// Taken by [`Group::add`], which is then the only thing that can await it.
    ended: Ending,
    retire: Retire,
}

impl Membership {
    /// `ended` is this actor's own [`ended()`](crate::LocalActor::ended);
    /// `retire` ends it.
    ///
    /// Both must name the SAME actor. Prefer
    /// [`membership!`](crate::membership), which names it once and cannot cross
    /// them.
    ///
    /// **`retire` returns a future so that a caller can choose.**
    /// [`abort`](crate::LocalActor::abort) is synchronous and
    /// [`stop`](crate::LocalActor::stop) is not, so a synchronous `FnOnce()`
    /// would have made `abort` the only expressible answer while the prose took
    /// credit for a choice the signature had already made.
    ///
    /// `abort` is the recommendation, and it has evidence rather than taste:
    /// `stop` puts a message in the mailbox, and a supervised actor polls its
    /// inbox only inside the run loop and never across a backoff sleep. So a
    /// `stop`-based teardown of a member sitting in backoff cannot return until
    /// that backoff elapses.
    ///
    /// **Both arguments are caller code, and a panic in either is caught.**
    /// Which one panicked decides what the group does, and the two answers are
    /// different because the consequences are:
    ///
    /// - `retire`, in **either** half — the closure or the future it returns —
    ///   costs **one member**. The rest are still retired and the group's ending
    ///   is whatever it was already going to be. There is nobody to report it
    ///   to, so it is discarded.
    /// - `ended` costs the **group**, as [`GroupEnded::EndingPanicked`]. Nothing
    ///   can be asked of the policy about a member whose reason never arrived,
    ///   and carrying on would leave a caller believing that member is watched.
    ///
    /// No [`Ended`] is invented for the second case, which is why it is a
    /// [`GroupEnded`] variant rather than a reason: no variant of `Ended` means
    /// "the future that was going to say why panicked instead", and reporting
    /// one that does not fit would be worse than the gap it filled.
    /// [`membership!`](crate::membership) cannot reach it — a generated
    /// `ended()` is a watch read — so it is a hand-written-future case only.
    #[must_use = "a `Membership` nothing adds watches nothing; give it to \
                  `Group::add`"]
    pub fn new<F>(
        ended: impl Future<Output = Ended> + Send + 'static,
        retire: impl FnOnce() -> F + Send + 'static,
    ) -> Self
    where
        F: Future<Output = ()> + Send + 'static,
    {
        Self {
            ended: Box::pin(ended),
            retire: Retire {
                end: Box::new(move || Box::pin(retire())),
            },
        }
    }
}

/// By hand, because neither half has a [`Debug`](fmt::Debug) to derive from: one
/// is a boxed future and the other a boxed closure.
impl fmt::Debug for Membership {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Membership").finish_non_exhaustive()
    }
}

/// One membership: how to end the member, its name, and the stamp saying which
/// membership under that name it is.
///
/// The ending is not here. It went to the conductor when the member was added,
/// which is the whole reason [`Retire`] is a type of its own.
///
/// A named struct and not a tuple, for `names.rs`'s reason: three fields read at
/// several call sites are several chances to read the stamp as a count.
struct Entry {
    name: ActorId,
    generation: u64,
    retire: Retire,
}

/// One member's ending, and which membership it belongs to.
///
/// The first two fields are [`Entry`]'s first two, in that order and no other:
/// the conductor compares them field for field, and two records that mean the
/// same pair while listing it in two orders is how a transposed comparison gets
/// written and not seen.
struct Reported {
    name: ActorId,
    generation: u64,
    /// `Err` carries a panic message: the future that was to say why this member
    /// ended panicked instead, and there is no [`Ended`] that means that.
    reason: Result<Ended, String>,
}

/// The membership list, and the two flags the conductor reads.
struct State {
    /// Insertion order, so [`Group::names`] is ordered and teardown has a
    /// direction. A `Vec` and not a map: here the order is the point.
    members: Vec<Entry>,
    /// Stamped endings [`Group::add`] has filed and the conductor has not taken
    /// yet. Drained to empty at the top of every iteration — see [`conduct`].
    staged: Vec<Stamped>,
    /// The next generation stamp.
    next: u64,
    /// Set by [`Group::stop`], read by the conductor.
    stopping: bool,
    /// Set by the conductor once the group has ended, so that a later
    /// [`Group::add`] can say so instead of filing a member nothing watches.
    ///
    /// `closed` and not `settled`, because [`Shared`] already has a `settled`
    /// holding the answer itself. Two fields one hop apart, one a `bool` and one
    /// a channel, both named for the same event, is a reader's problem and was
    /// this file's.
    closed: bool,
}

/// What a [`Group`] and its conductor share.
struct Shared {
    state: Mutex<State>,
    /// Fired by every mutation of the list, and by this struct's own `Drop`.
    ///
    /// An [`Arc`] the conductor holds a second handle on, rather than a field it
    /// reaches through the [`Weak`]: the conductor *waits* on this, and one that
    /// had to upgrade in order to wait would keep alive the very state whose
    /// drop it is waiting for.
    wake: Arc<Notify>,
    /// The conductor's answer, as a receiver. The conductor holds the sender, so
    /// it can still answer after this state is gone.
    settled: watch::Receiver<Option<GroupEnded>>,
}

impl Shared {
    /// Poison recovery rather than propagation, for `names.rs`'s reason: one bad
    /// decision should cost one decision. Nothing here can panic while the lock
    /// is held — the policy runs on the conductor's task with no lock in hand —
    /// so this arm is unreachable in practice and must still be typed.
    fn lock(&self) -> MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

/// The wake that lets a dropped group be noticed at all.
///
/// The conductor waits on two things and neither of them is "the last [`Group`]
/// went away". Without this, a group nobody holds is noticed only when some
/// member happens to end — so a group with two healthy members and no holder
/// would leave every awaiter of [`Group::ended`] waiting for an answer that is
/// already decided.
impl Drop for Shared {
    fn drop(&mut self) {
        self.wake.notify_one();
    }
}

/// Watches a set of actors, and ends them together when the policy says so.
///
/// Cheap to clone: every clone names the same group, so the code that adds
/// members and the code that awaits the ending can each hold one. The group
/// stops watching when the **last** clone is dropped, and answers
/// [`GroupEnded::Dropped`].
#[derive(Clone)]
pub struct Group {
    shared: Arc<Shared>,
}

impl Group {
    /// A group that watches nothing yet.
    ///
    /// `policy` is asked once per member, when that member's ending is final. It
    /// is held by the group's own task and **not** in the shared state, which is
    /// why `Send` is enough and `Sync` is not asked for.
    ///
    /// An empty group waits. `new` starts empty, and [`Verdict::Forget`] on the
    /// last member returns to empty; neither ends the group, because an
    /// [`add`](Self::add) may follow.
    ///
    /// # Panics
    ///
    /// Panics when called outside a tokio runtime, like every other spawn.
    #[must_use = "dropping the `Group` stops watching; hold it"]
    pub fn new(policy: impl FnMut(&ActorId, &Ended) -> Verdict + Send + 'static) -> Self {
        let wake = Arc::new(Notify::new());
        let (report, settled) = watch::channel(None);
        let shared = Arc::new(Shared {
            state: Mutex::new(State {
                members: Vec::new(),
                staged: Vec::new(),
                next: 0,
                stopping: false,
                closed: false,
            }),
            wake: Arc::clone(&wake),
            settled,
        });
        // Weak, so a group nobody holds any more is dropped even with members
        // still running. `Shared`'s own `Drop` is what wakes the task to find
        // out.
        n0_future::task::spawn(conduct(Arc::downgrade(&shared), wake, report, policy));
        Self { shared }
    }

    /// Watch `member` under `name`, replacing whatever held that name.
    ///
    /// Replacing rather than refusing, for
    /// [`Names::register`](super::Names::register)'s reason, which transfers
    /// verbatim: a name is freed asynchronously, so a registry that refused a
    /// taken name would refuse a restart nondeterministically. The displaced
    /// member coming back is what keeps replacing from being silent.
    ///
    /// A replacement keeps the displaced member's **position**. The position is
    /// that name's place in the teardown order, and re-registering a rebuilt
    /// actor is not a statement that it should now be torn down first.
    ///
    /// What comes back is a [`Retire`] and not a `Membership`, because the
    /// displaced member's ending is now the group's: an ending has one owner,
    /// and this group consumed it when that member was added. Ending it is the
    /// only thing left to do with it, and the type says so.
    ///
    /// A group that has already ended watches nothing more, so `member` is not
    /// filed and its `Retire` comes straight back rather than sitting where
    /// nothing will look at it.
    #[must_use = "the group is not watching this member — either `name` \
                  displaced it, or the group had already ended and never filed \
                  it; retire it, or say `let _ =` to mean that it runs on for \
                  whoever else holds a handle"]
    pub fn add(&self, name: ActorId, member: Membership) -> Option<Retire> {
        let Membership { ended, retire } = member;
        let mut held = self.shared.lock();
        if held.closed {
            return Some(retire);
        }
        // Every entry is stamped, and `Names`' own "the generation stamp, and
        // the race it is for" documents that race: without one, a displaced
        // member's still-pending ending is reported under a name its replacement
        // now owns, and the policy answers about an actor the caller has already
        // replaced.
        let generation = held.next;
        held.next += 1;
        let stamped = name.clone();
        held.staged.push(Box::pin(async move {
            // The caller's future, so it is guarded like the caller's policy and
            // the caller's `retire`. An unwind here would take the conductor
            // with it and orphan every member of the group.
            let reason = guarded(ended).await.map_err(|panicked| panicked.bare());
            Reported {
                name: stamped,
                generation,
                reason,
            }
        }));
        let entry = Entry {
            name,
            generation,
            retire,
        };
        let displaced = match held
            .members
            .iter()
            .position(|filed| filed.name == entry.name)
        {
            Some(at) => Some(std::mem::replace(&mut held.members[at], entry).retire),
            None => {
                held.members.push(entry);
                None
            }
        };
        drop(held);
        self.shared.wake.notify_one();
        displaced
    }

    /// Stop watching a member, without ending it and without asking the policy.
    ///
    /// Its ending stays with the group's task until it resolves, and is then
    /// discarded on the stamp rather than decided on. That is why this hands
    /// back a [`Retire`] rather than the `Membership` that went in: the ending
    /// is spent, and ending the member is what remains.
    #[must_use = "the group no longer watches this member; retire it, or say \
                  `let _ =` to mean that it runs on for whoever else holds a \
                  handle"]
    pub fn remove(&self, name: &ActorId) -> Option<Retire> {
        let mut held = self.shared.lock();
        let at = held.members.iter().position(|filed| &filed.name == name)?;
        let removed = held.members.remove(at).retire;
        drop(held);
        // Nothing the conductor has to do about it — the discard happens on the
        // stamp when that ending resolves. Fired anyway so that every mutation
        // of the list wakes the conductor and no later one can forget to; it
        // costs one iteration that drains an empty staging list.
        self.shared.wake.notify_one();
        Some(removed)
    }

    /// Every name, in the order it was added.
    ///
    /// An eager `Vec` for [`Names::names`](super::Names::names)' reason: the
    /// lock is released before the caller does anything with it, so a caller
    /// that awaits per name cannot be holding it.
    ///
    /// Named `names` and not `members` because it returns names.
    /// [`Pool::members`](super::Pool::members) returns handles, and two methods
    /// with one name returning different kinds of thing is the confusion this
    /// avoids.
    #[must_use]
    pub fn names(&self) -> Vec<ActorId> {
        self.shared
            .lock()
            .members
            .iter()
            .map(|entry| entry.name.clone())
            .collect()
    }

    /// Resolve when the group ends, and say why. Callable more than once.
    ///
    /// Holds a watch receiver and nothing else, so awaiting it neither keeps the
    /// group watching nor keeps a member alive.
    pub fn ended(&self) -> impl Future<Output = GroupEnded> + Send + 'static {
        let mut watching = self.shared.settled.clone();
        async move {
            // `wait_for` and not `changed()`, for the reason `LocalActor::ended`
            // gives at its own watch read: a fresh receiver marks the current
            // value as seen, so `changed()` would wait forever on a second call.
            // `GroupEnded` is `Clone` so that two awaiters both get one.
            match watching.wait_for(Option::is_some).await {
                Ok(seen) => match &*seen {
                    Some(how) => how.clone(),
                    // Unreachable: `wait_for` returned, so the predicate held.
                    // Written to be correct rather than to panic.
                    None => GroupEnded::Dropped,
                },
                // A dropped sender is a conductor that never answered, which a
                // runtime shutting down under it is the way to produce.
                // `Dropped` is the one of the four that is true of it: nothing
                // decides any more.
                Err(_) => GroupEnded::Dropped,
            }
        }
    }

    /// Retire every member and end the group, without asking the policy.
    ///
    /// Returns at once. The retiring happens on the group's own task, and
    /// [`ended`](Self::ended) is how a caller waits for it — which is also why
    /// this is not `async`: a synchronous request cannot be cancelled halfway
    /// through a teardown by a caller who dropped the future.
    ///
    /// Idempotent, and takes no deadline: `n0_future::time::timeout` composes
    /// around [`ended`](Self::ended), and a parameter here would duplicate it.
    /// **That composition is the answer to an unbounded wait, not a flourish** —
    /// teardown awaits every member, so one `retire` that never resolves means
    /// [`ended`](Self::ended) never resolves. The module's backpressure table
    /// carries the row.
    ///
    /// **It does not preempt an ending already under decision.** If a member's
    /// ending reached the policy before this call was observed, that decision
    /// stands and the group can end as [`GroupEnded::Member`] rather than
    /// [`GroupEnded::Stopped`] — every member is still retired, and the caller
    /// is told which of the two happened. Preempting would mean cancelling a
    /// policy mid-call, which is the one thing a synchronous closure cannot be
    /// asked to survive.
    pub fn stop(&self) {
        self.shared.lock().stopping = true;
        self.shared.wake.notify_one();
    }
}

impl fmt::Debug for Group {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let held = self.shared.lock();
        // `stopping` beside `closed`, because the question a reader debugs with
        // is "I called `stop()` and `ended()` has not resolved yet" — and
        // `closed` alone cannot separate a stop the conductor has not observed
        // yet from a stop nobody called. `staged` is the other half of that:
        // members filed but not yet watched.
        f.debug_struct("Group")
            .field("members", &held.members.len())
            .field("staged", &held.staged.len())
            .field("stopping", &held.stopping)
            .field("closed", &held.closed)
            .finish()
    }
}

/// The one task, and the only thing that decides anything.
///
/// It waits on two inputs with **no `select!`**: `tokio`'s `macros` feature is
/// off for `wasm32-unknown-unknown`, so a `select!` here would not compile for a
/// browser. The house pattern is a hand-written `poll_fn`, which `local.rs`
/// already uses for the actor loop's own two inputs.
///
/// # The wake discipline, because the actor loop's argument does not transfer
///
/// `local.rs` can say of its `poll_fn` that it cannot lose a wakeup, because *a
/// poll that returns `Pending` has taken nothing*. A [`Notified`] that returns
/// `Ready` **has taken the permit**. So this loop is written to one invariant:
///
/// > **One wake means drain to empty.**
///
/// The `Notified` is built fresh at every iteration and never held across a
/// decision, and the staging list is drained to empty at the top of every
/// iteration, before anything is awaited. A permit stored while the conductor is
/// inside the policy is taken by the *next* iteration's fresh `Notified`.
/// [`Notify`] coalesces — N adds can store one permit — so anything weaker loses
/// members silently.
///
/// [`Notified`]: tokio::sync::futures::Notified
async fn conduct(
    shared: Weak<Shared>,
    wake: Arc<Notify>,
    settled: watch::Sender<Option<GroupEnded>>,
    mut policy: impl FnMut(&ActorId, &Ended) -> Verdict + Send + 'static,
) {
    let mut endings: FuturesUnordered<Stamped> = FuturesUnordered::new();

    let how = loop {
        // The upgrade lives exactly as long as this block. A conductor holding
        // an `Arc` across the wait below would keep alive the state whose drop
        // it is watching for, and `Dropped` would never be reached.
        let teardown = {
            let Some(shared) = shared.upgrade() else {
                break GroupEnded::Dropped;
            };
            let stop_requested = {
                let mut held = shared.lock();
                // Drained to EMPTY, always, and before anything below is
                // awaited. This is the whole of the wake discipline above: a
                // `Notify` coalesces, so one permit can stand for N `add`s, and
                // a drain that took fewer than all of them would lose members
                // with nothing to report it.
                for staged in held.staged.drain(..) {
                    endings.push(staged);
                }
                held.stopping
            };
            stop_requested.then(|| closing(&shared))
        };
        if let Some(members) = teardown {
            retire_all(members).await;
            break GroupEnded::Stopped;
        }

        let mut waiting = std::pin::pin!(wake.notified());
        let reported = std::future::poll_fn(|cx| {
            // Endings first, and the arm polled second is skipped entirely when
            // the first is ready — which is what `select!` does too. It cannot
            // lose a wake: an unpolled `Notified` has taken nothing, and a
            // permit stored meanwhile is taken by the next iteration's.
            //
            // An empty set answers `Ready(None)` rather than `Pending`, which is
            // why this matches on `Some` instead of on readiness. An empty group
            // waits.
            if let Poll::Ready(Some(reported)) = Stream::poll_next(Pin::new(&mut endings), cx) {
                return Poll::Ready(Some(reported));
            }
            if waiting.as_mut().poll(cx).is_ready() {
                return Poll::Ready(None);
            }
            Poll::Pending
        })
        .await;
        let Some(reported) = reported else {
            // A wake. Everything it can be about is in the staging list, and the
            // top of this loop drains that to empty.
            continue;
        };

        let Some(shared) = shared.upgrade() else {
            break GroupEnded::Dropped;
        };
        {
            let mut held = shared.lock();
            let filed = held.members.iter().position(|entry| {
                entry.name == reported.name && entry.generation == reported.generation
            });
            // An ending whose stamp is no longer the one filed under that name
            // belongs to a member that was displaced or removed. Discarded,
            // never decided on.
            //
            // The `continue` leaves this block, so it drops `held` and then the
            // upgraded `Arc` — which is required, not incidental: carrying
            // either into the next iteration would deadlock on the drain above
            // or hide the drop this loop exists to notice. Said out loud because
            // a `continue` out of a lock scope is correct silently, and the next
            // edit here has to know it is load-bearing.
            let Some(at) = filed else { continue };
            // Off the books before the policy is asked, because both answers
            // want it gone: `Forget` carries on without it, and `StopAll` names
            // it rather than retiring an actor that has already ended.
            held.members.remove(at);
        }

        // An ending that panicked instead of arriving. The stamp was checked
        // first, above, so a member that was displaced or removed is discarded
        // here exactly as a healthy ending of its would be — a panic does not
        // buy an ending the right to be decided on.
        let reason = match reported.reason {
            Ok(reason) => reason,
            Err(message) => {
                retire_all(closing(&shared)).await;
                break GroupEnded::EndingPanicked {
                    member: reported.name,
                    message,
                };
            }
        };

        // The policy runs on this task, so a panic in it would otherwise leave
        // every member running and unwatched. Why that is caught, and the
        // `panic = "abort"` caveat on it, are on `GroupEnded::PolicyPanicked`,
        // where a caller meets them — this comment does not repeat them, because
        // two copies drift.
        match catch_unwind(AssertUnwindSafe(|| policy(&reported.name, &reason))) {
            Ok(Verdict::Forget) => {}
            Ok(Verdict::StopAll) => {
                retire_all(closing(&shared)).await;
                break GroupEnded::Member {
                    member: reported.name,
                    reason,
                };
            }
            Err(payload) => {
                retire_all(closing(&shared)).await;
                break GroupEnded::PolicyPanicked {
                    member: reported.name,
                    message: message_of(&*payload),
                };
            }
        }
    };

    // `send_replace` and not `send`, which fails when no receiver is left: the
    // last `Group` may have gone while this task was retiring, and an ending
    // that is not stored is one a receiver created a moment later would miss.
    settled.send_replace(Some(how));
}

/// Take every member off the books, newest first, and mark the group ended.
///
/// Reverse insertion order, because RFC-07 §9.8 names ordered shutdown as the
/// wanted thing this module delivers: a group that shuts down in an order is a
/// statement about several actors. Its observable effect is small and saying so
/// is not a hedge — [`retire_all`] starts every member concurrently, so the
/// order is a statement about the order retirement is *issued* in.
fn closing(shared: &Shared) -> Vec<Retire> {
    let mut held = shared.lock();
    // The two effects the rest of the module leans on, done under one lock so
    // that no `add` can interleave between them. `closed` is what makes a later
    // `add` hand its `Retire` straight back instead of filing a member nothing
    // watches, and clearing `staged` drops the endings of members that were
    // added but never drained — they are in this teardown either way, because
    // `add` files the entry and stages the ending as one locked step.
    held.closed = true;
    held.staged.clear();
    held.members
        .drain(..)
        .rev()
        .map(|entry| entry.retire)
        .collect()
}

/// Retire every member, building all of them before awaiting any.
///
/// The serial spelling — `for member in members { member.retire().await }` —
/// would let one member wedged in a turn that never yields delay the
/// *retirement* of every later member, which are then still serving. That is the
/// opposite of the fate sharing this module exists for.
///
/// What "concurrently" claims here, exactly: every retire future is built, and
/// therefore whatever synchronous work the caller's closure does runs, before
/// any of them is polled — and no member's retirement waits on another's to
/// complete. It does not claim an order the set does not promise.
///
/// Both halves are caught, and the reason is the same one this module gives for
/// catching the policy: an unwind through this loop would orphan every member
/// after the one that panicked, and — worse — return through [`conduct`] without
/// settling, so every awaiter of [`Group::ended`] would be told
/// [`GroupEnded::Dropped`] while the caller still holds a live [`Group`]. A
/// wrong answer, not a missing one. One bad member costs one member.
///
/// A panic here is **discarded**, because there is nobody to tell: teardown has
/// no reply channel and the group's own ending is already decided. That is
/// `schedule.rs`'s position for the same shape of problem, and it is the one
/// place in this module where something is dropped without a signal.
async fn retire_all(members: Vec<Retire>) {
    let mut retiring = FuturesUnordered::new();
    for member in members {
        // `catching` covers the closure, which runs HERE rather than at the
        // first poll, and `guarded` covers the future it returns. Two calls
        // because they are two pieces of caller code at two moments — the
        // module's own `Membership::new` argument, that `retire` is a closure
        // returning a future, is exactly what makes them two.
        match catching(member.end) {
            Ok(fut) => retiring.push(guarded(fut)),
            Err(_discarded) => {}
        }
    }
    while retiring.next().await.is_some() {}
}

/// Build a [`Membership`] from a handle, naming it once.
///
/// The one way to misuse [`Membership::new`] is to hand it one actor's ending
/// and another actor's `retire`. This names the handle once, so it cannot cross
/// them.
///
/// It **clones**, so the caller keeps its own handle and a dropped
/// [`Group`] leaves the actor running. It uses
/// [`abort`](crate::LocalActor::abort) rather than
/// [`stop`](crate::LocalActor::stop), for the reason
/// [`Membership::new`] gives: a `stop`-based teardown of a member sitting in a
/// supervision backoff cannot return until that backoff elapses.
///
/// A hand-written handle that is not `Clone`, or one whose ending is not its
/// own `ended()`, calls [`Membership::new`] directly. That is the constructor
/// this expands to.
///
/// **Pass the handle, not a reference to it.** `membership!(&reserve)` clones
/// the reference rather than the handle, so the expansion moves a `&Handle` into
/// a `'static` closure and the compiler reports a lifetime error pointing inside
/// this macro, where there is nothing to fix. Write `membership!(reserve)`; the
/// macro does the cloning.
///
/// ```ignore
/// let _ = group.add(ActorId::new("reserve")?, membership!(reserve));
/// ```
///
/// **The one `#[macro_export]` outside `macros.rs`, and deliberately.** The
/// other twenty-three are `service!` and its expansion helpers, which belong
/// together because they are one machine. This one is not part of that machine:
/// it builds a `compose` type, its whole body is two lines naming
/// [`Membership::new`], and a reader who finds it beside the type it constructs
/// has found it where they looked. It lands at the crate root regardless —
/// `#[macro_export]` always does — which is why the expansion writes
/// `$crate::compose::Membership` rather than a bare path.
#[macro_export]
macro_rules! membership {
    ($handle:expr) => {{
        let handle = ::core::clone::Clone::clone(&$handle);
        $crate::compose::Membership::new(handle.ended(), move || async move {
            handle.abort();
        })
    }};
}
