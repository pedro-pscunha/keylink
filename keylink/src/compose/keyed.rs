//! Endings for keyed actors, as futures rather than a callback.
//!
//! A local actor answers *when did you end, and why* itself:
//! [`ended()`](crate::LocalActor::ended) hands back a future. A keyed actor
//! mounted in a [`System`](crate::System) answers nothing — there is no per-actor
//! handle to ask, which is the gap [`Names`](super::Names) and
//! [`Group`](super::Group) both name and neither closes.
//!
//! **The signal is not missing. Its shape is.** A keyed system already computes
//! the true [`Ended`] for every actor that ends itself and hands it to
//! [`Lifecycle::ended`] — a supervisor that exhausted its budget arrives there as
//! [`Ended::GaveUp`], a panicked turn as [`Ended::Panicked`]. What no caller can
//! do with a `Lifecycle` is *await* one: it is a method the registry calls on its
//! own task, so a caller who wants to wait writes the channel by hand. This is
//! that channel, written once.
//!
//! ```ignore
//! let endings = Endings::new();
//! let system = ledger::System::builder(Eviction::Never)
//!     .lifecycle(endings.clone())
//!     .build();
//!
//! let id = ActorId::new("ledger/7")?;
//! // Before the mount. An ending reported before a waiter is filed reaches
//! // nobody — see the miss-window below.
//! let ended = endings.watch(&id);
//! system.mount(id.clone(), ledger::spawn_supervised(build, budget))?;
//!
//! // Somewhere else entirely.
//! match ended.await {
//!     Ended::GaveUp { after, error } => { /* the supervisor is out of budget */ }
//!     why => { /* stopped, evicted, panicked, aborted */ }
//! }
//! ```
//!
//! # A keyed actor as a group member, which is what this was built for
//!
//! [`Membership`](super::Membership) wants two values: a future that says when
//! the member ended, and a way to end it. A keyed actor could supply neither, so
//! [`Group`](super::Group) watched local actors only. It supplies both now — the
//! ending from here, the retiring from [`System::abort`](crate::System::abort):
//!
//! ```ignore
//! // One member gives up, the group ends.
//! let group = Group::new(|_who: &ActorId, _why: &Ended| Verdict::StopAll);
//!
//! let id = ActorId::new("ledger/7")?;
//! let ended = endings.watch(&id);
//! system.mount(id.clone(), ledger::spawn_supervised(build, budget))?;
//!
//! let retiring = system.clone();
//! let retired = id.clone();
//! let _ = group.add(
//!     id,
//!     Membership::new(ended, move || async move {
//!         retiring.abort(&retired).await;
//!     }),
//! );
//! ```
//!
//! [`abort`](crate::System::abort) and not
//! [`shutdown`](crate::System::shutdown), for the reason
//! [`Membership::new`](super::Membership::new) already gives about local actors:
//! a supervised member sitting in a backoff sleep polls no inbox, so a draining
//! teardown of one cannot return until that backoff elapses, while the group is
//! waiting to make its next decision. A caller who wants the drain writes
//! `shutdown` here and accepts that wait knowingly.
//!
//! # THE THING TO KNOW — the miss-window, and why it cannot be closed by argument order
//!
//! [`watch`](Endings::watch) files a waiter. [`ended`](Lifecycle::ended) resolves
//! whatever is filed **at the instant the report arrives** and files nothing for
//! next time. So an ending reported before its waiter exists reaches nobody, and
//! that waiter then waits for as long as the system lives — the system going away
//! is the one thing that still answers it, with [`Ended::Vanished`].
//!
//! Ask for the future before the actor can end:
//!
//! - **A hand-mounted actor** — [`watch`](Endings::watch) before
//!   [`System::mount`](crate::System::mount). The actor is running and
//!   addressable the moment `mount` returns.
//!   [`Endings`] must already be installed on that system, which means the
//!   [`lifecycle`](crate::SystemBuilder::lifecycle) call happened at build time.
//! - **A lazily spawned actor** — [`SystemBuilder::spawn_with`](crate::SystemBuilder::spawn_with)
//!   builds it on the first message from the network, so the window opens the
//!   moment the node serves. There is no call to be "before" from the caller's
//!   side, and a `watch` racing an id the network already mounted and killed is
//!   the honest limit of this type rather than a defect in it.
//!
//! [`Names::register`](super::Names::register) has the same race and does not
//! have this window, and the difference is worth reading rather than assuming.
//! `register` takes the ending **as an argument**: the future exists before the
//! registration does, so there is no interval in which the actor is watchable and
//! unwatched. Here the signal exists only as a callback the registry owns, so the
//! interval is real and no signature change closes it.
//!
//! **The alternative was to remember endings for names nobody asked about**, so
//! that a late `watch` could be answered out of a record. That is refused, and it
//! is the refusal this module shares with every other in `compose`: the record is
//! a map keyed by *arrivals* rather than by a membership the caller wrote, so a
//! system that mounts and ends a million lazily spawned ids grows a million
//! reasons nobody will ever read. Bounding it means evicting reasons, and a
//! record that may have evicted the answer is a `watch` that resolves or not
//! depending on load — nondeterminism bought with memory. Staying pending is at
//! least the same answer every time.
//!
//! # `Ended` is the true reason on one path of four
//!
//! Four things end a keyed actor and all four report through [`Lifecycle`], so
//! all four resolve a waiter. They do not carry equally good reasons, and this is
//! the whole of it:
//!
//! | how it ended | what `why` is | is it *why* |
//! |---|---|---|
//! | the actor ended itself — `reap` | the [`Ended`] its own loop returned | **yes.** [`GaveUp`](Ended::GaveUp), [`Panicked`](Ended::Panicked), [`Observer`](Ended::Observer), [`Feed`](Ended::Feed) all arrive intact |
//! | [`System::shutdown`](crate::System::shutdown) | synthesised from the join | no. [`Stopped`](Ended::Stopped) means *this shutdown drained it*, and an actor that ended on an observer's refusal a second earlier reads `Stopped` here |
//! | [`System::abort`](crate::System::abort) | [`Ended::Vanished`], a constant | no. It is what abort *did*, and the loop was never asked |
//! | eviction, on the idle clock | synthesised from the same join `shutdown` uses | no, and the same two spellings |
//!
//! So a group whose policy branches on [`Ended::GaveUp`] is reading the one path
//! that can produce it, which is also the only path that fires when **nobody
//! asked** for the teardown — and that is the case a supervisor exists for. A
//! policy that branches on [`Ended::Stopped`] to mean "somebody stopped it
//! deliberately" is reading a value that also means "it was already dead and the
//! shutdown found the corpse". [`System::shutdown`](crate::System::shutdown)
//! argues that at length; nothing here re-decides it.
//!
//! **Exactly one report fires per ending, which is why one waiter is enough.**
//! `shutdown`, `abort` and the sweeper each take the entry out of the map before
//! they tear it down and report afterwards, and `reap` reports only if the entry
//! was still there. So a waiter is resolved once and never twice, and a second
//! [`watch`](Endings::watch) on a name already ended is the miss-window above
//! rather than a duplicate.
//!
//! # Backpressure: none, because nothing here queues
//!
//! A map, a lock, and one [`oneshot`] per waiter.
//! [`ended`](Lifecycle::ended) removes an entry and sends into channels that are
//! never full, so it cannot wait — which is what [`Lifecycle`] requires of it,
//! since a mount and an eviction wait for the report to return.
//!
//! No task is spawned. [`Names`](super::Names) spawns one per registration
//! because it has to *await* an ending to act on it; this type is handed its
//! endings, so there is nothing to await and no `# Panics` for a runtime that is
//! not there.
//!
//! The map is bounded by the number of **live [`watch`](Endings::watch)
//! futures**, not by the number of names a system has seen: dropping the future
//! takes its waiter back out. A waiter for a name that is never mounted therefore
//! costs one entry for exactly as long as its caller holds the future, and
//! nothing after — the leak such a call could be is closed by that `Drop` rather
//! than merely documented. `std::mem::forget` on the future defeats it, as it
//! defeats every `Drop` in the crate.
//!
//! # One `Endings` on two systems merges their names
//!
//! Nothing scopes a waiter to a system, because [`Lifecycle`] is not told which
//! system is reporting. Install one [`Endings`] on two systems and a `"ledger"`
//! ending in either resolves every `"ledger"` waiter. That is usually wrong and
//! it is cheap to avoid: one [`Endings`] per system.
//!
//! # Cost
//!
//! Per [`watch`](Endings::watch): one `oneshot` channel, two [`ActorId`] clones —
//! one for the map key, one the future carries so its `Drop` can find its own
//! waiter — and a `u64` generation.
//!
//! Per ending with **no** waiter: one hash lookup. No allocation and no clone of
//! the [`Ended`], which matters because every ending in a system with one watched
//! actor takes this path. Per ending with `n` waiters: one `Ended` clone each,
//! since each waiter owns its answer.

use std::collections::hash_map;
use std::collections::HashMap;
use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError, Weak};
use std::task::{Context, Poll};

use tokio::sync::oneshot;

use crate::{ActorId, Ended, Lifecycle};

/// Who is waiting for which name.
///
/// A type alias because the `Weak` the future holds spells it a second time, and
/// a nested generic written twice is two chances to write it differently.
type Waiting = HashMap<ActorId, Vec<Waiter>>;

/// One caller waiting on one name.
///
/// A named struct and not `(u64, Sender<Ended>)` for [`Names`](super::Names)'
/// reason one file over, where the same pair is a `Registered`: a bare `u64`
/// beside anything reads as a count at every site that destructures it.
struct Waiter {
    /// Which waiter under this name it is, so that an abandoned future removes
    /// its own and not a later one's. The module doc argues why resolution needs
    /// no stamp and this path does.
    ///
    /// `generation` and not `slot`, because `names.rs` and `group.rs` both call
    /// this exact thing a generation. One concept under two names across three
    /// files is the drift the paragraph above claims to be avoiding.
    generation: u64,
    /// Resolves this waiter's future. Dropped without sending only when the
    /// future was dropped first.
    tell: oneshot::Sender<Ended>,
}

/// Endings for keyed actors, as futures rather than a callback.
///
/// Install it on a [`System`](crate::System) with
/// [`SystemBuilder::lifecycle`](crate::SystemBuilder::lifecycle), then ask for an
/// actor's ending with [`watch`](Self::watch) **before** that actor can end.
///
/// Cheap to clone: every clone names the same waiters, so the code that installs
/// it and the code that waits can each hold one. The clone handed to the builder
/// is what keeps the waiters alive for the life of the system.
pub struct Endings {
    waiting: Arc<Mutex<Waiting>>,
    next: Arc<AtomicU64>,
}

impl Endings {
    /// Nothing watched, and nothing installed yet.
    #[must_use]
    pub fn new() -> Self {
        Self {
            waiting: Arc::new(Mutex::new(HashMap::new())),
            next: Arc::new(AtomicU64::new(0)),
        }
    }

    /// A future that resolves when the actor mounted under `name` ends, and says
    /// why.
    ///
    /// **Ask for it before the actor can end.** The future is a waiter filed
    /// under `name` right here, and an ending reported before it is filed
    /// resolves nothing and is not remembered — so this future then waits for the
    /// life of the system. The module doc states the window per mounting path and
    /// why no signature closes it.
    ///
    /// The [`Ended`] is the registry's, with all four teardown paths' reasons
    /// mixed together; read the table in the module doc before branching on it,
    /// because only one of the four answers *why the loop exited*.
    ///
    /// Several waiters may hold `name` at once and each is answered. Dropping the
    /// future unfiles its waiter, so abandoning one costs nothing after the drop.
    ///
    /// It resolves [`Ended::Vanished`] when **every** [`Endings`] clone is
    /// dropped while it waits, the one you are holding included. Nothing can
    /// report through a registry that no longer exists, and resolving is louder
    /// than the alternative: an `Endings` that was built, watched and never
    /// installed would otherwise hang its callers with no signal at all.
    ///
    /// **It is not a signal that the system went away**, and this paragraph
    /// used to say it was. The trigger is the last strong reference dying, and
    /// [`watch`](Self::watch) takes `&self` — so any caller watching a second
    /// name is still holding a clone, as the module doc's own example is. A
    /// dropped [`System`](crate::System) reports nothing at all: nothing in
    /// `registry.rs` implements [`Drop`] for it, so the clone it held going away
    /// is one clone of several and the waiters simply stay pending.
    #[must_use = "a `watch` nobody awaits files a waiter and reads no ending; \
                  hold the future, or do not ask for it"]
    pub fn watch(&self, name: &ActorId) -> impl Future<Output = Ended> + Send + 'static {
        let (tell, ending) = oneshot::channel();
        let generation = self.next.fetch_add(1, Ordering::Relaxed);
        recovering(&self.waiting)
            .entry(name.clone())
            .or_default()
            .push(Waiter { generation, tell });
        Awaited {
            ending,
            filed: Some((name.clone(), generation)),
            waiting: Arc::downgrade(&self.waiting),
        }
    }

    /// The lock, for the one caller that has a `&self` to reach it through.
    fn lock(&self) -> MutexGuard<'_, Waiting> {
        recovering(&self.waiting)
    }
}

/// Take the lock, recovering from poison rather than propagating it.
///
/// A free function over the [`Mutex`] and not a method, because [`Awaited`]'s
/// [`Drop`] reaches the same map through a [`Weak`] and has no `&Endings` to call
/// a method on. It was written out a second time there, bare, with the argument
/// living only here — one policy in two places, and the copy carried no reason
/// for itself.
///
/// Poison recovery rather than propagation is [`Names`](super::Names)' argument
/// for the same helper: one bad report should cost one report. Nothing here can
/// panic while the lock is held — the sends happen after the guard is dropped —
/// so this arm is unreachable in practice and must still be typed.
fn recovering(waiting: &Mutex<Waiting>) -> MutexGuard<'_, Waiting> {
    waiting.lock().unwrap_or_else(PoisonError::into_inner)
}

impl Lifecycle for Endings {
    /// Nothing. A mount is not an ending, and this type is not a second
    /// [`Lifecycle`] for people who want both — install two.
    fn mounted(&self, _id: &ActorId) {}

    /// Answer every waiter filed under `id`, and file nothing for a waiter that
    /// arrives later.
    fn ended(&self, id: &ActorId, why: &Ended) {
        // The guard dies with this statement, which is `Registry::reap`'s
        // discipline for its own reason: `send` wakes the waiting task, and
        // waking under a lock that task's own `Drop` takes is an edge worth not
        // having at all rather than one worth arguing is unreachable.
        let Some(waiters) = self.lock().remove(id) else {
            return;
        };
        for waiter in waiters {
            // The receiver is gone when a future was dropped between the removal
            // above and now. That is a caller who stopped caring, so there is
            // nothing to report and nobody to report it to.
            let _ = waiter.tell.send(why.clone());
        }
    }
}

impl Default for Endings {
    fn default() -> Self {
        Self::new()
    }
}

/// By hand and with no bound, for [`Names`](super::Names)' reason: the derive
/// would demand nothing here, but writing it out is what keeps a later field from
/// silently deciding whether this type is shareable. Every clone names the same
/// waiters.
impl Clone for Endings {
    fn clone(&self) -> Self {
        Self {
            waiting: Arc::clone(&self.waiting),
            next: Arc::clone(&self.next),
        }
    }
}

/// By hand, because the map holds channel senders with no
/// [`Debug`](fmt::Debug) to derive from — and printing the names would be a
/// snapshot of a lock this can take at any moment.
impl fmt::Debug for Endings {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Endings")
            .field("watching", &self.lock().len())
            .finish()
    }
}

/// The future [`Endings::watch`] hands back.
///
/// A named type and not an `async` block, and the [`Drop`] impl below is the
/// whole reason: an abandoned waiter has to come back out of the map, and an
/// `async` block has nowhere to put that.
///
/// **`Awaited` and not `Watch`, because this crate already has a `Watch`.**
/// `watch.rs`'s is public, is the connection-departure future, and is the one
/// `crate::Watch` resolves to — so an intra-doc link written here meant the
/// transport type, silently and while rustdoc stayed green, because the link was
/// valid. Two futures of one name in one crate, both reached through a method
/// called `watch`, is a collision that produced exactly that defect inside its
/// own diff. This one is private, so the rename costs nothing outside this file.
struct Awaited {
    ending: oneshot::Receiver<Ended>,
    /// Which waiter to take back out if this is dropped before it resolves, and
    /// `None` once it has. A resolved waiter is already gone from the map — the
    /// report took it out to send — so the `Drop` below must not take the lock to
    /// discover that.
    filed: Option<(ActorId, u64)>,
    /// [`Weak`], so an `Awaited` nobody has resolved does not keep the waiters
    /// alive past the last [`Endings`]. That is what makes the dropped-registry
    /// case reachable, and it resolves [`Ended::Vanished`].
    waiting: Weak<Mutex<Waiting>>,
}

impl Future for Awaited {
    type Output = Ended;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Ended> {
        // `get_mut` rather than a projection: every field is `Unpin`, so this
        // future is, and the receiver is polled where it lies.
        let held = self.get_mut();
        let why = match Pin::new(&mut held.ending).poll(cx) {
            Poll::Pending => return Poll::Pending,
            Poll::Ready(Ok(why)) => why,
            // The sender was dropped without sending, and the only thing that
            // drops it is the map going away with the last `Endings`.
            Poll::Ready(Err(_)) => Ended::Vanished,
        };
        held.filed = None;
        Poll::Ready(why)
    }
}

impl Drop for Awaited {
    fn drop(&mut self) {
        // `mine` and not `generation`, which is what the field is called: the two
        // are compared on one line below, and at `generation != generation` that
        // line is both unreadable and over the width, so rustfmt breaks it into
        // three. A break record anchors on it, and a one-line anchor is worth a
        // shorter local.
        let Some((name, mine)) = self.filed.take() else {
            return;
        };
        let Some(waiting) = Weak::upgrade(&self.waiting) else {
            return;
        };
        let mut held = recovering(&waiting);
        if let hash_map::Entry::Occupied(mut under) = held.entry(name) {
            // By generation and never by name. Another `watch` under this name
            // may have filed a waiter since this one was abandoned, and taking
            // the whole entry would unwatch it silently — a live caller waiting
            // on a future nothing can now resolve.
            under.get_mut().retain(|waiter| waiter.generation != mine);
            // A name with no waiters left is a key nothing reads. Leaving it
            // would make the map grow with names watched once rather than with
            // names watched now, which is the bound this type claims.
            if under.get().is_empty() {
                under.remove();
            }
        }
    }
}
