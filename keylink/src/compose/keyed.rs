//! Endings for keyed actors, as futures rather than a callback.
//!
//! A local actor answers *when did you end, and why* itself:
//! [`ended()`](crate::LocalActor::ended) hands back a future. A keyed actor is
//! addressed by id, so [`Registry::ended`](crate::Registry::ended) is its direct
//! per-actor observation door.
//!
//! [`Endings`] remains the composition adapter: it turns the keyed
//! [`Lifecycle::ended`] callback into futures that [`Group`](super::Group) and
//! other compositions can own. It deliberately retains its miss-window
//! semantics; callers that only need one keyed ending should use `Registry::ended`.
//!
//! ```ignore
//! let endings = Endings::new();
//! let system = ledger::Registry::builder(Eviction::Never)
//!     .lifecycle(endings.clone())
//!     .build();
//!
//! let id = ActorId::new("ledger/7")?;
//! // Before the mount. An ending reported before a waiter is filed reaches
//! // nobody — see the miss-window below.
//! let ended = endings.watch(&id);
//! system.mount(id.clone(), ledger::restarting(build, budget).mountable())?;
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
//! the member ended, and a way to end it. A keyed system supplies both now:
//! [`Registry::ended`](crate::Registry::ended) is the exact-entry door for a member
//! already mounted, while this adapter fans lifecycle callbacks into futures for
//! code that must file the waiter before mounting. Retirement uses
//! [`Registry::abort`](crate::Registry::abort):
//!
//! ```ignore
//! // One member gives up, the group ends.
//! let group = Group::new(|_who: &ActorId, _why: &Ended| Verdict::StopAll);
//!
//! let id = ActorId::new("ledger/7")?;
//! let ended = endings.watch(&id);
//! system.mount(id.clone(), ledger::restarting(build, budget).mountable())?;
//!
//! let retiring = system.clone();
//! let retired = id.clone();
//! // Discarded because this group cannot answer anything else yet: it is new,
//! // and this is its first member. A group that may have ended between two
//! // `add` calls must branch on `Added::Refused` instead — see the rebuild
//! // recipe in `group.rs`.
//! let _ = group.add(
//!     id,
//!     Membership::new(ended, move || async move {
//!         retiring.abort(&retired).await;
//!     }),
//! );
//! ```
//!
//! [`abort`](crate::Registry::abort) and not
//! [`shutdown`](crate::Registry::stop), for the reason
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
//! that waiter then remains pending. Dropping the system alone reports nothing;
//! only dropping the **last** [`Endings`] clone closes the waiter channel and
//! resolves it as [`Ended::Vanished`].
//!
//! Ask for the future before the actor can end:
//!
//! - **A hand-mounted actor** — [`watch`](Endings::watch) before
//!   [`Registry::mount`](crate::Registry::mount). The actor is running and
//!   addressable the moment `mount` returns.
//!   [`Endings`] must already be installed on that system, which means the
//!   [`lifecycle`](crate::RegistryBuilder::lifecycle) call happened at build time.
//! - **A lazily spawned actor** — [`RegistryBuilder::spawn_with`](crate::RegistryBuilder::spawn_with)
//!   builds it on the first message from the network, so the window opens the
//!   moment the node serves. There is no call to be "before" from the caller's
//!   side, and a `watch` racing an id the network already mounted and killed is
//!   the honest limit of this type rather than a defect in it.
//!
//! If an old incarnation is still draining after a replacement mounts, reports
//! are separated by the registry's opaque incarnation identity. A `watch` filed
//! after the replacement's mount belongs to the replacement, not to the late old
//! report. Filing before that mount while the old incarnation is still active is
//! ambiguous by id alone; use the direct [`Registry::ended`](crate::Registry::ended)
//! door after mounting when exact-entry identity matters.
//!
//! [`Names::register`](super::Names::register) does the same adaptation without
//! this window, and the difference is worth reading rather than assuming.
//! `register` takes the ending **as an argument**: the future exists before the
//! registration does, so there is no interval in which the actor is watchable and
//! unwatched. For this adapter the signal arrives as a [`Lifecycle`] callback, so
//! the interval is real. Use [`Registry::ended`](crate::Registry::ended) when the
//! already-mounted entry itself is the observation source.
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
//! | [`Registry::stop`](crate::Registry::stop) | synthesised from the join | no. [`Stopped`](Ended::Stopped) means *this shutdown drained it*, and an actor that ended on an observer's refusal a second earlier reads `Stopped` here |
//! | [`Registry::abort`](crate::Registry::abort) | the loop's natural ending if it won the cancellation race; otherwise [`Ended::Vanished`] | natural only when the loop published first |
//! | eviction, on the idle clock | synthesised from the same join `shutdown` uses | no, and the same two spellings |
//!
//! So a group whose policy branches on [`Ended::GaveUp`] is reading a reason
//! produced only by a supervisor exhausting its restart budget. Other natural
//! endings — [`Feed`](Ended::Feed), [`Panicked`](Ended::Panicked), and
//! [`Observer`](Ended::Observer) — can also happen without anybody asking for
//! teardown. A policy that branches on [`Ended::Stopped`] to mean "somebody stopped it
//! deliberately" is reading a value that also means "it was already dead and the
//! shutdown found the corpse". [`Registry::stop`](crate::Registry::stop)
//! argues that at length; nothing here re-decides it.
//!
//! **Exactly one report fires per incarnation.** `shutdown`, `abort` and the
//! sweeper each take the entry out before teardown and report afterwards; `reap`
//! reports only if the entry identity still matches. The registry also carries
//! that opaque identity through its internal lifecycle callback, so an old actor
//! finishing after a replacement mounted under the same id resolves only the old
//! incarnation's waiters. A second [`watch`](Endings::watch) on a name already
//! ended is the miss-window above rather than a duplicate.
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
//! The map is bounded by the system's **currently mounted incarnations plus live
//! [`watch`](Endings::watch) futures**, never by historical endings. Tracking a
//! mounted incarnation even with no waiter is what distinguishes an old teardown
//! report from a replacement under the same id; its row is removed at that
//! incarnation's ending. Dropping a future takes its waiter back out. A waiter for
//! a name never mounted therefore costs one entry only while held.
//! `std::mem::forget` defeats that `Drop`, as it defeats every `Drop` in the crate.
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
//! Per mount and ending: one hash lookup and an active-incarnation row; the row
//! is removed at ending. With `n` waiters, the ending is cloned once per waiter,
//! since each waiter owns its answer.

use std::collections::hash_map;
use std::collections::{HashMap, VecDeque};
use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError, Weak};
use std::task::{Context, Poll};

use tokio::sync::oneshot;

use super::group::Stamp;
use crate::{ActorId, Ended, Lifecycle};

/// Who is waiting for which name.
///
/// A type alias because the `Weak` the future holds spells it a second time, and
/// a nested generic written twice is two chances to write it differently.
type Waiting = HashMap<ActorId, Watched>;

#[derive(Default)]
struct Watched {
    /// Waiters filed before the first mount callback for this id.
    pending: Vec<Waiter>,
    /// Live incarnations in mount order. More than one exists while an old
    /// teardown drains after a replacement has mounted under the same id.
    mounted: VecDeque<Incarnation>,
}

struct Incarnation {
    identity: usize,
    waiters: Vec<Waiter>,
}

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

fn resolve(waiters: Vec<Waiter>, why: &Ended) {
    for waiter in waiters {
        let _ = waiter.tell.send(why.clone());
    }
}

/// Endings for keyed actors, as futures rather than a callback.
///
/// Install it on a [`Registry`](crate::Registry) with
/// [`RegistryBuilder::lifecycle`](crate::RegistryBuilder::lifecycle), then ask for an
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
    /// resolves nothing and is not remembered — so this future remains pending
    /// until a later report under that id or until the last [`Endings`] clone is
    /// dropped. The module doc states the window per mounting path.
    ///
    /// The [`Ended`] is the registry's, with all four teardown paths' reasons
    /// mixed together; read the table in the module doc before branching on it,
    /// because teardown often reports what the operation did rather than why the
    /// loop exited.
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
    /// dropped [`Registry`](crate::Registry) reports nothing at all: nothing in
    /// `registry.rs` implements [`Drop`] for it, so the clone it held going away
    /// is one clone of several and the waiters simply stay pending.
    #[must_use = "a `watch` nobody awaits files a waiter and reads no ending; \
                  hold the future, or do not ask for it"]
    pub fn watch(&self, name: &ActorId) -> impl Future<Output = Ended> + Send + 'static {
        let (tell, ending) = oneshot::channel();
        let generation = self.next.fetch_add(1, Ordering::Relaxed);
        let mut waiting = recovering(&self.waiting);
        let watched = waiting.entry(name.clone()).or_default();
        let waiter = Waiter { generation, tell };
        if let Some(current) = watched.mounted.back_mut() {
            current.waiters.push(waiter);
        } else {
            watched.pending.push(waiter);
        }
        drop(waiting);
        Awaited {
            ending,
            filed: Some(Stamp {
                name: name.clone(),
                generation,
            }),
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
    /// Direct calls have no incarnation identity. Registry reports use
    /// `__mounted`, below, which carries the opaque identity needed across a
    /// remount race.
    fn mounted(&self, _id: &ActorId) {}

    /// Preserve the public callback's direct-use behavior: resolve every waiter
    /// under the id. Registry reports use `__ended` and resolve one incarnation.
    fn ended(&self, id: &ActorId, why: &Ended) {
        let Some(watched) = self.lock().remove(id) else {
            return;
        };
        resolve(
            watched
                .pending
                .into_iter()
                .chain(
                    watched
                        .mounted
                        .into_iter()
                        .flat_map(|mounted| mounted.waiters),
                )
                .collect(),
            why,
        );
    }

    fn __mounted(&self, id: &ActorId, incarnation: usize) {
        let mut waiting = self.lock();
        let watched = waiting.entry(id.clone()).or_default();
        let waiters = std::mem::take(&mut watched.pending);
        watched.mounted.push_back(Incarnation {
            identity: incarnation,
            waiters,
        });
    }

    fn __ended(&self, id: &ActorId, incarnation: usize, why: &Ended) {
        let waiters = {
            let mut waiting = self.lock();
            let hash_map::Entry::Occupied(mut under) = waiting.entry(id.clone()) else {
                return;
            };
            let Some(position) = under
                .get()
                .mounted
                .iter()
                .position(|mounted| mounted.identity == incarnation)
            else {
                return;
            };
            let Some(mounted) = under.get_mut().mounted.remove(position) else {
                return;
            };
            if under.get().pending.is_empty() && under.get().mounted.is_empty() {
                under.remove();
            }
            mounted.waiters
        };
        resolve(waiters, why);
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
    /// `None` once [`Future::poll`] has handed the value over.
    ///
    /// **Not `None` merely because the report was SENT**, and an earlier version
    /// of this comment said otherwise — that a resolved waiter is already gone
    /// from the map, so the `Drop` below need not take the lock to discover it.
    /// Only `poll` clears this, so a caller whose answer is sitting in the
    /// channel and who then drops the future without polling arrives at `Drop`
    /// holding `Some`, takes the lock, and finds the name already gone: the
    /// report removed the whole entry in order to send. That is why the vacant
    /// case below is a silent no-op rather than an assertion — an ordinary
    /// caller reaches it.
    filed: Option<Stamp>,
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
        let Some(Stamp {
            name,
            generation: mine,
        }) = self.filed.take()
        else {
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
            let watched = under.get_mut();
            watched.pending.retain(|waiter| waiter.generation != mine);
            for mounted in &mut watched.mounted {
                mounted.waiters.retain(|waiter| waiter.generation != mine);
            }
            // Active incarnations remain even with no waiters: their identities
            // are needed to keep a late old ending from resolving a replacement's
            // waiter. Once no incarnation or pending waiter remains, the key is
            // dead state and can go.
            if under.get().pending.is_empty() && under.get().mounted.is_empty() {
                let _ = under.remove_entry();
            }
        }
    }
}
