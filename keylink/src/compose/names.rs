//! Name → local actor, unregistered when the actor ends.
//!
//! [`System`](crate::System) can find an actor by [`ActorId`], and that is a
//! *keyed* system: its entries are mounted, served behind an ALPN, evicted on a
//! clock, and what you get back is a mailbox rather than a typed handle. Nothing
//! answers the smaller question — *what is the handle for the local actor called
//! `"ledger"`?* — so every user who needs two parts of a process to reach one
//! actor without threading a handle through both writes the same map.
//!
//! ```ignore
//! let names: Names<ledger::Handle<Ledger>> = Names::new();
//! let ledger = ledger::actor(Ledger::new());
//! names.register(ActorId::new("ledger")?, ledger.clone(), ledger.ended());
//!
//! // somewhere with no path to the handle
//! if let Some(ledger) = names.get(&ActorId::new("ledger")?) {
//!     ledger.post(1).await?;
//! }
//! ```
//!
//! The key is [`ActorId`] and not a new newtype, deliberately: a local actor's
//! name and a keyed actor's id are the same kind of thing, bounded the same way
//! (non-empty, at most [`MAX_KEY_LEN`](crate::MAX_KEY_LEN) bytes), and a name
//! that works here works when the same actor is later mounted.
//!
//! # Backpressure: none, because there is nothing here that queues
//!
//! A map and a lock. No channel, no buffer, no task per lookup. The map is
//! bounded by the number of live names and a name is removed when its actor ends,
//! so the only way it grows without limit is a caller registering names for
//! actors that never end — which is a caller leaking actors, and the map is then
//! the diagnostic rather than the leak.
//!
//! The one task this spawns is per registration, and it does one thing: await the
//! ending, then remove. It holds a [`Weak`] on the map, so a registry nobody
//! holds any more is dropped even with actors still running.
//!
//! # THE THING TO KNOW — a registered actor does not end when *your* handle drops
//!
//! The registry holds a handle. An actor runs until the last handle is dropped, so
//! registering one keeps it alive: dropping every handle you hold no longer ends
//! it, and the ending this registry waits for will never arrive on its own.
//!
//! That is not a defect to be fixed by holding something weaker — the whole point
//! of a registry is that the actor is reachable by name, and a registry of dead
//! names is not one. It is a lifecycle rule, and it is the one this type changes:
//!
//! - [`stop`](crate::LocalActor::stop), [`abort`](crate::LocalActor::abort), a
//!   panicking turn, an observer refusing and a feed ending **all** still end the
//!   actor, and all of them unregister it.
//! - "the last handle went out of scope" no longer does. Stop it, or
//!   [`unregister`](Names::unregister) it and drop what comes back.
//!
//! # Re-registering a live name replaces it, and says what it displaced
//!
//! [`register`](Names::register) hands back the handle it evicted. It does not
//! refuse, and refusing was the alternative worth writing down: a name is freed
//! by the eviction task *asynchronously*, some time after the actor ended, so a
//! registry that refused a taken name would refuse a restart nondeterministically
//! — the same restart succeeding or failing depending on whether a task had been
//! scheduled yet. Replacing is total, and the displaced handle coming back is
//! what keeps it from being silent.
//!
//! # The generation stamp, and the race it is for
//!
//! Each entry is stamped, and an eviction removes an entry **only if the stamp is
//! still its own**. Without that, a restart deletes the live actor: the first
//! incarnation is registered, the second replaces it under the same name, the
//! first then ends — and its eviction task removes the second one's entry,
//! because both are filed under one [`ActorId`]. The result is a name nothing
//! answers to and an actor nothing can reach, which is a leak arrived at through
//! bookkeeping.
//!
//! This is [`Subscribers`](crate::Subscribers)' argument exactly, one type over.
//! **The two are one generic registry waiting to be merged** — the differences are
//! the key ([`ActorId`] against
//! [`EndpointId`](crate::EndpointId)) and the eviction trigger (an
//! [`Ended`] against a [`Departure`](crate::Departure)) — and the merge was not
//! taken here because `subscribers.rs` is a file a parallel wave owns. It is
//! recorded in `RFC-07 §8` as debt rather than left for a reader to notice.
//!
//! [`Weak`]: std::sync::Weak

use std::collections::HashMap;
use std::fmt;
use std::future::Future;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError, Weak};

use crate::{ActorId, Ended};

/// One entry: a handle and the stamp that says which registration it is.
///
/// A named struct and not a tuple, because `(u64, H)` at four call sites is four
/// chances to read the stamp as a count.
#[derive(Debug)]
struct Registered<H> {
    generation: u64,
    handle: H,
}

/// Names for the actors in this process, unregistered when they end.
///
/// Cheap to clone: every clone names the same registry, so the code that
/// registers and the code that looks up can each hold one.
pub struct Names<H> {
    actors: Arc<Mutex<HashMap<ActorId, Registered<H>>>>,
    next: Arc<AtomicU64>,
}

impl<H> Names<H> {
    /// An empty registry.
    #[must_use]
    pub fn new() -> Self {
        Self {
            actors: Arc::new(Mutex::new(HashMap::new())),
            next: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Remember `handle` as `name`, until `ended` resolves.
    ///
    /// `ended` is that actor's own [`ended()`](crate::LocalActor::ended) —
    /// `handle.ended()` for a generated local handle. For a keyed actor,
    /// [`System::ended`](crate::System::ended) returns `None` when the id is not
    /// mounted, so unwrap that state explicitly before registering:
    /// `let ended = system.ended(&id).expect("the actor was just mounted");`.
    /// Passing the *wrong* actor's future is the one way to misuse this, and it is why the
    /// two arrive together at one call site rather than being registered separately.
    ///
    /// It is an argument rather than something asked of `handle` because `H` is
    /// whatever a caller holds: a local handle can expose its ending directly,
    /// while a keyed future comes from its [`System`](crate::System).
    ///
    /// Returns the handle it displaced, if the name was taken. Dropping that
    /// handle is what lets the displaced actor end, if nothing else holds one.
    #[must_use = "the displaced actor is still running, and this is the last \
                  handle to it; stop it, or say `let _ =` to mean that"]
    pub fn register(
        &self,
        name: ActorId,
        handle: H,
        ended: impl Future<Output = Ended> + Send + 'static,
    ) -> Option<H>
    where
        H: Send + 'static,
    {
        let generation = self.next.fetch_add(1, Ordering::Relaxed);
        let displaced = self
            .lock()
            .insert(name.clone(), Registered { generation, handle });

        // Weak, so a registry nobody holds any more is dropped even with actors
        // still registered. The task then wakes once, finds nothing, and ends.
        let actors = Arc::downgrade(&self.actors);
        n0_future::task::spawn(async move {
            // Every ending unregisters. Stopped, panicked, an observer refusing
            // and a feed ending are four things to report and one thing to do
            // about — and the reason is on the actor's own `ended()` for whoever
            // is awaiting that.
            let _reason: Ended = ended.await;
            if let Some(actors) = Weak::upgrade(&actors) {
                let mut held = actors.lock().unwrap_or_else(PoisonError::into_inner);
                // Compare-and-remove. A newer generation under this name is a
                // registration that already replaced us, and it is not ours to
                // evict.
                if held
                    .get(&name)
                    .is_some_and(|entry| entry.generation == generation)
                {
                    held.remove(&name);
                }
            }
        });

        displaced.map(|entry| entry.handle)
    }

    /// Forget a name without waiting for its actor to end.
    ///
    /// For an actor being handed over rather than stopped. The eviction task
    /// stays registered and finds nothing to do, which costs one wake-up.
    ///
    /// The handle comes back, and it may be the last one: dropping it ends the
    /// actor, which is usually what "unregister and forget" is meant to mean and
    /// is worth being deliberate about.
    #[must_use = "this may be the last handle to a running actor"]
    pub fn unregister(&self, name: &ActorId) -> Option<H> {
        self.lock().remove(name).map(|entry| entry.handle)
    }

    /// How many names are registered.
    #[must_use]
    pub fn registered(&self) -> usize {
        self.lock().len()
    }

    /// Every name, as of now.
    ///
    /// An eager `Vec` for [`Subscribers::snapshot`](crate::Subscribers::snapshot)'s
    /// reason: the lock is released before the caller does anything with it, so a
    /// caller that awaits per name cannot be holding it.
    #[must_use]
    pub fn names(&self) -> Vec<ActorId> {
        self.lock().keys().cloned().collect()
    }

    /// Poison recovery rather than propagation: one bad registration should cost
    /// one registration. Nothing here can panic while the lock is held, so this
    /// arm is unreachable in practice and must still be typed.
    fn lock(&self) -> MutexGuard<'_, HashMap<ActorId, Registered<H>>> {
        self.actors.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

impl<H: Clone> Names<H> {
    /// The handle for one name, if it is registered.
    ///
    /// Advisory in the same way [`is_running`](crate::LocalActor::is_running) is:
    /// the actor can end between this answer and the call made on it, and the
    /// call then reports the loss on its own — [`Unanswered`](crate::Unanswered)
    /// or [`NotQueued`](crate::NotQueued), by door. A
    /// registry that promised liveness would have to hold the lock across the
    /// caller's turn.
    #[must_use]
    pub fn get(&self, name: &ActorId) -> Option<H> {
        self.lock().get(name).map(|entry| entry.handle.clone())
    }
}

impl<H> Default for Names<H> {
    fn default() -> Self {
        Self::new()
    }
}

/// By hand, because `#[derive(Clone)]` would demand `H: Clone` — and a registry is
/// shared whether or not the handles inside it can be copied out. [`get`](Names::get)
/// is where that bound belongs.
impl<H> Clone for Names<H> {
    fn clone(&self) -> Self {
        Self {
            actors: Arc::clone(&self.actors),
            next: Arc::clone(&self.next),
        }
    }
}

impl<H> fmt::Debug for Names<H> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Names")
            .field("registered", &self.lock().len())
            .finish()
    }
}
