//! Which actor, and for how long. Never what a message is made of.
//!
//! A keyed map of tasks with a capacity and an eviction clock is worth having
//! without a network, and this half is arranged so that it stays that way:
//! everything is generic over `M`, the thing a mailbox carries, and nothing
//! decodes it, encodes it, or knows that a peer exists. `remote.rs` depends on
//! this file and serves it. This file does not depend back.
//!
//! # The five places that still point the wrong way
//!
//! Four type-parameter defaults and one impl header name [`Mail`], which is a
//! `remote.rs` type, and they are the whole of the dependency:
//!
//! - `System<M = Mail>`, `SystemBuilder<M = Mail>`, `Mounted<M = Mail>` and
//!   `Mailbox<M = Mail>`, because `keylink::System` written with no parameter
//!   has to go on meaning what it always meant, and a default is the only way
//!   to say that;
//! - `impl SystemBuilder<Mail>`, which exists for one associated constant,
//!   because a default applies in type position and not in expression position.
//!
//! All five are the same device with the same expiry. They are what a crate
//! split would have to break, and breaking them is a change to the public API
//! rather than a refactor — so they are listed here, together, instead of being
//! discovered one at a time. Nothing else here names a transport type, and
//! `rg 'Mail' registry.rs` is the check that says so.

use std::collections::HashMap;
use std::fmt;
use std::future::Future;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, PoisonError, RwLock, Weak};
use std::time::Duration;

use n0_future::task::JoinHandle;
use n0_future::time::Instant;
use tokio::sync::mpsc;

use super::remote::Mail;
use crate::ActorId;

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
#[doc(hidden)]
pub type Mailbox<M = Mail> = mpsc::Receiver<Box<M>>;

/// A live actor: the task, its mailbox, and the schema it accepts.
///
/// Produced by the generated `spawn` and handed to [`System::mount`] or
/// returned from a factory. Opaque on purpose: the registry owns the pairing of
/// a hash with a mailbox, and nothing else may take them apart.
///
/// `M` is the message the mailbox carries, defaulted to [`Mail`]. See
/// [`System`] for why it is a parameter at all.
#[derive(Debug)]
pub struct Mounted<M = Mail> {
    schema: u64,
    mail: mpsc::Sender<Box<M>>,
    task: JoinHandle<()>,
}

/// Spawn one actor's task and give back the handle the registry stores.
///
/// Called by the generated `spawn`, which supplies the loop.
///
/// # Panics
///
/// Panics when called outside a tokio runtime, like every other spawn.
#[doc(hidden)]
pub fn mount<M, F>(schema: u64, run: impl FnOnce(Mailbox<M>) -> F) -> Mounted<M>
where
    F: Future<Output = ()> + Send + 'static,
{
    let (mail, inbox) = mpsc::channel(MAILBOX);
    Mounted {
        schema,
        mail,
        task: n0_future::task::spawn(run(inbox)),
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
    /// Stop an actor that has not been addressed for this long.
    ///
    /// Eviction is [`System::stop`], not [`System::abort`]: queued messages
    /// drain and the state is dropped after them, never mid-turn.
    Idle(Duration),
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
pub(crate) type Factory<M> = Arc<dyn Fn(&ActorId) -> Option<Mounted<M>> + Send + Sync>;

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
    #[error("no actor is mounted as {id}")]
    NoSuchActor {
        /// The id that was addressed.
        id: ActorId,
    },
    /// The registry is full, so the factory was never called.
    #[error("the actor system is at its capacity of {capacity} live actors")]
    AtCapacity {
        /// The cap that was reached.
        capacity: usize,
    },
}

struct Entry<M> {
    schema: u64,
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
    /// three paused-clock tests at the bottom of this file are the only proof
    /// that this indirection did not quietly break that.
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
    fn route(&self, id: &ActorId) -> std::result::Result<(u64, mpsc::Sender<Box<M>>), RouteError> {
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
        let found = (mounted.schema, mounted.mail.clone());
        entries.insert(id.clone(), self.entry(mounted));
        Ok(found)
    }

    fn touch(
        &self,
        entries: &HashMap<ActorId, Entry<M>>,
        id: &ActorId,
    ) -> Option<(u64, mpsc::Sender<Box<M>>)> {
        let entry = entries.get(id)?;
        entry.last_seen.store(self.now(), Ordering::Relaxed);
        Some((entry.schema, entry.mail.clone()))
    }

    fn entry(&self, mounted: Mounted<M>) -> Entry<M> {
        Entry {
            schema: mounted.schema,
            mail: mounted.mail,
            task: mounted.task,
            last_seen: AtomicU64::new(self.now()),
        }
    }

    /// Drop every actor that has been idle past the budget.
    ///
    /// Dropping the entry drops the registry's sender. The task lives while any
    /// other sender does, so a message already in flight still lands and still
    /// answers; the task then sees its channel close and returns.
    fn sweep(&self) {
        let Eviction::Idle(budget) = self.eviction else {
            return;
        };
        let budget = u64::try_from(budget.as_millis()).unwrap_or(u64::MAX);
        let now = self.now();
        self.write().retain(|_, entry| {
            now.saturating_sub(entry.last_seen.load(Ordering::Relaxed)) < budget
        });
    }
}

/// A registry of live actors, served behind one ALPN.
///
/// Cheap to clone: every clone names the same registry.
///
/// `M` is what a mailbox carries, defaulted to [`Mail`] so that `System` still
/// means what it always did at every call site. It is a parameter because
/// everything here — the map, the cap, the eviction clock, the lifecycle — is
/// about *which* actor and never about what a message is made of. Naming
/// [`Mail`] in these fields would put a QUIC stream and a postcard payload
/// inside the one half of this module that has no business knowing either, and
/// no import list would show it.
///
/// # `error[E0283]: type annotations needed`
///
/// The default does not rescue every call site, and this is where somebody who
/// has just met that error is looking. A default applies in **type position**,
/// so there has to be a type position for it to apply in — and a system that is
/// built and then touched only through the methods that never mention `M`
/// ([`contains`](Self::contains), [`len`](Self::len),
/// [`is_empty`](Self::is_empty), [`evict_idle`](Self::evict_idle),
/// [`stop`](Self::stop), [`abort`](Self::abort)) never writes one:
///
/// ```compile_fail
/// let system = keylink::System::builder(keylink::Eviction::Never).build();
/// assert!(system.is_empty());
/// ```
///
/// Name the type and it compiles. That is the whole fix:
///
/// ```
/// use keylink::{Eviction, System};
/// let system: System = System::builder(Eviction::Never).build();
/// assert!(system.is_empty());
/// ```
///
/// Most code never meets it, because it pins `M` without trying: `service` and
/// `alpn` exist only on `System<Mail>`, and `mount(id, journal::spawn(..))`
/// pins it through [`Mounted`]. Moving `builder` to `impl System<Mail>` would
/// fix it outright, by inherent-method probing, and is declined: it puts a
/// registry concern into the transport half at the one point this module is
/// arranged to keep them apart.
pub struct System<M = Mail> {
    inner: Arc<Registry<M>>,
}

/// Hand-written, because the derive would bound the impl on `M: Clone` — and
/// [`Mail`], which owns a `oneshot::Sender` and a stream, is not. A clone here
/// is a clone of the `Arc` and never of a message.
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

    /// Mount an actor under `id`.
    ///
    /// Takes effect on the next message. Nothing is announced, no lock outside
    /// this registry is taken, and no connection is disturbed.
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
    ) -> std::result::Result<(u64, mpsc::Sender<Box<M>>), RouteError> {
        self.inner.route(id)
    }

    /// Stop routing to `id`, then wait for the actor to finish what it has.
    ///
    /// The queued messages are handled and answered; the state is dropped after
    /// them. Routing stops the instant this returns from the registry, so a
    /// later message gets
    /// [`RemoteError::NoSuchActor`](crate::RemoteError::NoSuchActor) on a live
    /// connection while every other actor on it carries on.
    ///
    /// Deliberately takes no deadline: `tokio::time::timeout` composes, and a
    /// parameter here would duplicate it.
    ///
    /// Returns whether an actor was mounted under that id.
    pub async fn stop(&self, id: &ActorId) -> bool {
        let Some(entry) = self.inner.write().remove(id) else {
            return false;
        };
        // Dropping the registry's sender is what ends the loop. The task lives
        // while any other sender does, which is what lets a message already in
        // flight finish rather than be cancelled.
        drop(entry.mail);
        let _ = entry.task.await;
        true
    }

    /// Stop routing to `id` and cancel what the actor is doing where it stands.
    ///
    /// The state is **destroyed**: the task owns it, `abort` drops that task's
    /// future at its next await point, and unflushed work goes with it. That is
    /// the difference from [`Self::stop`], and it is why the two are separate
    /// methods rather than one with a flag.
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

    /// Drop every actor that has been idle past the eviction budget.
    ///
    /// Called on a timer by the system itself. Public so that a program with its
    /// own clock, or a test, can make it happen instead of waiting for one.
    pub fn evict_idle(&self) {
        self.inner.sweep();
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
/// `M` is the system's message type, defaulted to [`Mail`] exactly as on
/// [`System`].
pub struct SystemBuilder<M = Mail> {
    capacity: usize,
    eviction: Eviction,
    factory: Option<Factory<M>>,
}

/// The cap, as the generic half spells it. [`SystemBuilder::DEFAULT_CAPACITY`]
/// is the public name and carries the reasoning for the number.
const DEFAULT_CAPACITY: usize = 10_000;

/// The one item a defaulted type parameter could not carry.
///
/// A default applies in type position and not in expression position, so a
/// `DEFAULT_CAPACITY` on `impl<M> SystemBuilder<M>` leaves `M` uninferrable and
/// `SystemBuilder::DEFAULT_CAPACITY` stops compiling at every call site that
/// ever named it. Pinning one instantiation is what makes that path resolve
/// again.
///
/// The cap is registry policy and has nothing to do with [`Mail`], so this
/// block is a compatibility shim and not a statement about where the constant
/// belongs. It moves back to the generic impl — a breaking change, taken
/// deliberately — the day `SystemBuilder` and [`Mail`] stop sharing a crate.
impl SystemBuilder<Mail> {
    /// The cap a system has when none is named.
    ///
    /// Ten thousand live actors was the number the design was costed against,
    /// so it is the number a system holds before it starts refusing.
    pub const DEFAULT_CAPACITY: usize = self::DEFAULT_CAPACITY;
}

impl<M> SystemBuilder<M> {
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
        if let Eviction::Idle(budget) = self.eviction {
            spawn_sweeper(&system.inner, budget);
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
fn spawn_sweeper<M: Send + 'static>(registry: &Arc<Registry<M>>, budget: Duration) {
    let registry = Arc::downgrade(registry);
    // Half the budget, so an actor is evicted between one and one and a half
    // budgets after its last message rather than up to two.
    let tick = (budget / 2).max(Duration::from_millis(1));
    n0_future::task::spawn(async move {
        loop {
            n0_future::time::sleep(tick).await;
            let Some(registry) = Weak::upgrade(&registry) else {
                return;
            };
            registry.sweep();
        }
    });
}

#[cfg(test)]
mod tests {
    //! Every test here runs at `M = ()`, and that instantiation is the
    //! assertion.
    //!
    //! The header above claims this half places no requirement on what a mailbox
    //! carries. Run against [`Mail`] — which is what `Mounted` means when nobody
    //! says otherwise — that claim is an argument, because `Mail` satisfies any
    //! bound one of these types might quietly grow. `()` satisfies almost none:
    //! it is `Send + 'static` and nothing else, with no `Context`, no stream, no
    //! serde and nothing a peer could have put there.
    //!
    //! So `System<()>`, `Mounted<()>` and `Mailbox<()>` compiling at all is the
    //! property the split exists for, checked rather than argued. A transport
    //! bound added to `mount`, `route`, `stop` or `build` would leave a
    //! `Mail`-typed test binary green and stops this one from compiling.
    //!
    //! Nothing here needs a message to *be* anything, which is the same point
    //! from the other side: mounting, the cap, eviction, routing and the
    //! lifecycle are all decisions about **which** actor. The tests that need a
    //! real payload live in `remote.rs`, where a payload exists.

    #![allow(clippy::expect_used, clippy::unwrap_used)]

    use super::*;

    /// An actor that does nothing at all, for testing the registry rather than
    /// any message it carries — so its mailbox carries a message that is not
    /// anything.
    fn idler() -> Mounted<()> {
        mount(1, |mut inbox: Mailbox<()>| async move {
            while inbox.recv().await.is_some() {}
        })
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
        let system = System::builder(Eviction::Idle(Duration::from_secs(60))).build();
        let busy = ActorId::new("busy").expect("valid");
        let idle = ActorId::new("idle").expect("valid");
        system.mount(busy.clone(), idler()).expect("free");
        system.mount(idle.clone(), idler()).expect("free");

        // Both are now older than the budget.
        tokio::time::advance(Duration::from_secs(90)).await;
        // Routing is what marks an actor as used.
        system.route(&busy).expect("mounted");
        system.evict_idle();

        assert!(system.contains(&busy), "the addressed actor must survive");
        assert!(!system.contains(&idle), "the idle actor must be gone");
    }

    /// The other half: an actor inside its budget survives however often the
    /// sweeper runs.
    #[tokio::test(start_paused = true)]
    async fn sweeping_does_not_evict_an_actor_inside_its_budget() {
        let system = System::builder(Eviction::Idle(Duration::from_secs(60))).build();
        let fresh = ActorId::new("fresh").expect("valid");
        system.mount(fresh.clone(), idler()).expect("free");

        tokio::time::advance(Duration::from_secs(59)).await;
        system.evict_idle();

        assert!(system.contains(&fresh));
    }

    /// `Eviction::Never` means never, however long nobody calls.
    #[tokio::test(start_paused = true)]
    async fn eviction_never_keeps_an_actor_nobody_addresses() {
        let system = System::builder(Eviction::Never).build();
        let forgotten = ActorId::new("forgotten").expect("valid");
        system.mount(forgotten.clone(), idler()).expect("free");

        tokio::time::advance(Duration::from_secs(60 * 60 * 24)).await;
        system.evict_idle();

        assert!(system.contains(&forgotten));
    }

    #[test]
    fn a_system_without_a_factory_never_grows_by_itself() {
        let system: System<()> = System::builder(Eviction::Never).build();
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
    #[tokio::test]
    async fn a_full_registry_refuses_by_hand_and_lazily() {
        let system = System::builder(Eviction::Never)
            .capacity(1)
            .spawn_with(|_| Some(idler()))
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
    }

    /// The two lifecycle methods, at a message type with no wire behind it.
    ///
    /// They are the likeliest place for a transport bound to arrive by accident,
    /// because both await a task and one of them drops a sender — and neither
    /// needs to know what that sender carries. Here it carries `()`.
    ///
    /// What each one does to state in flight is `remote.rs`'s and
    /// `tests/actors.rs`'s to prove, over a real actor with real state. This one
    /// only asks that unmounting works at all when `M` is nothing.
    #[tokio::test]
    async fn stop_and_abort_unmount_whatever_the_mailbox_carries() {
        let system = System::builder(Eviction::Never).build();
        let drained = ActorId::new("drained").expect("valid");
        let cancelled = ActorId::new("cancelled").expect("valid");
        system.mount(drained.clone(), idler()).expect("free");
        system.mount(cancelled.clone(), idler()).expect("free");
        assert_eq!(system.len(), 2);

        assert!(system.stop(&drained).await);
        assert!(system.abort(&cancelled).await);
        assert!(system.is_empty());

        assert!(!system.stop(&drained).await, "gone stays gone");
        assert!(!system.abort(&cancelled).await);
    }
}
