//! Keyed actors: many instances of state, one ALPN, one connection.
//!
//! An actor owns its state as `&mut self` and handles one message at a time, so
//! the obvious code is the correct code: no `Mutex`, no guard to hold across an
//! `.await`, no `clippy::await_holding_lock` to restructure around.
//!
//! # Mounting is a map write
//!
//! Actor identity travels **inside the request**, never in the ALPN. An ALPN is
//! a contract identifier — a hash of the types — not an instance identifier, and
//! `Endpoint::set_alpns` deep-copies the whole TLS server configuration under
//! the endpoint-wide datapath lock while every handshake scans the ALPN list
//! linearly. Announcing one ALPN per actor is therefore O(N) per mount and
//! O(N²) to start N actors, and it costs one connection per actor.
//!
//! So mounting is `HashMap::insert`, one connection carries every actor on a
//! node, and the existing one-stream-per-call design already gives per-message
//! concurrency between different actors. See RFC-02 §10.2 and RFC-04 §14.
//!
//! # What replaces the handshake gate
//!
//! One ALPN for the whole system means the handshake can only prove that the
//! peer agrees about the *envelope*. Per-actor drift is caught one layer down:
//! the request declares the schema hash of the actor's own message type, the
//! registry compares it against the hash the actor was mounted with, and the
//! payload stays [`Sealed`] until that comparison succeeds.

use std::collections::HashMap;
use std::fmt;
use std::future::Future;
use std::panic::AssertUnwindSafe;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock, PoisonError, RwLock, Weak};
use std::task::Poll;
use std::time::Duration;

use postcard_schema::Schema;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use tokio::time::Instant;

use crate::{
    derive_alpn, schema_hash, ActorId, Allowlist, Body, BodyReader, Context, Dispatch,
    EndpointAddr, Envelope, Node, Outcome, RemoteError, Result, Rpc, RpcClient, ServiceBuilder,
    WireResponse,
};

/// The protocol name every actor system answers on.
///
/// One name for the whole fleet, on purpose: the hashed contract below carries
/// no per-service type, so two crates that both serve actors speak the same
/// envelope and discriminate by the per-actor schema hash inside it.
const PROTOCOL: &str = "keylink/actors";

/// Hashed to produce the ALPN, exactly as a generated service does. A change to
/// [`ActorRequest`] or [`ActorReply`] moves it, and every peer is refused at the
/// handshake until both sides are rebuilt.
#[derive(Schema)]
#[expect(dead_code, reason = "read by the Schema derive, never by running code")]
struct Contract {
    request: Envelope<ActorRequest>,
    response: Envelope<WireResponse<ActorReply>>,
}

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

// ─────────────────────────────────────────────────────────────── the envelope

/// The request type of an actor system. One per system, not one per actor.
///
/// FROZEN. Every field change moves the system ALPN and rebuilds every peer in
/// the fleet at once. Per-actor variation belongs in the payload, which is
/// opaque and costs nothing here.
///
/// Three fields were considered and rejected.
///
/// **A method name**, because it duplicates the payload's own variant index and
/// a peer can make the two disagree.
///
/// **A protocol version**, because that is what the ALPN is.
///
/// **A per-actor sequence number**, with the actor reordering behind a buffer,
/// so that messages are handled in the order they were SENT rather than the
/// order they arrived. Rejected, and this one is a judgement rather than an
/// obvious call, so here is the reasoning.
///
/// A reorder buffer stalls while it waits for a number it has not seen. That is
/// fine when the message is merely late. It is unbounded when the message is
/// never coming — a cancelled call, a dropped stream, a caller that died
/// between two sends — and the buffer cannot tell any of those apart from slow.
/// The only way to bound the stall is a timeout, which means eventually giving
/// up and processing a gap, which means dropping a message that may still
/// arrive.
///
/// So the choice is not "ordered versus unordered". It is "out-of-order
/// delivery, which the caller can see and correct" against "a stall of
/// unpredictable length, ended by silently dropping someone's message". The
/// second is worse, and it would be keylink's failure rather than the caller's
/// decision.
///
/// Arrival order is therefore the order, the actor stamps sequence as it
/// processes, and a caller who needs send order gets it by awaiting each reply
/// or by carrying a sequence number in its own payload — where it owns the
/// buffering policy and the stall that comes with it. Said where a user meets
/// it, on the generated `Actor` trait and on `ActorRef`.
#[derive(Serialize, Deserialize, Schema)]
pub struct ActorRequest {
    /// Which actor, on the node this request is addressed to. An `ActorId` is
    /// not a route and not globally unique: the full address of an actor is
    /// `(EndpointId, ActorId)`, and the caller resolves both.
    pub actor: ActorId,
    /// The schema hash of the actor's own message type.
    pub schema: u64,
    /// The actor's `Request`, undecoded.
    ///
    /// Private, and reachable only as a [`Sealed`] through [`Self::seal`]. That
    /// is the whole ordering guarantee: there is no way to read these bytes that
    /// does not go past a hash comparison.
    payload: Vec<u8>,
}

impl ActorRequest {
    /// Encode one actor message for the wire.
    ///
    /// # Errors
    ///
    /// [`Error::Codec`](crate::Error::Codec) when the message does not encode.
    pub fn new<T: Serialize>(actor: ActorId, schema: u64, message: &T) -> Result<Self> {
        Ok(Self {
            actor,
            schema,
            payload: postcard::to_stdvec(message)?,
        })
    }

    /// Weld the declared hash onto the bytes, giving the only value from which
    /// the payload can be decoded.
    ///
    /// Consuming, so the two can never be paired with each other's partner: a
    /// [`Sealed`] always carries the hash that arrived with its own bytes.
    #[must_use]
    pub fn seal(self) -> (ActorId, Sealed) {
        (
            self.actor,
            Sealed {
                declared: self.schema,
                bytes: self.payload,
            },
        )
    }
}

/// Never prints the payload. See [`Sealed`].
impl fmt::Debug for ActorRequest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ActorRequest")
            .field("actor", &self.actor)
            .field("schema", &format_args!("{:016x}", self.schema))
            .field("payload", &format_args!("<{} bytes>", self.payload.len()))
            .finish()
    }
}

impl Rpc for ActorRequest {
    const SERVICE: &'static str = "Actors";

    /// One name for every actor message. The actor's own method is inside the
    /// sealed payload, where a layer cannot read it before the schema check —
    /// which is the point. Per-actor rules use [`Context::actor`].
    fn method(&self) -> &'static str {
        "Message"
    }

    fn actor(&self) -> Option<&ActorId> {
        Some(&self.actor)
    }
}

/// The reply of an actor system: the actor's own `Response`, undecoded.
///
/// No hash travels back. The request's hash already pinned both directions:
/// a reply exists only because the declared hash matched the one the actor was
/// mounted with, and that hash covers the response type too.
#[derive(Serialize, Deserialize, Schema)]
pub struct ActorReply {
    payload: Vec<u8>,
}

impl ActorReply {
    /// Encode an actor's reply.
    ///
    /// # Errors
    ///
    /// [`RemoteError::Failed`] when the response does not encode, which the
    /// caller sees instead of a truncated reply.
    pub fn encode<T: Serialize>(value: &T) -> std::result::Result<Self, RemoteError> {
        postcard::to_stdvec(value)
            .map(|payload| Self { payload })
            .map_err(|err| RemoteError::Failed(err.to_string()))
    }

    /// Decode a reply into the response type the request pinned.
    ///
    /// # Errors
    ///
    /// [`Error::Codec`](crate::Error::Codec) from a peer that answered with
    /// something other than the type its own hash promised.
    pub fn decode<T: DeserializeOwned>(&self) -> Result<T> {
        Ok(postcard::from_bytes(&self.payload)?)
    }
}

impl fmt::Debug for ActorReply {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ActorReply")
            .field("payload", &format_args!("<{} bytes>", self.payload.len()))
            .finish()
    }
}

/// Payload bytes that have not been schema-checked.
///
/// There is no way to read these bytes. `Sealed` has no accessor, no `Deref`,
/// no `into_inner`, and no `Debug` that prints content. [`Sealed::open`] is the
/// only route from bytes to a typed message, and it compares the declared hash
/// against the caller's before it decodes.
///
/// This is deliberate. postcard is not self-describing, so decoding before
/// checking is exactly the silent misparse the ALPN scheme exists to prevent —
/// see `same_shape_variant_swap_decodes_as_the_wrong_variant` in `node.rs`.
///
/// **Do not** add `bytes()`, `peek()`, `len()`, or a `Debug` that prints the
/// payload. Any of them reopens the hole.
pub struct Sealed {
    declared: u64,
    bytes: Vec<u8>,
}

impl Sealed {
    /// Decode the payload as `T`, but only if the hashes agree.
    ///
    /// `ours` comes from the registry entry — the hash the actor was mounted
    /// with — never from the caller's own frame.
    ///
    /// # Errors
    ///
    /// [`RemoteError::ActorSchemaMismatch`] when the hashes differ, and the
    /// bytes are dropped without being interpreted. [`RemoteError::Failed`]
    /// when they agree and the bytes still do not decode.
    pub fn open<T>(self, ours: u64) -> std::result::Result<T, RemoteError>
    where
        T: DeserializeOwned + Schema,
    {
        if self.declared != ours {
            return Err(RemoteError::ActorSchemaMismatch {
                ours,
                theirs: self.declared,
            });
        }
        postcard::from_bytes(&self.bytes).map_err(|err| RemoteError::Failed(err.to_string()))
    }
}

/// Names the hash and the size. Never the content.
impl fmt::Debug for Sealed {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Sealed")
            .field("declared", &format_args!("{:016x}", self.declared))
            .field("bytes", &format_args!("<{} sealed>", self.bytes.len()))
            .finish()
    }
}

// ──────────────────────────────────────────────────────────────── the mailbox

/// What an actor answers with: the reply, and a body to stream beside it.
type Answer = (Outcome<ActorReply>, Body);

/// One message on its way to an actor. Generated code destructures these.
///
/// Boxed on the way into the mailbox, which is not a micro-optimisation. A
/// `tokio::sync::mpsc` channel allocates its first block of 32 slots eagerly, so
/// the mailbox costs `32 * size_of::<T>()` whatever its capacity is. Measured at
/// ten thousand idle actors, on this machine, by `tests/actor_memory.rs`:
///
/// | slot type   | per idle actor | ten thousand |
/// |-------------|----------------|--------------|
/// | `Box<Mail>` | 1,684 bytes    | 16.8 MB      |
/// | `Mail`      | 8,724 bytes    | 87.2 MB      |
///
/// The 7,040 byte difference is the block: `Mail` is 224 bytes, and 32 of them
/// is 7,168. The cost of boxing is one allocation per message, on a path that
/// already allocates the payload.
#[doc(hidden)]
#[derive(Debug)]
pub struct Mail {
    /// Who is calling, and which actor they addressed.
    pub cx: Context,
    /// The actor's own request, still sealed.
    pub payload: Sealed,
    /// The payload streaming up beside the request.
    pub body: BodyReader,
    /// Where the answer goes.
    pub reply: oneshot::Sender<Answer>,
}

/// The receiving half of one actor's mailbox.
#[doc(hidden)]
pub type Mailbox = mpsc::Receiver<Box<Mail>>;

/// A live actor: the task, its mailbox, and the schema it accepts.
///
/// Produced by the generated `spawn` and handed to [`System::mount`] or
/// returned from a factory. Opaque on purpose: the registry owns the pairing of
/// a hash with a mailbox, and nothing else may take them apart.
#[derive(Debug)]
pub struct Mounted {
    schema: u64,
    mail: mpsc::Sender<Box<Mail>>,
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
pub fn mount<F>(schema: u64, run: impl FnOnce(Mailbox) -> F) -> Mounted
where
    F: Future<Output = ()> + Send + 'static,
{
    let (mail, inbox) = mpsc::channel(MAILBOX);
    Mounted {
        schema,
        mail,
        task: tokio::task::spawn(run(inbox)),
    }
}

/// A panic that was caught instead of killing an actor.
#[derive(Debug)]
pub struct Panic(String);

impl fmt::Display for Panic {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl From<Box<dyn std::any::Any + Send>> for Panic {
    fn from(panic: Box<dyn std::any::Any + Send>) -> Self {
        let message = panic
            .downcast_ref::<&str>()
            .map(|s| (*s).to_string())
            .or_else(|| panic.downcast_ref::<String>().cloned())
            .unwrap_or_else(|| "panicked".to_string());
        Self(format!("the actor panicked: {message}"))
    }
}

/// Run one future, turning a panic into a value instead of a dead actor.
///
/// A panicking handler must cost one message, not the whole actor. Without
/// this, one bad message drops the receiver, every later send fails, and the
/// actor is unreachable for the life of the process while the registry still
/// lists it as mounted — so callers would be told `NoSuchActor` about an id
/// that is, as far as anyone configured it, very much real.
///
/// Two things it does not fix, both by design. The state may be **logically
/// torn**, which is the same hazard as a poisoned `Mutex` and is recovered from
/// for the same reason. And `panic = "abort"` defeats it entirely.
#[doc(hidden)]
pub async fn guarded<F: Future>(fut: F) -> std::result::Result<F::Output, Panic> {
    let mut fut = Box::pin(fut);
    std::future::poll_fn(move |cx| {
        match std::panic::catch_unwind(AssertUnwindSafe(|| fut.as_mut().poll(cx))) {
            Ok(Poll::Pending) => Poll::Pending,
            Ok(Poll::Ready(value)) => Poll::Ready(Ok(value)),
            Err(panic) => Poll::Ready(Err(Panic::from(panic))),
        }
    })
    .await
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
pub(crate) type Factory = Arc<dyn Fn(&ActorId) -> Option<Mounted> + Send + Sync>;

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

struct Entry {
    schema: u64,
    mail: mpsc::Sender<Box<Mail>>,
    task: JoinHandle<()>,
    /// Milliseconds since the registry's epoch, as of the last message routed
    /// here. An atomic so that touching an actor stays on the read lock.
    last_seen: AtomicU64,
}

struct Registry {
    entries: RwLock<HashMap<ActorId, Entry>>,
    factory: Option<Factory>,
    capacity: usize,
    eviction: Eviction,
    /// `tokio::time::Instant`, not `std::time::Instant`. The two are the same
    /// clock in a running program, and only the first one a test can pause —
    /// which is what makes the eviction policy provable rather than sampled by
    /// a sleep long enough to usually work.
    epoch: Instant,
}

impl Registry {
    fn now(&self) -> u64 {
        u64::try_from(self.epoch.elapsed().as_millis()).unwrap_or(u64::MAX)
    }

    fn read(&self) -> std::sync::RwLockReadGuard<'_, HashMap<ActorId, Entry>> {
        // A panic under the lock must cost one message, not wedge the registry
        // for the life of the process. The same call the demo's handler makes.
        self.entries.read().unwrap_or_else(PoisonError::into_inner)
    }

    fn write(&self) -> std::sync::RwLockWriteGuard<'_, HashMap<ActorId, Entry>> {
        self.entries.write().unwrap_or_else(PoisonError::into_inner)
    }

    /// Find the actor, spawning it if the system was given a factory.
    fn route(
        &self,
        id: &ActorId,
    ) -> std::result::Result<(u64, mpsc::Sender<Box<Mail>>), RemoteError> {
        if let Some(found) = self.touch(&self.read(), id) {
            return Ok(found);
        }
        let Some(factory) = self.factory.as_ref() else {
            return Err(no_such_actor(id));
        };

        let mut entries = self.write();
        // Another message to the same new id may have won the race between the
        // two locks. One id is one actor, so the loser adopts the winner's.
        if let Some(found) = self.touch(&entries, id) {
            return Ok(found);
        }
        if entries.len() >= self.capacity {
            return Err(RemoteError::Failed(format!(
                "the actor system is at its capacity of {} live actors",
                self.capacity
            )));
        }
        let Some(mounted) = factory(id) else {
            return Err(no_such_actor(id));
        };
        let found = (mounted.schema, mounted.mail.clone());
        entries.insert(id.clone(), self.entry(mounted));
        Ok(found)
    }

    fn touch(
        &self,
        entries: &HashMap<ActorId, Entry>,
        id: &ActorId,
    ) -> Option<(u64, mpsc::Sender<Box<Mail>>)> {
        let entry = entries.get(id)?;
        entry.last_seen.store(self.now(), Ordering::Relaxed);
        Some((entry.schema, entry.mail.clone()))
    }

    fn entry(&self, mounted: Mounted) -> Entry {
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

fn no_such_actor(id: &ActorId) -> RemoteError {
    RemoteError::NoSuchActor { id: id.to_string() }
}

/// A registry of live actors, served behind one ALPN.
///
/// Cheap to clone: every clone names the same registry.
#[derive(Clone)]
pub struct System {
    inner: Arc<Registry>,
}

impl System {
    /// Configure a system. The eviction policy is an argument because there is
    /// no safe default for it.
    #[must_use]
    pub fn builder(eviction: Eviction) -> SystemBuilder {
        SystemBuilder {
            capacity: SystemBuilder::DEFAULT_CAPACITY,
            eviction,
            factory: None,
        }
    }

    /// The ALPN every actor system answers on.
    ///
    /// One node serves at most one actor system: a second would be a duplicate
    /// ALPN, which [`ServerBuilder::spawn`](crate::ServerBuilder::spawn)
    /// refuses. Actors of unrelated types share it, and the per-actor schema
    /// hash is what tells them apart.
    pub fn alpn() -> &'static [u8] {
        static CELL: OnceLock<Vec<u8>> = OnceLock::new();
        CELL.get_or_init(|| derive_alpn::<Contract>(PROTOCOL))
            .as_slice()
    }

    /// Build the service that serves this system. Register it on a
    /// [`Server`](crate::Server) like any other.
    #[must_use]
    pub fn service(&self, allow: Allowlist) -> ServiceBuilder<ActorRequest, ActorReply> {
        ServiceBuilder::new(Self::alpn(), allow, self.dispatch())
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
    pub fn mount(&self, id: ActorId, actor: Mounted) -> std::result::Result<(), MountError> {
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

    /// Stop routing to `id`, then wait for the actor to finish what it has.
    ///
    /// The queued messages are handled and answered; the state is dropped after
    /// them. Routing stops the instant this returns from the registry, so a
    /// later message gets [`RemoteError::NoSuchActor`] on a live connection
    /// while every other actor on it carries on.
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

    fn dispatch(&self) -> Dispatch<ActorRequest, ActorReply> {
        let system = self.clone();
        Arc::new(move |cx, request, body| {
            let system = system.clone();
            Box::pin(async move {
                match system.deliver(cx, request, body).await {
                    Ok(answer) => answer,
                    Err(refusal) => (Outcome::Err(refusal), Body::empty()),
                }
            })
        })
    }

    /// The dispatch order, each step failing before the next can run.
    ///
    /// 1. The envelope is decoded by the accept loop. Typed, shape-stable, no
    ///    user types involved.
    /// 2. Re-check the id, which arrived from a peer and never met our
    ///    constructor.
    /// 3. Look it up, spawning on first message if the system has a factory.
    /// 4. Compare the declared hash against the mounted one. **The payload is
    ///    still bytes.**
    /// 5. Hand the sealed payload to the actor, which decodes it — and can only
    ///    decode it — through [`Sealed::open`].
    async fn deliver(
        &self,
        cx: Context,
        request: ActorRequest,
        body: BodyReader,
    ) -> std::result::Result<Answer, RemoteError> {
        request
            .actor
            .validate()
            .map_err(|err| RemoteError::Failed(err.to_string()))?;

        let (ours, mail) = self.inner.route(&request.actor)?;
        if ours != request.schema {
            return Err(RemoteError::ActorSchemaMismatch {
                ours,
                theirs: request.schema,
            });
        }

        let (actor, payload) = request.seal();
        let (reply, answer) = oneshot::channel();
        mail.send(Box::new(Mail {
            cx,
            payload,
            body,
            reply,
        }))
        .await
        .map_err(|_| no_such_actor(&actor))?;

        answer.await.map_err(|_| no_such_actor(&actor))
    }
}

impl fmt::Debug for System {
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
pub struct SystemBuilder {
    capacity: usize,
    eviction: Eviction,
    factory: Option<Factory>,
}

impl SystemBuilder {
    /// The cap a system has when none is named.
    ///
    /// Ten thousand live actors was the number the design was costed against,
    /// so it is the number a system holds before it starts refusing.
    pub const DEFAULT_CAPACITY: usize = 10_000;

    /// Build an actor on the first message addressed to an unmounted id.
    ///
    /// Without this, a message to an unmounted id is
    /// [`RemoteError::NoSuchActor`] and the registry only ever grows by hand.
    /// With it, the registry grows from the network — which is why
    /// [`Self::capacity`] and the eviction policy are not optional decoration.
    /// Return `None` for an id this system will not serve.
    ///
    /// Runs under the registry's write lock, so it must not block. Building
    /// state and spawning a task is what it is for; reading a database is not.
    #[must_use]
    pub fn spawn_with<F>(mut self, factory: F) -> Self
    where
        F: Fn(&ActorId) -> Option<Mounted> + Send + Sync + 'static,
    {
        self.factory = Some(Arc::new(factory));
        self
    }

    /// The most actors this system will hold at once.
    ///
    /// A mount past it is [`MountError::AtCapacity`]; a lazy spawn past it is
    /// [`RemoteError::Failed`] naming the cap. This is the bound that keeps a
    /// lazily spawning system from being a memory-exhaustion target, so raising
    /// it is a statement about how much memory the process may spend.
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
    pub fn build(self) -> System {
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

impl fmt::Debug for SystemBuilder {
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
fn spawn_sweeper(registry: &Arc<Registry>, budget: Duration) {
    let registry = Arc::downgrade(registry);
    // Half the budget, so an actor is evicted between one and one and a half
    // budgets after its last message rather than up to two.
    let tick = (budget / 2).max(Duration::from_millis(1));
    tokio::task::spawn(async move {
        loop {
            tokio::time::sleep(tick).await;
            let Some(registry) = Weak::upgrade(&registry) else {
                return;
            };
            registry.sweep();
        }
    });
}

// ─────────────────────────────────────────────────────────────── the caller

/// A connection to one node's actor system.
///
/// One connection carries every actor on that node. Address one with the
/// generated `at`, which gives back a typed handle whose methods are the ones
/// written in `service!`.
#[derive(Debug)]
pub struct ActorClient {
    inner: RpcClient<ActorRequest, ActorReply>,
}

impl ActorClient {
    /// Dial a peer's actor system by public key.
    ///
    /// # Errors
    ///
    /// [`Error::SchemaMismatch`](crate::Error::SchemaMismatch) when the peer
    /// was built against a different envelope, and
    /// [`Error::Transport`](crate::Error::Transport) when it is unreachable or
    /// refuses us.
    pub async fn connect(node: &Node, peer: impl Into<EndpointAddr>) -> Result<Self> {
        Ok(Self {
            inner: RpcClient::connect(node, peer, System::alpn()).await?,
        })
    }

    /// Send one message to one actor. Called by the generated handle.
    ///
    /// # Errors
    ///
    /// [`Error::Remote`](crate::Error::Remote) carrying
    /// [`RemoteError::NoSuchActor`] or [`RemoteError::ActorSchemaMismatch`],
    /// plus any transport or codec failure.
    #[doc(hidden)]
    pub async fn call<Req, Resp>(
        &self,
        actor: &ActorId,
        schema: u64,
        request: &Req,
        body: Body,
    ) -> Result<(Resp, BodyReader)>
    where
        Req: Serialize,
        Resp: DeserializeOwned,
    {
        let request = ActorRequest::new(actor.clone(), schema, request)?;
        let (reply, body) = self.inner.call(request, body).await?;
        Ok((reply.decode()?, body))
    }

    /// Close the connection gracefully.
    ///
    /// # Errors
    ///
    /// Transport failures while closing.
    pub async fn close(self) -> Result<()> {
        self.inner.close().await
    }
}

/// The schema hash of one actor type's contract, under its service's protocol
/// name. Called by generated code; there is no reason to call it by hand.
#[doc(hidden)]
pub fn actor_schema<C: Schema>(protocol: &str) -> u64 {
    schema_hash::<C>(protocol)
}

#[cfg(test)]
mod tests {
    #![allow(clippy::expect_used, clippy::unwrap_used)]

    use super::*;

    /// Two types with IDENTICAL postcard encodings and different meanings. The
    /// whole reason `Sealed` exists: postcard writes no type identity, so one
    /// decodes cleanly as the other.
    #[derive(Serialize, Deserialize, Schema, Debug, PartialEq, Eq)]
    struct Brl {
        cents: u64,
    }

    #[derive(Serialize, Deserialize, Schema, Debug, PartialEq, Eq)]
    struct Usd {
        cents: u64,
    }

    fn sealed_brl(declared: u64) -> Sealed {
        let request = ActorRequest::new(
            ActorId::new("money").expect("valid"),
            declared,
            &Brl { cents: 4_471 },
        )
        .expect("encodes");
        request.seal().1
    }

    /// The fact the ordering exists to defeat, stated as a test rather than as
    /// a comment: the bytes DO decode as the wrong type.
    #[test]
    fn the_wrong_type_decodes_cleanly_from_the_same_bytes() {
        let bytes = postcard::to_stdvec(&Brl { cents: 4_471 }).expect("encodes");
        let wrong: Usd = postcard::from_bytes(&bytes).expect("no error, wrong meaning");
        assert_eq!(wrong, Usd { cents: 4_471 });
    }

    /// And with the hash in front of the decode, it is refused instead.
    #[test]
    fn open_refuses_a_payload_whose_declared_hash_is_not_ours() {
        let error = sealed_brl(1)
            .open::<Usd>(2)
            .expect_err("the hashes differ, so the bytes are never interpreted");
        assert_eq!(
            error,
            RemoteError::ActorSchemaMismatch { ours: 2, theirs: 1 }
        );
    }

    #[test]
    fn open_decodes_when_the_hashes_agree() {
        let value: Brl = sealed_brl(7).open(7).expect("the hashes agree");
        assert_eq!(value, Brl { cents: 4_471 });
    }

    /// A payload that survives the hash check and still does not decode is a
    /// refusal, not a panic and not a truncated reply.
    ///
    /// The payload here is empty, because almost nothing else fails: postcard
    /// reads a `String`'s length byte as a `u64` field quite happily, which is
    /// the same permissiveness the hash exists to stand in front of.
    #[test]
    fn open_reports_undecodable_bytes_as_a_refusal() {
        let request =
            ActorRequest::new(ActorId::new("x").expect("valid"), 7, &()).expect("encodes");
        let error = request
            .seal()
            .1
            .open::<Brl>(7)
            .expect_err("no bytes is not a Brl");
        assert!(matches!(error, RemoteError::Failed(_)), "got {error:?}");
    }

    /// `Debug` on a sealed payload must not be the accessor that the type has
    /// no accessor for.
    #[test]
    fn debug_never_prints_the_payload() {
        let secret = "muda-de-ipe-4471".to_string();
        let request =
            ActorRequest::new(ActorId::new("x").expect("valid"), 7, &secret).expect("encodes");

        let rendered = format!("{request:?}");
        assert!(!rendered.contains(&secret), "{rendered}");

        let rendered = format!("{:?}", request.seal().1);
        assert!(!rendered.contains(&secret), "{rendered}");
    }

    /// The envelope is frozen at three fields. postcard writes a struct as its
    /// fields back to back, so this pins the bytes a peer must produce.
    #[test]
    fn the_envelope_is_three_fields_in_this_order() {
        let request =
            ActorRequest::new(ActorId::new("ab").expect("valid"), 7, &9u8).expect("encodes");
        let encoded = postcard::to_stdvec(&request).expect("encodes");

        let mut expected = postcard::to_stdvec(&"ab".to_string()).expect("encodes");
        expected.extend(postcard::to_stdvec(&7u64).expect("encodes"));
        expected.extend(postcard::to_stdvec(&postcard::to_stdvec(&9u8).expect("encodes")).unwrap());

        assert_eq!(encoded, expected);
    }

    #[tokio::test]
    async fn guarded_turns_a_panic_into_a_value() {
        let caught = guarded(async { panic!("bad message") }).await;
        let error = caught.expect_err("the panic is caught");
        assert!(error.to_string().contains("bad message"), "{error}");
    }

    #[tokio::test]
    async fn guarded_is_transparent_when_nothing_panics() {
        assert_eq!(guarded(async { 7u8 }).await.expect("no panic"), 7);
    }

    /// An actor that does nothing at all, for testing the registry rather than
    /// any message it carries.
    fn idler() -> Mounted {
        mount(1, |mut inbox| async move {
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
        system.inner.route(&busy).expect("mounted");
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
        let system = System::builder(Eviction::Never).build();
        let error = system
            .inner
            .route(&ActorId::new("nobody").expect("valid"))
            .expect_err("nothing is mounted and nothing builds it");
        assert!(
            matches!(error, RemoteError::NoSuchActor { .. }),
            "got {error:?}"
        );
        assert!(system.is_empty());
    }
}
