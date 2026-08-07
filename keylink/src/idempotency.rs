//! Retry safety: a caller-supplied key so a repeated call runs at most once.
//!
//! A call that fails mid-flight leaves the caller unable to say whether it ran.
//! For `Ping` that costs nothing. For `Reserve(sku, qty)` the caller must choose
//! between a double booking and no booking at all, with no way to tell which
//! one it is choosing.
//!
//! An [`IdempotencyKey`] on the call removes the choice. The serving side
//! records the reply under `(peer, key)`, and a later call carrying the same key
//! is answered from that record instead of being run again.
//!
//! # What is guaranteed
//!
//! **The handler is entered at most once per key, on this peer, on this
//! service, for as long as the record or its tombstone survives.** A repeat is
//! answered with the recorded reply, byte for byte; once the reply has expired
//! or been evicted the repeat is *refused*, never re-run.
//!
//! # Who settles a key, and who does not
//!
//! Work nobody can promise did not run settles a key. A layer that refuses a
//! call **before** dispatch — an authorization rule, a [`QueueTimeout`] whose
//! budget expired while the message was still queued — leaves the key
//! claimable, because that refusal is proof nothing ran and nothing is owed. A
//! layer that gives up **after** dispatch, such as a
//! [`Timeout`](crate::Timeout) that drops the wait for an actor turn already
//! running or already queued to run, settles the key as
//! [`Interrupted`](IdempotencyRefusal::Interrupted): whether the work landed is
//! genuinely not known from there.
//!
//! The line is proof, not order. A message that reached a mailbox is one whose
//! turn runs whether the caller stayed or not, so releasing its key hands a
//! retry the same work to do twice — RFC-04 §16.12.
//!
//! That distinction is not visible in the [`Outcome`](crate::Outcome) a layer
//! returns — `Outcome::Err` is what a panicking actor turn produces too — so it
//! is carried out of band, by the progress marker on
//! [`Context`](crate::Context) that generated dispatch sets. See RFC-05.
//!
//! [`QueueTimeout`]: crate::QueueTimeout
//!
//! # What is NOT guaranteed
//!
//! - **Durability.** The record is memory. It dies with the process, and a
//!   restarted service will run a retried call again. keylink has no journal and
//!   this is not one. Durable execution is what [Restate] is for.
//!
//!   **A durable state makes that sentence sharper, and the sharper version is
//!   a double-apply.** While nothing survived a restart, re-running a retried
//!   call was a re-run against a state that had also been lost, so the work
//!   happened once in total. [`Persist`](crate::Persist) changed that half and
//!   not this one. The interleaving, all of it on the served path:
//!
//!   1. Turn *n* completes. The observer chain stores the state and `fsync`s it.
//!   2. [`Reply::answer`](crate::Reply) marks the key `completed`, and
//!      `Guard::record` writes the replayable reply into the **in-memory**
//!      ledger.
//!   3. The process dies before the reply frame reaches the peer.
//!   4. The peer retries the same key. The restarted process restores a state
//!      that **already contains turn *n***, its ledger is empty, `claim_key`
//!      answers `Claimed::Run`, and the handler runs turn *n* a second time.
//!
//!   Neither half is wrong on its own: the state is exactly as durable as it
//!   promises, and the ledger is exactly as volatile as it says. It is the pair
//!   that is new, and it is the reason effectively-once is a property of one
//!   process's lifetime rather than of a key. A durable ledger is what would
//!   close it, and there is no cheap version — it has to be written on the same
//!   commit as the state, or the two disagree in the other direction.
//! - **A reply forever.** Records are bounded three ways — [`MAX_RECORDS`]
//!   entries, [`MAX_RECORDED_BYTES`] of reply, and [`RECORD_TTL`] of age. Past
//!   any of them the reply is dropped and the key becomes
//!   [`IdempotencyRefusal::Forgotten`]: at-most-once still holds, and the answer
//!   does not.
//! - **A tombstone forever.** Tombstones are bounded too, by
//!   [`MAX_TOMBSTONES`]. Past that a key is genuinely vacant again, which
//!   [`KeyState::Fresh`] reports and which is the one state a caller cannot tell
//!   from "never used".
//! - **A payload.** Bodies are streamed and never held, so a reply that carries
//!   one cannot be recorded. The repeat is refused rather than answered with a
//!   header whose body never arrives.
//! - **Certainty after an interruption.** A call whose handler was entered and
//!   then dropped — a dead connection, a shutdown — leaves
//!   [`IdempotencyRefusal::Interrupted`]. Whether it took effect is genuinely
//!   unknown, and saying so is better than guessing either way.
//!
//! # What this takes from Restate, and what it rejects
//!
//! Taken: the reply is **replayed**, not refused — a caller that lost its answer
//! wants the answer, and refusing hands it nothing it did not already have. The
//! key is scoped to the service and the handler rather than to the whole node.
//! Retention is bounded rather than forever. And **attach**, where a second
//! caller waits for the running invocation instead of being turned away.
//!
//! **A serving side may now decline that first sentence, and this is where that is
//! said.** [`Records::None`] records no reply, so every repeat under it is refused
//! rather than replayed — the exact behaviour the paragraph above calls the wrong
//! one. It is not a reversal of the reasoning. "Refusing hands it nothing it did
//! not already have" is a claim about what is *good for the caller*, and it holds;
//! what [`Records`] adds is that the reply is the *caller's data*, and holding it
//! for [`RECORD_TTL`] is a cost the serving side may not be willing to pay on the
//! caller's behalf. So the trade is stated rather than assumed: a service that
//! declines it gives up the better answer knowingly, and says so on every repeat.
//!
//! Rejected: **durability**, and everything downstream of it — journaling,
//! replay of the execution itself, restart from a known point.
//!
//! ## Attach was rejected once, and the objection was right
//!
//! An earlier version of this module refused attach, citing RFC-04 §3.3's worry
//! about *"whether a dropped `Notified` can swallow a wakeup"*. That worry is
//! real and specific: [`Notify::notify_waiters`] wakes only the waiters
//! registered at the moment it is called, so a waiter that registers after the
//! notification misses it forever — and the failure is load-dependent, which is
//! to say it passes every test.
//!
//! It has a known answer, and this module uses it: [`Notified::enable`]
//! registers the waiter *before* the slot is re-read, so a settle that lands in
//! between is not lost. See RFC-05 §4.
//!
//! [Restate]: https://restate.dev
//! [`Notify::notify_waiters`]: tokio::sync::Notify::notify_waiters
//! [`Notified::enable`]: tokio::sync::futures::Notified::enable

use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use bytes::Bytes;
use iroh::EndpointId;
use n0_future::time::Instant;
use serde::{Deserialize, Serialize};
use tokio::sync::Notify;

use crate::middleware::{Progress, Reached};
use crate::{IdempotencyKey, RemoteError};

/// Settled records the ledger holds before it starts evicting.
///
/// Judgement, not measurement. Large enough that an ordinary retry lands inside
/// it, small enough that the ledger is a rounding error beside one connection.
pub const MAX_RECORDS: usize = 1024;

/// Recorded reply bytes the ledger holds before it starts evicting.
///
/// A record is a reply frame, which is capped at [`MAX_HEADER`](crate::MAX_HEADER)
/// — a megabyte each. Counting entries alone would therefore bound the ledger at
/// a gigabyte, so the bytes are counted too, and a reply that cannot fit here on
/// its own is never recorded.
pub const MAX_RECORDED_BYTES: usize = 1 << 20;

/// Tombstones the ledger holds before it starts dropping them.
///
/// A tombstone is a key and nothing else, and a key is bounded by
/// [`MAX_KEY_LEN`](crate::MAX_KEY_LEN), so this pool is cheap by construction —
/// eight times [`MAX_RECORDS`] costs on the order of a megabyte. It is separate
/// from the record bound on purpose: a tombstone is what keeps an evicted key
/// from being silently re-executed, so it must outlive the reply it replaced by
/// a wide margin.
pub const MAX_TOMBSTONES: usize = 8192;

/// How long a settled record answers with its reply before it becomes a
/// tombstone.
///
/// The reason a volume bound is not enough on its own: with only a volume bound
/// the at-most-once *window* is a function of unrelated traffic, so an operator
/// cannot say how long a retry stays safe without knowing what every other
/// caller is doing. A duration is answerable.
///
/// On [`n0_future::time::Instant`], not [`std::time::Instant`], which panics on
/// `wasm32-unknown-unknown`.
pub const RECORD_TTL: Duration = Duration::from_secs(600);

/// The longest an attaching call is held waiting for one already in flight.
///
/// A caller names its own budget in [`Idempotency::attaching_for`]; this caps
/// it, because the budget arrives from the network and a peer that asks to be
/// held for a year is asking us to hold a QUIC stream for a year.
pub const MAX_ATTACH: Duration = Duration::from_secs(30);

/// Calls with keys one peer may have in dispatch at once.
///
/// Above QUIC's 100 concurrent bidirectional streams, so a peer using one
/// connection never meets it, and a peer opening connection after connection
/// does. Without it a claimed-but-unsettled slot is invisible to both volume
/// bounds — a running claim is deliberately never evicted — so N connections
/// would admit 100×N unaccounted claims.
pub const MAX_RUNNING_PER_PEER: usize = 128;

/// Calls with keys the whole ledger may have in dispatch at once.
///
/// The per-peer cap alone is not a bound: [`Allowlist::Anyone`](crate::Allowlist)
/// makes the number of peers a property of the network rather than of the
/// configuration.
pub const MAX_RUNNING: usize = 4096;

/// Attaching calls one peer may have parked at once, waiting on a call already
/// in flight.
///
/// A parked attacher holds no claim, so neither running cap can see it or the
/// QUIC stream pair it is holding open: one claimed key plus 40 connections of
/// 100 streams each parks 4000 tasks for [`MAX_ATTACH`] and meets no bound.
/// Same number as [`MAX_RUNNING_PER_PEER`], because that is also the legitimate
/// ceiling — a [`Peer`](crate::Peer) re-issue produces at most one attacher per
/// running claim, and running claims are capped there.
pub const MAX_WAITING_PER_PEER: usize = 128;

/// Attaching calls the whole ledger may have parked at once.
///
/// Two levels for the reason the running pair has two:
/// [`Allowlist::Anyone`](crate::Allowlist) makes the peer count a property of
/// the network. A quarter of [`MAX_RUNNING`] rather than the same number,
/// because a waiter produces nothing — it costs a task and a stream pair, and
/// it may still be refused when its budget runs out — so the node spends the
/// smaller share of its budget on calls it has already decided not to run.
pub const MAX_WAITING: usize = 1024;

/// Why a call carrying an idempotency key was refused without being run.
///
/// Grouped under one [`RemoteError`] variant rather than spread across six,
/// because [`RemoteError`] belongs to every service keylink will ever carry and
/// this vocabulary belongs to one feature.
#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, postcard_schema::Schema, thiserror::Error,
)]
#[non_exhaustive]
pub enum IdempotencyRefusal {
    /// The key was empty, or longer than [`MAX_KEY_LEN`](crate::MAX_KEY_LEN).
    ///
    /// Raised on the receive path, because `derive(Deserialize)` never calls a
    /// constructor and a peer's key has therefore never been checked.
    #[error("the key is malformed: {0}")]
    Malformed(String),

    /// Another call with this key is being handled right now.
    ///
    /// Not an error about the key. It is the answer to "did my call run?" —
    /// yes, it is running, so do not send the work a second time. To wait for it
    /// instead, ask with [`Idempotency::attaching_for`]; this is also what an
    /// attaching call is told when its budget runs out, because it is still the
    /// truth.
    #[error("another call with this key is in flight - ask again when it finishes")]
    InFlight,

    /// A call with this key entered the handler and never produced a reply.
    ///
    /// The connection died, the service shut down, or a layer above the handler
    /// stopped waiting part way through. Whether the work took effect is
    /// unknown, and no record can make it known. Decide out of band, then use a
    /// different key.
    #[error("a call with this key was interrupted - whether it took effect is unknown")]
    Interrupted,

    /// The call ran, and its reply is not something this ledger holds.
    ///
    /// **Three causes, one variant, and they share the only thing a caller acts
    /// on:** it ran, there is no reply, do not send the work again — read the
    /// result out of your own domain. All three are permanent properties rather
    /// than evictions, which is the axis RFC-05 §5 draws between this variant and
    /// [`Forgotten`](Self::Forgotten).
    ///
    /// - **The reply carried a payload.** A body is streamed and never held, so
    ///   recording one would contradict the property that makes a 4 GiB transfer
    ///   cost a bounded amount of memory. The header alone would decode into a
    ///   reply whose body never arrives.
    /// - **The reply frame was larger than the per-record ceiling.** One reply over
    ///   the whole byte budget cannot be evicted into fitting, so it is never
    ///   stored.
    /// - **The service holds no replies at all** — [`Records::None`]. A retention
    ///   choice by the serving side, not a property of this method or this reply.
    ///
    /// The message names none of the three, deliberately. Which one applies is a
    /// property of the *service and the method* rather than of the call, so it is
    /// answerable from the service's own configuration and is not something to put
    /// on a wire that every service shares. `Display` said "carried a payload"
    /// until [`Records`] existed, and that sentence was about to start being false
    /// on a service that had never declared a `recv` method.
    #[error("a call with this key already ran, and this service does not hold its reply")]
    Unrecordable,

    /// This key ran, and its reply is no longer held.
    ///
    /// The record aged past [`RECORD_TTL`] or was evicted under pressure — the two
    /// producers are `expire` and `evict`, and both are evictions. The at-most-once
    /// promise still holds, which is what the tombstone is for, and the answer is
    /// gone. Distinct from a vacant key on purpose: silently running the work again
    /// is the one thing this module exists to prevent.
    ///
    /// **A reply that was never recorded is [`Unrecordable`](Self::Unrecordable),
    /// not this.** RFC-05 §5 is explicit that the axis between the two variants is
    /// *a permanent property* against *an eviction*, and that merging them "would
    /// make the query lie about why". [`Records::None`] is a permanent property of
    /// the service, so it settles there. An earlier version of this crate settled
    /// it here and had to widen this sentence to *"or was never recorded"*, which
    /// put a configured choice under a name that means pressure — the wrong side of
    /// the line RFC-05 drew.
    #[error("a call with this key already ran, and its reply is no longer held")]
    Forgotten,

    /// This peer has too many calls with keys in dispatch at once.
    ///
    /// A claimed slot is never evicted, so unbounded claims are unbounded
    /// memory. Bounded by [`MAX_RUNNING_PER_PEER`] and [`MAX_RUNNING`].
    ///
    /// Also what an attaching call is told when [`MAX_WAITING_PER_PEER`] or
    /// [`MAX_WAITING`] is reached. One variant rather than two, because
    /// `postcard_schema` hashes the variant list into the ALPN: a second
    /// spelling of "too many at once" would move the protocol identity and
    /// break every handshake, to say something the caller acts on identically.
    ///
    /// So `limit` names the cap that was reached and **not which cap it was**,
    /// and since [`MAX_WAITING_PER_PEER`] and [`MAX_RUNNING_PER_PEER`] are both
    /// 128 for the reason each doc gives, a per-peer refusal cannot be told from
    /// its sibling at all. That is the price of the one variant, and it is
    /// charged where it costs least: a caller does the same thing either way —
    /// stop adding load and ask again — and an operator who needs to know which
    /// bound is binding has [`Idempotency::attaching_for`] on one side of it and
    /// a bare key on the other, which is a question about the call that was
    /// made rather than about the answer it got.
    #[error("too many calls with idempotency keys are in flight - the limit is {limit}")]
    TooManyRunning {
        /// The cap that was reached.
        limit: u32,
    },

    /// This service does not accept an idempotency key on this method.
    ///
    /// A serving side decides which of its methods a key is worth recording for
    /// — see [`Keyable`] — and this is what a key on any other method is answered
    /// with.
    ///
    /// **Refused rather than ignored, and that is the whole of why this variant
    /// exists.** A caller that wrote `with_idempotency` believes the call runs
    /// at most once. Running it and recording nothing would hand that caller a
    /// guarantee it does not have, silently, on the one path it cannot check —
    /// and it would be discovered by a double booking rather than by an error.
    /// Every other refusal in this enum is here for the same reason: the answer
    /// a caller can act on beats the answer that looks like success.
    ///
    /// **No layer can produce this instead.** The key is claimed *before* the
    /// [`Middleware`](crate::Middleware) chain runs — `serve_one` claims, then
    /// calls `next` — so a layer never meets an unclaimed key and could only
    /// refuse one already in the ledger. The decision has to live where the
    /// claim is.
    ///
    /// `method` names the method in the `CamelCase` spelling
    /// [`Rpc::method`](crate::Rpc::method) returns. Redundant to the caller,
    /// which knows which call it made — one stream per call, so this refusal
    /// arrives on that call's own stream — and carried anyway for the reason
    /// [`RemoteError::NoSuchActor`](crate::RemoteError::NoSuchActor) carries the
    /// id it was given: a log line read away from the call site has to say what
    /// it is about. `String` rather than `&'static str`, because this crosses
    /// the wire inside [`RemoteError`] and has to `Deserialize`.
    #[error("this service does not accept an idempotency key on {method}")]
    NotKeyable {
        /// The method a key was attached to, `CamelCase` as `service!` wrote it.
        method: String,
    },
}

impl From<IdempotencyRefusal> for RemoteError {
    fn from(refusal: IdempotencyRefusal) -> Self {
        Self::Idempotency(refusal)
    }
}

/// Which methods of this service accept an idempotency key.
///
/// The **serving** side decides this, and until this type existed it could not.
/// A peer attached a key to whatever it liked, and one pool of [`MAX_RECORDS`]
/// entries served the whole service — so a method whose reply is worth nothing
/// to record, a quote lookup asked a thousand times a second, evicted the
/// records of the methods that needed them. The ledger has no way to tell a
/// record it should keep from one it should never have taken. This is where that
/// is said.
///
/// Set it with [`ServiceBuilder::keyable`](crate::ServiceBuilder::keyable).
/// [`Any`](Self::Any) is what `ServiceBuilder::new` writes and what this crate did
/// before the type existed.
///
/// **There is deliberately no `Default`, and this enum is exhaustive.** Both follow
/// [`Allowlist`](crate::Allowlist), which is the nearest thing in the crate — a
/// per-service policy a caller constructs and hands to a builder — and whose doc
/// says *"There is deliberately no `Default`. Admitting everyone is a decision you
/// make"*. `Any` is the permissive setting here for the same reason `anyone()` is
/// there, so nobody should reach it by leaving a field out; the default lives in
/// `ServiceBuilder::new`, where it is written once and is visible. And
/// `CONTRIBUTING.md` reserves `#[non_exhaustive]` for error enums rather than for
/// ones a caller constructs, which is what this is.
///
/// # Why this is not called `Keyed`
///
/// `Keyed` is the obvious name and it is taken. In this crate a **keyed** thing
/// is one addressed by an [`ActorId`](crate::ActorId) —
/// [`KeyedService`](crate::KeyedService), `KeyedClient`, `keyed_service`,
/// `keyed_alpn`, `run_keyed` — and `keylink::Keyed` would sit two lines below
/// `keylink::KeyedService` in the same export list meaning a different key
/// entirely. `rpc.rs`'s `Claimed` was renamed *out of* the word for exactly this,
/// and its doc comment records the decision; taking the word back for the
/// idempotency side would undo a rename this crate already paid for.
///
/// **Keyable** is what the type actually decides: whether a key may be attached
/// to a method. `Keyable::Any`, `Keyable::None` — nothing here is ever said of an
/// actor, so there is nothing to disambiguate. `Keying` and `Idempotent` were the
/// other two candidates: the first keeps the stem a reader has to disambiguate,
/// and the second reads as a property of the reply rather than of the method.
///
/// # Method names are `CamelCase`
///
/// These are the names `service!` writes into the generated `Request` enum,
/// which is what [`Rpc::method`](crate::Rpc::method) returns. `rpc
/// reserve_stock(..)` is `"ReserveStock"` here. Reach for the `snake_case`
/// spelling — which is the first thing a reader does — and the policy accepts
/// nothing, silently, because a name that matches no method simply never
/// matches.
///
/// ```ignore
/// keylink::service! {
///     service Till;
///     rpc reserve(sku: String, qty: u32) -> u64;
///     rpc quote(sku: String) -> u64;
/// }
///
/// // A key on `Reserve` is recorded. A key on `Quote` is refused with
/// // `IdempotencyRefusal::NotKeyable`.
/// till::service(handler, Allowlist::anyone())
///     .keyed(Keyable::Only(&["Reserve"]))
///     .build()
/// ```
///
/// # A name matching no method is not a startup error, and it could be
///
/// Nothing checks these names against the service's own methods, so a typo is
/// discovered on the first call rather than at `spawn`. Checking would need the
/// method list *at the builder*, which means a `const METHODS` on
/// [`Rpc`](crate::Rpc) that every generated service carries — a change to the
/// macro, for a mistake whose refusal already names the method it did not match.
/// Left undone deliberately, and recorded here rather than discovered.
///
/// # `&'static`, so the policy is code
///
/// A runtime-computed list is not representable, and that is the intent: which
/// methods are worth recording is a property of the service's design, not of its
/// data. A decision that has to change while the service runs is
/// [`Admission`](crate::Admission)'s shape — a port with a live policy behind it
/// — and nobody has asked for one here.
#[derive(Debug, Clone)]
pub enum Keyable {
    /// Every method accepts one. What this crate did before this type existed,
    /// and what `ServiceBuilder::new` writes.
    Any,
    /// These, and nothing else.
    Only(&'static [&'static str]),
    /// Everything except these.
    Except(&'static [&'static str]),
    /// No method accepts one, and no ledger is built.
    None,
}

impl Keyable {
    /// Whether a key on `method` is accepted rather than refused.
    ///
    /// `&'static str`, matching [`Rpc::method`](crate::Rpc::method), which is
    /// where every real caller's argument comes from. A shorter lifetime would
    /// mean `contains` could not be used against a `&'static [&'static str]` and
    /// the loop would have to be spelled out — a borrow relaxed for nobody.
    pub(crate) fn accepts(&self, method: &'static str) -> bool {
        match self {
            Self::Any => true,
            Self::Only(named) => named.contains(&method),
            Self::Except(named) => !named.contains(&method),
            Self::None => false,
        }
    }

    /// Whether this policy needs a ledger allocated for it at all.
    ///
    /// [`None`](Self::None) is the one variant that does not, and that is what
    /// it is *for*: a service that keys nothing holds no map, no queues and no
    /// counters, and a reader can see the allocation not happen.
    ///
    /// `Only(&[])` accepts nothing either and still gets one. Two spellings of
    /// the same absence would be two things to keep in step, and what `Only(&[])`
    /// builds is an empty `HashMap` that every call refuses before reaching. So
    /// this is an implication rather than an equivalence — **no ledger means no
    /// key is accepted, and the converse is not claimed** — and no refusal is
    /// ever decided by the ledger's absence. [`accepts`](Self::accepts) decides
    /// every refusal, including every one of `None`'s.
    pub(crate) fn needs_a_ledger(&self) -> bool {
        !matches!(self, Self::None)
    }
}

/// Whether this service holds a caller's reply bytes at all.
///
/// **The motive is retention, not memory**, and the memory version of this
/// argument was written first and is wrong. See "What this is not for" below,
/// because getting it the wrong way round is how the whole type gets justified on
/// a saving it does not make.
///
/// Set it with [`ServiceBuilder::records`](crate::ServiceBuilder::records).
/// [`Memory`](Self::Memory) is what `ServiceBuilder::new` writes and what this
/// crate did before the type existed. **There is deliberately no `Default`**, for
/// the reason [`Allowlist`](crate::Allowlist) has none: the value that keeps a
/// caller's payloads in RAM for ten minutes is a decision somebody makes, not one
/// they get by leaving a field out.
///
/// # What [`None`](Self::None) is for
///
/// A reply frame is a caller's data. Under [`Memory`](Self::Memory) it sits in this
/// process's heap for up to [`RECORD_TTL`] — ten minutes — so that a retry can be
/// answered with it. For a great many services that is exactly the trade the
/// ledger exists to make. For a service handling data it would rather not hold, it
/// is a ten-minute retention window nobody asked for, on bytes it was only ever
/// asked to pass through once.
///
/// [`None`](Self::None) is at-most-once with no retention. The key still claims, so
/// the work still runs once; the frame is never written into the ledger, so nothing
/// survives the reply's own trip to the wire.
///
/// # What this is NOT for, and the number that says so
///
/// **Not saving memory.** [`MAX_RECORDED_BYTES`] does double duty and reading it as
/// a per-key figure inverts the whole argument: `Guard::record` uses it as a
/// **per-record ceiling** (`frame.len() > limits.bytes`) and `evict` uses the same
/// value as the **total pool across every peer** (`inner.bytes > limits.bytes`). So
/// the record pool is 1 MiB *in total*, and what [`None`](Self::None) saves is that
/// one megabyte once per process — roughly what its own tombstones cost, since
/// [`MAX_TOMBSTONES`]'s doc puts that pool *"on the order of a megabyte"* too.
///
/// A feature justified on that saving would not pay for itself. Justified on
/// retention it pays immediately, because the thing it removes is not a quantity of
/// bytes but a duration they are held for.
///
/// # What it costs, including the part that is not free
///
/// It costs the answer. Every repeat is [`IdempotencyRefusal::Unrecordable`] rather
/// than the reply, and a caller reconciles against its own state — which is what
/// `README.md`'s limit 5 already tells it to do for a method whose reply cannot be
/// recorded.
///
/// **And it shortens the at-most-once window by about 11%, which is not nothing.**
/// Under [`Memory`](Self::Memory) a settled key occupies a slot in the record pool
/// and *then* becomes a tombstone, so its total life is bounded by [`MAX_RECORDS`]
/// plus [`MAX_TOMBSTONES`] of churn. Under [`None`](Self::None) it goes straight to
/// the tombstone pool and forgoes the record pool's grace entirely:
/// 8192 / (8192 + 1024) — **about 89% of the window**, so roughly an 11% cut. It
/// lands on the one degradation `README.md`'s limit 4 says a caller **cannot
/// detect**, because past [`MAX_TOMBSTONES`] a key reads [`KeyState::Fresh`] and
/// runs again. Small, and stated rather than sold as free.
///
/// Held by `records_none_shortens_the_at_most_once_window`, which is also why the
/// claim above is a measured ratio and not the phrase "does not affect the key
/// state" that stood here first and was false.
///
/// # Not `Copy`, on purpose
///
/// A tier below memory is the shape this would grow into, and it would hold a port
/// — an `Arc<dyn ..>` — rather than a discriminant. `Copy` today would have to be
/// withdrawn then, and withdrawing a trait impl is a breaking change no attribute
/// covers.
#[derive(Debug, Clone)]
pub enum Records {
    /// Memory, bounded by [`MAX_RECORDS`], [`MAX_RECORDED_BYTES`] and
    /// [`RECORD_TTL`]. What this crate did before this type existed.
    Memory,
    /// Nowhere. The key still claims, so the work still runs at most once, and
    /// every repeat is refused instead of answered.
    None,
}

/// What the ledger knows about one key, for one peer.
///
/// Answered by the generated `key_state` query, which is the only way to learn
/// [`Fresh`](Self::Fresh) — every other state can be discovered by retrying, and
/// `Fresh` cannot, because discovering it by retrying means executing.
///
/// Exhaustive on purpose. `#[non_exhaustive]` would be contradictory three ways:
/// this crate reserves that attribute for error enums rather than for ones a
/// caller matches; `postcard_schema` derives from the variant list and ignores
/// the attribute, so a seventh variant moves the ALPN and breaks every handshake
/// anyway — the attribute would advertise source compatibility on a hard wire
/// break; and it forces the `_ =>` arm this crate spent eleven catch-all-free
/// macros deleting.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, postcard_schema::Schema)]
pub enum KeyState {
    /// No record. Either never used, or its tombstone was dropped past
    /// [`MAX_TOMBSTONES`].
    ///
    /// A call with this key would run **if the service accepts a key on the
    /// method** — see [`Keyable`]. On a method the service does not key, every call
    /// is [`IdempotencyRefusal::NotKeyable`] and every key reads `Fresh` forever,
    /// because nothing was ever recorded. That is a fact about the ledger rather
    /// than a prediction about the call, and this variant only ever claimed the
    /// former.
    Fresh,
    /// A call is in dispatch under this key now.
    Running,
    /// Settled. The reply is held and would be replayed byte for byte.
    Done,
    /// Entered and no reply came out. Whether it took effect is unknown.
    Interrupted,
    /// It ran, and this ledger does not hold its reply: it carried a payload, or
    /// it was over the per-record ceiling, or the service holds none
    /// ([`Records::None`]). A permanent property of the method or of the service,
    /// rather than an eviction — which is the whole of what distinguishes this from
    /// [`Forgotten`](Self::Forgotten). See
    /// [`IdempotencyRefusal::Unrecordable`] for the three causes.
    Unrecordable,
    /// It ran; the reply is no longer held. At-most-once still holds.
    Forgotten,
}

/// A key, and what to do when it collides with a call already in flight.
///
/// `attach` without a key is meaningless, so it is not representable: the budget
/// lives inside the value that carries the key. This is also the wire type —
/// one type, client side and envelope side, so the two cannot drift.
///
/// ```ignore
/// // refuse on collision, which is what a bare key has always done
/// client.with_idempotency(key.clone()).submit(order).await?;
///
/// // or wait for the call already running under it, and take its answer
/// client
///     .with_idempotency(Idempotency::attaching_for(key, Duration::from_secs(5)))
///     .submit(order)
///     .await?;
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, postcard_schema::Schema)]
pub struct Idempotency {
    key: IdempotencyKey,
    /// How long the server may hold this call waiting for one already in flight
    /// under the same key. `None` refuses immediately.
    ///
    /// Millis rather than a `Duration`, matching
    /// [`RemoteError::TimedOut`](crate::RemoteError::TimedOut), so `Duration`'s
    /// `Schema` impl never enters the hashed contract.
    attach_for_millis: Option<u64>,
}

impl Idempotency {
    /// Refuse on collision with a call already in flight.
    #[must_use]
    pub fn new(key: IdempotencyKey) -> Self {
        Self {
            key,
            attach_for_millis: None,
        }
    }

    /// Wait up to `budget` for a call already in flight under this key, then
    /// answer with whatever it settled as.
    ///
    /// Capped server-side by [`MAX_ATTACH`], which is 30 seconds. A budget that
    /// runs out is [`IdempotencyRefusal::InFlight`] — the same answer a
    /// non-attaching call gets, and still the truth.
    ///
    /// **The cap is not yours to raise, and asking for more is not an error
    /// here.** The budget is written on the envelope and read by the peer, so a
    /// number refused on this side would stop nobody: a hand-rolled peer sends
    /// whatever it likes, and the serving side caps it because holding a QUIC
    /// stream open is its cost to pay, not the caller's. So `attaching_for(key,
    /// Duration::from_secs(120))` is accepted and waits 30 seconds.
    ///
    /// A handler that routinely runs longer than [`MAX_ATTACH`] cannot be waited
    /// out. Ask again — a second attach re-parks for another budget — or stop
    /// waiting and ask [`key_state`](crate::RpcClient::key_state), which reports
    /// without running anything.
    ///
    /// Only a *running* call is waited for. A key that already settled is
    /// answered at once, whether that answer is the reply, `Interrupted`,
    /// `Unrecordable` or `Forgotten`.
    #[must_use]
    pub fn attaching_for(key: IdempotencyKey, budget: Duration) -> Self {
        Self {
            key,
            attach_for_millis: Some(u64::try_from(budget.as_millis()).unwrap_or(u64::MAX)),
        }
    }

    /// The key itself.
    #[must_use]
    pub fn key(&self) -> &IdempotencyKey {
        &self.key
    }

    /// How long the caller asked to be held, already capped at [`MAX_ATTACH`].
    ///
    /// Capped here rather than at the constructor, because the value that
    /// matters is the one that arrived from the network and never met a
    /// constructor.
    #[must_use]
    pub fn attach_budget(&self) -> Option<Duration> {
        self.attach_for_millis
            .map(|millis| Duration::from_millis(millis).min(MAX_ATTACH))
    }
}

/// Keeps every `with_idempotency(key)` call site reading the way it always has.
impl From<IdempotencyKey> for Idempotency {
    fn from(key: IdempotencyKey) -> Self {
        Self::new(key)
    }
}

/// What one key is worth, at the moment it is asked about.
#[derive(Debug)]
enum Slot {
    /// A call is in dispatch under this key now. Never evicted: dropping it
    /// would let a concurrent duplicate into the handler. The handle is what an
    /// attaching call waits on, and it is fired when the slot is replaced.
    Running(Arc<Notify>),
    /// The reply frame, ready to be written again byte for byte.
    Done(Bytes),
    /// The handler was entered and no reply came out.
    Interrupted,
    /// The handler ran, and its reply is not something this ledger can hold.
    Unrecordable,
    /// The handler ran, and the reply has since expired or been evicted.
    Forgotten,
}

impl Slot {
    /// What a repeat of this key gets.
    fn replay(&self) -> Claim {
        match self {
            Self::Running(notify) => Claim::Running(Arc::clone(notify)),
            Self::Done(frame) => Claim::Replay(frame.clone()),
            Self::Interrupted => Claim::Refused(IdempotencyRefusal::Interrupted.into()),
            Self::Unrecordable => Claim::Refused(IdempotencyRefusal::Unrecordable.into()),
            Self::Forgotten => Claim::Refused(IdempotencyRefusal::Forgotten.into()),
        }
    }

    /// What a query about this key gets.
    fn state(&self) -> KeyState {
        match self {
            Self::Running(_) => KeyState::Running,
            Self::Done(_) => KeyState::Done,
            Self::Interrupted => KeyState::Interrupted,
            Self::Unrecordable => KeyState::Unrecordable,
            Self::Forgotten => KeyState::Forgotten,
        }
    }

    /// Recorded bytes this slot is holding, counted once so `Inner::bytes`
    /// cannot drift.
    ///
    /// Exhaustive, with no catch-all, so that a sixth variant is a compile error
    /// here rather than a silent zero. The failure a catch-all buys: a new
    /// variant that holds bytes counts as 0, `inner.bytes` drifts below the
    /// truth, and `evict` stops firing at the cap it exists to enforce. The
    /// ledger grows without bound and every test stays green, because no test
    /// asserts on a variant that does not exist yet.
    fn held_bytes(&self) -> usize {
        match self {
            Self::Done(frame) => frame.len(),
            Self::Running(_) | Self::Interrupted | Self::Unrecordable | Self::Forgotten => 0,
        }
    }
}

/// One slot and when it reached its current state.
#[derive(Debug)]
struct Entry {
    slot: Slot,
    /// When this entry settled — or, for [`Slot::Running`], when it was claimed.
    /// Only settled entries are aged out; a running claim has no deadline,
    /// because the transport is what bounds it.
    at: Instant,
}

/// One peer's records. Peers never share a bucket, and that is the whole
/// security property — see [`Ledger`].
#[derive(Debug, Default)]
struct Bucket {
    slots: HashMap<IdempotencyKey, Entry>,
    /// Keys holding a replayable reply, oldest first. The eviction order for
    /// this peer's records, and the expiry order too, since both are by age.
    held: VecDeque<IdempotencyKey>,
    /// Keys that ran and hold no reply: interrupted, unrecordable, forgotten.
    /// Oldest first, and bounded separately, because a tombstone is cheap and
    /// has to outlive the record it replaced.
    spent: VecDeque<IdempotencyKey>,
    /// [`Slot::Running`] keys, which appear in neither queue.
    running: usize,
}

#[derive(Debug, Default)]
struct Inner {
    peers: HashMap<EndpointId, Bucket>,
    /// Attaching calls parked right now, per peer. Beside `peers` rather than
    /// inside a [`Bucket`], because a waiter outlives the records it is waiting
    /// on: the settle it is woken by can leave the bucket empty, and an empty
    /// bucket is removed.
    waiters: HashMap<EndpointId, usize>,
    held: usize,
    bytes: usize,
    spent: usize,
    running: usize,
    waiting: usize,
}

/// Every bound the ledger enforces, in one value so a test can move them.
#[derive(Debug, Clone, Copy)]
struct Limits {
    records: usize,
    bytes: usize,
    tombstones: usize,
    running: usize,
    running_per_peer: usize,
    waiting: usize,
    waiting_per_peer: usize,
    ttl: Duration,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            records: MAX_RECORDS,
            bytes: MAX_RECORDED_BYTES,
            tombstones: MAX_TOMBSTONES,
            running: MAX_RUNNING,
            running_per_peer: MAX_RUNNING_PER_PEER,
            waiting: MAX_WAITING,
            waiting_per_peer: MAX_WAITING_PER_PEER,
            ttl: RECORD_TTL,
        }
    }
}

/// The answer to "has this key been used?".
#[derive(Debug)]
pub(crate) enum Claim {
    /// Nobody has used this key. Run the call, then settle the guard.
    Fresh(Guard),
    /// Write these bytes. The handler is not entered.
    Replay(Bytes),
    /// Refuse. The handler is not entered.
    Refused(RemoteError),
    /// A call is in dispatch under this key. A caller that asked to attach waits
    /// on this handle and claims again; one that did not is refused
    /// [`IdempotencyRefusal::InFlight`].
    Running(Arc<Notify>),
}

/// Every recorded reply for one service.
///
/// # Scope
///
/// One ledger per **service**, and every record inside it is keyed by the
/// **peer** iroh proved at the handshake. Both halves are load bearing.
///
/// Per service, because a record is an encoded reply of *that service's*
/// response type. Sharing one ledger across a node would let a key recorded
/// against one schema be replayed into another, which is the postcard
/// variant-confusion the derived ALPN exists to refuse — reintroduced one layer
/// below the gate that refuses it.
///
/// Per peer, because a key is a string a peer chose. A shared namespace would
/// let one peer read another peer's reply by guessing `order-1`, which is an
/// information leak before it is a collision. It is also what makes `attach`
/// safe for free: one peer can never wait on another peer's call.
#[derive(Debug)]
pub(crate) struct Ledger {
    inner: Mutex<Inner>,
    limits: Limits,
    /// Where the replies recorded here go. Held by the ledger rather than
    /// threaded down from `serve_one`, because `Guard::record` is the one place
    /// that reads it and the guard already holds the ledger.
    records: Records,
}

impl Ledger {
    pub(crate) fn new(records: Records) -> Arc<Self> {
        Self::with_limits(Limits::default(), records)
    }

    fn with_limits(limits: Limits, records: Records) -> Arc<Self> {
        Arc::new(Self {
            inner: Mutex::new(Inner::default()),
            limits,
            records,
        })
    }

    /// Take the key, or say what already holds it.
    ///
    /// This is the whole at-most-once property: the slot is claimed *before* the
    /// handler is entered, and the only way back to vacant is for the guard to
    /// be released — which happens exactly when nothing ran.
    pub(crate) fn claim(self: &Arc<Self>, peer: EndpointId, key: IdempotencyKey) -> Claim {
        let now = Instant::now();
        let mut guard = self.lock();
        // Dereferenced once, on purpose: field-splitting borrows work through a
        // `&mut Inner` and do NOT work through a `MutexGuard`, because deref
        // borrows the whole guard. Every function here that touches a bucket and
        // a counter in the same breath needs this line.
        let inner = &mut *guard;
        self.expire(inner, peer, now);

        let total = inner.running;
        // `get`, never `entry`: an `or_default()` above these checks inserts a
        // bucket for a peer the global cap is about to turn away, and nothing
        // removes it, because `release` and `evict` prune only a bucket they
        // took the last slot out of. With `Allowlist::Anyone` the number of
        // distinct peers is a property of the network, so that is unbounded
        // memory in the one module whose whole purpose is bounds.
        if let Some(bucket) = inner.peers.get(&peer) {
            if let Some(entry) = bucket.slots.get(&key) {
                return entry.slot.replay();
            }
            if bucket.running >= self.limits.running_per_peer {
                return refused_over(self.limits.running_per_peer);
            }
        }
        if total >= self.limits.running {
            return refused_over(self.limits.running);
        }
        {
            let bucket = inner.peers.entry(peer).or_default();
            bucket.slots.insert(
                key.clone(),
                Entry {
                    slot: Slot::Running(Arc::new(Notify::new())),
                    at: now,
                },
            );
            bucket.running += 1;
        }
        inner.running += 1;

        Claim::Fresh(Guard {
            ledger: Arc::clone(self),
            peer,
            key,
            settled: false,
            progress: None,
        })
    }

    /// Take a place to wait in, or refuse the attach before it parks.
    ///
    /// The other half of the running caps, and it has to be a separate count:
    /// an attaching call holds **no** claim, so `Bucket::running` and
    /// `Inner::running` see neither the task nor the QUIC stream pair it holds
    /// open for [`MAX_ATTACH`]. Counted in the ledger rather than per
    /// connection, because a peer's waiters are spread across however many
    /// connections it opened and a per-connection value cannot see the others.
    ///
    /// # Errors
    ///
    /// [`IdempotencyRefusal::TooManyRunning`] once [`MAX_WAITING_PER_PEER`] or
    /// [`MAX_WAITING`] is reached.
    pub(crate) fn wait(self: &Arc<Self>, peer: EndpointId) -> Result<Waiting, RemoteError> {
        let mut guard = self.lock();
        let inner = &mut *guard;
        if inner.waiters.get(&peer).copied().unwrap_or(0) >= self.limits.waiting_per_peer {
            return Err(over(self.limits.waiting_per_peer));
        }
        if inner.waiting >= self.limits.waiting {
            return Err(over(self.limits.waiting));
        }
        *inner.waiters.entry(peer).or_default() += 1;
        inner.waiting += 1;
        Ok(Waiting {
            ledger: Arc::clone(self),
            peer,
        })
    }

    /// What this peer's key is worth, without touching it.
    ///
    /// Peer-scoped by construction: the id is the one iroh proved on the
    /// connection the question arrived on, so a peer cannot name another's.
    pub(crate) fn key_state(&self, peer: EndpointId, key: &IdempotencyKey) -> KeyState {
        let now = Instant::now();
        let mut guard = self.lock();
        let inner = &mut *guard;
        self.expire(inner, peer, now);
        inner
            .peers
            .get(&peer)
            .and_then(|bucket| bucket.slots.get(key))
            .map_or(KeyState::Fresh, |entry| entry.slot.state())
    }

    /// Poison recovery rather than propagation, the same call
    /// `demo/src/main.rs` makes about its own state: one bad request should cost
    /// one request. Nothing here can panic while the lock is held, so this arm
    /// is unreachable in practice and must still be typed.
    fn lock(&self) -> std::sync::MutexGuard<'_, Inner> {
        self.inner.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Move a claimed slot to its settled state and make room for it.
    fn settle(&self, peer: EndpointId, key: &IdempotencyKey, slot: Slot) {
        let mut guard = self.lock();
        let inner = &mut *guard;
        let added = slot.held_bytes();
        let holds_reply = matches!(slot, Slot::Done(_));

        let Some(bucket) = inner.peers.get_mut(&peer) else {
            return;
        };
        let Some(previous) = bucket.slots.insert(
            key.clone(),
            Entry {
                slot,
                at: Instant::now(),
            },
        ) else {
            // Nothing was claimed, so nothing is being settled. Unreachable
            // through `Guard`, which only exists for a slot this ledger made.
            bucket.slots.remove(key);
            return;
        };

        // Waiters are woken while the new slot is already in place, so a waiter
        // that re-claims sees the settled value rather than racing the write.
        let was_running = matches!(previous.slot, Slot::Running(_));
        if let Slot::Running(notify) = previous.slot {
            notify.notify_waiters();
            bucket.running = bucket.running.saturating_sub(1);
        }
        if holds_reply {
            bucket.held.push_back(key.clone());
        } else {
            bucket.spent.push_back(key.clone());
        }

        if was_running {
            inner.running = inner.running.saturating_sub(1);
        }
        if holds_reply {
            inner.held += 1;
            inner.bytes += added;
        } else {
            inner.spent += 1;
        }
        self.evict(inner);
    }

    /// Give a claimed key back, because nothing ran under it.
    ///
    /// The counterpart of [`Guard::record`], and the reason a layer that refuses
    /// before dispatch costs the caller nothing: the key is vacant again, so the
    /// caller's retry is a first attempt rather than a permanent refusal.
    fn release(&self, peer: EndpointId, key: &IdempotencyKey) {
        let mut guard = self.lock();
        let inner = &mut *guard;
        let Some(bucket) = inner.peers.get_mut(&peer) else {
            return;
        };
        let Some(entry) = bucket.slots.remove(key) else {
            return;
        };
        // Only a running claim is releasable; a settled one is somebody's
        // answer. Restoring it keeps the counters honest either way.
        let Slot::Running(notify) = entry.slot else {
            bucket.slots.insert(key.clone(), entry);
            return;
        };
        notify.notify_waiters();
        bucket.running = bucket.running.saturating_sub(1);
        let empty = bucket.slots.is_empty();
        inner.running = inner.running.saturating_sub(1);
        if empty {
            inner.peers.remove(&peer);
        }
    }

    /// Age one peer's settled records out, oldest first.
    ///
    /// Both queues are in insertion order, so age is monotone along them and
    /// this stops at the first entry still inside its budget. Scoped to the
    /// peer being served rather than sweeping every bucket, so a claim costs
    /// what that peer's own history costs and nothing else.
    ///
    /// A record past its TTL becomes a tombstone rather than vanishing. The
    /// reply is gone; the promise is not.
    fn expire(&self, inner: &mut Inner, peer: EndpointId, now: Instant) {
        let ttl = self.limits.ttl;
        let Some(bucket) = inner.peers.get_mut(&peer) else {
            return;
        };
        while let Some(key) = bucket.held.front().cloned() {
            let Some(entry) = bucket.slots.get_mut(&key) else {
                bucket.held.pop_front();
                inner.held = inner.held.saturating_sub(1);
                continue;
            };
            if now.saturating_duration_since(entry.at) < ttl {
                break;
            }
            let freed = entry.slot.held_bytes();
            entry.slot = Slot::Forgotten;
            bucket.held.pop_front();
            bucket.spent.push_back(key);
            inner.bytes = inner.bytes.saturating_sub(freed);
            inner.held = inner.held.saturating_sub(1);
            inner.spent += 1;
        }
    }

    /// Bring both pools back inside their bounds.
    ///
    /// Always from the peer holding the most, so a peer that floods the ledger
    /// evicts its own records first and stops being the largest holder before it
    /// can reach anyone else's. A global oldest-first policy would let one peer
    /// flush every other peer's records, which is a denial of service dressed as
    /// a cache.
    ///
    /// Records are **downgraded** rather than dropped: the reply goes and the
    /// key stays, as a tombstone, so a retry of an evicted key is refused
    /// [`IdempotencyRefusal::Forgotten`] instead of silently running again. That
    /// is also why the two pools are counted separately — a tombstone that still
    /// counted as a record would make this loop unable to reach its own bound.
    fn evict(&self, inner: &mut Inner) {
        while inner.held > self.limits.records || inner.bytes > self.limits.bytes {
            let Some(peer) = largest(inner, |bucket| bucket.held.len()) else {
                return;
            };
            let Some(bucket) = inner.peers.get_mut(&peer) else {
                return;
            };
            let Some(key) = bucket.held.pop_front() else {
                return;
            };
            let freed = match bucket.slots.get_mut(&key) {
                Some(entry) => {
                    let freed = entry.slot.held_bytes();
                    entry.slot = Slot::Forgotten;
                    bucket.spent.push_back(key);
                    freed
                }
                None => 0,
            };
            inner.bytes = inner.bytes.saturating_sub(freed);
            inner.held = inner.held.saturating_sub(1);
            inner.spent += 1;
        }

        while inner.spent > self.limits.tombstones {
            let Some(peer) = largest(inner, |bucket| bucket.spent.len()) else {
                return;
            };
            let Some(bucket) = inner.peers.get_mut(&peer) else {
                return;
            };
            let Some(key) = bucket.spent.pop_front() else {
                return;
            };
            bucket.slots.remove(&key);
            let empty = bucket.slots.is_empty();
            inner.spent = inner.spent.saturating_sub(1);
            if empty {
                inner.peers.remove(&peer);
            }
        }
    }
}

/// The peer holding the most of whatever `size` counts, ties broken by id so
/// the order is deterministic rather than whatever the hasher happened to seed.
fn largest(inner: &Inner, size: impl Fn(&Bucket) -> usize) -> Option<EndpointId> {
    inner
        .peers
        .iter()
        .filter(|(_, bucket)| size(bucket) > 0)
        .max_by_key(|(id, bucket)| (size(bucket), **id))
        .map(|(id, _)| *id)
}

/// One refusal, built in one place, so every cap reports the same way.
fn over(limit: usize) -> RemoteError {
    IdempotencyRefusal::TooManyRunning {
        limit: u32::try_from(limit).unwrap_or(u32::MAX),
    }
    .into()
}

/// The same refusal, on the path that answers a claim rather than a wait.
fn refused_over(limit: usize) -> Claim {
    Claim::Refused(over(limit))
}

/// A claimed key, held for as long as the call is being handled.
///
/// Settle it with [`Guard::record`]. Dropping it unsettled asks the call's
/// progress marker what happened, which is the whole of the defect RFC-05 §2
/// records: a refusal that never reached the handler used to be recorded as the
/// call's settled reply and replayed forever, while a call that really was
/// interrupted has to stay unknown.
///
/// Deciding inside `Drop` rather than at the one call site is deliberate. The
/// serving task can be **aborted** — the connection ended, the server shut
/// down — and then there is nobody left to settle the key by hand. `Drop` is
/// the only code that still runs, so it is where the decision has to live.
#[derive(Debug)]
pub(crate) struct Guard {
    ledger: Arc<Ledger>,
    peer: EndpointId,
    key: IdempotencyKey,
    settled: bool,
    /// How far the call got. `None` for a guard nothing is watching, which
    /// settles `Interrupted` on drop — the conservative answer, and the one the
    /// ledger gave before anything watched.
    progress: Option<Progress>,
}

impl Guard {
    /// Watch how far this call gets, so an abort settles honestly.
    #[must_use]
    pub(crate) fn watching(mut self, progress: Progress) -> Self {
        self.progress = Some(progress);
        self
    }

    /// Record the reply frame this call produced.
    ///
    /// `carries_body` is what makes a reply unrecordable: the frame is the
    /// header, and a header replayed without the payload behind it decodes into
    /// a body that never arrives.
    pub(crate) fn record(mut self, frame: &Bytes, carries_body: bool) {
        self.settled = true;
        // Three causes, one slot, and that is the whole of Wave 2's wire cost:
        // nothing. `Slot::Unrecordable` reads "the handler ran, and its reply is
        // not something this ledger can hold", which is true word for word of a
        // reply that carried a payload, of one over the per-record ceiling, and of
        // a service configured to hold none.
        //
        // NOT `Slot::Forgotten`, which was the first attempt. RFC-05 §5 draws the
        // axis this sits on — a permanent property versus an eviction — and says
        // merging the two "would make the query lie about why". `Forgotten` has two
        // producers, `expire` and `evict`, and both are evictions. A configured
        // retention policy is a permanent property, so it belongs on this side of
        // that line.
        //
        // Reusing this variant is also what keeps the refusal and the QUERY honest
        // for free: `Slot::state` already maps it to `KeyState::Unrecordable`, so a
        // caller that asks gets the same story a caller that retries gets. A new
        // variant would have had to be minted twice, on two enums, to say what this
        // one already says.
        //
        // The arm order that mattered under the old design does not exist any more.
        // Both causes land here, so there is nothing to sequence and README limit 5
        // is untouched.
        let recordable = matches!(self.ledger.records, Records::Memory)
            && !carries_body
            && frame.len() <= self.ledger.limits.bytes;
        let slot = if recordable {
            Slot::Done(frame.clone())
        } else {
            Slot::Unrecordable
        };
        self.ledger.settle(self.peer, &self.key, slot);
    }
}

impl Drop for Guard {
    fn drop(&mut self) {
        if self.settled {
            return;
        }
        // Nothing ran and somebody was in a position to know it, so nothing is
        // owed and the key goes back. Anything else — entered and unfinished,
        // finished with nobody left to record it, or still in a mailbox whose
        // turn nobody stayed to watch — is genuinely unknown.
        if self.progress.as_ref().map(Progress::reached) == Some(Reached::NotDispatched) {
            self.ledger.release(self.peer, &self.key);
        } else {
            self.ledger.settle(self.peer, &self.key, Slot::Interrupted);
        }
    }
}

/// One attaching call's place in the queue, held for as long as it waits.
///
/// RAII for the reason [`Guard`] is: the ordinary way a waiter stops waiting is
/// that its task is **aborted** — the caller's connection ended — and a
/// decrement written on the return paths of `claim_key` does not run then.
/// `Drop` does. Getting this wrong leaks a place per abandoned attach until the
/// cap is reached with nobody waiting, and every attach after that is refused
/// for as long as the process lives.
#[derive(Debug)]
pub(crate) struct Waiting {
    ledger: Arc<Ledger>,
    peer: EndpointId,
}

impl Drop for Waiting {
    fn drop(&mut self) {
        let mut guard = self.ledger.lock();
        let inner = &mut *guard;
        inner.waiting = inner.waiting.saturating_sub(1);
        let Some(count) = inner.waiters.get_mut(&self.peer) else {
            return;
        };
        *count = count.saturating_sub(1);
        // The entry goes at zero, for the reason `claim` creates no bucket on a
        // refusing path: a map keyed by peer is a map an open network chooses
        // the size of.
        if *count == 0 {
            inner.waiters.remove(&self.peer);
        }
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::expect_used, clippy::unwrap_used)]

    use super::*;

    /// Two distinct, valid endpoint ids. Reused from `authz.rs`, which needs the
    /// same thing for the same reason.
    const A: &str = "a09aa5f47a6759802ff955f8dc2d2a14a5c99d23be97f864127ff9383455a4f0";
    const B: &str = "d04ab232742bb4ab3a1368bd4615e4e6d0224ab71a016baf8520a332c9778737";

    fn peer(hex: &str) -> EndpointId {
        use std::str::FromStr;
        EndpointId::from_str(hex).expect("a valid endpoint id")
    }

    fn key(text: &str) -> IdempotencyKey {
        IdempotencyKey::new(text).expect("a valid key")
    }

    fn frame(len: usize) -> Bytes {
        Bytes::from(vec![7u8; len])
    }

    /// A ledger holding `max` records. Named `holding` rather than `records`,
    /// which now means [`Records`] two hundred lines up.
    fn holding(max: usize) -> Limits {
        Limits {
            records: max,
            ..Limits::default()
        }
    }

    /// The ordinary ledger every test below this line wants: memory-backed, with
    /// the shipped bounds.
    fn ledger() -> Arc<Ledger> {
        Ledger::new(Records::Memory)
    }

    /// The same, with the bounds moved. One test door rather than two, so
    /// `Records::Memory` is stated once for every test that is not about it.
    fn bounded(limits: Limits) -> Arc<Ledger> {
        Ledger::with_limits(limits, Records::Memory)
    }

    /// A ledger that claims keys and records no replies.
    fn recordless() -> Arc<Ledger> {
        Ledger::new(Records::None)
    }

    fn refusal(claim: Claim) -> IdempotencyRefusal {
        match claim {
            Claim::Refused(RemoteError::Idempotency(refusal)) => refusal,
            Claim::Running(_) => IdempotencyRefusal::InFlight,
            other => panic!("expected an idempotency refusal, got {other:?}"),
        }
    }

    fn recorded(claim: Claim) -> Bytes {
        match claim {
            Claim::Replay(bytes) => bytes,
            other => panic!("expected a replay, got {other:?}"),
        }
    }

    fn guard(claim: Claim) -> Guard {
        match claim {
            Claim::Fresh(guard) => guard,
            other => panic!("expected a fresh claim, got {other:?}"),
        }
    }

    fn waited(place: Result<Waiting, RemoteError>) -> IdempotencyRefusal {
        match place {
            Err(RemoteError::Idempotency(refusal)) => refusal,
            Err(other) => panic!("expected an idempotency refusal, got {other:?}"),
            Ok(_) => panic!("expected a refusal, got a place to wait in"),
        }
    }

    /// [`Keyable`], which decides whether anything below this module runs at all.
    ///
    /// Exhaustive over the four variants here, in one place, so the wire test
    /// `a_key_on_a_method_the_service_does_not_key_is_refused` only has to prove
    /// the policy is *wired in* rather than that every variant is right. Same
    /// split as `key_state_reports_every_state` beside
    /// `a_key_state_query_crosses_the_wire`.
    mod policy {
        use super::*;

        /// Both directions for every variant. One direction per variant would
        /// pass for a policy that answered a constant, which is the failure this
        /// exists to refuse: `Only` that accepted everything and `Except` that
        /// refused everything are each half-green under a one-sided test.
        ///
        /// Made red by `scripts/break-once.sh` replacing
        /// `Self::Only(named) => named.contains(&method),` with the same line plus
        /// `|| true`. Watched: `2 passed; 3 failed` of this module's five —
        /// `every_variant_accepts_and_refuses_what_it_says`,
        /// `an_empty_only_accepts_nothing_and_still_needs_a_ledger` and
        /// `a_snake_case_name_matches_nothing`. Three tests catching one edit is
        /// the intended overlap: each states a different consequence of `Only`
        /// meaning what it says.
        ///
        /// Made red a second way, on the other end of the sweep, by replacing
        /// `Self::Any => true,` with `Self::Any => false,`. Watched: `assertion
        /// failed: Keyable::Any.accepts("Reserve")`, `3 passed; 2 failed`,
        /// the second being
        /// `the_default_policy_keys_every_method`.
        #[test]
        fn every_variant_accepts_and_refuses_what_it_says() {
            assert!(Keyable::Any.accepts("Reserve"));
            assert!(Keyable::Any.accepts("Quote"));

            assert!(Keyable::Only(&["Reserve"]).accepts("Reserve"));
            assert!(!Keyable::Only(&["Reserve"]).accepts("Quote"));

            assert!(!Keyable::Except(&["Quote"]).accepts("Quote"));
            assert!(Keyable::Except(&["Quote"]).accepts("Reserve"));
        }

        /// Split out from the sweep above because it is the variant with a
        /// second consequence — no ledger — and the two halves of `None` belong
        /// beside each other.
        ///
        /// Made red by `scripts/break-once.sh` replacing `Self::None => false,`
        /// with `Self::None => true,`. Watched: `assertion failed:
        /// !Keyable::None.accepts("Reserve")`, `4 passed; 1 failed`, and no other test in the crate noticed — which is
        /// why this one exists.
        #[test]
        fn keyed_none_accepts_no_method() {
            assert!(!Keyable::None.accepts("Reserve"));
            assert!(!Keyable::None.needs_a_ledger());
        }

        /// The empty list is the spelling `needs_a_ledger` deliberately does not
        /// recognise, and this pins the asymmetry rather than leaving it in prose:
        /// it accepts nothing and still builds a ledger, so "no ledger" is a
        /// consequence of `None` and never the cause of a refusal.
        ///
        /// Red under the same `Self::Only(..) || true` break recorded on the sweep
        /// above; watched there.
        #[test]
        fn an_empty_only_accepts_nothing_and_still_needs_a_ledger() {
            assert!(!Keyable::Only(&[]).accepts("Reserve"));
            assert!(Keyable::Only(&[]).needs_a_ledger());
        }

        /// `Any` is the behaviour that existed before the type did, which is the
        /// half of invariant 8 this module can hold.
        ///
        /// There is no `Keyable::default()` to assert against, on purpose — `Any` is
        /// the permissive setting and `Allowlist`'s doc makes the argument: *"There
        /// is deliberately no `Default`. Admitting everyone is a decision you
        /// make"*. That nobody gets `Any` by accident is the absence of the impl,
        /// which the compiler holds; that a service naming no policy gets it is held
        /// over a real connection by
        /// `tests/idempotency.rs::the_defaults_reproduce_todays_behaviour`.
        ///
        /// Made red by `scripts/break-once.sh` replacing `Self::Any => true,` with
        /// `Self::Any => false,`. Watched: `assertion failed:
        /// Keyable::Any.accepts("Reserve")`, `3 passed; 2 failed`.
        #[test]
        fn the_permissive_policy_keys_every_method() {
            assert!(Keyable::Any.accepts("Reserve"));
            assert!(Keyable::Any.accepts("Datasheet"));
        }

        /// `CamelCase`, because a reader reaches for the other spelling first and
        /// the failure is silent — a `snake_case` name matches no method, so the
        /// policy refuses every call and says nothing about why.
        ///
        /// Red under the same `Self::Only(..) || true` break recorded on the sweep
        /// above; watched there. `assertion failed:
        /// !Keyable::Only(&["reserve"]).accepts("Reserve")`.
        #[test]
        fn a_snake_case_name_matches_nothing() {
            assert!(!Keyable::Only(&["reserve"]).accepts("Reserve"));
        }
    }

    /// [`Records`], which decides what a settled key holds rather than whether it
    /// settles.
    ///
    /// The line these tests draw: every one of them claims a key and every one of
    /// them proves the claim still happened. `Records::None` is about the reply
    /// bytes and nothing else, and a test that only checked the refusal would not
    /// tell a reader whether the key was claimed at all.
    mod records {
        use super::*;

        /// **The reply is not held, and the key is.** Both halves, because either
        /// alone describes a different feature: holding neither is a ledger that
        /// does nothing, and holding both is `Records::Memory`.
        ///
        /// Made red by `scripts/break-once.sh` replacing
        /// `Guard::record`'s `recordable` flag to
        /// ignore the retention switch — its `matches!` on `self.ledger.records`
        /// replaced by `true`. Watched:
        /// `panicked at .., expected an idempotency refusal, got
        /// Replay(b"\x07\x07\x07\x07\x07\x07\x07\x07\x07\x07\x07\x07\x07\x07\x07\x07")`,
        /// from this module's `refusal(..)` helper.
        #[test]
        fn records_none_settles_the_key_and_holds_no_reply() {
            let ledger = recordless();
            guard(ledger.claim(peer(A), key("k"))).record(&frame(16), false);

            assert_eq!(
                refusal(ledger.claim(peer(A), key("k"))),
                IdempotencyRefusal::Unrecordable,
                "the key must be spent, so the work cannot run again"
            );
            assert_eq!(
                ledger.lock().bytes,
                0,
                "and not one byte of the reply may be held"
            );
        }

        /// **`Unrecordable` and not `Forgotten`**, which is the axis RFC-05 §5 draws
        /// and the one this settle sits on: a permanent property of the service, not
        /// an eviction.
        ///
        /// Asserted beside a genuinely evicted key in the same test, because the
        /// claim is that the two are *told apart*. A settle that answered
        /// `Forgotten` would make this half green and the other half
        /// indistinguishable from it.
        ///
        /// Made red by `scripts/break-once.sh` replacing
        /// `Guard::record`'s settled slot from
        /// `Slot::Unrecordable` to `Slot::Forgotten`. Watched: `assertion `left == right` failed: a configured
        /// retention policy is not an eviction`, `left: Forgotten`,
        /// `right: Unrecordable`.
        #[test]
        fn a_retention_refusal_is_told_apart_from_an_eviction() {
            let ledger = Ledger::with_limits(holding(1), Records::None);
            guard(ledger.claim(peer(A), key("kept"))).record(&frame(8), false);

            assert_eq!(
                refusal(ledger.claim(peer(A), key("kept"))),
                IdempotencyRefusal::Unrecordable,
                "a configured retention policy is not an eviction"
            );

            let evicting = Ledger::with_limits(holding(1), Records::Memory);
            guard(evicting.claim(peer(A), key("old"))).record(&frame(8), false);
            guard(evicting.claim(peer(A), key("new"))).record(&frame(8), false);
            assert_eq!(
                refusal(evicting.claim(peer(A), key("old"))),
                IdempotencyRefusal::Forgotten,
                "and an eviction still says so, or the two names have merged"
            );
        }

        /// The counter side, and the one that says which pool the key landed in. A
        /// retention settle is a **tombstone**, bounded by [`MAX_TOMBSTONES`] rather
        /// than [`MAX_RECORDS`] — the right pool, because a tombstone is what keeps
        /// the work from running twice and has to outlive the reply it never had.
        ///
        /// Made red by `scripts/break-once.sh` replacing
        /// `Guard::record`'s `recordable` flag to
        /// ignore the retention switch, its `matches!` replaced by `true`. Watched: `assertion `left == right` failed: a key
        /// with no reply belongs in the tombstone pool, not the record pool`,
        /// `left: 1`, `right: 0`.
        #[test]
        fn a_retention_settle_is_a_tombstone_not_a_record() {
            let ledger = recordless();
            guard(ledger.claim(peer(A), key("k"))).record(&frame(16), false);

            let inner = ledger.lock();
            assert_eq!(
                inner.held, 0,
                "a key with no reply belongs in the tombstone pool, not the record pool"
            );
            assert_eq!(inner.spent, 1, "and it does belong in the tombstone pool");
        }

        /// **The cost this mode is not free of**, and the reason `Records`'s doc
        /// carries a ratio rather than the words "does not affect the key state".
        ///
        /// Under `Records::Memory` a settled key occupies a record slot and *then*
        /// becomes a tombstone, so two pools of churn have to pass before its
        /// tombstone is dropped and the key reads `Fresh` again. Under
        /// `Records::None` it enters the tombstone pool directly and forgoes the
        /// record pool's grace, so the at-most-once window is shorter — and
        /// `README.md`'s limit 4 is explicit that a key going `Fresh` again is the
        /// one degradation a caller **cannot detect**.
        ///
        /// Two ledgers, one bound, one difference. With `records: 4` the memory
        /// ledger's oldest key is still a tombstone after four more settles, because
        /// four of the eight it saw are holding records; the recordless one has
        /// spent all eight tombstones and dropped it.
        ///
        /// Made red by `scripts/break-once.sh` replacing
        /// `Guard::record`'s `recordable` flag to
        /// `false` — its `matches!` replaced by `false` — which makes both ledgers
        /// recordless. Watched: `panicked at .., under Memory the oldest key must
        /// still be remembered`, `0 passed; 1 failed; 145 filtered out`. It is the
        /// FIRST assertion that fires, not the second, and that is the right way
        /// round: with both ledgers recordless the two arms agree, and the arm that
        /// has to notice is the one claiming the longer window still exists.
        #[test]
        fn records_none_shortens_the_at_most_once_window() {
            let bounds = Limits {
                records: 4,
                tombstones: 4,
                ..Limits::default()
            };
            let settle_eight = |ledger: &Arc<Ledger>| {
                for n in 0..8 {
                    guard(ledger.claim(peer(A), key(&format!("k{n}")))).record(&frame(8), false);
                }
            };

            let recording = Ledger::with_limits(bounds, Records::Memory);
            settle_eight(&recording);
            let recordless = Ledger::with_limits(bounds, Records::None);
            settle_eight(&recordless);

            assert!(
                !matches!(recording.claim(peer(A), key("k0")), Claim::Fresh(_)),
                "under Memory the oldest key must still be remembered"
            );
            assert!(
                matches!(recordless.claim(peer(A), key("k0")), Claim::Fresh(_)),
                "a record pool the key passes through first is what buys the longer window"
            );
        }

        /// The state query agrees with the claim, for free: `Slot::state` maps this
        /// slot to `KeyState::Unrecordable`, so a caller that asks instead of
        /// retrying is told the same story.
        ///
        /// That is the argument against minting a variant for this: a new refusal
        /// would have left `key_state` answering the old one until `KeyState` gained
        /// a variant too — two names on two enums to say what this one says.
        ///
        /// Made red by `scripts/break-once.sh` replacing
        /// `Guard::record`'s settled slot to
        /// `Slot::Interrupted` —
        /// the other spent state, and the tempting one, since nothing was recorded.
        /// Watched: `assertion `left == right` failed`, `left: Interrupted`,
        /// `right: Unrecordable`. It is the wrong answer: `Interrupted` means whether
        /// the work took effect is unknown, and here it is known to have completed.
        #[test]
        fn a_retention_settle_reports_the_same_state_it_refuses_with() {
            let ledger = recordless();
            guard(ledger.claim(peer(A), key("k"))).record(&frame(16), false);

            assert_eq!(ledger.key_state(peer(A), &key("k")), KeyState::Unrecordable);
        }

        /// `Records::None` must not touch the paths that decide whether the work
        /// ran. A guard dropped without a reply is still `Interrupted`, and one
        /// dropped with nothing dispatched is still claimable — the two halves of
        /// RFC-05 §2, asked of a ledger that records no replies.
        ///
        /// Made red by `scripts/break-once.sh` replacing
        /// `if self.progress.as_ref().map(Progress::reached) == Some(Reached::NotDispatched) {`
        /// with `if false {`. Watched: `panicked at .., a key nothing ran under must
        /// still be claimable`, `0 passed; 1 failed; 144 filtered out`.
        #[test]
        fn records_none_changes_nothing_about_who_settles() {
            let ledger = recordless();
            drop(guard(ledger.claim(peer(A), key("entered"))));
            assert_eq!(
                refusal(ledger.claim(peer(A), key("entered"))),
                IdempotencyRefusal::Interrupted,
                "entered and unfinished is still unknown"
            );

            drop(guard(ledger.claim(peer(A), key("nothing"))).watching(Progress::new()));
            assert!(
                matches!(ledger.claim(peer(A), key("nothing")), Claim::Fresh(_)),
                "a key nothing ran under must still be claimable"
            );
        }

        /// `Memory` is the behaviour that existed before the type did, which is the
        /// half of invariant 8 this module can hold.
        ///
        /// The other half — that a service naming no policy *gets* `Memory` — is not
        /// assertable here, because there is no `Records::default()` to ask. That is
        /// deliberate: the default lives in `ServiceBuilder::new` so nobody reaches a
        /// ten-minute retention window by leaving a field out, the same argument
        /// `Allowlist`'s doc makes about `anyone()`. It is held over a real
        /// connection instead, by
        /// `tests/idempotency.rs::the_defaults_reproduce_todays_behaviour`.
        ///
        /// Made red by `scripts/break-once.sh` replacing
        /// `Guard::record`'s `recordable` flag to
        /// `false`, its `matches!` replaced by `false`. Watched: `panicked at .., expected a replay, got
        /// Refused(Idempotency(Unrecordable))`, from this module's `recorded(..)`
        /// helper.
        #[test]
        fn memory_records_and_replays() {
            let ledger = Ledger::new(Records::Memory);
            guard(ledger.claim(peer(A), key("k"))).record(&frame(16), false);
            assert_eq!(
                recorded(ledger.claim(peer(A), key("k"))),
                frame(16),
                "`Memory` must replay, which is what this crate did before the type"
            );
        }
    }

    mod claim {
        use super::*;

        #[test]
        fn an_unused_key_is_fresh() {
            let ledger = ledger();
            assert!(matches!(
                ledger.claim(peer(A), key("first")),
                Claim::Fresh(_)
            ));
        }

        #[test]
        fn a_recorded_key_replays_the_same_bytes() {
            let ledger = ledger();
            guard(ledger.claim(peer(A), key("k"))).record(&frame(16), false);

            assert_eq!(recorded(ledger.claim(peer(A), key("k"))), frame(16));
        }

        /// Byte for byte, not value for value. The property is asserted in three
        /// doc comments and, until this test, by nothing executable — and it is
        /// the one an over-the-wire test cannot check, because postcard is
        /// canonical: a decode-and-re-encode produces the same bytes, so the
        /// wire cannot tell a replayed frame from a rebuilt one.
        ///
        /// It is why the ledger holds an encoded frame rather than a typed
        /// reply, and why a design that made the ledger a `Middleware` was
        /// rejected: a layer sees `(Outcome<Resp>, Body)`, so it would have to
        /// decode and re-encode, and this test is what would stop being true.
        #[test]
        fn a_replayed_reply_is_byte_identical() {
            let ledger = ledger();
            // Deliberately not a run of one repeated byte: a frame that is
            // uniform cannot show a re-encoding that reordered anything.
            let original = Bytes::from((0u8..=255).cycle().take(777).collect::<Vec<u8>>());
            guard(ledger.claim(peer(A), key("k"))).record(&original, false);

            let replayed = recorded(ledger.claim(peer(A), key("k")));
            assert_eq!(
                replayed, original,
                "the recorded frame must be written back verbatim"
            );
            assert_eq!(replayed.len(), 777);
        }

        #[test]
        fn a_different_key_is_fresh_even_after_one_is_recorded() {
            let ledger = ledger();
            guard(ledger.claim(peer(A), key("one"))).record(&frame(16), false);

            assert!(matches!(ledger.claim(peer(A), key("two")), Claim::Fresh(_)));
        }

        /// The property that keeps one peer from reading another's reply by
        /// guessing a key. If this ever goes green with a shared namespace, the
        /// ledger has become an information leak.
        #[test]
        fn two_peers_using_the_same_key_do_not_collide() {
            let ledger = ledger();
            guard(ledger.claim(peer(A), key("shared"))).record(&frame(16), false);

            assert!(
                matches!(ledger.claim(peer(B), key("shared")), Claim::Fresh(_)),
                "peer B must not be answered from peer A's record"
            );
        }

        #[test]
        fn a_second_claim_while_the_first_runs_is_refused_as_in_flight() {
            let ledger = ledger();
            let _running = guard(ledger.claim(peer(A), key("k")));

            assert_eq!(
                refusal(ledger.claim(peer(A), key("k"))),
                IdempotencyRefusal::InFlight
            );
        }

        /// A handler that was entered and produced nothing leaves uncertainty,
        /// not a vacant key. Re-running it is the double booking this module
        /// exists to prevent.
        #[test]
        fn a_guard_dropped_without_a_reply_leaves_the_key_interrupted() {
            let ledger = ledger();
            drop(guard(ledger.claim(peer(A), key("k"))));

            assert_eq!(
                refusal(ledger.claim(peer(A), key("k"))),
                IdempotencyRefusal::Interrupted
            );
        }

        /// The other half of the same rule, and the one that was missing: a key
        /// nothing ever entered goes back to vacant, so the caller's retry is a
        /// first attempt rather than a permanent refusal.
        #[test]
        fn a_guard_dropped_with_nothing_dispatched_leaves_the_key_claimable() {
            let ledger = ledger();
            // A fresh marker says `NotDispatched`, which is what a layer that
            // refused without calling through leaves behind.
            drop(guard(ledger.claim(peer(A), key("k"))).watching(Progress::new()));

            assert!(
                matches!(ledger.claim(peer(A), key("k")), Claim::Fresh(_)),
                "a key nothing ran under must be claimable again"
            );
        }

        /// The other side of the same marker: entered, and no reply. Settling
        /// this as vacant would let the work run twice.
        #[test]
        fn a_guard_dropped_after_dispatch_leaves_the_key_interrupted() {
            let ledger = ledger();
            let progress = Progress::new();
            progress.dispatched();
            drop(guard(ledger.claim(peer(A), key("k"))).watching(progress));

            assert_eq!(
                refusal(ledger.claim(peer(A), key("k"))),
                IdempotencyRefusal::Interrupted
            );
        }

        #[test]
        fn a_reply_that_carries_a_body_is_not_replayed() {
            let ledger = ledger();
            guard(ledger.claim(peer(A), key("k"))).record(&frame(16), true);

            assert_eq!(
                refusal(ledger.claim(peer(A), key("k"))),
                IdempotencyRefusal::Unrecordable
            );
        }

        /// One reply larger than the whole byte budget cannot be evicted into
        /// fitting, so it must never be stored in the first place.
        #[test]
        fn a_reply_larger_than_the_byte_budget_is_not_replayed() {
            let ledger = bounded(Limits {
                bytes: 64,
                ..Limits::default()
            });
            guard(ledger.claim(peer(A), key("k"))).record(&frame(65), false);

            assert_eq!(
                refusal(ledger.claim(peer(A), key("k"))),
                IdempotencyRefusal::Unrecordable
            );
        }
    }

    mod bounds {
        use super::*;

        #[test]
        fn the_record_count_stays_inside_its_bound() {
            let ledger = bounded(holding(4));
            for n in 0..64 {
                guard(ledger.claim(peer(A), key(&format!("k{n}")))).record(&frame(8), false);
            }

            assert_eq!(ledger.lock().held, 4);
        }

        #[test]
        fn the_recorded_bytes_stay_inside_their_bound() {
            let ledger = bounded(Limits {
                bytes: 64,
                ..Limits::default()
            });
            for n in 0..64 {
                guard(ledger.claim(peer(A), key(&format!("k{n}")))).record(&frame(16), false);
            }

            assert!(
                ledger.lock().bytes <= 64,
                "held {} bytes over a 64 byte budget",
                ledger.lock().bytes
            );
        }

        /// A peer that floods the ledger must evict itself, never a quiet peer.
        /// A global oldest-first policy passes every other test in this module
        /// and fails this one.
        #[test]
        fn a_flooding_peer_cannot_evict_a_quiet_peers_record() {
            let ledger = bounded(holding(4));
            guard(ledger.claim(peer(B), key("quiet"))).record(&frame(8), false);
            for n in 0..64 {
                guard(ledger.claim(peer(A), key(&format!("k{n}")))).record(&frame(8), false);
            }

            assert_eq!(
                recorded(ledger.claim(peer(B), key("quiet"))),
                frame(8),
                "the quiet peer's record must survive the flood"
            );
        }

        /// Evicting a running slot would let a concurrent duplicate into the
        /// handler, which is the one thing the ledger must never do.
        #[test]
        fn a_running_claim_is_never_evicted() {
            let ledger = bounded(holding(1));
            let _running = guard(ledger.claim(peer(A), key("running")));
            for n in 0..64 {
                guard(ledger.claim(peer(A), key(&format!("k{n}")))).record(&frame(8), false);
            }

            assert_eq!(
                refusal(ledger.claim(peer(A), key("running"))),
                IdempotencyRefusal::InFlight,
                "the in-flight claim must outlive every settled record"
            );
        }

        /// The defect this replaced: an evicted key used to be vacant, so the
        /// work ran a second time and nobody could tell.
        #[test]
        fn an_evicted_key_is_refused_not_rerun() {
            let ledger = bounded(holding(2));
            guard(ledger.claim(peer(A), key("old"))).record(&frame(8), false);
            guard(ledger.claim(peer(A), key("new"))).record(&frame(8), false);
            guard(ledger.claim(peer(A), key("newest"))).record(&frame(8), false);

            assert_eq!(
                refusal(ledger.claim(peer(A), key("old"))),
                IdempotencyRefusal::Forgotten,
                "the oldest record must leave a tombstone, not a vacancy"
            );
        }

        /// Tombstones are cheap, not free. Past their own bound the key really
        /// is vacant again, and `KeyState::Fresh` is what says so.
        #[test]
        fn a_tombstone_is_dropped_once_its_own_bound_is_reached() {
            let ledger = bounded(Limits {
                records: 1,
                tombstones: 2,
                ..Limits::default()
            });
            for n in 0..8 {
                guard(ledger.claim(peer(A), key(&format!("k{n}")))).record(&frame(8), false);
            }

            assert!(
                matches!(ledger.claim(peer(A), key("k0")), Claim::Fresh(_)),
                "the oldest tombstone must be the one that left"
            );
            assert!(ledger.lock().spent <= 2);
        }

        #[test]
        fn running_claims_are_bounded_per_peer() {
            let ledger = bounded(Limits {
                running_per_peer: 2,
                ..Limits::default()
            });
            let _one = guard(ledger.claim(peer(A), key("a")));
            let _two = guard(ledger.claim(peer(A), key("b")));

            assert_eq!(
                refusal(ledger.claim(peer(A), key("c"))),
                IdempotencyRefusal::TooManyRunning { limit: 2 }
            );
            assert!(
                matches!(ledger.claim(peer(B), key("c")), Claim::Fresh(_)),
                "one peer's flood must not close the door on another"
            );
        }

        #[test]
        fn running_claims_are_bounded_in_total() {
            let ledger = bounded(Limits {
                running: 2,
                ..Limits::default()
            });
            let _one = guard(ledger.claim(peer(A), key("a")));
            let _two = guard(ledger.claim(peer(B), key("b")));

            assert_eq!(
                refusal(ledger.claim(peer(B), key("c"))),
                IdempotencyRefusal::TooManyRunning { limit: 2 }
            );
        }

        /// The bound the running caps cannot supply: an attaching call holds no
        /// claim, so one claimed key and enough connections parks unboundedly
        /// many tasks and stream pairs and meets neither cap.
        #[test]
        fn waiting_attachers_are_bounded_per_peer() {
            let ledger = bounded(Limits {
                waiting_per_peer: 2,
                ..Limits::default()
            });
            let _one = ledger.wait(peer(A)).expect("the first place is free");
            let _two = ledger.wait(peer(A)).expect("and so is the second");

            assert_eq!(
                waited(ledger.wait(peer(A))),
                IdempotencyRefusal::TooManyRunning { limit: 2 }
            );
            assert!(
                ledger.wait(peer(B)).is_ok(),
                "one peer's attachers must not close the door on another"
            );
        }

        #[test]
        fn waiting_attachers_are_bounded_in_total() {
            let ledger = bounded(Limits {
                waiting: 2,
                ..Limits::default()
            });
            let _one = ledger.wait(peer(A)).expect("free");
            let _two = ledger.wait(peer(B)).expect("free");

            assert_eq!(
                waited(ledger.wait(peer(B))),
                IdempotencyRefusal::TooManyRunning { limit: 2 }
            );
        }

        /// The RAII half, and the one that decides whether the cap is a bound or
        /// a fuse. A waiter usually stops waiting because its task was aborted;
        /// if the place did not come back, the cap would be reached once and
        /// then refuse every attach for as long as the process lives, with
        /// nobody waiting.
        #[test]
        fn a_dropped_waiter_gives_its_place_back() {
            let ledger = bounded(Limits {
                waiting_per_peer: 1,
                ..Limits::default()
            });
            let first = ledger.wait(peer(A)).expect("free");
            assert_eq!(
                waited(ledger.wait(peer(A))),
                IdempotencyRefusal::TooManyRunning { limit: 1 }
            );

            drop(first);

            assert!(
                ledger.wait(peer(A)).is_ok(),
                "the place must come back when the waiter does not"
            );
        }
    }

    mod ttl {
        use super::*;

        /// `tokio::time::pause` rather than a sleep, because a wall-clock
        /// assertion is a flake waiting for a loaded CI box. `n0_future::time`
        /// re-exports tokio's clock on native, so pausing tokio pauses this.
        #[tokio::test(start_paused = true)]
        async fn a_record_expires_after_its_ttl() {
            let ledger = bounded(Limits {
                ttl: Duration::from_secs(60),
                ..Limits::default()
            });
            guard(ledger.claim(peer(A), key("k"))).record(&frame(8), false);
            assert_eq!(recorded(ledger.claim(peer(A), key("k"))), frame(8));

            tokio::time::advance(Duration::from_secs(61)).await;

            assert_eq!(
                refusal(ledger.claim(peer(A), key("k"))),
                IdempotencyRefusal::Forgotten,
                "an expired record must leave a tombstone, not its reply"
            );
            assert_eq!(ledger.lock().bytes, 0, "the reply's bytes must be freed");
        }

        #[tokio::test(start_paused = true)]
        async fn a_running_claim_does_not_expire() {
            let ledger = bounded(Limits {
                ttl: Duration::from_secs(60),
                ..Limits::default()
            });
            let _running = guard(ledger.claim(peer(A), key("k")));

            tokio::time::advance(Duration::from_secs(600)).await;

            assert_eq!(
                refusal(ledger.claim(peer(A), key("k"))),
                IdempotencyRefusal::InFlight,
                "a claim with no reply yet has no deadline but the transport's"
            );
        }
    }

    mod state {
        use super::*;

        #[test]
        fn key_state_reports_every_state() {
            let ledger = bounded(holding(1));
            assert_eq!(ledger.key_state(peer(A), &key("never")), KeyState::Fresh);

            let running = guard(ledger.claim(peer(A), key("running")));
            assert_eq!(
                ledger.key_state(peer(A), &key("running")),
                KeyState::Running
            );
            drop(running);
            assert_eq!(
                ledger.key_state(peer(A), &key("running")),
                KeyState::Interrupted
            );

            guard(ledger.claim(peer(A), key("body"))).record(&frame(8), true);
            assert_eq!(
                ledger.key_state(peer(A), &key("body")),
                KeyState::Unrecordable
            );

            guard(ledger.claim(peer(A), key("kept"))).record(&frame(8), false);
            assert_eq!(ledger.key_state(peer(A), &key("kept")), KeyState::Done);

            // One record fits, so the second recording evicts the first.
            guard(ledger.claim(peer(A), key("later"))).record(&frame(8), false);
            assert_eq!(ledger.key_state(peer(A), &key("kept")), KeyState::Forgotten);
        }

        #[test]
        fn key_state_is_peer_scoped() {
            let ledger = ledger();
            guard(ledger.claim(peer(A), key("mine"))).record(&frame(8), false);

            assert_eq!(ledger.key_state(peer(A), &key("mine")), KeyState::Done);
            assert_eq!(
                ledger.key_state(peer(B), &key("mine")),
                KeyState::Fresh,
                "one peer must not learn that another used a key"
            );
        }
    }

    /// The counters and the maps must agree, on every path.
    ///
    /// `Inner` tracks `held`, `bytes`, `spent` and `running` beside per-bucket
    /// queues, and every one of `claim`, `settle`, `release`, `expire` and
    /// `evict` moves both halves. A counter that drifts below the truth makes
    /// `evict` stop firing at the bound it exists to enforce, and the ledger
    /// grows without limit while every other test stays green — which is
    /// exactly the failure the exhaustive `match` in `held_bytes` was written to
    /// prevent, one level up. Nothing checked the arithmetic itself until this.
    mod consistency {
        use super::*;

        /// Recompute every counter from the maps and compare. The maps are the
        /// truth; the counters are a cache of them.
        ///
        /// The waiter count is the one thing here that is *not* recomputable
        /// from the maps: a place to wait in is held by a `Waiting` living on
        /// some other task's stack, and `Inner` records how many there are
        /// rather than which. So this checks the two counts against each other
        /// and against the pruning rule, and
        /// `the_waiting_count_follows_the_live_guards` is what checks them
        /// against the guards themselves.
        fn assert_agrees(ledger: &Ledger, whereabouts: &str) {
            let inner = ledger.lock();
            let mut held = 0;
            let mut bytes = 0;
            let mut spent = 0;
            let mut running = 0;
            let mut waiting = 0;
            for count in inner.waiters.values() {
                assert_ne!(
                    *count, 0,
                    "{whereabouts}: a peer with no waiters must be removed from the map, \
                     not left in it at zero"
                );
                waiting += *count;
            }
            assert_eq!(inner.waiting, waiting, "{whereabouts}: waiting");
            for bucket in inner.peers.values() {
                // Fix 1's guard. A bucket is created by the claim that inserts
                // the first slot and removed by whatever takes the last one
                // out, so an empty one is a peer nothing removes — unbounded,
                // because `Allowlist::Anyone` makes the peer count a property
                // of the network.
                assert!(
                    !bucket.slots.is_empty(),
                    "{whereabouts}: an empty bucket must be removed, not left behind"
                );
                assert_eq!(
                    bucket.held.len() + bucket.spent.len() + bucket.running,
                    bucket.slots.len(),
                    "{whereabouts}: a bucket's queues plus its running claims must \
                     account for every slot it holds"
                );
                held += bucket.held.len();
                spent += bucket.spent.len();
                running += bucket.running;
                for entry in bucket.slots.values() {
                    bytes += entry.slot.held_bytes();
                }
            }
            assert_eq!(inner.held, held, "{whereabouts}: held");
            assert_eq!(inner.bytes, bytes, "{whereabouts}: bytes");
            assert_eq!(inner.spent, spent, "{whereabouts}: spent");
            assert_eq!(inner.running, running, "{whereabouts}: running");
        }

        /// Every settled path, mixed, against tight bounds so eviction and the
        /// tombstone pool both fire many times rather than never.
        #[test]
        fn the_counters_agree_with_the_maps_on_every_path() {
            let ledger = bounded(Limits {
                records: 3,
                bytes: 64,
                tombstones: 4,
                ..Limits::default()
            });
            assert_agrees(&ledger, "empty");

            // Held across the whole loop, so every probe below runs with a live
            // waiter for a peer whose bucket comes and goes underneath it. That
            // is the reason the waiter counts live beside `peers` rather than
            // inside a `Bucket`: the bucket is removed when its last slot goes,
            // and the waiter is still waiting.
            let _waiter = ledger.wait(peer(A)).expect("a free place");

            for n in 0..40u8 {
                let k = key(&format!("k{n}"));
                let p = if n % 3 == 0 { peer(B) } else { peer(A) };
                match n % 5 {
                    // recorded, sometimes too large to hold
                    0 => guard(ledger.claim(p, k)).record(&frame(usize::from(n) + 1), false),
                    // unrecordable
                    1 => guard(ledger.claim(p, k)).record(&frame(8), true),
                    // interrupted, by dropping an unwatched guard
                    2 => drop(guard(ledger.claim(p, k))),
                    // released, because nothing was dispatched
                    3 => drop(guard(ledger.claim(p, k)).watching(Progress::new())),
                    // claimed and left running for a while
                    _ => {
                        let running = guard(ledger.claim(p, k.clone()));
                        assert_agrees(&ledger, "while a claim is running");
                        running.record(&frame(4), false);
                    }
                }
                assert_agrees(&ledger, &format!("after n={n}"));
            }

            // And a re-claim of an evicted key, which is the path that both
            // removes a tombstone and adds a record in one call.
            for n in 0..20u8 {
                guard(ledger.claim(peer(A), key(&format!("k{n}")))).record(&frame(2), false);
                assert_agrees(&ledger, &format!("after re-claiming k{n}"));
            }
        }

        /// The TTL path moves entries between the two pools without going
        /// through `settle`, so it gets its own check.
        #[tokio::test(start_paused = true)]
        async fn the_counters_agree_after_records_expire() {
            let ledger = bounded(Limits {
                ttl: Duration::from_secs(60),
                ..Limits::default()
            });
            for n in 0..10u8 {
                guard(ledger.claim(peer(A), key(&format!("k{n}")))).record(&frame(8), false);
            }
            assert_agrees(&ledger, "before the ttl");

            tokio::time::advance(Duration::from_secs(61)).await;
            // `expire` is lazy, so a claim is what drives it.
            let _ = ledger.claim(peer(A), key("wake"));
            assert_agrees(&ledger, "after the ttl");
            assert_eq!(ledger.lock().bytes, 0, "expired records free their bytes");
        }

        /// Fix 1's own case, and the only path that reached it: the GLOBAL cap
        /// refuses a peer the ledger has never seen. Taking the bucket with
        /// `entry(peer).or_default()` before the check meant the refusal itself
        /// introduced the peer, and nothing ever removed it — `release` and
        /// `evict` prune only a bucket they took the last slot out of, and this
        /// one never had one.
        #[test]
        fn a_claim_the_global_cap_refuses_leaves_no_bucket_behind() {
            let ledger = bounded(Limits {
                running: 1,
                ..Limits::default()
            });
            let _running = guard(ledger.claim(peer(A), key("a")));

            assert_eq!(
                refusal(ledger.claim(peer(B), key("b"))),
                IdempotencyRefusal::TooManyRunning { limit: 1 }
            );
            // The probe is the assertion. A second, narrower one here would
            // never be the one that fires, which makes it decoration.
            assert_agrees(&ledger, "after a globally refused claim");
        }

        /// The waiter counts are what `assert_agrees` cannot recompute — a
        /// place to wait in is a value on the waiting task's stack, and `Inner`
        /// records how many exist rather than which — so they are checked
        /// against the guards themselves here.
        #[test]
        fn the_waiting_count_follows_the_live_guards() {
            let ledger = ledger();
            let mut places = Vec::new();
            for _ in 0..4 {
                places.push(ledger.wait(peer(A)).expect("free"));
                places.push(ledger.wait(peer(B)).expect("free"));
                assert_agrees(&ledger, "while places are being taken");
            }
            {
                let inner = ledger.lock();
                assert_eq!(inner.waiting, 8);
                assert_eq!(inner.waiters.get(&peer(A)), Some(&4));
                assert_eq!(inner.waiters.get(&peer(B)), Some(&4));
            }

            while let Some(place) = places.pop() {
                let live = places.len();
                drop(place);
                assert_eq!(
                    ledger.lock().waiting,
                    live,
                    "the count must be one per guard still alive"
                );
                assert_agrees(&ledger, "while places are being given back");
            }

            assert!(
                ledger.lock().waiters.is_empty(),
                "the last waiter to leave must take its map entry with it"
            );
        }
    }

    mod attach {
        use super::*;

        /// The wakeup, end to end: a second caller waits on the running slot and
        /// is answered from the first caller's record.
        #[tokio::test]
        async fn a_waiter_is_woken_when_the_slot_settles() {
            let ledger = ledger();
            let running = guard(ledger.claim(peer(A), key("k")));

            let Claim::Running(notify) = ledger.claim(peer(A), key("k")) else {
                panic!("expected a running slot");
            };

            // Registered BEFORE the settle, which is the hazard RFC-04 §3.3
            // named: `notify_waiters` wakes only the waiters already there.
            let notified = notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();

            running.record(&frame(16), false);
            notified.await;

            assert_eq!(recorded(ledger.claim(peer(A), key("k"))), frame(16));
        }

        /// The half that made §3.3 right to worry: a waiter that registers after
        /// the settle would sleep forever, so `enable()` is not decoration.
        #[tokio::test]
        async fn a_waiter_that_registers_late_still_sees_the_settled_slot() {
            let ledger = ledger();
            let running = guard(ledger.claim(peer(A), key("k")));
            let Claim::Running(notify) = ledger.claim(peer(A), key("k")) else {
                panic!("expected a running slot");
            };

            running.record(&frame(16), false);

            // No `enable()` before the settle, so this wakeup is genuinely lost.
            // The re-claim under the lock is what makes that survivable, and it
            // is why the accept path re-reads the slot before it awaits.
            assert_eq!(recorded(ledger.claim(peer(A), key("k"))), frame(16));
            drop(notify);
        }

        #[test]
        fn a_budget_is_capped_at_max_attach() {
            let idem = Idempotency::attaching_for(key("k"), Duration::from_secs(3600));
            assert_eq!(idem.attach_budget(), Some(MAX_ATTACH));
        }

        #[test]
        fn a_bare_key_does_not_attach() {
            let idem: Idempotency = key("k").into();
            assert_eq!(idem.attach_budget(), None);
            assert_eq!(idem.key(), &key("k"));
        }
    }
}
