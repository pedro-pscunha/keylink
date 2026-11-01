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
//! call that is **cut off** after dispatch, because its connection ended or its
//! server shut down while an actor turn was running or already queued to run,
//! settles the key as [`Interrupted`](IdempotencyRefusal::Interrupted): whether
//! the work landed is genuinely not known from there.
//!
//! A [deadline](crate::ServiceBuilder::deadline) is not a cut-off. It answers the
//! caller and goes on waiting for the call, so the key settles as what the call
//! really returned and a retry is handed that answer.
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

use std::collections::{BTreeSet, HashMap, VecDeque};
use std::fmt;
use std::num::NonZeroUsize;
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

/// Recorded reply bytes the ledger holds **in total, across every peer**, before
/// it starts evicting.
///
/// A record is a reply frame, which the ledger caps at [`MAX_RECORDED_FRAME`] — a
/// megabyte each. Counting entries alone would therefore bound the ledger at a
/// gigabyte, so the bytes are counted too.
///
/// The cap named here is the **ledger's**, not the wire's. They hold the same value
/// today and are not the same bound: [`MAX_HEADER`](crate::MAX_HEADER) bounds a
/// header frame's payload on the way out, and this pool bounds what is kept
/// afterwards.
///
/// This is the pool and nothing else. What one reply may cost on its own is
/// [`MAX_RECORDED_FRAME`], a separate number. Until the two were split this name
/// carried both duties, and its own doc read as a per-key figure to anyone who
/// came to it from [`MAX_RECORDS`].
pub const MAX_RECORDED_BYTES: usize = 1 << 20;

/// The largest reply frame the ledger will record, length prefix included.
///
/// [`MAX_RECORDED_BYTES`] bounds the pool; this bounds one entry in it. A reply
/// over this is answered normally and then refused on the retry as
/// [`IdempotencyRefusal::Unrecordable`] — a permanent property of the reply, which
/// is what makes it different from eviction.
///
/// **This must never exceed [`MAX_RECORDED_BYTES`], and the ceiling equalling the
/// wire's cap is what keeps it inside.** A frame larger than the whole pool cannot
/// be held no matter what else the ledger is doing, so refusing it is a permanent
/// property and [`IdempotencyRefusal::Unrecordable`] is the truthful answer. Admit
/// it instead and `evict` downgrades it in the same lock, which reports an
/// **eviction** for something that was never storable — the merge RFC-05 §5 says
/// *"would make the query lie about why"*.
/// `the_limits_the_readme_names_are_the_ones_the_ledger_uses` asserts the relation,
/// because nothing else related the two numbers and their first values inverted it.
///
/// **The four bytes, which are a real fact about the wire and not this number.**
/// [`MAX_HEADER`](crate::MAX_HEADER) bounds the **postcard payload**:
/// `encode_header` computes `buf.len() - PREFIX` and checks that. It then returns
/// the whole buffer, so the ledger stores `payload + PREFIX`. A maximal payload is
/// therefore a **1048580-byte frame**, four bytes over the 1 MiB pool — so at
/// today's pool those four sizes are permanently unrecordable, and this ceiling
/// says so by stopping at the payload cap. Setting it to `MAX_HEADER + PREFIX`
/// would admit exactly those four and answer `Forgotten`.
///
/// **When the pool rises the answer changes**, which is why RFC-08 §8 holds the two
/// together: an 8 MiB pool can hold a maximal frame, refusing it becomes wrong, and
/// `MAX_HEADER + PREFIX` becomes the truthful never-rejects ceiling. **The split is
/// sound only once the pool can hold one maximal frame.**
///
/// A service lowers it with
/// [`Limits::frame_bytes`](crate::Limits::frame_bytes) to stop one large reply
/// from evicting many small ones. There is no floor to state: the smallest useful
/// value is the postcard encoding of that service's own response type, which
/// differs per service and has no name here. So any non-zero value can still
/// refuse every reply of some response type.
pub const MAX_RECORDED_FRAME: usize = crate::wire::MAX_HEADER as usize;

/// The invariant that relates the two byte bounds, enforced at **compile time**.
///
/// A ceiling above the pool admits a frame that cannot be stored, so `Guard::record`
/// writes `Done`, `evict` downgrades it in the same lock, and the retry is told
/// `Forgotten` — an eviction — for a reply that was never storable. RFC-05 §5
/// refuses that merge: it *"would make the query lie about why"*.
///
/// **A `const` block rather than an assertion in a test, and clippy is the reason
/// the stronger form got used.** The first version of this guard was
/// `assert!(MAX_RECORDED_FRAME <= MAX_RECORDED_BYTES)` inside
/// `the_limits_the_readme_names_are_the_ones_the_ledger_uses`, and
/// `clippy::assertions_on_constants` rejected it: *"this assertion has a constant
/// value"*. That lint is right, and the remedy is not to silence it — both operands
/// are `const`, so the relation can be checked when the crate is built instead of
/// when a test is run. **The violation is now unbuildable rather than merely
/// caught**, and no test can be skipped past it.
///
/// The message is a plain literal because a `const` panic cannot format arguments,
/// so the two values are named rather than printed.
///
/// Made red by raising [`MAX_RECORDED_FRAME`] to `crate::wire::MAX_HEADER as usize +
/// crate::wire::PREFIX`. Watched, on `cargo check -p keylink`, and the whole message
/// comes through rather than a truncation: `error[E0080]: evaluation panicked:
/// MAX_RECORDED_FRAME exceeds MAX_RECORDED_BYTES: a frame ceiling above the whole
/// pool admits a reply that cannot be stored, so evict answers Forgotten for
/// something permanently unrecordable`, pointing at the `const _: () = assert!(`
/// line below.
const _: () = assert!(
    MAX_RECORDED_FRAME <= MAX_RECORDED_BYTES,
    "MAX_RECORDED_FRAME exceeds MAX_RECORDED_BYTES: a frame ceiling above the whole \
     pool admits a reply that cannot be stored, so evict answers Forgotten for \
     something permanently unrecordable"
);

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

/// The longest an attaching call is held waiting for one already in flight,
/// **unless the serving side sets [`Limits::attach`]**, whose default this is.
///
/// A caller names its own budget in [`Idempotency::attaching_for`] and the
/// serving side caps it, because the budget arrives from the network and a peer
/// that asks to be held for a year is asking us to hold a QUIC stream for a
/// year. That the ceiling is the *serving* side's is why it is configurable:
/// the cost being bounded is the server's task and its stream pair.
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
/// The per-peer cap alone is not a bound: [`Allowlist::Anyone`](crate::Allowlist::Anyone)
/// makes the number of peers a property of the network rather than of the
/// configuration.
pub const MAX_RUNNING: usize = 4096;

/// Attaching calls one peer may have parked at once, waiting on a call already
/// in flight.
///
/// A parked attacher holds no claim, so neither running cap can see it or the
/// QUIC stream pair it is holding open: one claimed key plus 40 connections of
/// 100 streams each parks 4000 tasks for [`MAX_ATTACH`] and meets no bound.
/// **The count and the duration are the two factors of that product**, and both are
/// the serving side's to set: the count through [`Limits::waiting_per_peer`], which
/// defaults to this constant, and the duration through [`Limits::attach`]. This is a
/// `const` and sets nothing by itself — a distinction this crate had just finished
/// removing from [`MAX_ATTACH`]'s own doc when it reappeared here.
/// Same number as [`MAX_RUNNING_PER_PEER`], because that is also the legitimate
/// ceiling — a [`Peer`](crate::Peer) re-issue produces at most one attacher per
/// running claim, and running claims are capped there.
pub const MAX_WAITING_PER_PEER: usize = 128;

/// Attaching calls the whole ledger may have parked at once.
///
/// Two levels for the reason the running pair has two:
/// [`Allowlist::Anyone`](crate::Allowlist::Anyone) makes the peer count a property of
/// the network. A quarter of [`MAX_RUNNING`] rather than the same number,
/// because a waiter produces nothing — it costs a task and a stream pair, and
/// it may still be refused when its budget runs out — so the node spends the
/// smaller share of its budget on calls it has already decided not to run.
pub const MAX_WAITING: usize = 1024;

/// How many queued disk reconciliations one claim pays for.
///
/// Each is an `fsync`, so this is a latency ceiling on the claim path and not a
/// tuning knob: the queue still drains, over as many claims as it takes.
///
/// **What is measured, and what could not be.** The fixture is a 1500-record
/// store reopened at `pool_records(64)`, where `Ledger::new` queues 2616
/// entries and the first keyed call used to pay for all of them — **8770 ms**.
///
/// The drain **clears the backlog** and that number is exact: at 8 it takes 7
/// entries off the queue per call after the reconcile, so 2616 of them are gone
/// in about **374 keyed calls**, against the 2000 this was accepted at. At 12 it
/// was 11 per call and about 240.
///
/// The **latency of the one call that pays is inconclusive on this machine**,
/// and it is reported that way rather than as a midpoint. Four runs at each
/// value, first call, minutes apart:
///
/// | `DRAIN_PER_CLAIM` | first call, four runs        |
/// |-------------------|------------------------------|
/// | 12                | 38.9, 44.0, 62.5, 64.9 ms    |
/// | 8                 | 32.8, 51.4, 63.8, 82.0 ms    |
///
/// A **fixed** configuration spans 32 to 82 ms, so the spread within one value
/// is larger than the difference between two — 8's worst sample is worse than
/// every sample at 12. Against a 60 ms acceptance line that is not a pass and
/// not a fail; it is a measurement the machine cannot resolve, and picking the
/// value off it would be picking off noise.
///
/// So the constant is set from the quantity that IS resolvable — the backlog
/// clears in about a fifth of the calls it was allowed — and the smaller of the
/// two candidates is taken, because a smaller ceiling cannot be the thing that
/// makes a call slow. Whatever the per-`fsync` cost turns out to be on a given
/// disk, 8 of them is the least this can spend and still drain.
///
/// Gated, because the queue it bounds is only ever non-empty on a service with
/// a store, and `--no-default-features` builds one that cannot have one.
#[cfg(feature = "persist")]
const DRAIN_PER_CLAIM: usize = 8;

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
    ///
    /// # A supervised actor's restart lands here
    ///
    /// A panicked turn on a [supervised](crate::LocalActor::spawn_supervised) actor
    /// whose [`Observer`](crate::Observer) refuses the fault restarts from the last
    /// committed state — so the turn's effect on that state is gone, and a retry under
    /// the key still reads this. The ledger is not told: it survives a restart intact.
    /// **The refusal is then coarser than what the crate knows**: the crate knows the
    /// turn's effect on the state did not survive, and this variant can only say that
    /// whether the work took effect is unknown.
    /// `turns.rs`'s `supervise` carries the chain, why the gap is left open, and which
    /// parts of it nothing guards.
    ///
    /// [`key_state`](crate::RpcClient::key_state) asks about a key without running
    /// anything, which is the only way to learn a state you are not willing to
    /// discover by executing.
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
    /// [`RemoteError::NoSuchActor`] carries the
    /// id it was given: a log line read away from the call site has to say what
    /// it is about. `String` rather than `&'static str`, because this crosses
    /// the wire inside [`RemoteError`] and has to `Deserialize`.
    #[error("this service does not accept an idempotency key on {method}")]
    NotKeyable {
        /// The method a key was attached to, `CamelCase` as `service!` wrote it.
        method: String,
    },

    /// This service records keys durably and could not write this one, so the
    /// handler was never entered.
    ///
    /// The one fact a caller most wants after a refusal — *nothing ran, send it
    /// again* — and it used to arrive as prose inside [`RemoteError`]'s untyped
    /// variant, whose own doc says it is not a discriminator. Both this file
    /// and `reply.rs` justified that on the grounds that minting a variant
    /// would shift the contract hash. `AGENTS.md` rules such a move free until
    /// keylink has a stable version, so the justification is repealed and both
    /// copies of it are deleted.
    ///
    /// **Here rather than on [`RemoteError`]**, because this file and
    /// `reply.rs` both rule that this vocabulary belongs to one enum: one
    /// variant on `RemoteError` rather than six, because `RemoteError` belongs
    /// to every service keylink will ever carry and this belongs to one
    /// feature.
    ///
    /// **Appended rather than inserted.** `reply.rs` rules that an insert
    /// renumbers every later variant, which turns one refusal into another on a
    /// peer built against the old order.
    ///
    /// Told apart from [`Interrupted`](Self::Interrupted) by which side of the
    /// handler the failure is on: this one is before it, so the work provably
    /// did not happen. `Interrupted` is after, where it is unknowable.
    #[error(
        "this service could not record the key, so nothing ran - the same key is safe to send again"
    )]
    NotClaimed,
    /// This key was claimed for different received message bytes or authority.
    #[error("this idempotency key belongs to a different request or authority")]
    RequestMismatch,
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
/// ones a caller constructs, which is what this is. [`Records`] is the one
/// exception in this file and its own doc says why: a feature-gated variant
/// makes an exhaustive match break on a feature the matching crate did not turn
/// on. Every variant here is unconditional, so that reason does not reach it.
///
/// # Why this is not called `Keyed`
///
/// `Keyed` is the obvious name and it is taken. In this crate a **keyed** thing
/// is one addressed by an [`ActorId`](crate::ActorId) —
/// [`KeyedService`](crate::KeyedService), `RegistryClient`, `registry.service(..)`,
/// `registry_alpn`, `run_keyed` — and `keylink::Keyed` would sit two lines below
/// `keylink::KeyedService` in the same export list meaning a different key
/// entirely. `rpc.rs`'s `Claimed` was renamed *out of* the word for exactly this,
/// and its doc comment records the decision; taking the word back for the
/// idempotency side would undo a rename this crate already paid for.
///
/// **Keyable** is what the type actually decides: whether a key may be attached to a
/// method.
///
/// It keeps the stem, and an earlier version of this comment rejected `Keying` *for*
/// keeping the stem — which does not distinguish the winner from the loser and was
/// simply a bad argument. The stem is not the problem and cannot be, since
/// `IdempotencyKey` is the caller-side name. **What distinguishes them is grammar
/// and referent.** `Keyed` is an adjective this crate has already bound to a
/// different noun — a keyed *actor*, addressed by an [`ActorId`](crate::ActorId).
/// `Keying` is a gerund: it names an activity and says nothing about what is being
/// selected. `Keyable` is an adjective whose subject is a **method**, which is
/// exactly what the variants select over — so `Only(&["Reserve"])` reads as *these
/// methods are keyable*, and no sentence built from it can be read as being about an
/// actor. `Idempotent` was the third candidate and reads as a property of the reply
/// rather than of the method.
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
///     .keyable(Keyable::Only(&["Reserve"]))
///     .build()
/// ```
///
/// # Names are checked against this binary's contract at startup
///
/// [`Only`](Self::Only) and [`Except`](Self::Except) are compared with
/// [`Rpc::METHODS`](crate::Rpc::METHODS) when the typed service builder is erased.
/// A typo or a method disabled by `cfg` is returned from server startup as
/// [`ConfigError::UnknownMethod`](crate::ConfigError::UnknownMethod).
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
    ///
    /// **A method added later is keyable by default here**, which is the mirror of
    /// [`Only`](Self::Only)'s typo hazard and worse in kind: the failure is silent
    /// and it is the exact problem this type exists to prevent, arriving through the
    /// policy meant to prevent it. A new method's records begin evicting the ones
    /// that matter and nothing says so. [`Only`](Self::Only) is the safer choice for
    /// a service that expects to grow, because there the failure of omission is a
    /// refusal that names the method.
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

    /// The first selected name absent from `known`, preserving policy order.
    pub(crate) fn first_unknown(&self, known: &'static [&'static str]) -> Option<&'static str> {
        match self {
            Self::Only(named) | Self::Except(named) => {
                named.iter().copied().find(|method| !known.contains(method))
            }
            Self::Any | Self::None => None,
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
/// **Not saving memory.** Reading [`MAX_RECORDED_BYTES`] as a per-key figure
/// inverts the whole argument: it is the **total pool across every peer**, which is
/// the bound `evict` reads. What one reply may cost on its own is a different
/// number, [`MAX_RECORDED_FRAME`], read in `Guard::record`. So the record pool is
/// 1 MiB *in total*, and what [`None`](Self::None) saves is that
/// one megabyte once per process, and it does not get that megabyte back for free:
/// the same keys still occupy the tombstone pool, which [`MAX_TOMBSTONES`] bounds
/// separately.
///
/// **This paragraph used to make that point from a defect, and the defect is
/// gone.** It read *"[`MAX_RECORDED_BYTES`] does double duty"* and quoted the two
/// comparisons to prove that one value was serving as both bounds. That was true of
/// the tree it was written against. The two bounds are now two constants, so the
/// premise no longer exists — but the conclusion never depended on the conflation,
/// only on the pool being a pool. It is restated above without it.
///
/// **No figure for the comparison, deliberately.** An earlier version said the saving
/// was "roughly what its own tombstones cost", resting on [`MAX_TOMBSTONES`]'s own
/// doc calling that pool *"on the order of a megabyte"* — a number in another
/// comment, measured by nothing, that this comment cannot keep true. The sentence
/// only has to establish that memory is **not** the motive, and the *direction* does
/// that: what is saved is one bounded pool while another bounded pool keeps its
/// entries.
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
/// **And it shortens the at-most-once window, which is not nothing.** Under
/// [`Memory`](Self::Memory) a settled key occupies a slot in the record pool and
/// *then* becomes a tombstone, so its total life is bounded by [`MAX_RECORDS`] plus
/// [`MAX_TOMBSTONES`] of churn. Under [`None`](Self::None) it goes straight to the
/// tombstone pool and forgoes the record pool's grace entirely — so the window is
/// [`MAX_TOMBSTONES`] of churn instead of [`MAX_TOMBSTONES`] **plus**
/// [`MAX_RECORDS`].
///
/// **The operands rather than the quotient, deliberately.** Both are `pub const` and
/// either may move; a percentage written here would rot silently the moment one did,
/// and the reader can divide. It lands on the one degradation `README.md`'s limit 4
/// says a caller **cannot detect**, because past [`MAX_TOMBSTONES`] a key reads
/// [`KeyState::Fresh`] and runs again. Stated rather than sold as free.
///
/// `records_none_shortens_the_at_most_once_window` holds the **direction** — that the
/// window is strictly shorter — and not the ratio, because it sets its own
/// [`MAX_RECORDS`]/[`MAX_TOMBSTONES`] to keep the test fast. The direction is the
/// claim worth guarding, and it is what the phrase "does not affect the key state"
/// that stood here first got wrong.
///
/// # Not `Copy`, on purpose
///
/// A tier below memory is the shape this would grow into, and it would hold a port
/// — an `Arc<dyn ..>` — rather than a discriminant. `Copy` today would have to be
/// withdrawn then, and withdrawing a trait impl is a breaking change no attribute
/// covers.
///
/// # Why this one IS `#[non_exhaustive]` when the rule says otherwise
///
/// The rule — in `CONTRIBUTING.md`, and restated on [`Keyable`] and
/// [`KeyState`] — reserves the attribute for error enums rather than for ones a
/// caller constructs and matches. This is one a caller constructs, and it is
/// still the exception, because **one of its variants is feature-gated**.
///
/// Cargo unifies features across a build. A downstream crate that matches this
/// exhaustively compiles while nothing enables `persist`, and stops compiling
/// the moment an unrelated crate in the same build turns it on — a break its
/// author did not cause and cannot prevent. `#[non_exhaustive]` costs that
/// author a `_ =>` arm and removes the trap.
///
/// It does **not** block constructing the existing variants, so the argument
/// above about a caller building one of these is untouched.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum Records {
    /// Memory, bounded by [`MAX_RECORDS`], [`MAX_RECORDED_BYTES`] and
    /// [`RECORD_TTL`] — the three that evict — and by [`MAX_RECORDED_FRAME`], which
    /// refuses one reply rather than evicting any. What this crate did before this
    /// type existed.
    Memory,
    /// Nowhere. The key still claims, so the work still runs at most once, and
    /// every repeat is refused instead of answered.
    None,
    /// A durable store, so that a key answered before a restart is still
    /// answered after one.
    ///
    /// The two above hold their records for as long as the process lives, which
    /// is the window every bound on [`Limits`] is about. This one is the
    /// answer to the failure those bounds cannot describe: the process ends,
    /// every settled key reads [`KeyState::Fresh`] again, and the next retry
    /// runs work that already ran. For a treasury that is a payment made twice.
    ///
    /// # What it costs
    ///
    /// **Two `fsync`s per keyed call**, one before the handler is entered and
    /// one before its reply is written. `keylink/examples/durable_ledger.rs`
    /// measures a comparable store at about 7.6 ms against about 18 µs for an
    /// unobserved turn, so this is three orders of magnitude of latency on the
    /// keyed path and it is the whole reason this is not the default.
    ///
    /// # What it does not buy
    ///
    /// Nothing about another machine, exactly as
    /// [`Files`](keylink_persist::file::Files) says of state: the local-file
    /// adapter survives losing the process and not losing the disk.
    ///
    /// And nothing about the **in-flight** half. A key claimed when the process
    /// died recovers as [`KeyState::Interrupted`], because the caller it
    /// belonged to held a live stream and no disk carries one of those forward.
    /// That is the conservative answer and the correct one — it refuses the
    /// retry rather than running the work a second time.
    #[cfg(feature = "persist")]
    Durable(crate::durable::DurableRecords),
}

impl Records {
    /// Whether a reply recorded here can be replayed to a repeat of its key.
    ///
    /// Exhaustive, with no catch-all, for the reason [`Slot::held_bytes`] is: a
    /// fourth variant that holds replies would otherwise default to `false`,
    /// every reply would settle [`Unrecordable`](KeyState::Unrecordable), and
    /// no test would fail — because no test asserts on a variant that does not
    /// exist yet.
    fn holds_replies(&self) -> bool {
        match self {
            Self::Memory => true,
            Self::None => false,
            #[cfg(feature = "persist")]
            Self::Durable(_) => true,
        }
    }

    /// The store behind this setting, if there is one.
    ///
    /// The one reader of the variant, so that every durable path in this module
    /// asks the same question in the same place.
    #[cfg(feature = "persist")]
    fn store(&self) -> Option<&crate::durable::DurableRecords> {
        match self {
            Self::Memory | Self::None => None,
            Self::Durable(records) => Some(records),
        }
    }

    /// Whether a key leaving this ledger has to leave a store too.
    ///
    /// Separate from [`store`](Self::store) because `evict` runs under the
    /// ledger's lock, where a `&DurableRecords` it cannot await on would be a
    /// borrow it has no use for.
    ///
    /// **That sentence is true and it is not the reason the split is forced.**
    /// [`store`](Self::store) is `#[cfg(feature = "persist")]` and `evict`,
    /// `release` and `expire` are not, so without that feature they cannot call
    /// it at all — an `E0599` at `--no-default-features`, which
    /// `.mise/tasks/features` is the only gate step to reach. Uselessness is
    /// the softer half of the argument; not compiling is the binding one.
    pub(crate) fn stores_durably(&self) -> bool {
        match self {
            Self::Memory | Self::None => false,
            #[cfg(feature = "persist")]
            Self::Durable(_) => true,
        }
    }
}

/// What the ledger knows about one key, for one peer.
///
/// Answered by the generated `key_state` query, which is the only way to learn
/// [`Fresh`](Self::Fresh) — every other state can be discovered by retrying, and
/// `Fresh` cannot, because discovering it by retrying means executing.
///
/// Exhaustive on purpose. `#[non_exhaustive]` would be contradictory three ways:
/// this crate reserves that attribute for error enums rather than for ones a
/// caller matches — [`Records`] is the one exception, and only because a
/// feature-gated variant makes an exhaustive match break on a feature the
/// matching crate did not turn on, which is not true of anything here;
/// `postcard_schema` derives from the variant list and ignores
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
    /// [`RemoteError::TimedOut`], so `Duration`'s
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
    /// Capped server-side by [`Limits::attach`], which defaults to
    /// [`MAX_ATTACH`], 30 seconds. A budget that runs out is
    /// [`IdempotencyRefusal::InFlight`] — the same answer a non-attaching call
    /// gets, and still the truth.
    ///
    /// **The cap is not yours to raise, and asking for more is not an error
    /// here.** The budget is written on the envelope and read by the peer, so a
    /// number refused on this side would stop nobody: a hand-rolled peer sends
    /// whatever it likes, and the serving side caps it because holding a QUIC
    /// stream open is its cost to pay, not the caller's. So `attaching_for(key,
    /// Duration::from_secs(120))` is accepted and waits for whatever the service
    /// it is calling allows — 30 seconds at the default.
    ///
    /// **The side that pays now holds the knob, and [`Limits::attach`] is it.**
    /// That clause above names the serving side as the party whose stream and
    /// whose task are spent, and until that setter existed it was also the party
    /// with no way to spend less: the ceiling was a crate constant. Naming the
    /// cost and the control in one paragraph is the point — the argument for
    /// refusing the *caller* a raise is the same argument for giving the
    /// *service* a setting.
    ///
    /// A handler that routinely runs longer than the served ceiling cannot be
    /// waited out. Ask again — a second attach re-parks for another budget — or
    /// stop waiting and ask [`key_state`](crate::RpcClient::key_state), which
    /// reports without running anything. **A caller cannot read that ceiling**:
    /// it is not on the wire, and the refusal is the same
    /// [`InFlight`](IdempotencyRefusal::InFlight) at any value.
    ///
    /// Only a *running* call is waited for. A key that already settled is
    /// answered at once, whether that answer is the reply, `Interrupted`,
    /// `Unrecordable` or `Forgotten`.
    ///
    /// # Attaching to a service that holds no replies
    ///
    /// On a [`Records::None`] service the wait always ends in
    /// [`IdempotencyRefusal::Unrecordable`], because there is no reply to take.
    /// **The wait is still the right behaviour and it is not refused early**, and
    /// the reasoning is worth having, because refusing early looks like the obvious
    /// optimisation.
    ///
    /// What the wait buys is a *definitive* answer: `Unrecordable` means the work
    /// ran, there is no reply, reconcile against your own domain and stop.
    /// Refusing the attach at once could only answer
    /// [`InFlight`](IdempotencyRefusal::InFlight), whose documented meaning is *ask
    /// again when it finishes* — so a caller told that immediately asks again, and
    /// again, for as long as the handler runs. One bounded park becomes a re-dial
    /// loop, which is more load on both sides and ends at the worse of the two
    /// answers.
    ///
    /// The park is already priced. A waiter holds no claim, so
    /// [`MAX_WAITING_PER_PEER`] and [`MAX_WAITING`] exist to bound exactly this,
    /// and [`Limits::attach`] caps how long any one of them lasts. Held by
    /// `an_attacher_on_a_recordless_service_waits_and_is_told_it_ran`.
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

    /// How long the caller asked to be held, under `cap`.
    ///
    /// Capped here rather than at the constructor, because the value that
    /// matters is the one that arrived from the network and never met a
    /// constructor.
    ///
    /// **`cap` is a parameter rather than [`MAX_ATTACH`], and the reason is that
    /// the ceiling belongs to the service being called.** It is
    /// [`Limits::attach`], read off the ledger by the serving side's `claim_key`,
    /// which is the one place that holds both this value and the ledger. This
    /// type is built on the *calling* side and holds no ledger, so a constant was
    /// the only ceiling it could name on its own — and that is what made
    /// [`MAX_ATTACH`] the one ledger bound no operator could set.
    ///
    /// Taking the cap as an argument rather than returning the raw request is
    /// deliberate: an uncapped accessor returns a number a **peer** chose, and
    /// nothing in a signature would then say that its one caller must clamp it.
    ///
    /// **What the parameter makes unrepresentable is reading the budget without
    /// naming a ceiling. It does not make a bad ceiling unrepresentable, and the
    /// difference matters because this doc once claimed otherwise.**
    /// `attach_budget(Duration::MAX)` compiles, and so would
    /// `attach_budget(MAX_ATTACH)` at a future call site — which is the operator's
    /// setting ignored again, by the same shape this change removed.
    ///
    /// That residue is the design's own argument turned on itself, so it is written
    /// here rather than left for a reader to notice: the case against clamping in
    /// `claim_key` is that deleting a `min` there **breaks no type**, and passing a
    /// permissive `cap` breaks no type either. What the parameter buys is a
    /// narrower hole — from *any* caller silently forgetting to clamp, to a caller
    /// having to name a ceiling and being able to name a bad one — and a hole that
    /// has to be typed out is one a reviewer can see. It is one call site today.
    ///
    /// A newtype over the cap would close it. It is not worth a wrapper for one
    /// caller, which is a judgement about this call site and not about the
    /// technique; if a second serving path ever reads a budget, that is the moment
    /// to reach for it.
    ///
    /// **[`Attaching`] arrived after that paragraph and did not close it**, which is
    /// worth saying because it is a wrapper and looks like the one described above.
    /// It removed a different hole: [`Duration::ZERO`](Duration) naming two acts at
    /// one call site. `Attaching::UpTo(Duration::MAX)` is still a bad ceiling typed
    /// out in full, so what remains here is what remained before.
    ///
    /// `None` means the caller did not ask to attach at all. That is refused
    /// [`IdempotencyRefusal::InFlight`] rather than capped, so `cap` never
    /// applies to it.
    ///
    /// **`None` has exactly that one producer, and [`Attaching::Forbidden`] was
    /// deliberately kept out of it.** Routing the operator's refusal through this
    /// return would give the same wire answer and cost the meaning of this sentence:
    /// `a_bare_key_does_not_attach` would then pass for two reasons at once and stop
    /// testing what its name says. So `claim_key` refuses
    /// [`Attaching::Forbidden`] on its own line, above the call to this method, and
    /// `cap` reaching here is always a duration the operator chose to hold for.
    ///
    /// # `pub(crate)`, and the parameter is the reason
    ///
    /// This was `pub` until the cap became an argument. The only correct argument is
    /// the serving side's [`Limits::attach`], and **no caller outside this crate can
    /// obtain it**: it is not on the wire, and `Limits` exposes no getter. A public
    /// method whose one valid argument is unobtainable from outside is a leak rather
    /// than an API, and `CONTRIBUTING.md`'s test agrees — no macro, no test, no
    /// example and no README line names it.
    ///
    /// Issue #13 lists "changing what the `pub` `Idempotency::attach_budget`
    /// returns" as one of three surfaces that stop being free at publish. This change
    /// is exactly that, so the choice was to price the surface or remove it.
    /// Removing it is free while the crate is unpublished and leaves nothing to
    /// price.
    #[must_use]
    pub(crate) fn attach_budget(&self, cap: Duration) -> Option<Duration> {
        self.attach_for_millis
            .map(|millis| Duration::from_millis(millis).min(cap))
    }
}

/// Keeps every `with_idempotency(key)` call site reading the way it always has.
impl From<IdempotencyKey> for Idempotency {
    fn from(key: IdempotencyKey) -> Self {
        Self::new(key)
    }
}

/// What one key is worth, at the moment it is asked about.
///
/// `pub(crate)` for one consumer, `durable.rs`, which turns a slot into the byte
/// a disk holds and back. Keeping the codec out of this file is what stops the
/// ledger's rules and the record layout being edited as if they were one thing.
#[derive(Debug)]
pub(crate) enum Slot {
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
    fingerprint: [u8; 32],
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
    /// What `held.len()` was when [`refile`] last wrote this peer into
    /// [`Inner::by_held`], and the same for `spent` and [`Inner::by_spent`].
    ///
    /// Remembered rather than recomputed, because the stale index entry is
    /// keyed by the **old** length and the queue no longer holds it. Zero means
    /// nothing is filed: an empty queue is absent from the index, exactly as
    /// the scan this replaced skipped an empty bucket.
    filed_held: usize,
    filed_spent: usize,
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
    /// Keys this ledger has dropped from memory and a durable store still
    /// holds.
    ///
    /// Pushed by `evict` when a tombstone falls out of its pool, which runs
    /// under this lock and cannot await. Drained by the next claim, which can.
    /// Empty forever on a service with no store.
    ///
    /// **Not bounded by the evictions between two claims, and not drained in
    /// one go.** Ordinary traffic queues about one entry per settle, because a
    /// settle evicts at most one tombstone and adds at most one. The eviction
    /// `Ledger::new` runs over a recovered set is the exception that sets the
    /// shape here: it is bounded by what the store held, which was **2616
    /// entries** at a 1500-record fixture reopened under narrower bounds, and
    /// every entry is an `fsync`. So a claim drains at most
    /// [`DRAIN_PER_CLAIM`] of them, from the **front**, and the queue is a
    /// [`VecDeque`] so that costs `O(DRAIN_PER_CLAIM)` rather than shifting the
    /// remainder. The backlog still clears; it clears over several claims.
    ///
    /// A **downgrade** is deliberately not in here. `expire` and `evict` turn a
    /// held reply into a tombstone, and the disk keeps the reply — which costs
    /// space and loses nothing, because a recovered ledger runs the same
    /// eviction over the same bounds the moment it is built.
    pending: VecDeque<(EndpointId, IdempotencyKey)>,
    /// Every peer holding at least one replayable reply, as `(len, id)`, so the
    /// largest holder is one `last()` rather than a scan of every peer.
    ///
    /// `evict` runs on every settle, and it ran `max_by_key` over `peers` twice
    /// each time, under the one lock the whole service shares. Interleaved A/B,
    /// 21 samples per arm, 200 keyed calls per sample, release profile, median
    /// ns per call: **27.3x at 4096 peers, 11.0x at 1024, 5.8x at 256, 2.4x at
    /// 64, 1.2x at 8.** `BTreeSet::last` on `(len, id)` and
    /// `max_by_key` on `(len, id)` select the same peer, because ids are unique
    /// and so the tie-break is total either way.
    ///
    /// Written only by [`refile`] and [`drop_peer`]. The `debug_assert` in
    /// [`Ledger::lock`] is what makes a forgotten call visible.
    by_held: BTreeSet<(usize, EndpointId)>,
    /// The same, for tombstones.
    by_spent: BTreeSet<(usize, EndpointId)>,
}

#[cfg(debug_assertions)]
impl Inner {
    /// Every value derived from `peers`, checked against `peers`.
    ///
    /// **Six facts, not five.** The four counters and `waiting` all stay
    /// correct while [`Inner::by_held`] and [`Inner::by_spent`] rot, because
    /// nothing but [`refile`] writes them — so a five-fact check cannot see the
    /// one mistake the indexes made possible, which is a settle path that
    /// forgot to call it.
    ///
    /// Unbounded on purpose, and the cost was measured rather than assumed:
    /// `mise run test`, warm, ran **16.02 s without this check and 16.43 s with
    /// it**. That is one sample per arm on a machine whose repeated runs of one
    /// configuration have spanned a factor of two elsewhere in this file, so
    /// read it as *the suite did not get noticeably slower* and not as a
    /// percentage.
    ///
    /// **What that measurement does NOT cover.** It is O(peers x slots) on
    /// every [`Ledger::lock`], which is every keyed path, and the suite never
    /// approaches the shipped bounds — a bucket set may hold
    /// [`MAX_RECORDS`] + [`MAX_TOMBSTONES`] + [`MAX_RUNNING`] slots, so a
    /// debug-profile service
    /// near them walks orders of magnitude more per call than any test here.
    /// `debug_assertions` is off in release, so no shipped binary pays it, and
    /// a debug build at production scale will.
    ///
    /// A peer-count ceiling was written and then removed: it would buy the
    /// difference back by switching the check off exactly where a large ledger
    /// makes the drift most likely.
    fn agrees(&self) {
        let mut held = 0;
        let mut bytes = 0;
        let mut spent = 0;
        let mut running = 0;
        let mut by_held = BTreeSet::new();
        let mut by_spent = BTreeSet::new();
        for (peer, bucket) in &self.peers {
            held += bucket.held.len();
            spent += bucket.spent.len();
            running += bucket.running;
            bytes += bucket
                .slots
                .values()
                .map(|entry| entry.slot.held_bytes())
                .sum::<usize>();
            if !bucket.held.is_empty() {
                by_held.insert((bucket.held.len(), *peer));
            }
            if !bucket.spent.is_empty() {
                by_spent.insert((bucket.spent.len(), *peer));
            }
        }
        debug_assert_eq!(self.held, held, "`held` disagrees with the buckets");
        debug_assert_eq!(self.bytes, bytes, "`bytes` disagrees with the slots");
        debug_assert_eq!(self.spent, spent, "`spent` disagrees with the buckets");
        debug_assert_eq!(
            self.running, running,
            "`running` disagrees with the buckets"
        );
        debug_assert_eq!(
            self.waiting,
            self.waiters.values().sum::<usize>(),
            "`waiting` disagrees with `waiters`"
        );
        debug_assert_eq!(
            self.by_held, by_held,
            "`by_held` disagrees with the buckets: a `refile` call is missing"
        );
        debug_assert_eq!(
            self.by_spent, by_spent,
            "`by_spent` disagrees with the buckets: a `refile` call is missing"
        );
    }
}

/// Every bound the **idempotency ledger** enforces, in one value.
///
/// Set it with [`ServiceBuilder::limits`](crate::ServiceBuilder::limits). Each
/// method consumes and returns the value, so a departure from the default is one
/// chain:
///
/// ```
/// use std::num::NonZeroUsize;
/// use keylink::Limits;
///
/// // The const-friendly conversion this crate uses everywhere a bound is written,
/// // because `NonZeroUsize::new` is total and `unwrap` is denied in a `const`.
/// const SMALL: NonZeroUsize = match NonZeroUsize::new(64) {
///     Some(n) => n,
///     None => NonZeroUsize::MIN,
/// };
///
/// let limits = Limits::default().pool_records(SMALL).frame_bytes(SMALL);
/// ```
///
/// The name says `ledger`, not `keylink`: [`MAX_HEADER`](crate::MAX_HEADER),
/// [`MAX_KEY_LEN`](crate::MAX_KEY_LEN),
/// [`RegistryBuilder::capacity`](crate::RegistryBuilder::capacity) and
/// [`RegistryBuilder::mailbox_depth`](crate::RegistryBuilder::mailbox_depth) are all
/// keylink bounds and none of them is in here.
///
/// **[`MAX_ATTACH`] was on that list, as the one bound "genuinely the ledger's
/// and still absent", and the reason given for its absence was wrong.** It said
/// the cap is applied in `Idempotency::attach_budget`, "which holds no
/// ledger" — true of the method and false of its only caller. The serving side's
/// `claim_key` binds the ledger before it reads the budget, so it holds this
/// value at the instant it applies the cap, and it now passes it in. The ceiling
/// is [`attach`](Self::attach), applied where the ledger is in scope. That the
/// number arrives from the network is the *reason* the serving side owns the
/// ceiling rather than a reason it cannot name one: what a peer sends is a
/// request, and only a ceiling somebody configured makes it bounded.
///
/// # Why this type has a `Default` when `Records` and `Keyable` do not
///
/// Because nine of its ten fields are tuning decisions whose wrong value is
/// **visible in a refusal** — every one of them refuses through
/// [`IdempotencyRefusal`]. Slower or fatter, and both visible and recoverable.
///
/// **Visible at the SMALL end of the range, and three fields go quiet at the large
/// one.** A value too low refuses, loudly and per call. A value too high can put its
/// own bound out of reach, and a bound that never fires reports nothing at all — so
/// [`pool_records`](Self::pool_records), [`pool_bytes`](Self::pool_bytes) and
/// [`attach`](Self::attach) each document that end of their own range, because no
/// refusal will. The safety of this `Default` is a claim about departures downward.
/// Only the four running and waiting bounds put the exceeded figure *in* the
/// refusal, via [`TooManyRunning`](IdempotencyRefusal::TooManyRunning); the pool
/// bounds and the ceiling answer
/// [`Forgotten`](IdempotencyRefusal::Forgotten) or
/// [`Unrecordable`](IdempotencyRefusal::Unrecordable), which name the outcome and
/// not the number.
///
/// **[`tombstones`](Self::tombstones) is the exception, and it is why this
/// paragraph is written per field rather than about the type.** Past it a key
/// reads `Claim::Fresh` and the work runs a second time, which README
/// limit 4 names as the one degradation a caller **cannot detect** — `Fresh` is
/// what "never used" looks like too. So the safety of this `Default` is not that
/// every field is harmless. It is that the default `tombstones` is **eight times**
/// the default [`pool_records`](Self::pool_records), and **that margin is the
/// whole of the safety**. A caller who narrows the margin takes on a failure the
/// crate cannot report.
///
/// That is also what separates this type from
/// [`Retention`](crate::Retention),
/// [`Eviction`](crate::Eviction) and [`Allowlist`](crate::Allowlist), which have
/// no `Default` at all: each of those is *one* decision, so it can be judged
/// whole, and the wrong answer is a policy a caller never chose. `Limits` is ten
/// decisions in one value, and "this type is a tuning decision" is not a
/// well-formed sentence about it.
/// [`RegistryBuilder::mailbox_depth`](crate::RegistryBuilder::mailbox_depth)'s rustdoc
/// draws the same line between a bound and a policy — it is **one knob**, not a bag
/// of them, so it is the precedent for the *distinction* rather than for defaulting
/// ten numbers at once. Nothing in this crate had a precedent for that.
///
/// # Not `Copy`, and no getters
///
/// For the reason [`Records`] is not `Copy`: a later field could hold a port
/// behind an `Arc`, and withdrawing a trait impl is a breaking change no attribute
/// covers. A `Limits` is built once per service, so `Copy` buys nothing
/// measurable. Adding `Copy` later is not breaking; removing it is.
///
/// The fields are private and there are no getters, so a configured `Limits`
/// cannot be read back. That is deliberate for now — adding a getter later is not
/// breaking — and the operator-facing need is already met on the wire, because the
/// refusal carries the number. [`Debug`] is hand-written for the same reason:
/// a derived one would publish every private field name as the readback nobody
/// decided to offer.
///
/// # Three shapes this type is not
///
/// Issue #3's precedent, applied to issue #13: an argument for refusing a shape
/// belongs where somebody meets the shape, and that is here rather than in the
/// issue that raised it.
///
/// **Public fields behind `#[non_exhaustive]`, rejected.** The pitch is a struct
/// literal that names two bounds and takes the rest from `..Default::default()`,
/// with the attribute keeping a tenth field non-breaking. **The two are mutually
/// exclusive.** A `#[non_exhaustive]` struct cannot be built by literal from
/// another crate at all — functional update included — so
/// `Limits { pool_records: n, ..Default::default() }` does not compile outside
/// `keylink`, and the syntax the design was wanted for is exactly the syntax the
/// attribute removes. Drop the attribute and the field list is frozen for good
/// instead.
///
/// **And the property the attribute was wanted for is already here without it.**
/// The fields are private, so no dependent crate can write a struct literal for
/// this type at all, and `Default` plus setters is the only construction route
/// there is. A tenth private field with a tenth setter is therefore purely
/// additive — which is what [`attach`](Self::attach) was, and it is why that
/// addition is no more breaking after this crate is published than before. The
/// attribute would buy nothing that privacy has not already bought.
///
/// **A constructor taking every bound at once, rejected.**
/// `Limits::new(1024, 1048576, 8192, ..)` compiles with any two of its `usize`
/// arguments transposed and [`Limits::default().pool_records(n)`](Self::pool_records)
/// cannot, because the name is at the call site rather than in the position. And the
/// second reason is [`attach`](Self::attach) itself, which arrived as evidence while
/// this paragraph was being written: it took this type from nine bounds to ten, which
/// is one more positional parameter for every existing caller of such a constructor
/// and no change at all for a caller of the setters.
///
/// **A setter returning `Result`, rejected.** The shape follows `Files::limit` in
/// `keylink-persist`, whose own doc gives the reason a builder documents its floor
/// rather than refusing at it: *"clamping upward is a silent fallback, and panicking
/// on a configuration value is a crash where a refused store already says the same
/// thing at the same volume. So the floor is a documented number."* Here the
/// equivalent of that refused store is an [`IdempotencyRefusal`], and it arrives at
/// the moment the bound binds, naming the bound. A `Result` at configuration time
/// would report the same fact earlier and less specifically, and only for the bounds
/// whose bad values are namable — which is not the set that matters, since
/// [`tombstones`](Self::tombstones) is the field whose wrong value nothing reports
/// at all.
///
/// # Two ways to configure this and have it bound nothing
///
/// Both compile and both read as configured:
///
/// - Under [`Keyable::None`] **no ledger is built at all**, so a `Limits` handed
///   to the builder is discarded whole.
/// - Under [`Records::None`] nothing is held, so
///   [`pool_records`](Self::pool_records), [`frame_bytes`](Self::frame_bytes) and
///   [`pool_bytes`](Self::pool_bytes) bound nothing. The running, waiting and
///   tombstone bounds still apply.
#[derive(Clone)]
pub struct Limits {
    pool_records: NonZeroUsize,
    frame_bytes: NonZeroUsize,
    pool_bytes: NonZeroUsize,
    tombstones: NonZeroUsize,
    running: NonZeroUsize,
    running_per_peer: NonZeroUsize,
    waiting: NonZeroUsize,
    waiting_per_peer: NonZeroUsize,
    ttl: Duration,
    attach: Attaching,
}

/// Every bound constant in this module is a non-zero literal, so the `None` arm is
/// unreachable. It returns `MIN` rather than panicking, which is the shape
/// [`APPLIED_BOUND`](crate::APPLIED_BOUND) already uses at `call.rs:215` and for the
/// reason its own inline comment gives there: `NonZeroUsize::new` is total, and this
/// crate denies `unwrap` in a `const` as everywhere else.
///
/// So the eight `NonZeroUsize` fields cost no panic path, and the eight `pub const`
/// bounds they default from stay `usize` and public. Converting in a `const fn`
/// rather than at each field is what keeps those two facts compatible.
///
/// **This said `nine` and the number was wrong, in the direction that made it look
/// like a field count.** Measured with `rg -c 'pub const [A-Z_]+: usize'` on this
/// module: **8**. Ten `pub const` bounds live here and two of them —
/// [`RECORD_TTL`] and [`MAX_ATTACH`] — are [`Duration`]s that never meet this
/// function, which is the whole reason the two figures are not the same figure.
const fn nonzero(n: usize) -> NonZeroUsize {
    match NonZeroUsize::new(n) {
        Some(n) => n,
        None => NonZeroUsize::MIN,
    }
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            pool_records: nonzero(MAX_RECORDS),
            frame_bytes: nonzero(MAX_RECORDED_FRAME),
            pool_bytes: nonzero(MAX_RECORDED_BYTES),
            tombstones: nonzero(MAX_TOMBSTONES),
            running: nonzero(MAX_RUNNING),
            running_per_peer: nonzero(MAX_RUNNING_PER_PEER),
            waiting: nonzero(MAX_WAITING),
            waiting_per_peer: nonzero(MAX_WAITING_PER_PEER),
            ttl: RECORD_TTL,
            attach: Attaching::UpTo(MAX_ATTACH),
        }
    }
}

impl Limits {
    /// Settled records held before `Ledger::evict` starts dropping the oldest.
    ///
    /// Defaults to [`MAX_RECORDS`]. One of the two pool bounds, counted across
    /// every peer; the other is [`pool_bytes`](Self::pool_bytes), and **whichever
    /// binds first governs**.
    ///
    /// Zero would hold nothing, so every retry would read
    /// [`Forgotten`](IdempotencyRefusal::Forgotten) — at-most-once intact, the
    /// answer gone. Not [`Records::None`]'s
    /// [`Unrecordable`](IdempotencyRefusal::Unrecordable): that is a permanent
    /// property of the service, and this would be an eviction that already
    /// happened.
    ///
    /// # What a maximal value does, and it is the opposite of bounding
    ///
    /// `Ledger::evict` drops records *while* the held count is over this number
    /// **or** the byte total is over [`pool_bytes`](Self::pool_bytes), so
    /// [`NonZeroUsize::MAX`] here makes one half of that condition unreachable and
    /// not the whole of it. **Maximal on both, and the loop body never runs: the
    /// record pool evicts nothing, and what bounds it is [`ttl`](Self::ttl) alone.**
    /// Maximal here with a small `pool_bytes` and the byte half still fires, so the
    /// pool still evicts — this paragraph asserted the conclusion from one field
    /// until a review read the condition. Where nothing evicts, every settled reply
    /// is held until it expires and the memory that costs is whatever the traffic
    /// was.
    ///
    /// Nothing refuses, which is what makes this end of the range quieter than the
    /// other one. Zero is loud — every retry reads
    /// [`Forgotten`](IdempotencyRefusal::Forgotten) — while an unreachable bound
    /// produces no refusal at all, because an eviction that does not happen has
    /// nothing to report.
    ///
    /// [`pool_bytes`](Self::pool_bytes) is the other half of that same `while`
    /// condition and goes unreachable the same way, so a pool bounded by neither
    /// needs both set high. Held by `a_maximal_pool_evicts_nothing`.
    #[must_use]
    pub fn pool_records(mut self, records: NonZeroUsize) -> Self {
        self.pool_records = records;
        self
    }

    /// The largest single reply frame the ledger will record, prefix included.
    ///
    /// Defaults to [`MAX_RECORDED_FRAME`], which equals the wire's **payload** cap.
    /// A frame is that payload plus the four-byte length prefix, so the default
    /// **refuses the four largest frames the wire can carry**. That is the truthful
    /// answer while the pool is [`MAX_RECORDED_BYTES`] — the argument lives in
    /// [`MAX_RECORDED_FRAME`]'s own four-bytes paragraph, and it changes when the
    /// pool rises. Lower it to stop one large reply from evicting many small ones. A
    /// reply above it is answered normally, and the retry is refused
    /// [`Unrecordable`](IdempotencyRefusal::Unrecordable).
    ///
    /// **The number counts the whole frame, length prefix included**, because the
    /// ledger stores exactly the bytes the wire wrote. So headroom for one reply's
    /// payload is the figure set minus four.
    ///
    /// **There IS a hard floor, and it is not the useful one.** A frame is the
    /// four-byte prefix plus a postcard-encoded envelope, and that envelope is never
    /// empty — it carries two option tags before the message — so `frame.len()` is
    /// always **greater** than four. **Any value of four or below therefore refuses
    /// every reply**, which is arithmetic from the prefix width rather than a
    /// measurement of the ledger.
    ///
    /// Stated as *four or below* rather than *under four*, which was the first
    /// wording here and left the boundary out: at exactly four nothing records
    /// either, because no frame is that small. This line before that said *"zero
    /// would leave only a zero-length frame recordable"* — a frame size that cannot
    /// exist, at a value `NonZeroUsize` already forbids.
    ///
    /// **Above four there is no floor this method can state**, and that is what
    /// separates it from the one bound in this workspace that has one.
    /// `keylink_persist`'s file limit states its floor as a named constant,
    /// so a caller can read the threshold and stay above it. The smallest useful
    /// value here is the postcard encoding of one service's own response type: it
    /// differs per service and has no name anywhere. So **any non-zero value can
    /// still refuse every reply of some response type**, and this method cannot say
    /// which value that is.
    #[must_use]
    pub fn frame_bytes(mut self, bytes: NonZeroUsize) -> Self {
        self.frame_bytes = bytes;
        self
    }

    /// Recorded reply bytes held in total, across every peer, before
    /// `Ledger::evict` starts dropping the oldest.
    ///
    /// Defaults to [`MAX_RECORDED_BYTES`]. The other pool bound beside
    /// [`pool_records`](Self::pool_records).
    ///
    /// Zero would evict the whole pool on every settle, so every retry would read
    /// [`Forgotten`](IdempotencyRefusal::Forgotten).
    ///
    /// Setting this **below** [`frame_bytes`](Self::frame_bytes) is legal and it
    /// is not the refusal a reader expects: a reply under the frame ceiling and
    /// over the pool is recorded `Done` and then immediately downgraded to
    /// [`Forgotten`](IdempotencyRefusal::Forgotten) by the `evict` that
    /// `Ledger::settle` runs in the same lock. It never reads
    /// [`Unrecordable`](IdempotencyRefusal::Unrecordable), because that state and
    /// `Done` are exclusive for one key.
    ///
    /// **A `const` assertion forbids that relation between the two default
    /// constants, and it does not constrain this setter.** It fires at build time on
    /// [`MAX_RECORDED_FRAME`] against [`MAX_RECORDED_BYTES`], because a *shipped*
    /// default in that relation would report an eviction for a reply that was never
    /// storable. A caller who chooses it deliberately gets the behaviour described
    /// above, which is why it is documented rather than rejected.
    ///
    /// At [`NonZeroUsize::MAX`] this bound is **unreachable**, so the byte half of
    /// `Ledger::evict`'s condition never fires — see
    /// [`pool_records`](Self::pool_records), where the argument is written once for
    /// both halves. A pool that evicts nothing is bounded by [`ttl`](Self::ttl).
    #[must_use]
    pub fn pool_bytes(mut self, bytes: NonZeroUsize) -> Self {
        self.pool_bytes = bytes;
        self
    }

    /// Tombstones held **in total, across every peer**, before the oldest are
    /// dropped.
    ///
    /// Not per peer — `Inner.spent` is one count for the whole ledger, and `evict`
    /// compares that single number against this bound. The same shape as
    /// [`pool_bytes`](Self::pool_bytes), and worth stating precisely **here of all
    /// places**: this is the one bound whose wrong value the caller cannot detect,
    /// so a reader who budgets it per peer under-provisions it by however many peers
    /// the service has.
    ///
    /// Defaults to [`MAX_TOMBSTONES`]. **This is the one bound on this type that
    /// is a correctness control rather than a tuning decision, and
    /// `NonZeroUsize` is not enough to make it safe.**
    ///
    /// A tombstone is what keeps an evicted key from being silently re-executed.
    /// Drop one and its key reads `Claim::Fresh` again, the work runs a
    /// second time, and README limit 4 names that as the one degradation a caller
    /// **cannot detect** — `Fresh` is what "never used" looks like too. So a small
    /// value loses at-most-once *undetectably*, and no refusal reports it.
    ///
    /// The bound that matters is not zero, it is the **margin over
    /// [`pool_records`](Self::pool_records)**: a tombstone must outlive the reply
    /// it replaced by a wide margin, and the default margin is eight times. Narrow
    /// it and a key can settle, lose its record to the record pool, then lose its
    /// tombstone to churn, all inside one retry window.
    #[must_use]
    pub fn tombstones(mut self, tombstones: NonZeroUsize) -> Self {
        self.tombstones = tombstones;
        self
    }

    /// Keyed calls the whole ledger may have in dispatch at once.
    ///
    /// Defaults to [`MAX_RUNNING`]. Zero would refuse every keyed call.
    ///
    /// # What a value above `u32::MAX` does, and it is the opposite of refusing
    ///
    /// The exceeded figure reaches the wire inside
    /// [`TooManyRunning`](IdempotencyRefusal::TooManyRunning), whose `limit` is a
    /// `u32`, so `over` clamps anything larger to 4294967295 rather than truncating
    /// it to a small wrong number.
    ///
    /// **But that clamp is only ever observed when a refusal fires, and a refusal
    /// fires only when the counter reaches the bound.** So a value above
    /// [`u32::MAX`] does not refuse — it makes the bound **unreachable**, because
    /// nothing refuses until more than 4294967295 keyed calls are in dispatch at
    /// once. Setting a bound that large disables it; it does not make it loud.
    ///
    /// The same reasoning covers [`running_per_peer`](Self::running_per_peer),
    /// [`waiting`](Self::waiting) and
    /// [`waiting_per_peer`](Self::waiting_per_peer), which is why it is written once
    /// here rather than four times.
    #[must_use]
    pub fn running(mut self, running: NonZeroUsize) -> Self {
        self.running = running;
        self
    }

    /// Keyed calls one peer may have in dispatch at once.
    ///
    /// Defaults to [`MAX_RUNNING_PER_PEER`]. Checked before
    /// [`running`](Self::running), so it governs first. Zero would refuse every
    /// keyed call from every peer.
    ///
    /// A value above [`u32::MAX`] **disables** this bound rather than refusing
    /// loudly — see [`running`](Self::running) for why the `u32` clamp is never
    /// observed at such a value.
    #[must_use]
    pub fn running_per_peer(mut self, running: NonZeroUsize) -> Self {
        self.running_per_peer = running;
        self
    }

    /// Attaching calls the whole ledger may have parked at once.
    ///
    /// Defaults to [`MAX_WAITING`]. Zero would refuse every attaching call
    /// [`TooManyRunning { limit: 0 }`](IdempotencyRefusal::TooManyRunning), which
    /// names a capacity, names the *running* count rather than the waiter count,
    /// and names it zero — wrong quantity and wrong cause at once. `NonZeroUsize`
    /// keeps the value out of that range without minting a refusal variant, which
    /// would move both ALPN pins.
    ///
    /// A caller that does not want to be parked at all has its own switch on the
    /// *calling* side: not asking to attach is answered
    /// [`InFlight`](IdempotencyRefusal::InFlight), which is the right refusal. The
    /// serving side's switch is [`Attaching::Forbidden`] on
    /// [`attach`](Self::attach), which answers the same refusal for the same reason
    /// and spends no place here either. Neither is this bound: this one is a count of
    /// the attachers that WERE admitted.
    ///
    /// A value above [`u32::MAX`] **disables** this bound rather than refusing
    /// loudly — see [`running`](Self::running) for why the `u32` clamp is never
    /// observed at such a value.
    #[must_use]
    pub fn waiting(mut self, waiting: NonZeroUsize) -> Self {
        self.waiting = waiting;
        self
    }

    /// Attaching calls one peer may have parked at once.
    ///
    /// Defaults to [`MAX_WAITING_PER_PEER`]. **Checked first**, before
    /// [`waiting`](Self::waiting) — but checked first is not the same as governing,
    /// and this doc used to say it *alone* governed whatever `waiting` held. It does
    /// not. If it passes, `waiting` is still checked, so **whichever of the two is
    /// smaller binds**: at the defaults 128 is below 1024 so this one binds for one
    /// peer, and set `waiting` below this and `waiting` binds even for a single
    /// peer. Being checked first only decides which refusal a caller sees when both
    /// are exceeded.
    ///
    /// Zero would refuse every attaching call, with the
    /// same misnamed refusal described on [`waiting`](Self::waiting).
    ///
    /// A value above [`u32::MAX`] **disables** this bound rather than refusing
    /// loudly — see [`running`](Self::running) for why the `u32` clamp is never
    /// observed at such a value.
    #[must_use]
    pub fn waiting_per_peer(mut self, waiting: NonZeroUsize) -> Self {
        self.waiting_per_peer = waiting;
        self
    }

    /// How long a settled record answers with its reply before it becomes a
    /// tombstone.
    ///
    /// Defaults to [`RECORD_TTL`]. A [`Duration`] rather than a non-zero type,
    /// because the language offers no `NonZero` for one — the only field on this
    /// type that can be set to zero.
    ///
    /// Zero makes every record [`Forgotten`](IdempotencyRefusal::Forgotten) at the
    /// next claim: at-most-once holds and no answer survives.
    ///
    /// **There is no floor to document, and that is a fact about the crate rather
    /// than an omission.** Any value above zero stores something. What decides
    /// whether a value is useful is the caller's own retry interval — a `ttl`
    /// below it behaves exactly like zero — and the crate cannot know that
    /// interval. That is precisely why it is not a floor.
    #[must_use]
    pub fn ttl(mut self, ttl: Duration) -> Self {
        self.ttl = ttl;
        self
    }

    /// Whether an attaching call is admitted at all, and the longest one is held
    /// waiting for a call already in flight under its key.
    ///
    /// Defaults to [`Attaching::UpTo`]`(`[`MAX_ATTACH`]`)`, 30 seconds. The duration
    /// inside that variant is a [`Duration`] rather than a non-zero type for
    /// [`ttl`](Self::ttl)'s reason: the language offers no `NonZero` for one, so a
    /// hold of zero is representable here and on `ttl` and nowhere else on this type.
    ///
    /// [`Attaching::Forbidden`] is the other act, and it is the one that admits
    /// nothing: `claim_key` matches it on its own line, above the budget it would
    /// otherwise read and before `Ledger::wait`, so nothing parks and no
    /// [`waiting`](Self::waiting) place is spent.
    ///
    /// **That line sits there for a reason, and not merely because it moved there.**
    /// Routing this refusal through `Idempotency::attach_budget`'s `None` would answer the
    /// same thing on the wire and cost a guard: `a_bare_key_does_not_attach` asserts that
    /// `None` means *the caller did not ask*, and a second producer makes it pass for two
    /// reasons at once while its name still claims one. That method's own doc carries the
    /// argument in full.
    ///
    /// **[`Attaching`] carries the argument for both acts, and this setter deliberately
    /// does not repeat it**: which refusal each one produces, what a zero hold still
    /// spends, and why zero is not spelled [`Forbidden`](Attaching::Forbidden). That
    /// argument used to sit here as well, at length. The edit that moved the operator's
    /// refusal out of `Idempotency::attach_budget` and into `claim_key` had to land in
    /// every copy of it, and **this setter's copy is the one it missed** — the paragraph
    /// above went on saying that `attach_budget` answers `None` for
    /// [`Forbidden`](Attaching::Forbidden), which after that edit it never does. One home
    /// is what stops the next such edit having to be right in a place its author is not
    /// reading. Everything below is about the [`UpTo`](Attaching::UpTo) arm.
    ///
    /// **This is a ceiling on a number a peer chose, and the smaller of the two
    /// governs.** A caller names its own budget in
    /// [`Idempotency::attaching_for`]; lowering this can only shorten a wait and
    /// never lengthen one, so a peer asking for one second is held for one second
    /// whatever this says.
    ///
    /// It is the second factor of the exhaustion [`MAX_WAITING_PER_PEER`] prices:
    /// a parked attacher holds a task and a QUIC stream pair that neither running
    /// cap can see, and the cost of a flood is *waiters × duration*. Before this
    /// setter existed a caller could set the count and not the duration, so it
    /// controlled one factor of a product.
    ///
    /// # No layer can stand in for this, and the ordering is why
    ///
    /// `serve_one` finishes `claim_key` — the whole park, and the re-claim after
    /// it — **before** it calls the layer chain. So a
    /// [`QueueTimeout`](crate::QueueTimeout) layer, and a
    /// [deadline](crate::ServiceBuilder::deadline), start their clocks once this
    /// wait is already over, and neither can bound it.
    /// This setter is the only thing that bounds its **duration** —
    /// [`waiting`](Self::waiting) and
    /// [`waiting_per_peer`](Self::waiting_per_peer) bound how many such waits run
    /// at once, which is the other factor and not this one.
    ///
    /// # There is a top of the range, and it does not raise the ceiling
    ///
    /// **A [`UpTo`](Attaching::UpTo) larger than any budget a peer sends disables the
    /// cap**, and past that point the wait is bounded by the *peer's* number alone.
    /// [`Idempotency::attaching_for`] carries whole milliseconds in a `u64` and
    /// saturates, so the largest request is [`u64::MAX`] of them — about 584
    /// million years. Set this near that and a peer holds a stream for as long as
    /// it likes. The bound is disabled by a large value rather than made loud, the
    /// same shape [`running`](Self::running) describes for its `u32`.
    #[must_use]
    pub fn attach(mut self, attach: Attaching) -> Self {
        self.attach = attach;
        self
    }
}

/// Whether this service admits an attaching call, and for how long it holds one.
///
/// What [`Limits::attach`] takes. The variants are two different acts by the
/// operator, and a [`Duration`] on its own could name only the first:
///
/// - [`UpTo`](Self::UpTo) **admits** the attacher, and bounds the hold.
/// - [`Forbidden`](Self::Forbidden) **refuses to admit it**, before a waiting place
///   is spent and before anything parks.
///
/// `UpTo` rather than a name like `HeldFor`, because the duration is a **ceiling on
/// a number the peer chose** and the smaller of the two governs.
/// [`Limits::attach`] has said that since it existed, and a name reading *hold for
/// exactly this long* would invert it at every call site.
///
/// # Why zero does not mean *forbidden*, and why this type exists
///
/// Because zero already means something else on a reachable path.
/// `UpTo(Duration::ZERO)` carries the caller through the whole accept path: it
/// spends a [`waiting`](Limits::waiting) place, re-claims the key once — so a call
/// that settles in that instant is still answered from the record — and only then
/// expires. It stops the *hold*, not the park. What zero does not buy is the machinery
/// not running, and an earlier version of [`Limits::attach`]'s own doc said zero "turns
/// the feature off": wrong on that path, and the sentence this type exists to make
/// unwritable.
///
/// **A zero ceiling is still an admission, and that is why it reaches that path at
/// all.** Clamping the peer's request against this ceiling leaves it with a budget of
/// zero rather than with no budget, and a caller holding a budget is one the accept path
/// admits. So it goes on to ask for a waiting place, and it is the *expiry* that turns it
/// away. [`Forbidden`](Self::Forbidden) is refused before any budget is read, so it never
/// asks.
///
/// So a second meaning stacked on the same zero would be a value whose act a
/// reader has to look up. A variant is the act, in the word the operator typed.
///
/// # The two settings answer with different refusals
///
/// `UpTo(Duration::ZERO)` is [`InFlight`](IdempotencyRefusal::InFlight) when a place
/// was free, and [`TooManyRunning`](IdempotencyRefusal::TooManyRunning) when the
/// waiting pool was already full, because `Ledger::wait` is reached either way and
/// refuses with the count it exceeded. [`Forbidden`](Self::Forbidden) is `InFlight` at
/// every pool state, because it never reaches that call.
/// `rpc::tests::a_forbidden_ceiling_is_refused_without_a_waiting_place` and
/// `rpc::tests::a_zero_hold_still_spends_a_waiting_place` hold the pair, against one
/// exhausted pool so that neither can drift from the other.
///
/// # Three acts, and each has its own door
///
/// A caller that wants never to be parked does not ask to attach, which is the
/// *calling* side — see [`Limits::waiting`]. A service that will admit an attacher and
/// hold it for no time sets [`UpTo(Duration::ZERO)`](Self::UpTo). A service that will
/// not admit one sets [`Forbidden`](Self::Forbidden). Only the first was expressible
/// before [`Limits::attach`] took this enum, and the second was standing in for the
/// third.
///
/// **A `bool` beside the duration, rejected**, and twice over: two fields that
/// interact where one does the work, and a flag at a call site that names nothing.
/// `middleware.rs` states this crate's rule for the shape — *"a known value set is
/// an enum"* — and `authz.rs` records what an [`Option`] on one costs.
///
/// **A second setter writing the same field, rejected**, and for a sharper reason:
/// it permits `.attach_refused().attach(Duration::from_secs(5))`, where the last
/// call silently wins and neither line looks wrong on its own.
///
/// # Examples
///
/// ```
/// use std::time::Duration;
/// use keylink::{Attaching, Limits};
///
/// // Admit an attacher and hold it for at most five seconds.
/// let bounded = Limits::default().attach(Attaching::UpTo(Duration::from_secs(5)));
///
/// // Admit nothing. Every attaching call is refused at once.
/// let none = Limits::default().attach(Attaching::Forbidden);
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Attaching {
    /// Admit the attacher, and hold it for at most this long.
    ///
    /// A ceiling on a number the peer chose, so the smaller of the two governs.
    /// [`Attaching`]'s own doc describes what the zero end of this range does.
    /// [`Limits::attach`] describes the top of it, where a value above any budget a peer
    /// sends disables the cap.
    UpTo(Duration),
    /// Admit no attacher at all.
    ///
    /// `claim_key` refuses this on its own line, above the budget it would otherwise
    /// read, and before `Ledger::wait` — so no [`waiting`](Limits::waiting) place is
    /// spent, nothing parks, and nothing re-claims.
    ///
    /// # What choosing this over a zero hold gives up, and it is the re-claim
    ///
    /// `UpTo(Duration::ZERO)` also holds for no time, and it is **not** the same
    /// setting. It admits the attacher, which buys one re-claim of the key before the
    /// budget expires — so a twin call that settles in that instant is still answered
    /// **from the record**. This setting has no re-claim to offer, so that caller
    /// reads [`InFlight`](IdempotencyRefusal::InFlight) and has to ask again for an
    /// answer that already existed. That is the whole behavioural difference between
    /// the two, and the only reason to keep both: a zero hold pays a waiting place
    /// for a chance at the recorded reply, and this pays nothing and forgoes it.
    ///
    /// # The refusal does not say which of two things happened
    ///
    /// A caller that never asked to attach is refused
    /// [`InFlight`](IdempotencyRefusal::InFlight) as well, so on the wire the
    /// operator's setting and that caller are one answer. Recorded rather than left
    /// for a reader to discover: the **caller** knows which of the two it did, and
    /// *the call is running, ask again when it finishes* is true of both. What the
    /// refusal does not tell a caller is whether asking to attach would have helped.
    /// Against this setting it never would.
    ///
    /// The collapse is on the wire and **not** inside
    /// `Idempotency::attach_budget`, whose `None` keeps its single meaning. That
    /// method's own doc gives the reason it was kept out.
    Forbidden,
}

impl fmt::Debug for Limits {
    /// Hand-written, and the field list is deliberately absent.
    ///
    /// A derived `Debug` on a public struct with private fields publishes every
    /// private field name and value, which would make it the readback that
    /// [`Limits`] deliberately does not offer. Same reason
    /// [`ServiceBuilder`](crate::ServiceBuilder) hand-writes its own.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Limits").finish_non_exhaustive()
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
/// Each entry additionally commits to its request and selected authority
/// (principal and holder). Message bytes are the received encoding, excluding
/// retry-control metadata. A different encoding, including across contract
/// versions, is a typed mismatch even when it expresses the same logical call.
/// A different binding is refused before attachment or replay; the same
/// commitment is retained by durable records.
#[derive(Debug)]
pub(crate) struct Ledger {
    #[cfg(feature = "persist")]
    contract_version: crate::Version,
    inner: Mutex<Inner>,
    limits: Limits,
    /// Where the replies recorded here go. Held by the ledger rather than
    /// threaded down from `serve_one`, because `Guard::record` is the one place
    /// that reads it and the guard already holds the ledger.
    records: Records,
}

impl Ledger {
    /// One constructor, taking the limits.
    ///
    /// There used to be a `new(records)` default shim over a
    /// `with_limits(limits, records)`. Once the builder always holds a [`Limits`]
    /// the shim had no caller, so it is gone rather than kept for symmetry — a
    /// deleted shim stays deleted.
    pub(crate) fn new(
        limits: Limits,
        records: Records,
        contract: crate::version::RecordCodec,
    ) -> Arc<Self> {
        #[cfg(not(feature = "persist"))]
        let _ = contract;
        #[cfg(feature = "persist")]
        let inner = records.store().map_or_else(Inner::default, |store| {
            recover(store.take_recovered(contract.upgrade.as_ref()))
        });
        #[cfg(not(feature = "persist"))]
        let inner = Inner::default();
        let ledger = Arc::new(Self {
            #[cfg(feature = "persist")]
            contract_version: contract.current,
            inner: Mutex::new(inner),
            limits,
            records,
        });
        // The bounds belong to this build, not to whatever wrote the store. A
        // ledger recovered from a disk written under larger limits is brought
        // inside the ones configured here before it answers anything, which is
        // the same sweep a running ledger does after every settle.
        {
            let mut guard = ledger.lock();
            let inner = &mut *guard;
            ledger.evict(inner);
        }
        ledger
    }

    /// Whether this service admits an attaching call, and its ceiling if it does.
    ///
    /// The one bound read from outside the ledger, because the clamp is applied on the
    /// accept path: `claim_key` matches this value, and only an
    /// [`UpTo`](Attaching::UpTo)'s duration reaches `Idempotency::attach_budget`. A
    /// crate-private reader rather than a getter on [`Limits`], which deliberately
    /// offers none.
    ///
    /// Returns an [`Attaching`] and not a [`Duration`], so that
    /// [`Attaching::Forbidden`] reaches the accept path as a value rather than as a
    /// duration a reader has to interpret.
    pub(crate) fn attach_cap(&self) -> Attaching {
        self.limits.attach
    }

    /// Take the key, or say what already holds it.
    ///
    /// This is the whole at-most-once property: the slot is claimed *before* the
    /// handler is entered, and the only way back to vacant is for the guard to
    /// be released — which happens exactly when nothing ran.
    pub(crate) fn claim(
        self: &Arc<Self>,
        peer: EndpointId,
        key: IdempotencyKey,
        fingerprint: [u8; 32],
    ) -> Claim {
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
                if entry.fingerprint != fingerprint {
                    return Claim::Refused(IdempotencyRefusal::RequestMismatch.into());
                }
                return entry.slot.replay();
            }
            if bucket.running >= self.limits.running_per_peer.get() {
                return refused_over(self.limits.running_per_peer);
            }
        }
        if total >= self.limits.running.get() {
            return refused_over(self.limits.running);
        }
        {
            let bucket = inner.peers.entry(peer).or_default();
            bucket.slots.insert(
                key.clone(),
                Entry {
                    slot: Slot::Running(Arc::new(Notify::new())),
                    fingerprint,
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
            fingerprint,
            // Nothing is watching yet, which `Drop` reads as `NotDispatched` —
            // the truth about a key nothing has been dispatched under.
            // `serve_one` fills it before anything is.
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
        if inner.waiters.get(&peer).copied().unwrap_or(0) >= self.limits.waiting_per_peer.get() {
            return Err(over(self.limits.waiting_per_peer));
        }
        if inner.waiting >= self.limits.waiting.get() {
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

    /// Take the key, and put the claim on a disk before anything runs.
    ///
    /// [`claim`](Self::claim) is the memory half and this is the whole
    /// operation. The order is the property: the disk is told a key was taken
    /// **before** the handler is entered, so a process that dies mid-call leaves
    /// a claim behind, and the claim is what a restart reads as
    /// [`Interrupted`](KeyState::Interrupted) instead of as vacant.
    ///
    /// On a service with no store this is [`claim`](Self::claim) and one
    /// `matches!`.
    pub(crate) async fn claim_durably(
        self: &Arc<Self>,
        peer: EndpointId,
        key: IdempotencyKey,
        fingerprint: [u8; 32],
    ) -> Claim {
        // Moved rather than cloned: every arm but `Fresh` drops the key, and the
        // `Fresh` arm's guard is holding it, so the write below borrows it back
        // instead of being handed a second copy.
        let claim = self.claim(peer, key, fingerprint);
        let Claim::Fresh(guard) = claim else {
            return claim;
        };
        if let Err(refusal) = self.write_claim(peer, &guard.key, fingerprint).await {
            // Nothing ran, so the key goes back and the caller's retry is a
            // first attempt. If the write in fact reached the disk and reported
            // otherwise, that retry reads `Interrupted` instead — refused rather
            // than run, which is the direction to be wrong in.
            guard.give_back();
            return Claim::Refused(refusal);
        }
        Claim::Fresh(guard)
    }

    /// Record that this key is taken, before the handler is entered.
    ///
    /// # Errors
    ///
    /// [`IdempotencyRefusal::NotClaimed`], which says the one thing the caller
    /// can act on: nothing ran, and the same key is safe to send again. The
    /// store's own error is not carried, because it names a disk the caller has
    /// no access to and cannot do anything about.
    async fn write_claim(
        &self,
        peer: EndpointId,
        key: &IdempotencyKey,
        fingerprint: [u8; 32],
    ) -> Result<(), RemoteError> {
        #[cfg(feature = "persist")]
        if let Some(store) = self.records.store() {
            if store.claimed(peer, key, fingerprint).await.is_err() {
                return Err(IdempotencyRefusal::NotClaimed.into());
            }
        }
        #[cfg(not(feature = "persist"))]
        let _ = (peer, key, fingerprint);
        Ok(())
    }

    /// Record what this key settled as, before its reply is written.
    ///
    /// # Errors
    ///
    /// [`IdempotencyRefusal::Unrecordable`], which is what happened word for
    /// word: the handler ran, and its reply is not something this ledger can
    /// hold. It is **not** [`Interrupted`](IdempotencyRefusal::Interrupted),
    /// whose own doc says the handler produced no reply — here it produced one
    /// and the ledger is discarding it.
    async fn write_settled(
        &self,
        peer: EndpointId,
        key: &IdempotencyKey,
        slot: &Slot,
        fingerprint: [u8; 32],
    ) -> Result<(), RemoteError> {
        #[cfg(feature = "persist")]
        if let Some(store) = self.records.store() {
            if store
                .write(peer, key, slot, fingerprint, self.contract_version)
                .await
                .is_err()
            {
                return Err(IdempotencyRefusal::Unrecordable.into());
            }
        }
        #[cfg(not(feature = "persist"))]
        let _ = (peer, key, slot, fingerprint);
        Ok(())
    }

    /// Take the keys memory has dropped off the disk, and shorten the store if
    /// it is carrying more than it holds.
    ///
    /// Called by `claim_key`, once, before it claims anything. `pub(crate)` for
    /// that one caller: it used to run inside [`claim_durably`](Self::claim_durably),
    /// which the two attach paths also reach, and an attaching caller never
    /// receives [`Claim::Fresh`].
    ///
    /// Returns nothing, including no error, and that is the decision rather
    /// than an omission: every failure here is a store that will refuse the
    /// claim its caller is about to write, on this same call, with the sentence
    /// that names it. A key that could not be removed goes back in the queue,
    /// so nothing is dropped by not reporting it here.
    #[allow(
        clippy::unused_async,
        reason = "there is nothing to await without the `persist` feature, and the caller \
                  should not have to know which features are on"
    )]
    pub(crate) async fn housekeep(&self) {
        #[cfg(feature = "persist")]
        {
            let Some(store) = self.records.store() else {
                return;
            };
            // A bounded prefix, and never the whole queue. Every entry is one
            // `fsync`, so taking all of them puts the entire backlog on
            // whichever caller happens to be first — measured at **8770 ms on
            // one call** against a 1500-record store reopened under narrower
            // bounds, where `Ledger::new` had already queued 2616 entries.
            //
            // From the front. Taking from the tail is O(k) too, and it starves:
            // the failure path below returns what it could not write to the
            // back, so under sustained traffic the front would never be reached
            // and its records would stay on the disk for the life of the
            // process.
            let batch: Vec<_> = {
                let mut guard = self.lock();
                let take = guard.pending.len().min(DRAIN_PER_CLAIM);
                guard.pending.drain(..take).collect()
            };
            if !batch.is_empty() {
                let left = store
                    .reconcile(batch, |peer, key| self.reconcile_for(peer, key))
                    .await;
                if !left.is_empty() {
                    // Back in the queue for the next claim. A store that
                    // refuses one write refuses the next, so retrying the rest
                    // here would turn one broken disk into a loop on the call
                    // path.
                    self.lock().pending.extend(left);
                    return;
                }
            }
            let live = {
                let guard = self.lock();
                guard.held + guard.spent + guard.running
            };
            store.compact_if_worthwhile(live).await;
        }
    }

    /// What the disk should be told about `key`, read off memory right now.
    ///
    /// Called by [`DurableRecords::reconcile`](crate::durable::DurableRecords::reconcile)
    /// while that store's lock is held, which is what orders it against a
    /// concurrent claim. **It reads the current state rather than replaying the
    /// operation that queued the key**, so a key that was evicted and then
    /// claimed again is left to its own claim instead of being erased out from
    /// under a running handler.
    #[cfg(feature = "persist")]
    fn reconcile_for(&self, peer: EndpointId, key: &IdempotencyKey) -> crate::durable::Reconcile {
        use crate::durable::Reconcile;

        let guard = self.lock();
        match guard
            .peers
            .get(&peer)
            .and_then(|bucket| bucket.slots.get(key))
        {
            None => Reconcile::Erase,
            Some(entry) => match &entry.slot {
                Slot::Running(_) => Reconcile::Untouched,
                settled => Reconcile::Write(crate::durable::encode_slot(
                    settled,
                    entry.fingerprint,
                    self.contract_version,
                )),
            },
        }
    }

    /// Poison recovery rather than propagation, the same call
    /// `demo/src/main.rs` makes about its own state: one bad request should cost
    /// one request. Nothing here can panic while the lock is held, so this arm
    /// is unreachable in practice and must still be typed.
    fn lock(&self) -> std::sync::MutexGuard<'_, Inner> {
        let guard = self.inner.lock().unwrap_or_else(PoisonError::into_inner);
        #[cfg(debug_assertions)]
        guard.agrees();
        guard
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
        let Some(fingerprint) = bucket.slots.get(key).map(|entry| entry.fingerprint) else {
            return;
        };
        let Some(previous) = bucket.slots.insert(
            key.clone(),
            Entry {
                slot,
                fingerprint,
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
        refile(inner, peer);

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

    /// Ask the next [`housekeep`](Ledger::housekeep) to make the disk agree with
    /// memory about this key.
    ///
    /// Beside [`release`](Ledger::release)'s own queueing and for the same
    /// reason: this runs where nothing can await, and the store is told what
    /// memory says now rather than what happened.
    fn queue_reconcile(&self, peer: EndpointId, key: &IdempotencyKey) {
        if self.records.stores_durably() {
            self.lock().pending.push_back((peer, key.clone()));
        }
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
            drop_peer(inner, peer);
        }
        // The key is vacant again, and a store still holding its claim would
        // answer `Interrupted` to a retry of a call that provably never ran —
        // inverting this module's own header, which says a refusal before
        // dispatch leaves the key claimable. Queued rather than written,
        // because this runs under the lock.
        if self.records.stores_durably() {
            inner.pending.push_back((peer, key.clone()));
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
        let durable = self.records.stores_durably();
        // Collected rather than pushed inside the loop, because `inner.pending`
        // and the bucket below are two fields of one `&mut Inner`.
        let mut downgraded = Vec::new();
        let mut moved = false;
        let Some(bucket) = inner.peers.get_mut(&peer) else {
            return;
        };
        loop {
            // Peeked by reference. The common case is a front record still
            // inside its budget, and an owned key is needed only to move one
            // between the queues — so cloning to reach the `break` below spent
            // an allocation on every claim that expired nothing at all.
            let freed = match bucket.held.front() {
                None => break,
                Some(key) => match bucket.slots.get(key) {
                    None => None,
                    Some(entry) if now.saturating_duration_since(entry.at) < ttl => break,
                    Some(entry) => Some(entry.slot.held_bytes()),
                },
            };
            let Some(key) = bucket.held.pop_front() else {
                break;
            };
            // Here and not at the bottom of the loop. The `continue` below also
            // pops, so a flag set only on the full move would let a changed
            // queue skip the `refile` — the exact drift `Inner::agrees` exists
            // to report. The flag means "the queue changed", not "a key
            // completed the move".
            inner.held = inner.held.saturating_sub(1);
            moved = true;
            let Some(freed) = freed else {
                continue;
            };
            if let Some(entry) = bucket.slots.get_mut(&key) {
                entry.slot = Slot::Forgotten;
            }
            if durable {
                downgraded.push(key.clone());
            }
            bucket.spent.push_back(key);
            inner.bytes = inner.bytes.saturating_sub(freed);
            inner.spent += 1;
        }
        // One call for the whole loop: it moves keys between two queues of the
        // same peer, and nothing reads the indexes until it returns.
        //
        // Conditional, because the common case is a front record still inside
        // its budget and this runs on every `claim` and every `key_state`. A
        // `refile` that files what is already filed costs a `HashMap` lookup
        // and two comparisons, which is the same hot path two allocations were
        // just taken off.
        if moved {
            refile(inner, peer);
        }
        // A disk tombstone retains 34 fixed bytes: binding/version tag,
        // request-and-authority commitment and state. Its former reply is up to
        // `frame_bytes`. Without this the store would keep every expired reply
        // until its key left the tombstone pool, so `pool_bytes` would bind
        // memory and nothing else — and a restart would read `Done` for a key
        // that had already read `Forgotten`.
        inner
            .pending
            .extend(downgraded.into_iter().map(|key| (peer, key)));
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
        while inner.held > self.limits.pool_records.get()
            || inner.bytes > self.limits.pool_bytes.get()
        {
            let Some(peer) = inner.by_held.last().map(|&(_, peer)| peer) else {
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
                    bucket.spent.push_back(key.clone());
                    freed
                }
                None => 0,
            };
            refile(inner, peer);
            inner.bytes = inner.bytes.saturating_sub(freed);
            inner.held = inner.held.saturating_sub(1);
            inner.spent += 1;
            // The reply leaves the disk when it leaves memory, for the reason
            // `expire` gives: otherwise `pool_bytes` bounds one tier of two.
            if self.records.stores_durably() {
                inner.pending.push_back((peer, key));
            }
        }

        while inner.spent > self.limits.tombstones.get() {
            let Some(peer) = inner.by_spent.last().map(|&(_, peer)| peer) else {
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
                drop_peer(inner, peer);
            } else {
                refile(inner, peer);
            }
            // The one place a key leaves this ledger entirely. A store that
            // kept it would grow for the life of the disk, so the removal is
            // queued for the next claim, which is the next code that can await.
            if self.records.stores_durably() {
                inner.pending.push_back((peer, key));
            }
        }
    }
}

/// Put back what a store held when this process opened it.
///
/// Every entry arrives already decoded and validated by
/// [`DurableRecords::open`](crate::durable::DurableRecords::open), so there is
/// nothing to refuse here and nothing to skip.
///
/// **Every recovered entry is stamped `now`**, because [`Instant`] measures
/// this process's uptime and a stored one would mean nothing here. So a restart
/// restarts the [`ttl`](Limits::ttl) clock, and a record that was nine minutes
/// old gets a fresh ten. That is a longer at-most-once window rather than a
/// shorter one, which is the direction to be wrong in, and it is the only
/// honest reading of a monotonic clock across two processes.
#[cfg(feature = "persist")]
fn recover(stored: Vec<crate::durable::Recovered>) -> Inner {
    let mut inner = Inner::default();
    let now = Instant::now();
    for (peer, key, fingerprint, slot) in stored {
        let holds_reply = matches!(slot, Slot::Done(_));
        let bytes = slot.held_bytes();
        let bucket = inner.peers.entry(peer).or_default();
        // The port hands back one entry per key, so this cannot fire. Checked
        // rather than assumed, because what it would cost is a counter that
        // disagrees with the map and an `evict` that can never reach its bound.
        if bucket.slots.contains_key(&key) {
            continue;
        }
        bucket.slots.insert(
            key.clone(),
            Entry {
                slot,
                fingerprint,
                at: now,
            },
        );
        if holds_reply {
            bucket.held.push_back(key);
            inner.held += 1;
            inner.bytes += bytes;
        } else {
            bucket.spent.push_back(key);
            inner.spent += 1;
        }
    }
    // The one path into a bucket that does not go through `claim` or `settle`,
    // so it is the one path no `refile` reaches. `Ledger::new` runs `evict`
    // over this set the moment it is built, and `evict` reads nothing but the
    // indexes — an unfilled index there evicts nothing at all, and every bound
    // this build configures would be silently unenforced on a recovered ledger.
    let peers: Vec<EndpointId> = inner.peers.keys().copied().collect();
    for peer in peers {
        refile(&mut inner, peer);
    }
    inner
}

/// Put this peer's queue lengths back in the indexes.
///
/// The one place [`Inner::by_held`] and [`Inner::by_spent`] are written for a
/// peer that still exists. Call it after **any** change to `held.len()` or
/// `spent.len()`; adjacent changes inside one loop iteration can share a call,
/// but the index must agree with the queues before anything reads it again —
/// and `evict` reads it on its very next turn round the loop.
///
/// Idempotent, so a call one line too many costs a comparison and nothing else.
fn refile(inner: &mut Inner, peer: EndpointId) {
    let Some(bucket) = inner.peers.get_mut(&peer) else {
        return;
    };
    let held = bucket.held.len();
    let spent = bucket.spent.len();
    let filed_held = std::mem::replace(&mut bucket.filed_held, held);
    let filed_spent = std::mem::replace(&mut bucket.filed_spent, spent);
    if filed_held != held {
        inner.by_held.remove(&(filed_held, peer));
        if held > 0 {
            inner.by_held.insert((held, peer));
        }
    }
    if filed_spent != spent {
        inner.by_spent.remove(&(filed_spent, peer));
        if spent > 0 {
            inner.by_spent.insert((spent, peer));
        }
    }
}

/// Drop a peer and its index entries in one move.
///
/// The one way a bucket leaves [`Inner::peers`]. A bare `peers.remove` cannot
/// be right: the lengths the indexes are keyed by live in the bucket, so once
/// it is gone there is nothing left to remove them with.
fn drop_peer(inner: &mut Inner, peer: EndpointId) {
    let Some(bucket) = inner.peers.remove(&peer) else {
        return;
    };
    inner.by_held.remove(&(bucket.filed_held, peer));
    inner.by_spent.remove(&(bucket.filed_spent, peer));
}

/// One refusal, built in one place, so every cap reports the same way.
/// `NonZeroUsize` because every caller passes a [`Limits`] field, and taking the
/// field's own type keeps a plain count from arriving here by mistake.
///
/// The `u32` clamp used to be unreachable: every limit was a `pub const` of at
/// most 4096. [`Limits`]'s setters make it live, so the four methods whose value
/// reaches this refusal say in their own rustdoc that a value above [`u32::MAX`]
/// is named as 4294967295.
fn over(limit: NonZeroUsize) -> RemoteError {
    IdempotencyRefusal::TooManyRunning {
        limit: u32::try_from(limit.get()).unwrap_or(u32::MAX),
    }
    .into()
}

/// The same refusal, on the path that answers a claim rather than a wait.
fn refused_over(limit: NonZeroUsize) -> Claim {
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
    fingerprint: [u8; 32],
    ledger: Arc<Ledger>,
    peer: EndpointId,
    key: IdempotencyKey,
    settled: bool,
    /// How far the call got.
    ///
    /// `None` until `serve_one` calls [`watching`](Self::watching) with the
    /// call's own.
    ///
    /// **An absent marker reads `NotDispatched`, and that is item 2 of the
    /// ledger audit.** It used to settle `Interrupted` on drop, so a guard
    /// dropped between the claim and the dispatch — which is exactly what an
    /// abort during the durable claim write is — refused a retry of a call that
    /// provably never ran. A guard nobody has watched yet is a guard nothing
    /// has been dispatched under, so the default belongs on that side.
    ///
    /// The default is written in [`Drop`] rather than by handing `claim` a
    /// fresh `Progress` to hold, which is how this was first fixed:
    /// `Progress` is an `Arc<AtomicU8>`, so that spent one allocation on every
    /// keyed call to name a state, and `serve_one` overwrote it unused a moment
    /// later.
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
    ///
    /// # Errors
    ///
    /// [`IdempotencyRefusal::Unrecordable`] when a durable store refused the
    /// record. The caller must not be sent its reply then, because a reply this
    /// ledger cannot replay is a reply the caller will ask for again.
    /// `Records::Memory` and `Records::None` cannot fail and this is always
    /// `Ok` for them.
    ///
    /// # `before_publish`, and the one moment it runs
    ///
    /// Called once, **after** the store has accepted the record and **before** the
    /// slot is published, which is the instant attaching retries are woken and a
    /// replay can be written. It exists for the capability a reply carries: a blob
    /// grant has to exist before anybody can be handed the address, and installing
    /// it after the publish leaves a window in which an attacher on another worker
    /// replays a new blob reply and fetches before the grant is there. It is not
    /// called when the store refuses the record, so a reply this ledger cannot
    /// replay grants nothing.
    pub(crate) async fn record(
        mut self,
        frame: &Bytes,
        carries_body: bool,
        before_publish: impl FnOnce(),
    ) -> Result<(), RemoteError> {
        // Three causes, one slot, and that is the whole of Wave 2's wire cost:
        // nothing. `Slot::Unrecordable` reads "the handler ran, and its reply is
        // not something this ledger can hold", which is true word for word of a
        // reply that carried a payload, of one over the frame ceiling, and of
        // a service configured to hold none.
        //
        // The frame ceiling is the one this reads, and it is NOT the pool. A reply
        // under the ceiling and over the pool is recordable here and evicted a
        // moment later, so it reads Forgotten rather than Unrecordable. Before the
        // two bounds were split they were one number, which made the difference
        // invisible.
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
        let recordable = self.ledger.records.holds_replies()
            && !carries_body
            && frame.len() <= self.ledger.limits.frame_bytes.get();
        let slot = if recordable {
            Slot::Done(frame.clone())
        } else {
            Slot::Unrecordable
        };

        // **The disk is told before the caller is.** Storing after the reply
        // narrows the window and does not close it: a record lost after its
        // reply went out puts the caller back on a `Fresh` key, and the work
        // runs a second time. That is the failure this whole setting exists to
        // remove, so the await is here rather than after the write.
        //
        // **`settled` is set after this await and not before it.** A serving
        // task can be aborted while parked here — `rpc.rs` shuts down every
        // in-flight call when its connection ends — and that drops this guard.
        // A guard that had already set the flag returns from `Drop` having
        // settled nothing, so the slot stays [`Slot::Running`], which is never
        // evicted. The key would answer `InFlight` for the life of the process,
        // and after `running` such aborts the service refuses every keyed call.
        //
        // Bound rather than written one line lower, because the await sits
        // inside the `if let` head below and there is no position between them.
        let stored = self
            .ledger
            .write_settled(self.peer, &self.key, &slot, self.fingerprint)
            .await;
        self.settled = true;
        if let Err(refusal) = stored {
            // It RAN and produced a reply this ledger cannot hold, which is
            // `Slot::Unrecordable` word for word. NOT `Slot::Interrupted`,
            // whose own doc says the handler was entered and no reply came out
            // — here one came out and is being discarded.
            self.ledger.settle(self.peer, &self.key, Slot::Unrecordable);
            // The disk still holds `TAG_CLAIMED`, which recovers as
            // `Interrupted`, so leaving it there would give a live retry and a
            // post-restart retry two different answers for one event. Queued
            // rather than written: the write that just failed is the one that
            // would have to be retried, and the next `housekeep` is where a
            // store gets its next chance. If the process dies before then the
            // disk still says `Interrupted`, which is the conservative
            // direction to be wrong in.
            self.ledger.queue_reconcile(self.peer, &self.key);
            return Err(refusal);
        }
        before_publish();
        self.ledger.settle(self.peer, &self.key, slot);
        Ok(())
    }

    /// Give the key back, because the call is not going to run.
    ///
    /// The one caller is [`Ledger::claim_durably`], on the path where the store
    /// refused the claim it had just taken in memory. `Drop` cannot do this: a
    /// dropped guard with nothing watching it settles
    /// [`Interrupted`](Slot::Interrupted), which would refuse a retry of a key
    /// under which nothing ever happened.
    pub(crate) fn give_back(mut self) {
        self.settled = true;
        self.ledger.release(self.peer, &self.key);
    }

    /// Give the key back, and wait for the store to agree before returning.
    ///
    /// The caller is about to be told that nothing ran. A queued removal is not
    /// enough here, and that is not a preference: a refused call is often the
    /// last thing a process does, and a removal drained by "the next claim" is
    /// a removal that never happens. The disk would keep the claim, and the
    /// retry this refusal invites would read
    /// [`Interrupted`](KeyState::Interrupted) forever.
    ///
    /// [`Drop`] still cannot do this, so a guard released by an **aborted**
    /// task leaves its removal queued and its key reading `Interrupted` until
    /// the next claim drains it. That is the conservative direction — refused
    /// rather than run twice — and it is the residue of `Drop` not being able
    /// to await, not a second policy.
    pub(crate) async fn give_back_durably(mut self) {
        self.settled = true;
        self.ledger.release(self.peer, &self.key);
        self.ledger.housekeep().await;
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
        let reached = self
            .progress
            .as_ref()
            .map_or(Reached::NotDispatched, Progress::reached);
        if reached == Reached::NotDispatched {
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

    /// Every bound a test sets is a non-zero literal written three lines away.
    fn nz(n: usize) -> NonZeroUsize {
        NonZeroUsize::new(n).expect("no test sets a bound to zero")
    }

    /// A ledger holding `max` records. Named `holding` rather than `records`,
    /// which now means [`Records`] two hundred lines up.
    ///
    /// Built through the **setter**, not a struct literal, and so is every
    /// `Limits` in this module. `mod tests` is a child of the defining module, so
    /// a literal would still compile after the fields went private — and it would
    /// leave all ten setters unexercised, which makes `pool_bytes` writing
    /// `frame_bytes` a defect the whole suite stays green through.
    fn holding(max: usize) -> Limits {
        Limits::default().pool_records(nz(max))
    }

    /// The ordinary ledger every test below this line wants: memory-backed, with
    /// the shipped bounds.
    fn ledger() -> Arc<Ledger> {
        Ledger::new(
            Limits::default(),
            Records::Memory,
            crate::version::RecordCodec::testing(),
        )
    }

    /// The same, with the bounds moved. One test door rather than two, so
    /// `Records::Memory` is stated once for every test that is not about it.
    fn bounded(limits: Limits) -> Arc<Ledger> {
        Ledger::new(
            limits,
            Records::Memory,
            crate::version::RecordCodec::testing(),
        )
    }

    /// A ledger that claims keys and records no replies.
    fn recordless() -> Arc<Ledger> {
        Ledger::new(
            Limits::default(),
            Records::None,
            crate::version::RecordCodec::testing(),
        )
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

    /// [`Guard::record`], driven to completion with no runtime at all.
    ///
    /// Every ledger in this module is [`Records::Memory`] or [`Records::None`],
    /// and **neither of those can park**: the only await inside `record` is a
    /// durable store's, and these have none. So the tests here stay `#[test]`
    /// and keep saying what they are about, instead of forty-five of them
    /// acquiring a runtime to observe a future that is ready on its first poll.
    ///
    /// That is also an assertion rather than a convenience. `Poll::Pending`
    /// below means a memory ledger has started awaiting something, which is a
    /// latency regression on every keyed call in the crate and would otherwise
    /// be invisible — the tests would simply get slower.
    fn settle(guard: Guard, frame: &Bytes, carries_body: bool) {
        settle_then(guard, frame, carries_body, || {});
    }

    /// [`settle`], with the hook [`Guard::record`] runs between the store and the
    /// publish.
    fn settle_then(guard: Guard, frame: &Bytes, carries_body: bool, hook: impl FnOnce()) {
        use std::future::Future as _;
        use std::task::{Context, Poll, Waker};

        let mut recording = std::pin::pin!(guard.record(frame, carries_body, hook));
        let mut cx = Context::from_waker(Waker::noop());
        match recording.as_mut().poll(&mut cx) {
            Poll::Ready(Ok(())) => {}
            Poll::Ready(Err(refusal)) => {
                panic!("a ledger with no store refused a record: {refusal}")
            }
            Poll::Pending => panic!("a ledger with no store parked while recording"),
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
        /// Made red by `scripts/break-once.sh` making `Keyable::accepts`' `Only`
        /// arm answer `true` unconditionally — its `contains` call ORed with `true`.
        /// Watched: `2 passed; 3 failed` of this module's five, the three being
        /// `every_variant_accepts_and_refuses_what_it_says`,
        /// `an_empty_only_accepts_nothing_and_still_needs_a_ledger` and
        /// `a_snake_case_name_matches_nothing`. Three tests catching one edit is the
        /// intended overlap: each states a different consequence of `Only` meaning
        /// what it says. Over the wire the same edit reddens
        /// `a_key_on_a_method_the_service_does_not_key_is_refused`,
        /// `a_keyed_actor_service_applies_the_same_policy` and
        /// `a_refused_key_never_reaches_the_ledger`.
        ///
        /// Made red a second way, on the other end of the sweep, by making the `Any`
        /// arm answer `false`. Watched: `assertion failed:
        /// Keyable::Any.accepts("Reserve")`, `3 passed; 2 failed`, the second being
        /// `the_permissive_policy_keys_every_method`. Whole-lib it also reddens
        /// `rpc::tests::an_attacher_over_the_cap_is_refused_before_it_parks` and
        /// `rpc::tests::a_parked_attacher_keeps_its_place_for_the_whole_wait`, which
        /// build their `Keys` from `Keyable::Any`.
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
        /// Made red by `scripts/break-once.sh` making `Keyable::accepts`' `None` arm
        /// answer `true`. Watched: `assertion failed:
        /// !Keyable::None.accepts("Reserve")`, `4 passed; 1 failed` in this module.
        /// No other test in this module notices, which is why this one exists — and
        /// the claim stops at this module on purpose, because a count or a
        /// "the only one" reaching further is a value this file does not control.
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
        /// Made red by `scripts/break-once.sh` making `Keyable::accepts`' `Any` arm
        /// answer `false`. Watched: `assertion failed:
        /// Keyable::Any.accepts("Reserve")`, `3 passed; 2 failed` in this module.
        /// See the sweep above for what else that edit reaches.
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

    /// What a durable write leaves behind when it does not succeed — because
    /// the serving task was **aborted** while it was parked, or because the
    /// store **refused** it.
    ///
    /// A peer disconnect aborts every in-flight call, so the first two run in
    /// production the first time a caller's connection drops mid-call. Neither
    /// can be reached with a store that answers: the abort has to land while a
    /// write is still parked, so `Stalling` never answers one.
    #[cfg(feature = "persist")]
    mod aborts {
        use std::future::Future;
        use std::task::{Context, Poll, Waker};

        use keylink_persist::{Entries, EntryKey, Held, StateId};

        use super::*;

        /// A store that accepts a claim, holds nothing, and never finishes a
        /// write.
        struct Stalling;

        struct Stalled;

        impl Entries for Stalling {
            type Held = Stalled;

            async fn hold(&self, _id: &StateId) -> keylink_persist::Result<Stalled> {
                Ok(Stalled)
            }
        }

        impl Held for Stalled {
            async fn load(&self) -> keylink_persist::Result<Vec<(EntryKey, Vec<u8>)>> {
                Ok(Vec::new())
            }

            async fn put(&mut self, _key: &EntryKey, _bytes: &[u8]) -> keylink_persist::Result<()> {
                std::future::pending().await
            }

            async fn remove(&mut self, _key: &EntryKey) -> keylink_persist::Result<()> {
                std::future::pending().await
            }

            async fn compact(&mut self) -> keylink_persist::Result<()> {
                Ok(())
            }
        }

        /// A future polled once and dropped, which is what an abort does.
        ///
        /// It asserts the poll parked, so a store that quietly answered would
        /// fail here rather than turn the test below green for the wrong
        /// reason.
        fn abort<F: Future>(future: F) {
            let mut future = std::pin::pin!(future);
            let mut cx = Context::from_waker(Waker::noop());
            assert!(
                future.as_mut().poll(&mut cx).is_pending(),
                "the store answered, so nothing was aborted mid-write"
            );
        }

        /// A future driven to completion, for the parts of the setup that do
        /// answer.
        pub(super) fn now<F: Future>(future: F) -> F::Output {
            let mut future = std::pin::pin!(future);
            let mut cx = Context::from_waker(Waker::noop());
            match future.as_mut().poll(&mut cx) {
                Poll::Ready(value) => value,
                Poll::Pending => panic!("the setup parked"),
            }
        }

        /// A store that accepts a claim, holds nothing, and refuses every write.
        struct Refusing;

        struct Refused;

        impl Entries for Refusing {
            type Held = Refused;

            async fn hold(&self, _id: &StateId) -> keylink_persist::Result<Refused> {
                Ok(Refused)
            }
        }

        impl Held for Refused {
            async fn load(&self) -> keylink_persist::Result<Vec<(EntryKey, Vec<u8>)>> {
                Ok(Vec::new())
            }

            async fn put(&mut self, _key: &EntryKey, _bytes: &[u8]) -> keylink_persist::Result<()> {
                Err(keylink_persist::Error::TooLarge {
                    offered: 1,
                    limit: 0,
                })
            }

            async fn remove(&mut self, _key: &EntryKey) -> keylink_persist::Result<()> {
                Err(keylink_persist::Error::TooLarge {
                    offered: 1,
                    limit: 0,
                })
            }

            async fn compact(&mut self) -> keylink_persist::Result<()> {
                Ok(())
            }
        }

        pub(super) fn ledger_on<E: Entries>(store: &E) -> Arc<Ledger> {
            let id = StateId::new("test/ledger").expect("a valid id");
            let records = now(crate::durable::DurableRecords::open(store, &id))
                .expect("nobody else holds this ledger");
            Ledger::new(
                Limits::default(),
                Records::Durable(records),
                crate::version::RecordCodec::testing(),
            )
        }

        fn stalling() -> Arc<Ledger> {
            ledger_on(&Stalling)
        }

        pub(super) fn dispatched() -> Progress {
            let progress = Progress::new();
            progress.dispatched();
            progress
        }

        /// The handler ran, the record write parked, and the task was killed.
        /// The key must not be left claimed forever.
        #[test]
        fn an_abort_during_the_settle_does_not_leak_a_running_slot() {
            let ledger = stalling();
            // Dispatched on purpose, and not the marker `claim` builds: this
            // test is about the call that DID enter the handler, and a
            // `NotDispatched` guard releases instead, which would be green for
            // the wrong reason.
            let claimed = guard(ledger.claim(peer(A), key("k"), [0; 32])).watching(dispatched());
            abort(claimed.record(&frame(16), false, || {}));

            assert_eq!(ledger.key_state(peer(A), &key("k")), KeyState::Interrupted);
        }

        /// Nothing ran: the claim write parked and the task was killed before
        /// the handler was ever entered. The key must be claimable, which is
        /// the same answer `claim_durably`'s own error path already gives.
        #[test]
        fn an_abort_during_the_claim_write_leaves_the_key_claimable() {
            let ledger = stalling();
            abort(ledger.claim_durably(peer(A), key("k"), [0; 32]));

            assert_eq!(ledger.key_state(peer(A), &key("k")), KeyState::Fresh);
        }

        /// A store that refuses the claim refuses the call, and the caller is
        /// told the one thing it can act on: nothing ran.
        ///
        /// The refusal used to be [`RemoteError`]'s untyped variant carrying
        /// that sentence as prose, which no caller can match on.
        #[test]
        fn a_store_that_refuses_the_claim_says_nothing_ran() {
            let ledger = ledger_on(&Refusing);
            let claim = now(ledger.claim_durably(peer(A), key("k"), [0; 32]));

            assert_eq!(refusal(claim), IdempotencyRefusal::NotClaimed);
        }
    }

    /// Where [`Guard::record`] runs its hook: after the store has the record, and
    /// before the slot is published.
    ///
    /// The hook is the blob grant, and the order is the whole property. Published
    /// first, a retry attached to this call is woken and handed the new reply while
    /// the grant is still to come, and the fetch it makes next is refused. Run
    /// before the store, a record the store then refuses would have granted an
    /// address that no reply carries.
    #[cfg(feature = "persist")]
    mod publication {
        use std::sync::atomic::{AtomicUsize, Ordering};

        use keylink_persist::{Entries, EntryKey, Held, StateId};

        use super::aborts::{dispatched, ledger_on, now};
        use super::*;

        /// A store that accepts the first write, which is the claim, and refuses
        /// every write after it, which is the settle.
        struct ClaimsThenRefuses;

        struct Claimed(AtomicUsize);

        impl Entries for ClaimsThenRefuses {
            type Held = Claimed;

            async fn hold(&self, _id: &StateId) -> keylink_persist::Result<Claimed> {
                Ok(Claimed(AtomicUsize::new(0)))
            }
        }

        impl Held for Claimed {
            async fn load(&self) -> keylink_persist::Result<Vec<(EntryKey, Vec<u8>)>> {
                Ok(Vec::new())
            }

            async fn put(&mut self, _key: &EntryKey, _bytes: &[u8]) -> keylink_persist::Result<()> {
                if self.0.fetch_add(1, Ordering::SeqCst) == 0 {
                    Ok(())
                } else {
                    Err(keylink_persist::Error::TooLarge {
                        offered: 1,
                        limit: 0,
                    })
                }
            }

            async fn remove(&mut self, _key: &EntryKey) -> keylink_persist::Result<()> {
                Ok(())
            }

            async fn compact(&mut self) -> keylink_persist::Result<()> {
                Ok(())
            }
        }

        /// The key still reads `Running` when the hook runs, and `Done` once
        /// `record` has returned.
        ///
        /// Made red by `scripts/break-once.sh keylink/src/idempotency.rs`, swapping
        /// the two lines `Guard::record` ends with, the call of `before_publish` and
        /// the ledger's `settle` of the slot (a two-line edit that cannot quote its
        /// anchor without making it appear twice, so it is described and not
        /// registered), which publishes first. Watched: `the hook ran before the
        /// slot was published`, `left: Some(Done)`.
        #[test]
        fn the_hook_runs_before_the_slot_is_published() {
            let ledger = bounded(Limits::default());
            let claimed = guard(ledger.claim(peer(A), key("k"), [0; 32]));
            let seen = std::cell::Cell::new(None);

            settle_then(claimed, &frame(16), false, || {
                seen.set(Some(ledger.key_state(peer(A), &key("k"))));
            });

            assert_eq!(
                seen.get(),
                Some(KeyState::Running),
                "the hook ran before the slot was published"
            );
            assert_eq!(ledger.key_state(peer(A), &key("k")), KeyState::Done);
        }

        /// A record the store refuses runs no hook, so the reply it would have
        /// carried grants nothing.
        ///
        /// The claim is the first write and is accepted; the settle is the second
        /// and is refused with `TooLarge`, which `record` reports as
        /// `Unrecordable`. Made red by calling the hook on the refusal path, just
        /// before that path returns its `Err` (a two-line edit, described and not
        /// registered), which is what a hook run ahead of the store would do for a
        /// reply no caller is sent. Watched: `a refused record must grant nothing`.
        #[test]
        fn a_record_the_store_refuses_runs_no_hook() {
            let ledger = ledger_on(&ClaimsThenRefuses);
            let claimed = match now(ledger.claim_durably(peer(A), key("k"), [0; 32])) {
                Claim::Fresh(guard) => guard.watching(dispatched()),
                other => panic!("expected a fresh claim, got {other:?}"),
            };
            let ran = std::cell::Cell::new(false);

            let result = now(claimed.record(&frame(16), false, || ran.set(true)));

            assert!(result.is_err(), "the store refused the record");
            assert!(!ran.get(), "a refused record must grant nothing");
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
        /// Made red by `scripts/break-once.sh` forcing `Guard::record`'s
        /// `recordable` flag to ignore the retention switch — its `matches!` on
        /// `self.ledger.records` replaced by `true`. Watched:
        /// `panicked at .., expected an idempotency refusal, got
        /// Replay(b"\x07\x07\x07\x07\x07\x07\x07\x07\x07\x07\x07\x07\x07\x07\x07\x07")`,
        /// from this module's `refusal(..)` helper.
        ///
        /// That one edit also reddens
        /// `a_retention_refusal_is_told_apart_from_an_eviction`,
        /// `a_retention_settle_is_a_tombstone_not_a_record`,
        /// `a_retention_settle_reports_the_same_state_it_refuses_with` and
        /// `records_none_shortens_the_at_most_once_window`. Five tests for one edit
        /// is the intended overlap — each states a different consequence of the
        /// switch being honoured.
        #[test]
        fn records_none_settles_the_key_and_holds_no_reply() {
            let ledger = recordless();
            settle(
                guard(ledger.claim(peer(A), key("k"), [0; 32])),
                &frame(16),
                false,
            );

            assert_eq!(
                refusal(ledger.claim(peer(A), key("k"), [0; 32])),
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
            let ledger = Ledger::new(
                holding(1),
                Records::None,
                crate::version::RecordCodec::testing(),
            );
            settle(
                guard(ledger.claim(peer(A), key("kept"), [0; 32])),
                &frame(8),
                false,
            );

            assert_eq!(
                refusal(ledger.claim(peer(A), key("kept"), [0; 32])),
                IdempotencyRefusal::Unrecordable,
                "a configured retention policy is not an eviction"
            );

            let evicting = Ledger::new(
                holding(1),
                Records::Memory,
                crate::version::RecordCodec::testing(),
            );
            settle(
                guard(evicting.claim(peer(A), key("old"), [0; 32])),
                &frame(8),
                false,
            );
            settle(
                guard(evicting.claim(peer(A), key("new"), [0; 32])),
                &frame(8),
                false,
            );
            assert_eq!(
                refusal(evicting.claim(peer(A), key("old"), [0; 32])),
                IdempotencyRefusal::Forgotten,
                "and an eviction still says so, or the two names have merged"
            );
        }

        /// The counter side, and the one that says which pool the key landed in. A
        /// retention settle is a **tombstone**, bounded by [`MAX_TOMBSTONES`] rather
        /// than [`MAX_RECORDS`] — the right pool, because a tombstone is what keeps
        /// the work from running twice and has to outlive the reply it never had.
        ///
        /// Made red by `scripts/break-once.sh` forcing `Guard::record`'s
        /// `recordable` flag to ignore the retention switch, its `matches!` replaced
        /// by `true`. Watched: `assertion `left == right` failed: a key with no reply
        /// belongs in the tombstone pool, not the record pool`, `left: 1`,
        /// `right: 0`. See `records_none_settles_the_key_and_holds_no_reply` for the
        /// four other tests that edit reaches.
        #[test]
        fn a_retention_settle_is_a_tombstone_not_a_record() {
            let ledger = recordless();
            settle(
                guard(ledger.claim(peer(A), key("k"), [0; 32])),
                &frame(16),
                false,
            );

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
        /// still be remembered`. It is the FIRST assertion that fires, not the
        /// second, and that is the right way round: with both ledgers recordless the
        /// two arms agree, and the arm that has to notice is the one claiming the
        /// longer window still exists.
        ///
        /// **That break is deliberately wide.** Forcing every ledger recordless
        /// reddens thirteen tests, across `mod bounds`, `mod claim`, `mod state` and
        /// `mod ttl`, because every replay assertion in the crate rests on a
        /// recording ledger; in this module it also takes `memory_records_and_replays`
        /// and `a_retention_refusal_is_told_apart_from_an_eviction`. Width is the
        /// finding rather than a weakness — the switch is the only thing between
        /// `Records::Memory` and a ledger that answers nothing.
        #[test]
        fn records_none_shortens_the_at_most_once_window() {
            let bounds = Limits::default().pool_records(nz(4)).tombstones(nz(4));
            let settle_eight = |ledger: &Arc<Ledger>| {
                for n in 0..8 {
                    settle(
                        guard(ledger.claim(peer(A), key(&format!("k{n}")), [0; 32])),
                        &frame(8),
                        false,
                    );
                }
            };

            // Cloned rather than copied: `Limits` is deliberately not `Copy`, so a
            // later field can hold a port behind an `Arc`.
            let recording = Ledger::new(
                bounds.clone(),
                Records::Memory,
                crate::version::RecordCodec::testing(),
            );
            settle_eight(&recording);
            let recordless = Ledger::new(
                bounds,
                Records::None,
                crate::version::RecordCodec::testing(),
            );
            settle_eight(&recordless);

            assert!(
                !matches!(
                    recording.claim(peer(A), key("k0"), [0; 32]),
                    Claim::Fresh(_)
                ),
                "under Memory the oldest key must still be remembered"
            );
            assert!(
                matches!(
                    recordless.claim(peer(A), key("k0"), [0; 32]),
                    Claim::Fresh(_)
                ),
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
        /// Made red by `scripts/break-once.sh` settling `Guard::record`'s slot as
        /// `Slot::Interrupted` — the other spent state, and the tempting one, since
        /// nothing was recorded. Described rather than quoted because the record and
        /// the line it breaks are in one file, where a quoted anchor is what
        /// `break-once.sh` counts twice; the **replacement** is quoted, which is
        /// always safe, since the harness never searches it.
        /// Watched: `assertion `left == right` failed`, `left: Interrupted`,
        /// `right: Unrecordable`. It is the wrong answer: `Interrupted` means whether
        /// the work took effect is unknown, and here it is known to have completed.
        #[test]
        fn a_retention_settle_reports_the_same_state_it_refuses_with() {
            let ledger = recordless();
            settle(
                guard(ledger.claim(peer(A), key("k"), [0; 32])),
                &frame(16),
                false,
            );

            assert_eq!(ledger.key_state(peer(A), &key("k")), KeyState::Unrecordable);
        }

        /// `Records::None` must not touch the paths that decide whether the work
        /// ran. A guard dropped after dispatch is still `Interrupted`, and one
        /// dropped with nothing dispatched is still claimable — the two halves of
        /// RFC-05 §2, asked of a ledger that records no replies.
        ///
        /// Made red by `scripts/break-once.sh` making `Guard::drop`'s
        /// `Reached::NotDispatched` test unreachable. Search, **with its
        /// indentation**, which is what keeps it unique against this quote:
        /// `if reached == Reached::NotDispatched {`
        /// and replace with `if false {`, so every dropped guard settles rather than
        /// releasing. The replacement is quoted too, which is always safe.
        ///
        /// This record and that condition are in one file, and that is not a reason to
        /// describe rather than quote — first measured at `7a147c0` and re-measured
        /// here after item 2 of the ledger audit moved the line: the indented form
        /// counts **1** with this quote present while the bare form counts **2**. The
        /// indentation is the whole mechanism. Watched:
        /// `panicked at .., a key nothing ran under must still be claimable`. It also
        /// reddens
        /// `claim::a_guard_dropped_with_nothing_dispatched_leaves_the_key_claimable`,
        /// which is the test that owns that rule; this one asks it of a ledger that
        /// records no replies.
        #[test]
        fn records_none_changes_nothing_about_who_settles() {
            let ledger = recordless();
            // Watched with a DISPATCHED marker, which is what "entered" means.
            // A guard out of `claim` reads `NotDispatched` and releases, so an
            // unwatched drop here would be asking a different question.
            let entered = Progress::new();
            entered.dispatched();
            drop(guard(ledger.claim(peer(A), key("entered"), [0; 32])).watching(entered));
            assert_eq!(
                refusal(ledger.claim(peer(A), key("entered"), [0; 32])),
                IdempotencyRefusal::Interrupted,
                "entered and unfinished is still unknown"
            );

            drop(guard(ledger.claim(peer(A), key("nothing"), [0; 32])).watching(Progress::new()));
            assert!(
                matches!(
                    ledger.claim(peer(A), key("nothing"), [0; 32]),
                    Claim::Fresh(_)
                ),
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
            let ledger = Ledger::new(
                Limits::default(),
                Records::Memory,
                crate::version::RecordCodec::testing(),
            );
            settle(
                guard(ledger.claim(peer(A), key("k"), [0; 32])),
                &frame(16),
                false,
            );
            assert_eq!(
                recorded(ledger.claim(peer(A), key("k"), [0; 32])),
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
                ledger.claim(peer(A), key("first"), [0; 32]),
                Claim::Fresh(_)
            ));
        }

        #[test]
        fn a_recorded_key_replays_the_same_bytes() {
            let ledger = ledger();
            settle(
                guard(ledger.claim(peer(A), key("k"), [0; 32])),
                &frame(16),
                false,
            );

            assert_eq!(
                recorded(ledger.claim(peer(A), key("k"), [0; 32])),
                frame(16)
            );
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
            settle(
                guard(ledger.claim(peer(A), key("k"), [0; 32])),
                &original,
                false,
            );

            let replayed = recorded(ledger.claim(peer(A), key("k"), [0; 32]));
            assert_eq!(
                replayed, original,
                "the recorded frame must be written back verbatim"
            );
            assert_eq!(replayed.len(), 777);
        }

        #[test]
        fn a_different_key_is_fresh_even_after_one_is_recorded() {
            let ledger = ledger();
            settle(
                guard(ledger.claim(peer(A), key("one"), [0; 32])),
                &frame(16),
                false,
            );

            assert!(matches!(
                ledger.claim(peer(A), key("two"), [0; 32]),
                Claim::Fresh(_)
            ));
        }

        /// The property that keeps one peer from reading another's reply by
        /// guessing a key. If this ever goes green with a shared namespace, the
        /// ledger has become an information leak.
        #[test]
        fn two_peers_using_the_same_key_do_not_collide() {
            let ledger = ledger();
            settle(
                guard(ledger.claim(peer(A), key("shared"), [0; 32])),
                &frame(16),
                false,
            );

            assert!(
                matches!(
                    ledger.claim(peer(B), key("shared"), [0; 32]),
                    Claim::Fresh(_)
                ),
                "peer B must not be answered from peer A's record"
            );
        }

        #[test]
        fn a_second_claim_while_the_first_runs_is_refused_as_in_flight() {
            let ledger = ledger();
            let _running = guard(ledger.claim(peer(A), key("k"), [0; 32]));

            assert_eq!(
                refusal(ledger.claim(peer(A), key("k"), [0; 32])),
                IdempotencyRefusal::InFlight
            );
        }

        /// A key nothing ever entered goes back to vacant, so the caller's retry
        /// is a first attempt rather than a permanent refusal.
        ///
        /// The marker is written out even though `claim` now builds the same
        /// value, so the test states the rule instead of leaning on a default.
        #[test]
        fn a_guard_dropped_with_nothing_dispatched_leaves_the_key_claimable() {
            let ledger = ledger();
            // A fresh marker says `NotDispatched`, which is what a layer that
            // refused without calling through leaves behind.
            drop(guard(ledger.claim(peer(A), key("k"), [0; 32])).watching(Progress::new()));

            assert!(
                matches!(ledger.claim(peer(A), key("k"), [0; 32]), Claim::Fresh(_)),
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
            drop(guard(ledger.claim(peer(A), key("k"), [0; 32])).watching(progress));

            assert_eq!(
                refusal(ledger.claim(peer(A), key("k"), [0; 32])),
                IdempotencyRefusal::Interrupted
            );
        }

        #[test]
        fn a_reply_that_carries_a_body_is_not_replayed() {
            let ledger = ledger();
            settle(
                guard(ledger.claim(peer(A), key("k"), [0; 32])),
                &frame(16),
                true,
            );

            assert_eq!(
                refusal(ledger.claim(peer(A), key("k"), [0; 32])),
                IdempotencyRefusal::Unrecordable
            );
        }

        /// A reply over the frame ceiling is refused on the retry, and the pool has
        /// nothing to do with it.
        ///
        /// **The old rationale here was true only while the two bounds were one
        /// number.** It read *"One reply larger than the whole byte budget cannot
        /// be evicted into fitting, so it must never be stored in the first
        /// place"*, which justifies a per-response check by appealing to the pool.
        /// Once the two split, a reply over the ceiling and under the pool can fit,
        /// so the appeal no longer reaches the conclusion. The ceiling stands on
        /// its own ground instead: it is a permanent property of the reply, and
        /// [`Slot::Unrecordable`] is the state that says so.
        ///
        /// The test is also renamed, because *"the byte budget"* stopped being one
        /// thing. `pool_bytes` is left at its default here on purpose — nothing
        /// about this assertion involves it.
        ///
        /// **This is one of the two guards that hold the setter-to-field mapping**,
        /// which is the defect class a public `Limits` introduces: a setter writing
        /// the wrong field, with the suite green. Made red by
        /// `scripts/break-once.sh` swapping `Limits::frame_bytes`'s body to write the
        /// other field — anchored on `self.frame_bytes = bytes;` indented, which is
        /// unique against this unindented quote. Watched: `panicked at .., assertion
        /// `left == right` failed, left: Forgotten, right: Unrecordable`.
        ///
        /// `Forgotten` is the *right* half of the finding: under the swap the ceiling
        /// keeps its default so the 65-byte frame is recordable, settles `Done`, and
        /// then `evict` sees 65 over a 64-byte pool and downgrades it. So the swap
        /// does not merely fail — it produces exactly the refusal that
        /// `a_reply_under_the_ceiling_and_over_the_pool_is_forgotten_not_unrecordable`
        /// exists to distinguish, which is why the two bounds need separate guards
        /// rather than one.
        #[test]
        fn a_reply_over_the_frame_ceiling_is_not_replayed() {
            let ledger = bounded(Limits::default().frame_bytes(nz(64)));
            settle(
                guard(ledger.claim(peer(A), key("k"), [0; 32])),
                &frame(65),
                false,
            );

            assert_eq!(
                refusal(ledger.claim(peer(A), key("k"), [0; 32])),
                IdempotencyRefusal::Unrecordable
            );
        }

        /// The refusal a reply under the ceiling and over the pool gets, which is
        /// **not** the one above.
        ///
        /// This is the hazard the split creates, so it is the split's to guard.
        /// `Guard::record` writes `Done` or `Unrecordable`; `evict` downgrades
        /// `Done` to `Forgotten`. Those states are exclusive for one key, so a
        /// reply in this window goes `Done` and then `Forgotten` and never touches
        /// `Unrecordable`. No timing is needed: `settle` calls `evict` in the same
        /// lock, so the eviction has already happened when `record` returns.
        #[test]
        fn a_reply_under_the_ceiling_and_over_the_pool_is_forgotten_not_unrecordable() {
            let ledger = bounded(Limits::default().frame_bytes(nz(64)).pool_bytes(nz(8)));
            settle(
                guard(ledger.claim(peer(A), key("k"), [0; 32])),
                &frame(32),
                false,
            );

            assert_eq!(
                refusal(ledger.claim(peer(A), key("k"), [0; 32])),
                IdempotencyRefusal::Forgotten,
                "a frame inside the ceiling is recorded, then evicted by the pool"
            );
        }
    }

    mod bounds {
        use super::*;

        #[test]
        fn the_record_count_stays_inside_its_bound() {
            let ledger = bounded(holding(4));
            for n in 0..64 {
                settle(
                    guard(ledger.claim(peer(A), key(&format!("k{n}")), [0; 32])),
                    &frame(8),
                    false,
                );
            }

            assert_eq!(ledger.lock().held, 4);
        }

        /// `pool_bytes`, not `frame_bytes`: **sixty-four** frames of 16 bytes are each
        /// far inside the ceiling, and what is asserted is the sum they are allowed to
        /// reach together — four of them, once eviction has run.
        ///
        /// This line first said *"sixteen frames"*, against a `for n in 0..64` loop it
        /// sits three lines above. Nothing was red, because 16 and 64 both exceed a
        /// 64-byte budget, and the doc line was **new in this PR** so the figure was
        /// never checked against the code beside it. It became load-bearing the moment
        /// another guard's record started sending readers here.
        ///
        /// **This is the other of the two guards that hold the setter-to-field
        /// mapping.** Made red by `scripts/break-once.sh` swapping
        /// `Limits::pool_bytes`'s body to write the other field — anchored on
        /// `self.pool_bytes = bytes;` indented, unique against this unindented quote.
        /// Watched: `panicked at .., held 1024 bytes over a 64 byte budget`.
        ///
        /// The 1024 is the finding rather than an arbitrary number: under the swap
        /// the pool keeps its 1 MiB default, every 16-byte frame is far under the
        /// ceiling the value landed on, and `pool_records` at 1024 never binds — so
        /// all sixty-four frames are held and nothing evicts. A swap here is
        /// **not** a smaller bound; it is no bound at all.
        #[test]
        fn the_recorded_bytes_stay_inside_their_bound() {
            let ledger = bounded(Limits::default().pool_bytes(nz(64)));
            for n in 0..64 {
                settle(
                    guard(ledger.claim(peer(A), key(&format!("k{n}")), [0; 32])),
                    &frame(16),
                    false,
                );
            }

            assert!(
                ledger.lock().bytes <= 64,
                "held {} bytes over a 64 byte budget",
                ledger.lock().bytes
            );
        }

        /// The **top** of the range for both pool bounds, which their rustdoc argued
        /// only at the zero end.
        ///
        /// At [`NonZeroUsize::MAX`] neither half of `Ledger::evict`'s `while`
        /// condition can be satisfied, so its body never runs: nothing is evicted and
        /// no record is downgraded to a tombstone. What bounds the pool then is `ttl`
        /// alone. This is the quiet end of the range — a bound set to zero refuses
        /// every retry loudly, and a bound nothing can reach reports nothing at all,
        /// because an eviction that does not happen has nothing to say.
        ///
        /// Sixty-four records of sixteen bytes, the same shape as the two guards
        /// above, so the three differ in their bounds and in nothing else.
        ///
        /// Made red by `scripts/break-once.sh` replacing
        /// `while inner.held > self.limits.pool_records.get()` with
        /// `while inner.held > 0`, so eviction runs unconditionally. The literal
        /// carries its eight spaces of indentation and this record's copy has none.
        /// Watched, under `cargo test`:
        /// `assertion `left == right` failed: a bound nothing can reach must evict
        /// nothing`, `left: 0`, `right: 64`.
        ///
        /// **That edit reddens `the_record_count_stays_inside_its_bound` above and
        /// leaves `the_recorded_bytes_stay_inside_their_bound` GREEN.** Measured —
        /// `1 passed; 1 failed` on one run over both — after this record first claimed
        /// it reddened both. The reason is a property of the byte guard rather than of
        /// this one: it asserts `bytes <= 64`, and unconditional eviction empties the
        /// pool, which satisfies an upper bound. **An upper bound cannot see
        /// over-eviction**, so that guard is one-sided by construction and this edit
        /// is invisible to it.
        ///
        /// Worth the sentence because the first filter used to check this,
        /// `stays_inside`, matched only one of the two names — the other reads
        /// `stay_inside` — so a run that looked like it covered both covered one.
        #[test]
        fn a_maximal_pool_evicts_nothing() {
            let ledger = bounded(
                Limits::default()
                    .pool_records(NonZeroUsize::MAX)
                    .pool_bytes(NonZeroUsize::MAX),
            );
            for n in 0..64 {
                settle(
                    guard(ledger.claim(peer(A), key(&format!("k{n}")), [0; 32])),
                    &frame(16),
                    false,
                );
            }

            assert_eq!(
                ledger.lock().held,
                64,
                "a bound nothing can reach must evict nothing"
            );
            assert_eq!(
                ledger.lock().spent,
                0,
                "and must leave no tombstone behind, because nothing was dropped"
            );
            assert_eq!(
                ledger.lock().bytes,
                64 * 16,
                "with every recorded byte still held"
            );
        }

        /// The four-byte fact, pinned as a **fact about the wire** rather than as the
        /// ceiling's value — which is the distinction this branch got wrong once.
        ///
        /// `MAX_HEADER` bounds the postcard payload, and the ledger stores the whole
        /// frame, so a maximal payload arrives as `MAX_HEADER + PREFIX` bytes. That
        /// sum **exceeds the pool**, which is why the ceiling stops at the payload
        /// cap: those four sizes can never be held, so refusing them is permanent
        /// rather than an eviction.
        ///
        /// This lives in `src` rather than beside the pin in `tests/reliable.rs`
        /// because `PREFIX` is `pub(crate)` and an integration test is a separate
        /// crate. That visibility is what this assertion consumes — the constant's
        /// value no longer names `PREFIX`, and a widened visibility with no reader
        /// would be exactly the dead `pub` `CONTRIBUTING.md` argues against.
        ///
        /// Made red by raising the ceiling to include the prefix: change
        /// `MAX_RECORDED_FRAME`'s value from `crate::wire::MAX_HEADER as usize` to
        /// that plus `crate::wire::PREFIX`. Watched: `panicked at .., a maximal frame
        /// exceeds the pool, so the ceiling must stop at the payload cap: ceiling
        /// 1048580, maximal frame 1048584, pool 1048576`.
        #[test]
        fn a_maximal_frame_exceeds_the_pool_which_is_why_the_ceiling_omits_the_prefix() {
            let maximal_frame = crate::wire::MAX_HEADER as usize + crate::wire::PREFIX;

            assert!(
                maximal_frame > MAX_RECORDED_BYTES,
                "a maximal frame exceeds the pool, so the ceiling must stop at the \
                 payload cap: ceiling {MAX_RECORDED_FRAME}, maximal frame \
                 {maximal_frame}, pool {MAX_RECORDED_BYTES}"
            );
            assert_eq!(
                MAX_RECORDED_FRAME,
                crate::wire::MAX_HEADER as usize,
                "so the ceiling is the payload cap, and the prefix is not added to it"
            );
        }

        /// A peer that floods the ledger must evict itself, never a quiet peer.
        /// A global oldest-first policy passes every other test in this module
        /// and fails this one.
        #[test]
        fn a_flooding_peer_cannot_evict_a_quiet_peers_record() {
            let ledger = bounded(holding(4));
            settle(
                guard(ledger.claim(peer(B), key("quiet"), [0; 32])),
                &frame(8),
                false,
            );
            for n in 0..64 {
                settle(
                    guard(ledger.claim(peer(A), key(&format!("k{n}")), [0; 32])),
                    &frame(8),
                    false,
                );
            }

            assert_eq!(
                recorded(ledger.claim(peer(B), key("quiet"), [0; 32])),
                frame(8),
                "the quiet peer's record must survive the flood"
            );
        }

        /// Evicting a running slot would let a concurrent duplicate into the
        /// handler, which is the one thing the ledger must never do.
        #[test]
        fn a_running_claim_is_never_evicted() {
            let ledger = bounded(holding(1));
            let _running = guard(ledger.claim(peer(A), key("running"), [0; 32]));
            for n in 0..64 {
                settle(
                    guard(ledger.claim(peer(A), key(&format!("k{n}")), [0; 32])),
                    &frame(8),
                    false,
                );
            }

            assert_eq!(
                refusal(ledger.claim(peer(A), key("running"), [0; 32])),
                IdempotencyRefusal::InFlight,
                "the in-flight claim must outlive every settled record"
            );
        }

        /// The defect this replaced: an evicted key used to be vacant, so the
        /// work ran a second time and nobody could tell.
        #[test]
        fn an_evicted_key_is_refused_not_rerun() {
            let ledger = bounded(holding(2));
            settle(
                guard(ledger.claim(peer(A), key("old"), [0; 32])),
                &frame(8),
                false,
            );
            settle(
                guard(ledger.claim(peer(A), key("new"), [0; 32])),
                &frame(8),
                false,
            );
            settle(
                guard(ledger.claim(peer(A), key("newest"), [0; 32])),
                &frame(8),
                false,
            );

            assert_eq!(
                refusal(ledger.claim(peer(A), key("old"), [0; 32])),
                IdempotencyRefusal::Forgotten,
                "the oldest record must leave a tombstone, not a vacancy"
            );
        }

        /// Tombstones are cheap, not free. Past their own bound the key really
        /// is vacant again, and `KeyState::Fresh` is what says so.
        #[test]
        fn a_tombstone_is_dropped_once_its_own_bound_is_reached() {
            let ledger = bounded(Limits::default().pool_records(nz(1)).tombstones(nz(2)));
            for n in 0..8 {
                settle(
                    guard(ledger.claim(peer(A), key(&format!("k{n}")), [0; 32])),
                    &frame(8),
                    false,
                );
            }

            assert!(
                matches!(ledger.claim(peer(A), key("k0"), [0; 32]), Claim::Fresh(_)),
                "the oldest tombstone must be the one that left"
            );
            assert!(ledger.lock().spent <= 2);
        }

        #[test]
        fn running_claims_are_bounded_per_peer() {
            let ledger = bounded(Limits::default().running_per_peer(nz(2)));
            let _one = guard(ledger.claim(peer(A), key("a"), [0; 32]));
            let _two = guard(ledger.claim(peer(A), key("b"), [0; 32]));

            assert_eq!(
                refusal(ledger.claim(peer(A), key("c"), [0; 32])),
                IdempotencyRefusal::TooManyRunning { limit: 2 }
            );
            assert!(
                matches!(ledger.claim(peer(B), key("c"), [0; 32]), Claim::Fresh(_)),
                "one peer's flood must not close the door on another"
            );
        }

        #[test]
        fn running_claims_are_bounded_in_total() {
            let ledger = bounded(Limits::default().running(nz(2)));
            let _one = guard(ledger.claim(peer(A), key("a"), [0; 32]));
            let _two = guard(ledger.claim(peer(B), key("b"), [0; 32]));

            assert_eq!(
                refusal(ledger.claim(peer(B), key("c"), [0; 32])),
                IdempotencyRefusal::TooManyRunning { limit: 2 }
            );
        }

        /// The bound the running caps cannot supply: an attaching call holds no
        /// claim, so one claimed key and enough connections parks unboundedly
        /// many tasks and stream pairs and meets neither cap.
        #[test]
        fn waiting_attachers_are_bounded_per_peer() {
            let ledger = bounded(Limits::default().waiting_per_peer(nz(2)));
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
            let ledger = bounded(Limits::default().waiting(nz(2)));
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
            let ledger = bounded(Limits::default().waiting_per_peer(nz(1)));
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
            let ledger = bounded(Limits::default().ttl(Duration::from_secs(60)));
            settle(
                guard(ledger.claim(peer(A), key("k"), [0; 32])),
                &frame(8),
                false,
            );
            assert_eq!(recorded(ledger.claim(peer(A), key("k"), [0; 32])), frame(8));

            tokio::time::advance(Duration::from_secs(61)).await;

            assert_eq!(
                refusal(ledger.claim(peer(A), key("k"), [0; 32])),
                IdempotencyRefusal::Forgotten,
                "an expired record must leave a tombstone, not its reply"
            );
            assert_eq!(ledger.lock().bytes, 0, "the reply's bytes must be freed");
        }

        /// `ttl` is the one field on [`Limits`] that **can** be set to zero, because
        /// the language offers no `NonZero` for a [`Duration`]. So what zero does is
        /// asserted rather than claimed unrepresentable.
        ///
        /// No clock advance: the point is that the record is already expired when it
        /// is written. This is also why `ttl` has no floor to document — a value
        /// below the caller's own retry interval behaves exactly like this one, and
        /// the crate cannot know that interval.
        #[tokio::test(start_paused = true)]
        async fn a_ttl_of_zero_forgets_every_record_at_the_next_claim() {
            let ledger = bounded(Limits::default().ttl(Duration::ZERO));
            settle(
                guard(ledger.claim(peer(A), key("k"), [0; 32])),
                &frame(8),
                false,
            );

            assert_eq!(
                refusal(ledger.claim(peer(A), key("k"), [0; 32])),
                IdempotencyRefusal::Forgotten,
                "at-most-once holds and no answer survives"
            );
        }

        #[tokio::test(start_paused = true)]
        async fn a_running_claim_does_not_expire() {
            let ledger = bounded(Limits::default().ttl(Duration::from_secs(60)));
            let _running = guard(ledger.claim(peer(A), key("k"), [0; 32]));

            tokio::time::advance(Duration::from_secs(600)).await;

            assert_eq!(
                refusal(ledger.claim(peer(A), key("k"), [0; 32])),
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

            // Watched as dispatched, because `Interrupted` is what a call that
            // ENTERED the handler leaves. A guard out of `claim` reads
            // `NotDispatched` and releases the key instead.
            let entered = Progress::new();
            entered.dispatched();
            let running = guard(ledger.claim(peer(A), key("running"), [0; 32])).watching(entered);
            assert_eq!(
                ledger.key_state(peer(A), &key("running")),
                KeyState::Running
            );
            drop(running);
            assert_eq!(
                ledger.key_state(peer(A), &key("running")),
                KeyState::Interrupted
            );

            settle(
                guard(ledger.claim(peer(A), key("body"), [0; 32])),
                &frame(8),
                true,
            );
            assert_eq!(
                ledger.key_state(peer(A), &key("body")),
                KeyState::Unrecordable
            );

            settle(
                guard(ledger.claim(peer(A), key("kept"), [0; 32])),
                &frame(8),
                false,
            );
            assert_eq!(ledger.key_state(peer(A), &key("kept")), KeyState::Done);

            // One record fits, so the second recording evicts the first.
            settle(
                guard(ledger.claim(peer(A), key("later"), [0; 32])),
                &frame(8),
                false,
            );
            assert_eq!(ledger.key_state(peer(A), &key("kept")), KeyState::Forgotten);
        }

        #[test]
        fn key_state_is_peer_scoped() {
            let ledger = ledger();
            settle(
                guard(ledger.claim(peer(A), key("mine"), [0; 32])),
                &frame(8),
                false,
            );

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
        ///
        /// **Four bounds, one clause each, because this test's coverage is a
        /// function of which trigger each number fires.** It used to set one
        /// `bytes: 64` that did both duties. Mapping that to `frame_bytes` alone
        /// would have retired byte-driven eviction **while the test stayed green** —
        /// `pool_records` at 3 keeps `evict`'s first loop running, so nothing goes
        /// red and the doc paragraph above stays literally true.
        ///
        /// The recorded frames are 1, 6, 11, 16, 21, 26, 31 and 36 bytes — the
        /// `n % 5 == 0` arm below, at `frame(n + 1)`. Every number is chosen against
        /// that series:
        ///
        /// - `pool_records(3)` fires `evict`'s record loop, because eight frames are
        ///   recorded.
        /// - `frame_bytes(32)` fires `Guard::record`'s size refusal: **36 is the only
        ///   one of the eight over it**, so the `Unrecordable` arm is reached on a
        ///   size cause without swallowing the rest.
        /// - `pool_bytes(64)` fires `evict`'s byte loop. The seven frames that pass
        ///   the ceiling are 1 to 31, and the three largest of those are
        ///   21 + 26 + 31 = **78**, over 64. The old figure written here was 93,
        ///   which was 36 + 31 + 26 and counted the frame the ceiling now refuses —
        ///   a sum this test can no longer reach.
        /// - `tombstones(4)` fires `evict`'s tombstone loop.
        ///
        /// # The bounds are not the break target, and the difference matters
        ///
        /// **Raising `pool_bytes` does not redden this.** What is asserted is that the
        /// counters agree with the maps; removing a *trigger* reduces what the test
        /// covers without making any counter disagree with any map. So an edit to one
        /// of the four bounds is a **silent coverage reduction** — precisely the
        /// failure the four clauses exist to prevent, which is why proposing it as
        /// this guard's break would be that failure wearing the shape of its remedy.
        /// **The four bounds are a reviewable statement; a reviewer checks them by
        /// reading.**
        ///
        /// **So what has no one-literal break is the CHOICE OF BOUND VALUES, and not
        /// the counter invariant, which is strongly guarded.** An earlier version of
        /// this section was headed *"this guard has NO one-literal break"*, which drops
        /// that scope — and a reader who stopped at the heading would conclude the
        /// invariant was unguarded and follow a redirection to two **mapping** guards
        /// that answer a different question.
        ///
        /// The generalisation from *bound* literals to *every* literal is what failed:
        /// `scripts/break-once.sh` takes a file path, so any production line in this
        /// file is a legal target. Several redden this — `inner.spent += 1` to `+= 2`
        /// is another — because each makes a counter disagree with the sum over
        /// buckets, so **every** `assert_agrees` in the loop fails rather than one.
        ///
        /// Made red by dropping `evict`'s tombstone decrement, so the counter stays
        /// high while the buckets drain. The anchor is on one line, which was once
        /// load-bearing and is now only tidy: `.mise/tasks/anchors` read backticked
        /// spans two ways, and the reader that walked this file for record blocks kept
        /// the comment marker, so a wrapped quote reached it with a `///` inside it.
        /// One reader now, and it strips the marker — wrapping this is cosmetic:
        ///
        /// `inner.spent = inner.spent.saturating_sub(1);`
        ///
        /// replaced by the same line with `saturating_sub(0)`. Watched: `panicked at
        /// .., assertion `left == right` failed: after n=9: spent, left: 5, right: 0`.
        ///
        /// The `after n=9` is the guard working as designed: `assert_agrees` runs
        /// inside the loop, so it names the iteration at which the counter and the map
        /// parted rather than reporting a final total.
        ///
        /// What a bad pool-versus-frame mapping *does* redden — **both measured, both
        /// carrying their own break record, neither claimed from reasoning**:
        ///
        /// - `the_recorded_bytes_stay_inside_their_bound` — asserts the pool sum.
        ///   Swapping `pool_bytes`'s body gives `held 1024 bytes over a 64 byte
        ///   budget`.
        /// - `a_reply_over_the_frame_ceiling_is_not_replayed` — asserts the ceiling's
        ///   refusal. Swapping `frame_bytes`'s body gives `left: Forgotten, right:
        ///   Unrecordable`.
        ///
        /// Between them those two hold the mapping. This test holds the arithmetic.
        ///
        /// **The first version of this paragraph asserted both of those without
        /// running either**, which is the same substitution of a property for a
        /// measurement that this section objects to — written into the commit whose
        /// subject is that distinction. Both are now replays, and so is this guard's
        /// own break.
        #[test]
        fn the_counters_agree_with_the_maps_on_every_path() {
            let ledger = bounded(
                Limits::default()
                    .pool_records(nz(3))
                    .pool_bytes(nz(64))
                    .frame_bytes(nz(32))
                    .tombstones(nz(4)),
            );
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
                    0 => settle(
                        guard(ledger.claim(p, k, [0; 32])),
                        &frame(usize::from(n) + 1),
                        false,
                    ),
                    // unrecordable
                    1 => settle(guard(ledger.claim(p, k, [0; 32])), &frame(8), true),
                    // interrupted, by dropping an unwatched guard
                    2 => drop(guard(ledger.claim(p, k, [0; 32]))),
                    // released, because nothing was dispatched
                    3 => drop(guard(ledger.claim(p, k, [0; 32])).watching(Progress::new())),
                    // claimed and left running for a while
                    _ => {
                        let running = guard(ledger.claim(p, k.clone(), [0; 32]));
                        assert_agrees(&ledger, "while a claim is running");
                        settle(running, &frame(4), false);
                    }
                }
                assert_agrees(&ledger, &format!("after n={n}"));
            }

            // And a re-claim of an evicted key, which is the path that both
            // removes a tombstone and adds a record in one call.
            for n in 0..20u8 {
                settle(
                    guard(ledger.claim(peer(A), key(&format!("k{n}")), [0; 32])),
                    &frame(2),
                    false,
                );
                assert_agrees(&ledger, &format!("after re-claiming k{n}"));
            }
        }

        /// The TTL path moves entries between the two pools without going
        /// through `settle`, so it gets its own check.
        #[tokio::test(start_paused = true)]
        async fn the_counters_agree_after_records_expire() {
            let ledger = bounded(Limits::default().ttl(Duration::from_secs(60)));
            for n in 0..10u8 {
                settle(
                    guard(ledger.claim(peer(A), key(&format!("k{n}")), [0; 32])),
                    &frame(8),
                    false,
                );
            }
            assert_agrees(&ledger, "before the ttl");

            tokio::time::advance(Duration::from_secs(61)).await;
            // `expire` is lazy, so a claim is what drives it.
            let _ = ledger.claim(peer(A), key("wake"), [0; 32]);
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
            let ledger = bounded(Limits::default().running(nz(1)));
            let _running = guard(ledger.claim(peer(A), key("a"), [0; 32]));

            assert_eq!(
                refusal(ledger.claim(peer(B), key("b"), [0; 32])),
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
            let running = guard(ledger.claim(peer(A), key("k"), [0; 32]));

            let Claim::Running(notify) = ledger.claim(peer(A), key("k"), [0; 32]) else {
                panic!("expected a running slot");
            };

            // Registered BEFORE the settle, which is the hazard RFC-04 §3.3
            // named: `notify_waiters` wakes only the waiters already there.
            let notified = notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();

            settle(running, &frame(16), false);
            notified.await;

            assert_eq!(
                recorded(ledger.claim(peer(A), key("k"), [0; 32])),
                frame(16)
            );
        }

        /// The half that made §3.3 right to worry: a waiter that registers after
        /// the settle would sleep forever, so `enable()` is not decoration.
        #[tokio::test]
        async fn a_waiter_that_registers_late_still_sees_the_settled_slot() {
            let ledger = ledger();
            let running = guard(ledger.claim(peer(A), key("k"), [0; 32]));
            let Claim::Running(notify) = ledger.claim(peer(A), key("k"), [0; 32]) else {
                panic!("expected a running slot");
            };

            settle(running, &frame(16), false);

            // No `enable()` before the settle, so this wakeup is genuinely lost.
            // The re-claim under the lock is what makes that survivable, and it
            // is why the accept path re-reads the slot before it awaits.
            assert_eq!(
                recorded(ledger.claim(peer(A), key("k"), [0; 32])),
                frame(16)
            );
            drop(notify);
        }

        /// The arithmetic of the cap, and only that. **This test cannot see which
        /// cap the accept path passes in**, which is the half that matters and is
        /// held by `an_attach_ceiling_below_the_peers_request_governs` in
        /// `rpc.rs` — a ledger is what the argument comes from, and this type
        /// holds none.
        ///
        /// Both directions are asserted here because the `min` is what makes the
        /// smaller of the two govern: an assignment would satisfy the first
        /// assertion and fail the third.
        ///
        /// Made red by `scripts/break-once.sh` replacing
        /// `.map(|millis| Duration::from_millis(millis).min(cap))` with
        /// `.map(|millis| Duration::from_millis(millis))`, which is the cap going
        /// away rather than moving. Watched, under `cargo test`:
        /// `assertion `left == right` failed`, `left: Some(3600s)`,
        /// `right: Some(30s)`.
        ///
        /// **The two break sets are DISJOINT on the accept path, and this record used
        /// to claim they overlapped.** Measured, one `break-once.sh` run per guard:
        /// cap removal reddens three of the four guards in `rpc.rs` —
        /// `an_attach_ceiling_below_the_peers_request_governs`,
        /// `a_peer_over_the_attach_ceiling_is_held_for_the_default` and
        /// `an_attach_ceiling_of_zero_refuses_the_attacher_at_once` — and leaves
        /// `a_peer_under_the_attach_ceiling_keeps_its_own_budget` **green**, because
        /// under it the peer's request is already the smaller number. `.map(|_| cap)`
        /// reddens exactly that fourth one, plus this test's third assertion.
        ///
        /// The old wording said "two guards" and sent a reader to "one of those two",
        /// which named no guard at all: the guard `.map(|_| cap)` reddens is precisely
        /// the one cap removal cannot. That is also why the fourth guard exists, and
        /// `rpc.rs`'s own record says so one file away — two records disagreed and the
        /// registered one was the wrong one.
        #[test]
        fn a_budget_is_capped_at_the_cap_it_is_given() {
            let over = Idempotency::attaching_for(key("k"), Duration::from_secs(3600));
            assert_eq!(over.attach_budget(MAX_ATTACH), Some(MAX_ATTACH));
            assert_eq!(
                over.attach_budget(Duration::from_secs(1)),
                Some(Duration::from_secs(1)),
                "the ceiling is the argument, never the constant"
            );

            let under = Idempotency::attaching_for(key("k"), Duration::from_secs(1));
            assert_eq!(
                under.attach_budget(MAX_ATTACH),
                Some(Duration::from_secs(1)),
                "and a request under the ceiling is kept, not raised to it"
            );
        }

        /// Any cap, because `None` never meets one: a caller who did not ask to
        /// attach is refused rather than capped.
        ///
        /// **This guard is the reason [`Attaching::Forbidden`] is refused in
        /// `claim_key` rather than through this `None`.** A second producer would make
        /// both assertions below pass for two reasons at once, while the test's name
        /// still claims one — so the guard would stop testing what it says without
        /// anything going red.
        #[test]
        fn a_bare_key_does_not_attach() {
            let idem: Idempotency = key("k").into();
            assert_eq!(idem.attach_budget(MAX_ATTACH), None);
            assert_eq!(idem.attach_budget(Duration::ZERO), None);
            assert_eq!(idem.key(), &key("k"));
        }

        /// The default ceiling is the constant, not a second copy of thirty
        /// seconds. [`MAX_ATTACH`]'s own value is pinned against the README by
        /// `tests/reliable.rs::the_limits_the_readme_names_are_the_ones_the_ledger_uses`,
        /// so the two together are what stop the shipped ceiling drifting from the
        /// number a reader plans around.
        ///
        /// Reads the private field, because [`Limits`] offers no getters and
        /// `mod tests` is a child of the module that defines it. Going through
        /// [`Limits::attach`] instead would assert the setter and not the default.
        ///
        /// Made red by `scripts/break-once.sh` replacing
        /// `attach: Attaching::UpTo(MAX_ATTACH),` in `impl Default for Limits` with
        /// `attach: Attaching::UpTo(Duration::from_secs(1)),`. The search literal
        /// carries that line's twelve spaces of indentation, so this record's
        /// unindented copy of it is not a second occurrence — the same-file remedy
        /// rot mode 1 prescribes. Watched, under `cargo test`:
        /// `assertion `left == right` failed: the default ceiling must be the
        /// constant the README prints`, `left: For(1s)`, `right: For(30s)`. The same
        /// edit reddens `rpc.rs`'s
        /// `a_peer_over_the_attach_ceiling_is_held_for_the_default`.
        #[test]
        fn the_default_attach_ceiling_is_max_attach() {
            assert_eq!(
                Limits::default().attach,
                Attaching::UpTo(MAX_ATTACH),
                "the default ceiling must be the constant the README prints"
            );
        }
    }
    #[test]
    fn request_fingerprints_bind_running_settled_and_recovered_records() {
        let ledger = Ledger::new(
            Limits::default(),
            Records::Memory,
            crate::version::RecordCodec::testing(),
        );
        let first = guard(ledger.claim(peer(A), key("bound"), [1; 32]));
        assert_eq!(
            refusal(ledger.claim(peer(A), key("bound"), [2; 32])),
            IdempotencyRefusal::RequestMismatch
        );
        settle(first, &frame(16), false);
        assert_eq!(
            refusal(ledger.claim(peer(A), key("bound"), [2; 32])),
            IdempotencyRefusal::RequestMismatch
        );
        assert_eq!(
            recorded(ledger.claim(peer(A), key("bound"), [1; 32])),
            frame(16)
        );
        #[cfg(feature = "persist")]
        {
            let recovered = Arc::new(Ledger {
                contract_version: crate::Version::__new(0, 0),
                inner: Mutex::new(recover(vec![(
                    peer(A),
                    key("bound"),
                    [1; 32],
                    Slot::Done(frame(16)),
                )])),
                limits: Limits::default(),
                records: Records::Memory,
            });
            assert_eq!(
                refusal(recovered.claim(peer(A), key("bound"), [2; 32])),
                IdempotencyRefusal::RequestMismatch
            );
            assert_eq!(
                recorded(recovered.claim(peer(A), key("bound"), [1; 32])),
                frame(16)
            );
        }
    }
}
