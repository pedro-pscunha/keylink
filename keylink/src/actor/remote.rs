//! Everything a peer can influence on the keyed path: the wrapper that names an
//! actor, where a message came from, and the walk from an arriving request to a
//! slot in a mailbox.
//!
//! This half depends on the registry beside it and serves it, and the
//! dependency runs this way round on purpose: naming [`Registry`] here costs
//! nothing, and naming [`Addressed`] there would cost the split.
//!
//! # The handshake is the whole gate
//!
//! There used to be two gates, because there used to be one ALPN for every
//! actor system on a node. That ALPN could only prove the peer agreed about an
//! *envelope*, so per-actor drift was caught one layer down: the request
//! declared the schema hash of the actor's own message type, the registry
//! compared it against the hash the actor was mounted with, and the payload
//! stayed sealed bytes until that comparison succeeded.
//!
//! One ALPN **per service** replaces both. The keyed contract hashes
//! `Envelope<WireRequest<Addressed<Request>>>` against
//! `Envelope<WireResponse<Response>>`, so a peer built against different types
//! is refused at the handshake, and a `Registry` of one service's own generated
//! mailbox enum cannot physically hold an actor of another service — the
//! mismatch stops being a refusal and becomes unrepresentable.
//!
//! What that costs is stated where it is paid, and it is worth reading before
//! reaching for the old shape again: two actor *services* can no longer share
//! one QUIC connection. Each gets its own handshake, admission decision,
//! idempotency ledger, keep-alive and congestion window. RFC-04 §10.2's O(N²)
//! argument is about `Endpoint::set_alpns` and the number of ALPNs announced —
//! which is now one per hand-written service type, a bounded number — and NOT
//! about connection count. The two are easy to conflate and they are different
//! bills.
//!
//! # What is decoded, and when
//!
//! The whole request is decoded by the accept loop, before
//! [`ActorId::validate`](crate::ActorId::validate) and before routing. That is
//! an ordering change and not only a redaction one: the old shape deferred the
//! decode until a per-actor hash agreed, and this one does not defer it at all.
//! It is sound for the same reason the singleton path is sound — postcard is not
//! self-describing, so the ALPN is what establishes type identity, and it now
//! covers the keyed request type exactly as it covers the singleton one. See
//! [`derive_alpn`](crate::derive_alpn).
//!
//! Sound, and **a reduction in margin** — the half of the sentence that has to be
//! said out loud. The old shape could refuse an *already admitted* peer whose
//! actor had drifted without running `Deserialize` over one attacker-influenced
//! byte: the declared hash was compared first and the payload stayed sealed
//! behind it. Now the decoder runs for everything that gets past the handshake
//! and the allowlist, and nothing else stands between a drifted admitted peer and
//! postcard. That is the trade this section is recording, not only a redaction:
//! one gate is strictly less defence in depth than two, and it is accepted because
//! the remaining gate covers the exact type rather than an envelope.

use n0_future::time::Instant;
use postcard_schema::Schema;
use serde::{Deserialize, Serialize};

use super::registry::{Registry, RouteError};
use crate::turns::{current, lost, rendered, Reply};
use crate::{
    ActorId, Body, BodyReader, Caller, Context, Outcome, Queueing, RemoteError, Rpc, Serving,
};

// ─────────────────────────────────────────────────────────── the wire wrapper

/// One message, plus the actor on this node it is addressed to.
///
/// The keyed twin of a bare `Request`: a service's own request type travels
/// inside this, and a `Registry` of that service is what takes it apart. Nesting
/// is `WireRequest<Addressed<Request>>` and **never** the other way round — a
/// [`KeyState`](crate::KeyState) query names no actor, so the actor id has to
/// sit inside the arm that is a call rather than outside the arm that chooses.
///
/// # Private fields
///
/// `wire.rs` refuses `pub` on `Envelope` precisely so that a downstream crate
/// cannot put a value on the wire that never met a constructor, and
/// [`ActorId`] is in that family: it arrives from a peer, `derive(Deserialize)`
/// never calls `ActorId::new`, and `validate` is what stands in for the
/// constructor on the receiving side. Handing out the field would hand out a
/// second way to build one.
///
/// # The generic parameter really is inside the hash
///
/// Worth stating, because the obvious worry is the right one to have.
/// `postcard_schema`'s `NamedType.name` for this struct is the bare
/// `"Addressed"` — the generic argument is erased from the *name*. `T` is
/// nevertheless reached through the `message` field's own `NamedType`, so
/// `Addressed<ReqA>` and `Addressed<ReqB>` hash differently, and both differ
/// from a bare `ReqA`. The hash is sound; the name is just uninformative.
///
/// # Release note: this `Debug` prints the request
///
/// `derive(Debug)` here, so `{:?}` on one of these prints the service's own
/// payload. The generated mailbox enum derives nothing at all — it is never
/// printed and never serialized — so an `Addressed` is the one of the pair a
/// `{:?}` can leak through. Nothing in keylink logs a request, which is what
/// makes this latent rather than a leak —
/// a `Middleware` or a `tracing` field that prints one is the caller's decision
/// and is where a payload would reach a log.
#[doc(hidden)]
#[derive(Debug, Clone, Serialize, Deserialize, Schema)]
pub struct Addressed<T> {
    /// Which actor, on the node this request is addressed to. An `ActorId` is
    /// not a route and not globally unique: the full address of an actor is
    /// `(EndpointId, ActorId)`, and the caller resolves both.
    actor: ActorId,
    /// The service's own request.
    message: T,
}

impl<T> Addressed<T> {
    /// Address one message to one actor. Called by the generated keyed client.
    #[doc(hidden)]
    #[must_use]
    pub fn new(actor: ActorId, message: T) -> Self {
        Self { actor, message }
    }

    /// Take it apart. Consuming, so there is one reader and no way to hold the
    /// id apart from the message it arrived with.
    #[doc(hidden)]
    #[must_use]
    pub fn split(self) -> (ActorId, T) {
        (self.actor, self.message)
    }

    /// Look at the message without taking the envelope apart.
    ///
    /// [`split`](Self::split) stays the reader for a delivery, and this is not a
    /// second one: it borrows. Generated keyed dispatch needs it for the one
    /// decision it makes before delivering anything — whether this is the
    /// `__Status` question, which it answers itself — and splitting to look
    /// would mean rebuilding an envelope a peer sent, out of parts held apart,
    /// which is what `split` is consuming to prevent.
    #[doc(hidden)]
    #[must_use]
    pub fn message(&self) -> &T {
        &self.message
    }
}

/// Delegates to the message inside, which is the whole point of the wrapper.
///
/// The method name is the inner request's own, so per-method
/// [`Middleware`](crate::Middleware) and per-method authorization work on a
/// keyed actor exactly as they work on a singleton. The old opaque envelope
/// reported the constant `"Message"` here, because the real name sat inside a
/// payload no layer could read before a hash check — that is what is being
/// bought back.
impl<T: Rpc> Rpc for Addressed<T> {
    const SERVICE: &'static str = T::SERVICE;
    const METHODS: &'static [&'static str] = T::METHODS;

    fn method(&self) -> &'static str {
        self.message.method()
    }

    fn actor(&self) -> Option<&ActorId> {
        Some(&self.actor)
    }
}

// ───────────────────────────────────────────────────── what a mailbox carries

/// Where a message reached an actor from.
///
/// Owned, not borrowed. [`Caller<'a>::Peer`](Caller) holds a `&'a Context` and
/// `Caller` is covariant in `'a`, so a `Caller` that generated code was *handed*
/// never widens to `'static` — proved three ways in RFC-07 §1.6, and the reason
/// a mailbox message cannot simply carry a `Caller`. It carries this instead and
/// the loop borrows a [`Caller`] out of it for the length of one turn.
///
/// # A newtype over a private enum, so there is no door in from outside
///
/// Generated code never *constructs* one of these. It destructures a
/// mailbox variant keylink built and calls [`caller`](Self::caller) — the
/// only thing the expansion needs. So the two cases are a private enum and the
/// two constructors are `pub(crate)`, and a downstream crate meets an error
/// instead of a way to make the mistake below:
///
/// ```compile_fail
/// // Fails with `E0624: associated function `local` is private`, and
/// // `Origin::Peer(cx)` with `E0599`, there being no public variant to name.
/// //
/// // The codes are named HERE and deliberately NOT in the fence. rustdoc does
/// // not check the code after `compile_fail` — pinning `E0599` on the fence
/// // leaves this green, which was red-tested — so an annotated code asserts
/// // verification it does not perform. This project has shipped four guards
/// // that were satisfiable without proving their claim; a fence that reads
/// // `compile_fail,E0624` is how the fifth would look.
/// //
/// // So this asserts exactly one thing: it does not compile. What stops it
/// // passing for the WRONG reason — a rename making the name vanish rather
/// // than the privacy holding — is the unit test that calls `Origin::local()`
/// // from inside the crate. One pins that the name exists, this pins that it
/// // is out of reach.
/// let _origin = keylink::Origin::local();
/// ```
///
/// # Deliberately NOT `Clone`, and exactly how far that reaches
///
/// [`Context`] does derive `Clone`, and `Context.progress` is a
/// `Progress(Arc<AtomicU8>)` whose `Clone` shares the `Arc` **on purpose** —
/// that sharing is what lets a turn running on the actor's own task report back
/// to the accept task that dispatched it.
///
/// `Progress::reached()` is what `rpc.rs` reads to decide whether an idempotency
/// key is **recorded** or **released**. Two messages carrying clones of one
/// context would share one `AtomicU8` whose marks are `fetch_or` and never
/// cleared, so a message that reached `COMPLETED` would make a second message
/// read `Reached::Completed` and record a reply it never produced — a silently
/// wrong at-most-once answer, in the one place the crate promises otherwise.
///
/// Two things guard that, and both are narrower than they look:
///
/// - **No `Clone` on this type.** `representability.rs` holds it with a coherence
///   probe rather than a comment, so adding the derive is `E0119` and not a review
///   somebody has to remember to do. **Do not derive it.**
/// - **No public constructor**, per the section above: the `Origin` a peer's
///   message travels in is one keylink built.
///
/// ## What neither of them refuses, and this is the part to read
///
/// **Nobody clones an `Origin`. What a caller clones is the `Context` on its way
/// in.** [`deliver_keyed`] is `#[doc(hidden)] pub` and takes its `Context`
/// **by value**, so the hazard type-checks from any crate, shares one marker
/// across two deliveries, and never names `Origin` at all:
///
/// ```ignore
/// let _ = deliver_keyed::<S>(&system, cx.clone(), a, body_a).await;
/// let _ = deliver_keyed::<S>(&system, cx,         b, body_b).await;
/// ```
///
/// The marker is writable from outside as well: [`caller`](Self::caller)'s
/// `Caller::Peer(cx)` arm reaches `cx.progress()`, whose marks are `pub` because
/// generated dispatch calls them, and `middleware.rs`'s `Progress` documents at
/// length why there is nothing to seal them with.
///
/// So: the guard refuses the derive, and refuses downstream construction. It does
/// **not** refuse a caller cloning a `Context` into two deliveries. What keeps
/// that unreachable today is the call graph rather than the type — `serve_one`
/// builds one `Context` per request and generated keyed dispatch calls
/// [`deliver_keyed`] once with it — which is a fact about two call sites in
/// this crate: reviewable, and unenforced. `representability.rs` cannot state it,
/// because a compile-time probe can only refuse a shape that does not type-check.
///
/// Written this plainly because the sentence here used to claim that withholding
/// `Clone` made "the mistake a compile error rather than something a peer finds
/// out about" — false for the half that matters. **A guard whose doc overstates
/// it is worse than no guard: the next author trusts it and stops looking.**
/// RFC-07 §1.6 carries the same correction, on its third pass.
#[doc(hidden)]
#[derive(Debug)]
pub struct Origin(Provenance);

/// The two cases, private so that neither is a construction path.
#[derive(Debug)]
enum Provenance {
    /// From inside this process, through the generated `Handle`. There is no
    /// peer, no handshake and no proven key — so no idempotency key to settle
    /// and no queue budget to expire, which is why every mark a turn makes is
    /// `Option`-guarded rather than branched on the case.
    ///
    /// This case existed for a wave with nothing building it, because [`Origin`]
    /// was the shape the generated mailbox committed to and giving the local path
    /// a second shape to arrive in was the split this type exists to avoid. The
    /// local path arrives here now: `LocalActor::ask` and `LocalActor::notify`
    /// build one per message.
    Local,
    /// From a peer iroh proved at the handshake.
    Peer(Context),
}

impl Origin {
    /// A message from inside this process.
    ///
    /// `pub(crate)` and called from `turns.rs`: a `Handle` method hands the
    /// builder closure an origin rather than letting generated code write one, so
    /// the expansion never names either constructor. It carried an
    /// `allow(dead_code)` for one wave, while the mailbox it belongs in was still
    /// a boxed closure and nothing built a local origin at all.
    pub(crate) fn local() -> Self {
        Self(Provenance::Local)
    }

    /// A message from a peer, taking the `Context` **by value**: see this type's
    /// documentation for what that does and does not buy.
    pub(crate) fn peer(cx: Context) -> Self {
        Self(Provenance::Peer(cx))
    }

    /// Borrow a [`Caller`] out of this, for the length of one turn.
    ///
    /// `pub` because the loop that calls it is generated into the **user's**
    /// crate, where a private method is `E0624`. That is the same error Wave 0
    /// hit one layer up on `Context::new`, and this one was written without
    /// `pub` first — see RFC-07 §1.6's revision note.
    #[doc(hidden)]
    #[must_use]
    pub fn caller(&self) -> Caller<'_> {
        match &self.0 {
            Provenance::Local => Caller::Local,
            Provenance::Peer(cx) => Caller::Peer(cx),
        }
    }
}

// ────────────────────────────────────────── what a generated service declares

/// What a generated keyed service tells keylink about itself.
///
/// Implemented **only** by generated code, and it exists because there is
/// nothing else keylink can name. The generated `Actor` trait has no associated
/// types and is expanded into the *caller's* crate, so a free function in this
/// crate cannot reach a service's `Request`, its `Response`, or what its mailbox
/// carries except through a trait declared here.
///
/// `#[doc(hidden)] pub`, and a compatibility promise regardless: hidden hides it
/// from the docs, not from the compiler.
///
/// # [`Mail`](Self::Mail) is the service's own generated enum
///
/// It was an associated type standing in for one keylink-level struct, declared
/// as the seam the generated enum would arrive through. This is that arrival:
/// `Mail` is now `<service>::Mail`, one variant per declared method plus the one
/// a peer's request lands in, and [`deliver_keyed`] is unchanged by it — which
/// is what the seam was for.
#[doc(hidden)]
pub trait KeyedService: Send + 'static {
    /// The service's own request, as it arrives inside an [`Addressed`].
    type Request: Rpc + Send + 'static;
    /// The service's own response.
    type Response: Send + 'static;
    /// What this service's mailbox carries.
    type Mail: Send + 'static;

    /// Build the mailbox variant a peer's request lands in, out of the parts
    /// [`deliver_keyed`] holds.
    ///
    /// The [`Origin`] is made **here**, by keylink, and handed in: generated code
    /// receives one and never constructs one, which is what keeps both of
    /// `Origin`'s constructors `pub(crate)` while the enum that carries one is
    /// expanded in somebody else's crate.
    fn mail(
        origin: Origin,
        request: Self::Request,
        body: BodyReader,
        reply: Reply<(Self::Response, Body)>,
    ) -> Self::Mail;
}

// ────────────────────────────────────────────────────── serving the registry

fn no_such_actor(id: &ActorId) -> RemoteError {
    RemoteError::NoSuchActor { id: id.to_string() }
}

/// What a peer is told, which is deliberately less than what happened.
///
/// **The capacity refusal is a variant now, and the ALPN it costs is already
/// paid.** It was [`RemoteError::Failed`] carrying `RouteError`'s own sentence,
/// on the ground that a variant sits inside the hashed contract and moves every
/// service's ALPN — true, and the reason it stayed a string for as long as the
/// wire could not afford one. [`RemoteError::AtCapacity`] is that variant, and a
/// peer can match a cap it might wait out instead of scanning a sentence for the
/// word *capacity*.
impl From<RouteError> for RemoteError {
    fn from(err: RouteError) -> Self {
        match &err {
            RouteError::NoSuchActor { id } => no_such_actor(id),
            // `try_from` and not `as`: the cap is a `usize` here and a `u64` on
            // the wire, because `usize` has no wire schema — its width is the
            // compiler's choice. A cast would wrap silently on a target where
            // one is wider; this saturates, and a diagnostic must not be the
            // thing that panics.
            RouteError::AtCapacity { capacity } => Self::AtCapacity {
                capacity: u64::try_from(*capacity).unwrap_or(u64::MAX),
            },
        }
    }
}

/// Walk one arriving request to the actor it names, and wait for the answer.
///
/// **A free function taking `&Registry<..>`**, and that shape is deliberate: it
/// compiles identically whether `Registry` lives in this crate or in another one,
/// which an inherent impl cannot do. RFC-04 §16.8 splits the registry into its
/// own crate, and this wave is independent of whether that has happened. No
/// inherent impl on `Registry` names a service type.
///
/// # The dispatch order, each step failing before the next can run
///
/// 1. The request is decoded by the accept loop. Typed, and covered by the
///    handshake — see this module's header for why that is now the only gate.
/// 2. Re-check the id, which arrived from a peer and never met our constructor.
///    `derive(Deserialize)` builds an [`ActorId`] field by field, so this is not
///    a second opinion about `ActorId::new`; it is the only check the value has
///    ever had, and it runs **before** routing so that an oversized id costs no
///    lookup and no lazily spawned actor.
/// 3. Look it up, spawning on first message if the system has a factory.
/// 4. Wait for a mailbox permit, bounded by whatever budget a
///    [`QueueTimeout`](crate::QueueTimeout) put on the context.
/// 5. Hand the typed message to the actor.
///
/// # Why step 4 is bounded here, and not only in the loop
///
/// The loop's own deadline check runs when a message comes **off** the mailbox,
/// so it says nothing about one that never got on. Capacity is one and turns are
/// serial, so while an actor is inside a long turn every caller past the first
/// waits in `mail.send` — each holding a QUIC stream, a [`BodyReader`] and an
/// idempotency key claimed before dispatch — and learns `TimedOut` only once
/// that turn ends. A 50 ms budget against a ten second turn was honoured at ten
/// seconds, which is exactly the assumption `QueueTimeout` exists because nobody
/// can make. RFC-04 §16.7 called that "milder than it sounds"; §16.10 says why
/// it is not.
///
/// The refusal is the value the other two dispatch paths produce for the same
/// condition, so a peer cannot tell them apart. The progress marker is
/// deliberately untouched by it: a message refused before it entered the mailbox
/// has not run, so the idempotency guard sees `NotDispatched` and releases the
/// key. Marking there would settle it `Interrupted` and permanently refuse every
/// retry of a call that never happened.
///
/// # The send succeeding is itself a fact the key needs
///
/// Once the permit is in hand the message will be taken, and nothing above this
/// can stop it. A caller that gives up from here on — an aborted connection
/// task, because the connection ended — is giving
/// up on work that runs anyway, so `enqueued()` is marked and the key settles
/// `Interrupted` rather than being handed back to a retry that would do the work
/// a second time. RFC-04 §16.12.
///
/// It is marked *after* the send and not before, and that placement is the whole
/// of the precision: a caller refused at step 4 provably queued nothing, and its
/// key must still be claimable.
///
/// # Errors
///
/// [`RemoteError::BadActorId`] for a malformed id, [`RemoteError::AtCapacity`]
/// for a full registry, [`RemoteError::NoSuchActor`] when nothing is mounted and
/// no factory built one, [`RemoteError::TimedOut`] when a parking queue budget
/// expired, and [`RemoteError::MailboxFull`] when an opted-in shedding mailbox
/// had no slot.
///
/// And three more once the message *did* reach an actor and the answer did not
/// come back, which is [`lost`]'s vocabulary rather than this function's: a
/// [`RemoteError::Withheld`] carrying [`Restarting`](crate::Withheld::Restarting)
/// for an incarnation that ended with another coming,
/// [`Dropped`](crate::Withheld::Dropped) for a reply a live actor took out of its
/// turn and never answered, and [`Gone`](crate::Withheld::Gone) for an actor that
/// is genuinely finished. That last one was `NoSuchActor` — an answer about an
/// **id**, given after a delivery proved the id was real.
#[doc(hidden)]
pub async fn deliver_keyed<S: KeyedService>(
    system: &Registry<S::Mail>,
    cx: Context,
    request: Addressed<S::Request>,
    body: BodyReader,
) -> std::result::Result<(Outcome<S::Response>, Body), RemoteError> {
    let (actor, message) = request.split();
    // The id and the reason as two fields, so a peer can match on the one it
    // sent and log the one it did not know. `validate`'s own error is what fills
    // `reason` — the sentence the crate uses locally rather than a second
    // wording of the same rule.
    actor.validate().map_err(|err| RemoteError::BadActorId {
        id: actor.to_string(),
        reason: err.to_string(),
    })?;

    let route = system.route(&actor)?;
    // Cloned out so the wait at the bottom can read the verdict without the
    // sender still sitting beside it. `Fate` is a handle to one
    // `Arc<Mutex<Standing>>`, and the registry itself clones it out of the entry
    // to build every `Route`, so this reads the same cell the actor's own loop
    // settles. See the `drop(route)` below for what the separation is for.
    let fate = route.fate.clone();

    // Read before `cx` moves into the `Origin`, which is the only reason these
    // are bindings rather than expressions at their use sites.
    let budget = cx.queueing();
    let progress = cx.progress();
    let (reply, answer) = Reply::channel();
    // The registry mailbox carries the caller's chain alongside the message.
    // Remote delivery normally starts on a connection task outside any actor
    // turn, so this is `Chain::Empty`; keeping the live value makes the route
    // correct for any in-process caller that reuses this seam as well.
    let queued = (
        Box::new(S::mail(Origin::peer(cx), message, body, reply)),
        current(),
    );
    if route.shed {
        match route.mail.try_send(queued) {
            Ok(()) => {}
            Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => {
                return Err(RemoteError::MailboxFull);
            }
            Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => {
                return Err(no_such_actor(&actor));
            }
        }
    } else {
        let queueing = route.mail.send(queued);
        let sent = match budget {
            None => queueing.await,
            Some(budget) => n0_future::time::timeout(
                budget.deadline().saturating_duration_since(Instant::now()),
                queueing,
            )
            .await
            .map_err(|_| RemoteError::TimedOut {
                millis: budget.millis(),
            })?,
        };
        sent.map_err(|_| no_such_actor(&actor))?;
    }
    // **This clone dies here, and a keyed actor's graceful stop is what that
    // buys.** `drain` ends an actor's loop by dropping the registry's sender, and
    // says the task lives while any other sender does so that "a message already
    // in flight" can finish. A `#[defer]` reply parked in user state is not a
    // message in flight: the message was delivered and the turn ran to
    // completion. So a clone held across the wait below kept the loop open for
    // nothing — `shutdown` spent its whole budget and then aborted the actor it
    // was asked to drain, and the peer was told the same thing a budget later.
    // `Registry::sweep` reaches the same `drain`, so an **eviction** did it too,
    // with nobody having called anything: a parked reply does not touch
    // `last_seen`, so the actor expired on schedule and was destroyed instead of
    // drained.
    //
    // Not any earlier: the send future borrows `route.mail`, so this is the first
    // line where the clone is provably spent rather than still owed a permit.
    // `answer` and `fate` outlive it, which is the whole reason `fate` was cloned
    // out above.
    drop(route);
    // Below every refusal above it, so no refusal here can be mistaken for a
    // message that queued. Above the wait, because the wait is exactly what a
    // caller gives up on.
    progress.enqueued();

    // `rendered` and `lost`, not two matches written here: the singleton's served
    // path answers the same cases, and when the two were written separately they
    // disagreed — about how a panic reaches a peer, and then about whether a dead
    // reply channel has one cause or three. See their documentation.
    //
    // `lost` is where the three-way answer stopped being the local path's alone.
    // This arm was `no_such_actor(&actor)` for every cause: an ordinary keyed
    // actor with no supervisor anywhere near it, whose handler took
    // `reply.later()` and dropped the `Deferred` — the move that type's own
    // `#[must_use]` asks for — told the peer *no actor is mounted as X* about an
    // actor that answers the next call on this same connection.
    //
    // The cell the route handed back is settled by that actor's own loop before
    // it drops anything, so this read happens-after the verdict for the drop that
    // woke it. What it cannot close is the *late* direction, and
    // `deliver_unkeyed` states it in full: an actor that moved on between the
    // death and this read is described as it is now rather than as it was.
    answer
        .await
        .map(|answered| rendered(answered, budget.map_or(0, Queueing::millis)))
        .map_err(|_| lost(fate.standing()))
}

/// What the actor this envelope addresses is doing, without delivering anything
/// to it.
///
/// The non-enqueuing twin of [`deliver_keyed`], and the keyed half of what a
/// generated `__status` answers. Generated dispatch calls this **instead of**
/// that one, above it in the same closure, and nothing enters a mailbox — which
/// is the whole mechanism rather than an optimisation. `Handle::busy` states
/// the constraint: a message asking this question would queue behind the turn
/// that wedged the actor, and if it ever ran it would report `busy: true` about
/// itself while its own permit counted in `queued`. It would answer the state
/// of asking rather than the state of the actor.
///
/// Governance is untouched by that. The layer chain runs above the dispatch
/// closure, so a [`Middleware`](crate::Middleware) refuses `__Status` per peer
/// and per method exactly as it refuses any other; what the interception removes
/// is the mailbox, not the [`Allowlist`](crate::Allowlist) and not the layer.
///
/// # It reads, and routing is what inserts
///
/// [`Registry::serving`] takes the read lock, the way `Registry::contains` does.
/// `Registry::route` takes the write lock and builds from the factory a
/// [`RegistryBuilder::spawn_with`](crate::RegistryBuilder::spawn_with) system was
/// given, so this question asked through routing would *create* the actor it was
/// asked about. That is the one behaviour this door has that an ordinary keyed
/// call does not, and it is why the answer comes from here rather than from a
/// delivery that happens not to reach a handler.
///
/// # Errors
///
/// [`RemoteError::BadActorId`] for an id [`ActorId::validate`] refuses, checked
/// first and before any lookup exactly as [`deliver_keyed`] checks it, so it
/// says nothing about what is mounted. Then [`RemoteError::NoSuchActor`] when
/// nothing is mounted under a well-formed id — which deliberately does not
/// separate *never existed* from *was stopped*, for the reason that variant
/// carries.
#[doc(hidden)]
pub fn serving_keyed<M, T>(
    system: &Registry<M>,
    request: &Addressed<T>,
) -> std::result::Result<Serving, RemoteError> {
    let actor = &request.actor;
    actor.validate().map_err(|err| RemoteError::BadActorId {
        id: actor.to_string(),
        reason: err.to_string(),
    })?;
    system
        .serving(actor)
        .ok_or_else(|| RemoteError::NoSuchActor {
            id: actor.to_string(),
        })
}

impl<R: Rpc> crate::Carries<R> for Addressed<R> {
    fn carried(&self) -> &R {
        self.message()
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::expect_used, clippy::unwrap_used)]

    use super::*;
    use crate::middleware::Reached;
    use crate::MountError;

    /// A context in the shape `serve_one` builds one, with no endpoint anywhere
    /// near it: `Context::new` is `pub(crate)`, which is exactly why the two tests
    /// below can live here and nowhere downstream.
    fn a_context() -> Context {
        Context::new(
            iroh::SecretKey::from_bytes(&[7; 32]).public(),
            "Ledger",
            "Post",
        )
    }

    /// The provenance survives the newtype: each constructor lends out the
    /// `Caller` its case names, and `Local` carries nothing to attribute.
    #[test]
    fn each_origin_lends_out_the_caller_its_case_names() {
        assert!(matches!(Origin::local().caller(), Caller::Local));

        let cx = a_context();
        let peer = cx.peer();
        let origin = Origin::peer(cx);
        assert!(
            matches!(origin.caller(), Caller::Peer(cx) if cx.peer() == peer),
            "a peer's turn must be attributable to the key iroh proved"
        );
    }

    /// **The hole, pinned as a test rather than claimed as a guarantee.**
    ///
    /// `Origin` is not `Clone` and has no public constructor, and neither fact
    /// stops a caller from cloning the `Context` *before* two deliveries. This is
    /// what that costs: one `completed()` mark, made through one origin, is read
    /// back through the other — and `rpc.rs` reads exactly this to decide whether
    /// an idempotency key is recorded or released, so the second call would record
    /// a reply it never produced.
    ///
    /// Asserting the defect is deliberate. It is unreachable today because
    /// `serve_one` builds one context per request and generated dispatch delivers
    /// once with it — a property of two call sites, not of this type. If a later
    /// wave closes it for real, this test goes red and says so, which is the only
    /// way the documentation above gets corrected a fourth time instead of
    /// drifting.
    #[test]
    fn two_origins_over_one_cloned_context_still_share_one_marker() {
        let cx = a_context();
        let first = Origin::peer(cx.clone());
        let second = Origin::peer(cx);

        let Caller::Peer(ran) = first.caller() else {
            panic!("built from a context, so this is the peer arm");
        };
        ran.progress().completed();

        let Caller::Peer(never_ran) = second.caller() else {
            panic!("built from a context, so this is the peer arm");
        };
        assert_eq!(
            never_ran.progress().reached(),
            Reached::Completed,
            "the marker is shared, so withholding Clone on Origin does not close this"
        );
    }

    /// Two types with IDENTICAL postcard encodings and different meanings.
    ///
    /// The reason the ALPN hashes the request type rather than trusting the
    /// bytes: postcard writes no type identity, so one decodes cleanly as the
    /// other. This used to be the reason a payload stayed sealed until a
    /// per-actor hash agreed; it is now the reason
    /// `Envelope<WireRequest<Addressed<Request>>>` is what the keyed contract
    /// names.
    #[derive(Serialize, Deserialize, Schema, Debug, PartialEq, Eq)]
    struct Brl {
        cents: u64,
    }

    #[derive(Serialize, Deserialize, Schema, Debug, PartialEq, Eq)]
    struct Usd {
        cents: u64,
    }

    /// A request in the shape `service!` generates, so the [`Rpc`] delegation
    /// below is tested against something with more than one method.
    #[derive(Serialize, Deserialize, Schema, Debug, PartialEq, Eq)]
    enum Ledger {
        Post { cents: u64 },
        Balance {},
    }

    impl Rpc for Ledger {
        const SERVICE: &'static str = "Ledger";
        const METHODS: &'static [&'static str] = &["Post", "Balance"];

        fn method(&self) -> &'static str {
            match self {
                Self::Post { .. } => "Post",
                Self::Balance {} => "Balance",
            }
        }
    }

    /// The fact the ALPN exists to defeat, stated as a test rather than as a
    /// comment: the bytes DO decode as the wrong type.
    #[test]
    fn the_wrong_type_decodes_cleanly_from_the_same_bytes() {
        let bytes = postcard::to_stdvec(&Brl { cents: 4_471 }).expect("encodes");
        let wrong: Usd = postcard::from_bytes(&bytes).expect("no error, wrong meaning");
        assert_eq!(wrong, Usd { cents: 4_471 });
    }

    /// The wrapper is two fields in this order, and postcard writes a struct as
    /// its fields back to back — so this pins the bytes a peer must produce.
    #[test]
    fn the_wrapper_is_the_id_then_the_message() {
        let addressed = Addressed::new(ActorId::new("ab").expect("valid"), Brl { cents: 9 });
        let encoded = postcard::to_stdvec(&addressed).expect("encodes");

        let mut expected = postcard::to_stdvec(&"ab".to_string()).expect("encodes");
        expected.extend(postcard::to_stdvec(&Brl { cents: 9 }).expect("encodes"));

        assert_eq!(encoded, expected);
    }

    /// **The capability this wave buys**, at the one seam that delivers it: a
    /// layer above a keyed service reads the actor's own method name off the
    /// envelope, without decoding anything itself.
    ///
    /// The old envelope answered the constant `"Message"` here for every method
    /// of every actor, which is why per-method middleware and per-method authz
    /// were impossible on the keyed path.
    #[test]
    fn the_wrapper_reports_the_inner_methods_own_name() {
        let post = Addressed::new(ActorId::new("a").expect("valid"), Ledger::Post { cents: 1 });
        let balance = Addressed::new(ActorId::new("a").expect("valid"), Ledger::Balance {});

        assert_eq!(post.method(), "Post");
        assert_eq!(balance.method(), "Balance");
        assert_eq!(
            <Addressed<Ledger> as Rpc>::SERVICE,
            "Ledger",
            "the service name is the inner request's, so one name reaches both paths"
        );
    }

    /// And the id is readable from a layer too, which is what a per-actor rule
    /// runs on.
    #[test]
    fn the_wrapper_names_the_actor_it_addresses() {
        let addressed = Addressed::new(
            ActorId::new("obra/4471").expect("valid"),
            Ledger::Balance {},
        );
        assert_eq!(
            addressed.actor().map(ActorId::as_str),
            Some("obra/4471"),
            "a layer must be able to apply a per-actor rule before dispatch"
        );
    }

    /// `split` hands back exactly what `new` was given, in that order. The two
    /// are the only route in and the only route out.
    ///
    /// Made red by `scripts/break-once.sh keylink/src/actor/remote.rs` on
    /// `split`'s `(self.actor, self.message)` → `(ActorId::new("wrong")..,
    /// self.message)`. Watched: `left: "wrong"` against `right: "a"`.
    #[test]
    fn split_undoes_new() {
        let (id, message) =
            Addressed::new(ActorId::new("a").expect("valid"), Brl { cents: 4_471 }).split();
        assert_eq!(id.as_str(), "a");
        assert_eq!(message, Brl { cents: 4_471 });
    }

    /// The registry refuses in its own vocabulary and the wire translates, so
    /// this pins what a peer actually receives from each arm.
    ///
    /// The capacity sentence is written twice — once on [`MountError`] for a
    /// mount by hand, once on [`RouteError`] for a lazy spawn — because the two
    /// enums describe different sets of conditions. This is what stops the two
    /// copies from drifting: a peer met the same sentence before this
    /// translation existed, and must go on meeting it.
    ///
    /// **A third copy now, and this is where the three are held together.**
    /// [`RemoteError::AtCapacity`] is a variant rather than a `Failed` carrying
    /// `RouteError`'s rendered text, so a peer matches the cap instead of reading
    /// for the word. The `to_string` comparison is what keeps that variant's own
    /// `#[error]` from drifting off the two it replaced — and it is now a
    /// comparison between three sentences written in three files rather than two.
    ///
    /// The cap crosses as a `u64` where both local enums hold a `usize`, because
    /// `usize` has no wire schema. The number is asserted rather than the
    /// conversion, which is the only thing a peer can see.
    #[test]
    fn the_capacity_refusal_reads_the_same_by_hand_and_lazily() {
        let by_hand = MountError::AtCapacity { capacity: 10_000 };
        let lazily = RemoteError::from(RouteError::AtCapacity { capacity: 10_000 });

        assert_eq!(lazily, RemoteError::AtCapacity { capacity: 10_000 });
        assert_eq!(lazily.to_string(), by_hand.to_string());
        assert_eq!(
            lazily.to_string(),
            "the actor system is at its capacity of 10000 live actors",
        );
    }

    /// An unmounted id is `NoSuchActor` carrying the id as text, unchanged by
    /// the registry having its own error type now.
    #[test]
    fn an_unrouted_id_reaches_the_peer_as_no_such_actor() {
        let id = ActorId::new("nobody").expect("valid");
        assert_eq!(
            RemoteError::from(RouteError::NoSuchActor { id: id.clone() }),
            RemoteError::NoSuchActor {
                id: "nobody".to_string()
            }
        );
        assert_eq!(
            no_such_actor(&id),
            RemoteError::NoSuchActor {
                id: "nobody".to_string()
            }
        );
    }
}
