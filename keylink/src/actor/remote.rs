//! Everything a peer can influence on the keyed path: the wrapper that names an
//! actor, where a message came from, and the walk from an arriving request to a
//! slot in a mailbox.
//!
//! This half depends on the registry beside it and serves it, and the
//! dependency runs this way round on purpose: naming [`System`] here costs
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
//! is refused at the handshake, and a `System<Mail<Request, Response>>` cannot
//! physically hold an actor of another service — the mismatch stops being a
//! refusal and becomes unrepresentable.
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

use std::fmt;

use n0_future::time::Instant;
use postcard_schema::Schema;
use serde::{Deserialize, Serialize};
use tokio::sync::oneshot;

use super::registry::{RouteError, System};
use crate::{ActorId, Body, BodyReader, Caller, Context, Outcome, RemoteError, Rpc};

// ─────────────────────────────────────────────────────────── the wire wrapper

/// One message, plus the actor on this node it is addressed to.
///
/// The keyed twin of a bare `Request`: a service's own request type travels
/// inside this, and a `System` of that service is what takes it apart. Nesting
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

    fn method(&self) -> &'static str {
        self.message.method()
    }

    fn actor(&self) -> Option<&ActorId> {
        Some(&self.actor)
    }
}

// ───────────────────────────────────────────────────── what a mailbox carries

/// Where one keyed message's answer goes: the reply, and a body to stream
/// beside it.
///
/// An alias because generated code has to name this channel, and spelling
/// `tokio::sync::oneshot::Sender<..>` in the expansion would make every
/// `service!` user's build depend on tokio's module path rather than only on
/// tokio being present.
#[doc(hidden)]
pub type Answer<Resp> = oneshot::Sender<(Outcome<Resp>, Body)>;

/// Where a message reached an actor from.
///
/// Owned, not borrowed. [`Caller<'a>::Peer`](Caller) holds a `&'a Context` and
/// `Caller` is covariant in `'a`, so a `Caller` that generated code was *handed*
/// never widens to `'static` — proved three ways in RFC-07 §1.6, and the reason
/// a mailbox message cannot simply carry a `Caller`. It carries this instead and
/// the loop borrows a [`Caller`] out of it for the length of one turn.
///
/// # Deliberately NOT `Clone`, and this is the load-bearing part
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
/// The fix is ownership, never cloning: [`deliver_to_actor`] takes the
/// `Context` by value and moves it in here. Withholding `Clone` is what makes
/// the mistake a compile error rather than something a peer finds out about.
/// **Do not derive it.**
#[doc(hidden)]
#[derive(Debug)]
pub enum Origin {
    /// From inside this process, through the generated `Handle`. There is no
    /// peer, no handshake and no proven key.
    ///
    /// Unreachable today: the local handle runs its own loop in `local.rs` and
    /// does not go through a keyed mailbox. The variant exists because
    /// [`Origin`] is the shape the generated mailbox commits to, and giving the
    /// local path a second shape to arrive in is the split this type exists to
    /// avoid.
    Local,
    /// From a peer iroh proved at the handshake.
    Peer(Context),
}

impl Origin {
    /// Borrow a [`Caller`] out of this, for the length of one turn.
    ///
    /// `pub` because the loop that calls it is generated into the **user's**
    /// crate, where a private method is `E0624`. That is the same error Wave 0
    /// hit one layer up on `Context::new`, and this one was written without
    /// `pub` first — see RFC-07 §1.6's revision note.
    #[doc(hidden)]
    #[must_use]
    pub fn caller(&self) -> Caller<'_> {
        match self {
            Self::Local => Caller::Local,
            Self::Peer(cx) => Caller::Peer(cx),
        }
    }
}

/// One message on its way to an actor. Generated code destructures these.
///
/// **Boxed on the way into the mailbox**, and the reasoning for that — the
/// eagerly allocated block of 32 slots, the measured table, and why the
/// pressure grows with whatever a service puts in here — lives on
/// [`Mailbox`](crate::Mailbox), where the `Box` is actually written.
///
/// Typed in both directions, which is the wave's whole subject: `Req` and
/// `Resp` are the service's own, so nothing here is a `Vec<u8>` and nothing
/// downstream has to decode.
#[doc(hidden)]
pub struct Mail<Req, Resp> {
    /// Where the message came from, and who to attribute it to.
    pub origin: Origin,
    /// The service's own request, already decoded.
    pub request: Req,
    /// The payload streaming up beside the request.
    pub body: BodyReader,
    /// Where the answer goes.
    pub reply: Answer<Resp>,
}

/// Hand-written for the same reason [`Mounted`](crate::Mounted)'s is: a derive
/// writes `impl<Req: Debug, Resp: Debug> Debug`, which would make printing a
/// mailbox slot depend on two types nobody prints. The origin is the only field
/// worth naming anyway — a `BodyReader` is a stream and a `oneshot::Sender` is a
/// channel.
impl<Req, Resp> fmt::Debug for Mail<Req, Resp> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Mail")
            .field("origin", &self.origin)
            .finish_non_exhaustive()
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
/// # Why [`Mail`](Self::Mail) is an associated type rather than the concrete one
///
/// Today every implementor sets it to [`struct@Mail`]`<Request, Response>` and
/// the indirection buys nothing. It is the seam the generated mailbox enum
/// arrives through: when the mailbox stops being one struct and becomes a
/// per-service enum with a typed reply channel per variant, this associated type
/// is the only line that changes, and [`deliver_to_actor`] does not.
#[doc(hidden)]
pub trait KeyedService: Send + 'static {
    /// The service's own request, as it arrives inside an [`Addressed`].
    type Request: Rpc + Send + 'static;
    /// The service's own response.
    type Response: Send + 'static;
    /// What this service's mailbox carries.
    type Mail: Send + 'static;

    /// Build one mailbox slot out of the parts [`deliver_to_actor`] holds.
    fn mail(
        origin: Origin,
        request: Self::Request,
        body: BodyReader,
        reply: Answer<Self::Response>,
    ) -> Self::Mail;
}

// ────────────────────────────────────────────────────── serving the registry

fn no_such_actor(id: &ActorId) -> RemoteError {
    RemoteError::NoSuchActor { id: id.to_string() }
}

/// What a peer is told, which is deliberately less than what happened.
///
/// The capacity refusal is [`RemoteError::Failed`] and not a variant of its own,
/// because a cap is this node's configuration and not something the caller can
/// act on differently from any other refusal. Changing that would move every
/// service's ALPN, so it is a wire decision rather than a tidying one.
impl From<RouteError> for RemoteError {
    fn from(err: RouteError) -> Self {
        match &err {
            RouteError::NoSuchActor { id } => no_such_actor(id),
            RouteError::AtCapacity { .. } => Self::Failed(err.to_string()),
        }
    }
}

/// Walk one arriving request to the actor it names, and wait for the answer.
///
/// **A free function taking `&System<..>`**, and that shape is deliberate: it
/// compiles identically whether `System` lives in this crate or in another one,
/// which an inherent impl cannot do. RFC-04 §16.8 splits the registry into its
/// own crate, and this wave is independent of whether that has happened. No
/// inherent impl on `System` names a service type.
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
/// can stop it. A caller that gives up from here on — a
/// [`Timeout`](crate::Timeout) expiring, an aborted connection task — is giving
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
/// [`RemoteError::Failed`] for a malformed id or a full registry,
/// [`RemoteError::NoSuchActor`] when nothing is mounted and no factory built
/// one, and [`RemoteError::TimedOut`] when the queue budget expired before a
/// permit arrived.
#[doc(hidden)]
pub async fn deliver_to_actor<S: KeyedService>(
    system: &System<S::Mail>,
    cx: Context,
    request: Addressed<S::Request>,
    body: BodyReader,
) -> std::result::Result<(Outcome<S::Response>, Body), RemoteError> {
    let (actor, message) = request.split();
    actor
        .validate()
        .map_err(|err| RemoteError::Failed(err.to_string()))?;

    let mail = system.route(&actor)?;

    // Read before `cx` moves into the `Mail`, which is the only reason these
    // are bindings rather than expressions at their use sites.
    let budget = cx.queueing();
    let progress = cx.progress();
    let (reply, answer) = oneshot::channel();
    let queueing = mail.send(Box::new(S::mail(Origin::Peer(cx), message, body, reply)));
    let sent = match budget {
        None => queueing.await,
        // `saturating_duration_since` on `n0_future::time::Instant`, the clock
        // `Queueing` was stamped with — tokio's natively, `web_time`'s in a
        // browser, and `std::time::Instant` on neither, since that one panics on
        // `wasm32-unknown-unknown`. Reading the same clock as `middleware.rs` is
        // one fewer place that can drift from it.
        //
        // Dropping the send future is what makes the refusal honest: tokio does
        // not queue a message whose send was cancelled before a permit arrived,
        // so a message refused here provably never entered the mailbox. The
        // local path relies on the same property.
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
    // Below every refusal above it, so no refusal here can be mistaken for a
    // message that queued. Above the wait, because the wait is exactly what a
    // caller gives up on.
    progress.enqueued();

    answer.await.map_err(|_| no_such_actor(&actor))
}

#[cfg(test)]
mod tests {
    #![allow(clippy::expect_used, clippy::unwrap_used)]

    use super::*;
    use crate::MountError;

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
    #[test]
    fn the_capacity_refusal_reads_the_same_by_hand_and_lazily() {
        let by_hand = MountError::AtCapacity { capacity: 10_000 };
        let lazily = RemoteError::from(RouteError::AtCapacity { capacity: 10_000 });

        assert_eq!(
            lazily,
            RemoteError::Failed(
                "the actor system is at its capacity of 10000 live actors".to_string()
            )
        );
        assert_eq!(lazily.to_string(), by_hand.to_string());
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
