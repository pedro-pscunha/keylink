//! Everything a peer can influence: the ALPN, the envelope, and a payload that
//! stays sealed until a hash agrees.
//!
//! This half depends on the registry beside it and serves it, and the
//! dependency runs this way round on purpose: naming [`System`] here costs
//! nothing, and naming [`Sealed`] there would cost the split.
//!
//! # What replaces the handshake gate
//!
//! One ALPN for the whole system means the handshake can only prove that the
//! peer agrees about the *envelope*. Per-actor drift is caught one layer down:
//! the request declares the schema hash of the actor's own message type, the
//! registry compares it against the hash the actor was mounted with, and the
//! payload stays [`Sealed`] until that comparison succeeds.

use std::fmt;
use std::sync::{Arc, OnceLock};

use postcard_schema::Schema;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use tokio::sync::oneshot;

use super::registry::{RouteError, System};
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
    kinds: crate::WireKinds,
}

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
    actor: ActorId,
    /// The schema hash of the actor's own message type.
    schema: u64,
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

// ───────────────────────────────────────────────────── what a mailbox carries

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
/// | `Mail`      | 9,620 bytes    | 96.2 MB      |
///
/// The 7,936 byte difference is the block: `Mail` is 256 bytes, so 32 of them is
/// 8,192, against 256 for 32 pointers. The cost of boxing is one allocation per
/// message, on a path that already allocates the payload.
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

// ────────────────────────────────────────────────────── serving the registry

fn no_such_actor(id: &ActorId) -> RemoteError {
    RemoteError::NoSuchActor { id: id.to_string() }
}

/// What a peer is told, which is deliberately less than what happened.
///
/// The capacity refusal is [`RemoteError::Failed`] and not a variant of its own,
/// because a cap is this node's configuration and not something the caller can
/// act on differently from any other refusal. Changing that would move the
/// system ALPN, so it is a wire decision rather than a tidying one.
impl From<RouteError> for RemoteError {
    fn from(err: RouteError) -> Self {
        match &err {
            RouteError::NoSuchActor { id } => no_such_actor(id),
            RouteError::AtCapacity { .. } => Self::Failed(err.to_string()),
        }
    }
}

/// The half that knows there is a network: the ALPN, the service, and the walk
/// from an arriving [`ActorRequest`] to a [`Mail`] in a mailbox.
///
/// Written against `System<Mail>` rather than against `M`, so the registry
/// beside it stays free of every type named here. Inherent impls on a concrete
/// instantiation are what make that possible in one crate; two crates would need
/// an extension trait, because a transport crate cannot add inherent methods to
/// a registry crate's type.
impl System<Mail> {
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

        let (ours, mail) = self.route(&request.actor)?;
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
    use crate::MountError;

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
