//! The error hierarchy.
//!
//! Every fallible boundary in this crate is typed. There is no
//! `.map_err(|e| Error::Transport(e.to_string()))` anywhere, because every iroh
//! error `?`s straight into [`Error`] through the [`From`] impls below. That is
//! the single source of truth for "how does a transport failure become a
//! keylink failure": one list, in one place.
//!
//! # The dependency runs one way, and it runs from here
//!
//! [`Failure`] names what happened to a **call**, so it lives beside the enum it
//! wraps, and this module imports [`Withheld`] to say what happened to a turn.
//! `observe.rs` names what happened to a turn and does **not** import [`Error`]:
//! the reverse edge would make an observation module depend on the transport
//! vocabulary, and nothing on that side has a use for one.

use iroh::endpoint::{
    BindError, ClosedStream, ConnectError, ConnectionError, ReadError, WriteError,
};
use iroh::{EndpointId, KeyParsingError};

use crate::{RemoteError, Unanswered, Withheld};

/// Everything that can go wrong on a keylink call.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum Error {
    /// The connection failed, was refused, or dropped.
    ///
    /// A schema mismatch surfaces here as "peer doesn't support any known
    /// protocol" — the ALPN gate rejecting a peer built against different types.
    #[error("transport: {0}")]
    Transport(#[from] Transport),

    /// The bytes on the wire did not decode into the expected type.
    #[error("codec: {0}")]
    Codec(#[from] postcard::Error),

    /// A header frame announced more bytes than the cap allows. Checked before
    /// a single byte is queued, so announcing a huge frame costs the peer
    /// everything and us nothing.
    #[error("header frame of {len} bytes exceeds the {cap} byte cap")]
    FrameTooLarge {
        /// What the peer announced.
        len: u64,
        /// What we accept.
        cap: u32,
    },

    /// The stream ended part-way through a header frame. Deliberately distinct
    /// from a clean close between frames, which is not an error at all.
    #[error("stream ended {got} bytes into a {want} byte header frame")]
    FrameTruncated {
        /// Bytes that did arrive.
        got: usize,
        /// Bytes the frame announced.
        want: usize,
    },

    /// The peer closed cleanly while we were waiting for a response.
    #[error("peer closed the connection")]
    Closed,

    /// The serving side answered with a [`RemoteError`] instead of a value.
    ///
    /// Distinct from a transport failure: the call arrived and was understood.
    /// What happened next is whatever the inner error says, and this layer does
    /// **not** narrow it — the arm in `rpc.rs` that builds this wraps every
    /// `RemoteError`, including [`Withheld`](crate::RemoteError::Withheld)
    /// carrying a [`Panicked`](crate::Withheld::Panicked) and
    /// [`TimedOut`](crate::RemoteError::TimedOut).
    ///
    /// **It read *"remote refused: {0}"*, over a doc that said the call "was
    /// declined".** Neither is true of a handler that exploded or of a deadline
    /// that expired, so a client whose remote handler panicked read
    /// *remote refused: panicked: ..* about a call nobody declined. The word
    /// belongs to [`Withheld::Refused`] and its siblings, one layer in, which
    /// say which of them happened.
    #[error("remote error: {0}")]
    Remote(#[from] crate::RemoteError),

    /// The peer speaks a different schema.
    ///
    /// Our ALPN carries a hash of the contract, so a peer built against
    /// different types cannot complete the handshake. Surfacing this as a plain
    /// transport error would send whoever reads it looking at the network,
    /// which is the wrong layer entirely: nothing is wrong with the network.
    #[error("schema mismatch: the peer was built against a different contract (ours is {ours})")]
    SchemaMismatch {
        /// The ALPN we offered, whose trailing hash identifies our schema.
        ours: String,
    },

    /// A blocking call was made from inside an async runtime.
    ///
    /// Blocking there parks a worker thread on work that same runtime has to
    /// perform, which deadlocks. Refused up front rather than hung: a returned
    /// error is debuggable, a stalled process is not.
    #[error("blocking keylink call from inside an async runtime - use the async API here")]
    BlockingInAsync,

    /// A protocol handler panicked, and the router surfaced it during shutdown.
    ///
    /// Distinct from [`Error::Transport`] on purpose: nothing went wrong on the
    /// network. Our own handler code is broken, and flattening that into a
    /// transport error would send whoever reads the log looking at the wrong
    /// layer.
    #[error("a protocol handler panicked: {0}")]
    HandlerPanic(String),

    /// A browser `ReadableStream` could not be read as bytes.
    ///
    /// `wasm_streams` can only adapt a stream created with `type: "bytes"`,
    /// because it needs a BYOB reader. The ordinary JS shape,
    /// `new ReadableStream({start(c) {..}})`, has none — and the underlying
    /// `into_async_read` PANICS on it rather than returning an error, which is
    /// why `Body::from_web_stream` is fallible. (Not a link: that constructor
    /// exists only on wasm, and rustdoc runs on the host.)
    ///
    /// Not gated on the `web` feature on purpose: one enum shape on every
    /// target costs one dead variant on native and removes a `cfg` from a
    /// public type.
    #[error("the ReadableStream is locked to a reader, or it is not a byte stream")]
    NotAByteStream,

    /// A body ended before the length its sender declared. The streaming
    /// counterpart of [`Error::FrameTruncated`]: a short file is never mistaken
    /// for a complete one.
    #[error("body ended after {received} of {declared} declared bytes")]
    BodyTruncated {
        /// What the sender promised.
        declared: u64,
        /// What arrived.
        received: u64,
    },

    /// A body sent more bytes than its sender declared.
    #[error("body exceeded its declared length of {declared} bytes")]
    BodyOverrun {
        /// What the sender promised.
        declared: u64,
    },

    /// A body did not fit the caller-supplied buffer cap.
    #[error("body exceeds the {cap} byte cap the caller allowed")]
    BodyTooLarge {
        /// The cap the caller passed.
        cap: usize,
    },

    /// A well-formed but wrong-shaped reply. Impossible from a peer running the
    /// same generated code; reachable only from a hand-rolled implementation.
    #[error("expected a {expected} response, got {actual}")]
    UnexpectedResponse {
        /// The rpc that was called.
        expected: &'static str,
        /// The variant that came back.
        actual: &'static str,
    },

    /// The holder refused to serve this content.
    ///
    /// Distinct from [`Error::Transport`] for the same reason
    /// [`Error::SchemaMismatch`] is: nothing is wrong with the network, and a
    /// refusal reported as `io: connection lost` sends whoever reads it looking
    /// at one that is working. iroh-blobs answers a refusal with
    /// `ERR_PERMISSION` on the wire — a stream reset when one request is
    /// denied, an application close when the whole connection is — so the fact
    /// is available and only needed carrying up.
    ///
    /// The cause is a missing grant: this peer was never handed this address,
    /// or the address is a child of a hash sequence rather than the blob it was
    /// granted, or the grant fell off the end of a ledger bounded by
    /// [`MAX_GRANTS`](crate::MAX_GRANTS).
    #[error("the holder refused to serve blob {id}")]
    BlobRefused {
        /// The content that was refused.
        id: crate::BlobId,
    },

    /// Bad or missing configuration, reported at startup rather than on first use.
    #[error("config: {0}")]
    Config(#[from] ConfigError),

    /// Reading or writing a body against local storage failed.
    #[error("body io: {0}")]
    Io(#[from] std::io::Error),
}

/// What a call met, whichever surface carried it.
///
/// A local call answers with [`Unanswered`] and a served one with [`Error`], and
/// that one disagreement is the whole of why no function can be written against
/// both. This is the type a caller who has chosen not to know which surface it
/// holds names instead.
///
/// **Nothing reaches this by accident.** Every inherent method keeps its exact
/// error type — [`Unanswered`] locally, [`Error`] remotely — and this one is
/// reachable only through a bound the caller wrote. Writing that bound is the act
/// of giving up the knowledge, which is why the wider type is not a loss there
/// and would be one anywhere else.
///
/// # One [`Withheld`], from either side
///
/// [`Unanswered::Turn`] and an [`Error::Remote`] carrying a
/// [`RemoteError::Withheld`] both land on [`Turn`](Self::Turn). The shared
/// condition is matched in one arm regardless of which side produced it, and
/// that is the only thing a generic caller needs a new type for.
///
/// # Two arms are one-sided
///
/// [`Other`](Self::Other) has no local producer. [`Reentrant`](Self::Reentrant)
/// has no served producer: a peer's request arrives on a connection's task,
/// which has run no turn, so nothing on that side can build one.
///
/// # Nothing is erased, and the cost is one level of depth
///
/// [`Other`](Self::Other) carries the concrete [`Error`] rather than a
/// `Box<dyn Error>`, so every served condition stays matchable and the
/// `source()` chain survives. What it costs is depth — a caller matching a
/// refusal reads three levels through the bound where it reads two against a
/// client:
///
/// ```text
/// Error::Remote(RemoteError::Unauthorized(method))                   // against a client
/// Failure::Other(Error::Remote(RemoteError::Unauthorized(method)))   // through the bound
/// ```
///
/// # `Debug + Error`, and nothing else
///
/// [`Error`] is neither `Clone` nor `PartialEq`, so this cannot be either —
/// while [`Unanswered`] is `Clone + PartialEq + Eq`. A test that today writes
/// `assert_eq!(handle.get(1).await, Err(Unanswered::Turn(Withheld::Gone)))`
/// **cannot** write that through the bound, and the remedy is
/// [`withheld`](Self::withheld) rather than a hand-written `PartialEq`.
///
/// # `#[non_exhaustive]`, where the nearest neighbour rules the other way
///
/// [`Unanswered`] is deliberately exhaustive, and its heading argues why: its
/// losses are all local and all enumerated, a new one is added to [`Withheld`],
/// and exhaustiveness reaches a caller one level in. **The difference here is
/// the third arm.** [`Other`](Self::Other) carries [`Error`], which is the
/// transport vocabulary and grows on a schedule this crate does not own, so a
/// caller who matched this exhaustively would be broken by a wire condition
/// rather than by a design decision.
///
/// # There is no `From<NotQueued>`
///
/// A `tell` returns [`NotQueued`](crate::NotQueued) and is not a call this type
/// carries, so the conversion would have no caller and would be a third door
/// into an enum with two. [`Unanswered`] already has one if it is ever wanted.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum Failure {
    /// The turn was reached and its value was not handed back.
    ///
    /// [`Withheld`] carries which loss it was and what to do next. This is the
    /// one arm both sides produce.
    #[error("{0}")]
    Turn(Withheld),

    /// The call was made from inside the target actor's own turn.
    ///
    /// The actor is **still running** — it is running the turn this call was
    /// made from. **No served surface produces this**, and the argument is in
    /// [`Unanswered::Reentrant`]: a request from a peer arrives on a
    /// connection's task, which has run no turn.
    ///
    /// The text below is the **fourth** copy of one sentence, beside
    /// [`Unanswered::Reentrant`], [`NotQueued::Reentrant`](crate::NotQueued::Reentrant)
    /// and [`TurnLost::Reentrant`](crate::TurnLost::Reentrant). No `From` reaches
    /// this type from any of them, so `turns.rs`'s
    /// `one_vocabulary_and_every_loss_in_it_has_a_producer` compares this one
    /// directly rather than through a conversion.
    #[error("this call was made from inside the target actor's own turn: it would deadlock")]
    Reentrant,

    /// The actor's mailbox was full and it sheds, so the call was refused before
    /// any turn.
    ///
    /// **The second arm both sides produce**, and it is a variant for
    /// [`Turn`](Self::Turn)'s reason rather than a new one: a local caller reads
    /// [`Unanswered::Full`] and a peer reads
    /// [`RemoteError::MailboxFull`], and folding either into
    /// [`Other`](Self::Other) would leave the two sides matching one condition in
    /// two places again. It says *back off*; it does not say *retry now*.
    ///
    /// **It reads *call* where the other three read *message*, and that is
    /// deliberate.** This is the one of the four doors that took a call rather
    /// than a message — a `tell` answers [`NotQueued`](crate::NotQueued) and never
    /// arrives here. The same guard that holds the `Reentrant` texts equal asserts
    /// these two are **not**, so unifying them is a decision somebody makes rather
    /// than a tidy-up nothing notices.
    #[error("the actor's mailbox is full and it shed this call")]
    Full,

    /// Everything a call can meet that the shared vocabulary does not name.
    ///
    /// **No local surface produces this.** Every variant of [`Error`] except the
    /// two lifted ones — [`RemoteError::Withheld`] and
    /// [`RemoteError::MailboxFull`] — arrives here, unnarrowed, which is what
    /// keeps a schema mismatch and a timeout matchable through the bound.
    #[error("{0}")]
    Other(#[source] Error),
}

impl Failure {
    /// The shared condition, when this is one.
    ///
    /// [`Failure`] is not `PartialEq` and cannot be, because [`Error`] is not.
    /// [`Withheld`] is, so the comparable part stays comparable through here.
    ///
    /// **It returns `None` for [`Reentrant`](Self::Reentrant) and `None` for
    /// [`Other`](Self::Other), so it cannot tell them apart.** It replaces
    /// equality for one arm, not for three: a test asserting *"this was not a
    /// turn loss"* through it has asserted nothing about which of the other two
    /// it was. Match for that.
    ///
    /// ```
    /// use keylink::{Failure, Withheld};
    ///
    /// let failure = Failure::Turn(Withheld::Gone);
    /// assert_eq!(failure.withheld(), Some(&Withheld::Gone));
    ///
    /// // `None` discriminates nothing. This is the form that does.
    /// let failure = Failure::Reentrant;
    /// assert_eq!(failure.withheld(), None);
    /// assert!(matches!(failure, Failure::Reentrant));
    /// ```
    #[must_use]
    pub fn withheld(&self) -> Option<&Withheld> {
        match self {
            Self::Turn(withheld) => Some(withheld),
            _ => None,
        }
    }
}

impl From<Unanswered> for Failure {
    fn from(unanswered: Unanswered) -> Self {
        match unanswered {
            Unanswered::Turn(withheld) => Self::Turn(withheld),
            Unanswered::Reentrant => Self::Reentrant,
            Unanswered::Full => Self::Full,
        }
    }
}

impl From<Error> for Failure {
    fn from(error: Error) -> Self {
        match error {
            // THE LIFT. Without this arm the one condition both sides produce
            // arrives as `Other` on the served side and as `Turn` on the local
            // one, and the whole wave buys nothing.
            Error::Remote(RemoteError::Withheld(withheld)) => Self::Turn(withheld),
            // The same lift for the second such condition, added with the shed
            // policy that made a peer able to meet it. A served `MailboxFull` and
            // a local `Unanswered::Full` are one fact, so they get one arm.
            Error::Remote(RemoteError::MailboxFull) => Self::Full,
            other => Self::Other(other),
        }
    }
}

/// A failure that came from iroh rather than from keylink.
///
/// Kept as its own enum so callers can match "the network went wrong" in one
/// arm without enumerating six iroh types, and still reach the concrete cause.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum Transport {
    /// Binding the local endpoint failed.
    #[error(transparent)]
    Bind(#[from] BindError),
    /// Dialing the peer failed. A schema mismatch lands here.
    #[error(transparent)]
    Connect(#[from] ConnectError),
    /// An established connection failed or was closed.
    #[error(transparent)]
    Connection(#[from] ConnectionError),
    /// Writing to a stream failed.
    #[error(transparent)]
    Write(#[from] WriteError),
    /// Reading from a stream failed.
    #[error(transparent)]
    Read(#[from] ReadError),
    /// An operation was attempted on a stream that is already finished.
    #[error(transparent)]
    StreamClosed(#[from] ClosedStream),
}

/// Configuration that is wrong or missing.
///
/// Every variant is raised at startup. None of them can be reached by a running
/// service, because a service that reached one never started.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum ConfigError {
    /// A required environment variable is unset.
    #[error("{var} is not set - refusing to start")]
    MissingEnvVar {
        /// The variable that was expected.
        var: String,
    },
    /// A secret key was present but unparseable. Never silently replaced with a
    /// random one: that would change our `EndpointId` and bounce us off every
    /// peer's allowlist.
    #[error("{var} is not a valid secret key: {source}")]
    MalformedSecretKey {
        /// The variable that held it.
        var: String,
        /// Why it did not parse.
        source: KeyParsingError,
    },
    /// An allowlist entry was not a valid endpoint id. Rejected rather than
    /// skipped: a typo that silently shrinks an allowlist is an incident.
    #[error("malformed endpoint id: {entry:?}")]
    MalformedEndpointId {
        /// The offending text.
        entry: String,
        /// Why it did not parse.
        source: KeyParsingError,
    },
    /// The allowlist parsed to nothing at all.
    #[error("allowlist is empty - refusing to serve a protocol nobody can reach")]
    EmptyAllowlist,
    /// A data-driven authorization rule file is not valid TOML in the expected shape.
    #[error("invalid authorization rules: {error}")]
    MalformedRules {
        /// Why the file could not be parsed.
        #[source]
        error: toml::de::Error,
    },
    /// The same peer appeared in more than one authorization table.
    #[error("duplicate authorization rules for peer {id}")]
    DuplicatePeer {
        /// The repeated peer.
        id: EndpointId,
    },
    /// A peer rule names no methods.
    #[error("peer {id} allows no methods - refusing to admit an unusable peer")]
    EmptyPeerMethods {
        /// The peer whose method set was empty.
        id: EndpointId,
    },
    /// A method named by startup configuration is not in this binary's service contract.
    #[error("unknown method {named:?} for service {service}")]
    UnknownMethod {
        /// The generated service name.
        service: &'static str,
        /// The name configuration tried to select.
        named: String,
    },
    /// Two services were registered on the same ALPN.
    ///
    /// Caught rather than allowed to be last-one-wins: the second registration
    /// would silently shadow the first, and the shadowed service would simply
    /// never answer.
    #[error("two services registered the same ALPN: {0}")]
    DuplicateAlpn(String),
    /// A reserved ALPN was mounted by hand.
    ///
    /// keylink means to mount this protocol itself, with a policy attached.
    /// Mounting it by hand would put an unauthorized protocol on an otherwise
    /// allowlisted node, so it is refused at startup rather than warned about
    /// in a doc comment nobody reads twice.
    #[error("{0} is reserved - keylink mounts this protocol itself, with a policy")]
    ReservedAlpn(String),
    /// A service declares `blob` methods and was given no content store.
    ///
    /// Every `blob` method answers with an address, and an address is only
    /// worth anything if some store holds the bytes. Raised at startup rather
    /// than on the first call, where it would look like a transport failure to
    /// whoever asked.
    #[error(
        "service {service} declares blob methods but was given no store - add .with_blobs(..)"
    )]
    MissingBlobStore {
        /// The ALPN of the service that is missing one.
        service: String,
    },
    /// Two services on one node were given two different content stores.
    ///
    /// One node has one blobs ALPN, so it can serve one store. Sharing a
    /// [`Blobs`](crate::Blobs) between services is normal; two distinct stores
    /// is a question with no right answer, so it is refused rather than
    /// resolved by picking one.
    ///
    /// Two tenants that must not share a store need two nodes.
    #[error("two services were given different blob stores - one node has one content store")]
    ConflictingBlobStores,
    /// A bounded identifier was empty or too long.
    ///
    /// Raised by a constructor, and again on the receive path, because a peer's
    /// value never passed through our constructor.
    #[error("invalid {kind}: {reason}")]
    MalformedKey {
        /// Which identifier: "actor id", "idempotency key".
        kind: &'static str,
        /// Why it was refused.
        reason: String,
    },
}

/// `?` must reach [`Error`] from an iroh error in one step, or every call site
/// grows a `.map_err`. thiserror gives `Transport: From<X>`, which is not
/// enough on its own, so the second hop is generated here from one list.
macro_rules! transport_source {
    ($($ty:ty),+ $(,)?) => {
        $(
            impl From<$ty> for Error {
                fn from(source: $ty) -> Self {
                    Self::Transport(Transport::from(source))
                }
            }
        )+
    };
}

transport_source!(
    BindError,
    ConnectError,
    ConnectionError,
    WriteError,
    ReadError,
    ClosedStream,
);

/// `Result` specialised to [`Error`].
pub type Result<T> = std::result::Result<T, Error>;

#[cfg(test)]
mod tests {
    #![allow(clippy::expect_used)]

    use std::error::Error as _;

    use super::*;

    /// One condition, two producers, one arm.
    ///
    /// [`Withheld`] is what a local caller reads as [`Unanswered::Turn`] and a
    /// peer reads as [`RemoteError::Withheld`], and a caller through the bound
    /// must not have to know which produced it. **Both halves are asserted here
    /// rather than in two tests, because the claim is that they agree**: a lift
    /// that fired and a local conversion that did not would each pass a test of
    /// its own.
    ///
    /// The second half is the other one-sidedness. [`Unanswered::Reentrant`]
    /// stays [`Failure::Reentrant`] and does not fold into a turn loss — nothing
    /// served produces the value, so this conversion is the only place it can be
    /// lost.
    ///
    /// Made red by `scripts/break-once.sh keylink/src/error.rs`, twice, as `T1`
    /// and `T2`. Both quotes below are unindented and the code is not, which is
    /// what keeps each anchor unique in its own file.
    ///
    /// `T1`, the lift. Search
    /// `Error::Remote(RemoteError::Withheld(withheld)) => Self::Turn(withheld),`
    /// and send the value to the fallthrough instead:
    /// `Error::Remote(remote) => Self::Other(Error::Remote(remote)),`. Watched:
    /// `── selected=1`, `── exit=100`, and the served half reporting `left: None`
    /// against `right: Some(Gone)`.
    ///
    /// `T2`, the fold. Search `Unanswered::Reentrant => Self::Reentrant,` and
    /// give reentrancy a turn loss it never had:
    /// `Unanswered::Reentrant => Self::Turn(Withheld::Dropped),`. Watched:
    /// `── selected=1`, `── exit=100`, and the local half reporting
    /// `left: Some(Dropped)` against `right: None`.
    ///
    /// **The two breaks redden two different assertions, and that is what says
    /// the halves are independent.** `T1` stops on the served half's first
    /// assertion, `T2` on the reentrancy one below it, and neither red is the
    /// other's.
    #[test]
    fn a_peer_failure_carrying_a_withheld_lands_on_turn() {
        let lifted = Failure::from(Error::Remote(RemoteError::Withheld(Withheld::Gone)));
        assert_eq!(lifted.withheld(), Some(&Withheld::Gone));
        assert!(!matches!(lifted, Failure::Other(_)));

        let local = Failure::from(Unanswered::Turn(Withheld::Gone));
        assert_eq!(local.withheld(), Some(&Withheld::Gone));

        let reentrant = Failure::from(Unanswered::Reentrant);
        assert_eq!(reentrant.withheld(), None);
        assert!(matches!(reentrant, Failure::Reentrant));
    }

    /// The chain a caller `?`s into a report must still reach the wire condition.
    ///
    /// **This is a guard on an attribute, not on a body.** `Other` cannot carry
    /// `#[from]`, because the hand-written `From<Error>` above is what performs
    /// the lift and a derived one would be a second impl. Dropping `#[from]` and
    /// stopping there leaves a plain field, and a plain field reports
    /// `source() == None` — the same enum, the same match arms, the same
    /// `Display`, and a chain that ends one level early.
    ///
    /// Nothing else in the tree would notice: `#[source]` appears in
    /// `keylink/src` exactly here, against eleven `#[from]`, so there is no
    /// precedent test to redden.
    #[test]
    fn other_keeps_the_error_as_its_source() {
        let failure = Failure::Other(Error::Remote(RemoteError::TimedOut { millis: 250 }));

        let error = failure
            .source()
            .expect("`Other` hands back the error it carries");
        assert!(error.downcast_ref::<Error>().is_some());

        let remote = error
            .source()
            .expect("`Error::Remote` hands back the remote error");
        assert!(remote.downcast_ref::<RemoteError>().is_some());
    }
}
