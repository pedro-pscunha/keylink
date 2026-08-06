//! The error hierarchy.
//!
//! Every fallible boundary in this crate is typed. There is no
//! `.map_err(|e| Error::Transport(e.to_string()))` anywhere, because every iroh
//! error `?`s straight into [`Error`] through the [`From`] impls below. That is
//! the single source of truth for "how does a transport failure become a
//! keylink failure": one list, in one place.

use iroh::endpoint::{
    BindError, ClosedStream, ConnectError, ConnectionError, ReadError, WriteError,
};
use iroh::KeyParsingError;

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
    /// a single byte is buffered, so announcing a huge frame costs the peer
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

    /// The serving side refused the call.
    ///
    /// The handler, or a layer above it, said no. Distinct from a transport
    /// failure: the call arrived, was understood, and was declined.
    #[error("remote refused: {0}")]
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

    /// Bad or missing configuration, reported at startup rather than on first use.
    #[error("config: {0}")]
    Config(#[from] ConfigError),

    /// Reading or writing a body against local storage failed.
    #[error("body io: {0}")]
    Io(#[from] std::io::Error),
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
