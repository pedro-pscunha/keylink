//! Dependency-neutral conversion for generated singleton services.

use std::future::Future;
use std::pin::Pin;

use serde::{de::DeserializeOwned, Serialize};

use crate::IdempotencyKey;

/// Converts bounded bytes into declared arguments and declared return values into an output.
pub trait ServiceCodec: Clone + Send + Sync + 'static {
    /// Conversion failure.
    type Error: std::error::Error + Send + Sync + 'static;
    /// Encoded successful output.
    type Output: Send + 'static;

    /// Decode one declared argument object.
    fn decode<T: DeserializeOwned>(&self, bytes: &[u8]) -> Result<T, Self::Error>;

    /// Encode one declared return value.
    fn encode<T: Serialize>(&self, value: &T) -> Result<Self::Output, Self::Error>;
}

/// Failure while converting or invoking a generated singleton service.
#[derive(Debug, thiserror::Error)]
pub enum ExternalError<E>
where
    E: std::error::Error + Send + Sync + 'static,
{
    /// The method is not an enabled externally adaptable declaration.
    #[error("unknown external method")]
    UnknownMethod,
    /// The method exists, but the requested operation kind does not match it.
    #[error("external method kind mismatch: expected {expected:?}, got {actual:?}")]
    WrongKind {
        /// The generated method kind.
        expected: ExternalKind,
        /// The kind requested by the adapter.
        actual: ExternalKind,
    },
    /// The codec could not decode arguments or encode the return value.
    #[error("external service codec failed: {0}")]
    Codec(#[source] E),
    /// The existing generated Keylink client call failed.
    #[error("external service call failed: {0}")]
    Client(#[source] crate::Error),
}

/// Boxed invocation future returned by a generated external service.
pub type ExternalFuture<C> = Pin<
    Box<
        dyn Future<
                Output = Result<
                    ExternalOutput<<C as ServiceCodec>::Output>,
                    ExternalError<<C as ServiceCodec>::Error>,
                >,
            > + Send,
    >,
>;

/// A generated method kind that an external adapter can expose.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum ExternalKind {
    /// A bounded argument object and bounded return value.
    Rpc,
    /// A request body streams to the handler.
    Send,
    /// A response body streams from the handler.
    Recv,
    /// The handler grants content-addressed bytes.
    Blob,
}

/// One enabled generated method in the external adapter inventory.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ExternalMethod {
    name: &'static str,
    kind: ExternalKind,
}

impl ExternalMethod {
    /// Build a generated method descriptor.
    #[doc(hidden)]
    #[must_use]
    pub const fn new(name: &'static str, kind: ExternalKind) -> Self {
        Self { name, kind }
    }

    /// The declared Rust method name used by external routes.
    #[must_use]
    pub const fn name(self) -> &'static str {
        self.name
    }

    /// The declared operation kind.
    #[must_use]
    pub const fn kind(self) -> ExternalKind {
        self.kind
    }
}

/// The transport shape requested from a generated external method.
#[derive(Debug)]
pub enum ExternalCall {
    /// Bounded arguments and a bounded declared return value.
    Rpc {
        /// Optional process-lifetime replay key.
        key: Option<IdempotencyKey>,
    },
    /// Bounded arguments plus a streaming upload.
    Send {
        /// The upload that the generated client streams to the handler.
        body: crate::Body,
    },
    /// Bounded arguments plus a one-shot streaming download.
    Recv,
    /// Bounded arguments plus a granted content handle.
    Blob,
}

impl ExternalCall {
    /// The operation kind selected by this call.
    #[must_use]
    pub const fn kind(&self) -> ExternalKind {
        match self {
            Self::Rpc { .. } => ExternalKind::Rpc,
            Self::Send { .. } => ExternalKind::Send,
            Self::Recv => ExternalKind::Recv,
            Self::Blob => ExternalKind::Blob,
        }
    }
}

/// The exact generated output shape of an external method.
#[derive(Debug)]
pub enum ExternalOutput<O> {
    /// A bounded encoded declared return value.
    Json(O),
    /// A bounded encoded declared return value plus a one-shot body reader.
    Recv {
        /// The encoded declared return value.
        value: O,
        /// The response payload.
        body: crate::BodyReader,
    },
    /// A bounded encoded declared return value plus a granted content handle.
    Blob {
        /// The encoded declared return value.
        value: O,
        /// The granted content handle.
        handle: crate::BlobHandle,
    },
}

/// Generated conversion seam for one KEYED service, addressed per actor.
///
/// A second trait rather than a variant of [`ExternalService`], and the
/// separation is the point: the two take different clients — a singleton
/// `Client` against a `RegistryClient` — so a gateway configured for one
/// address space cannot be handed the other's client by mistake.
///
/// **Only the generated `External` marker implements it.** The registry client
/// itself does not, which is what stops an adapter reaching past the generated
/// method it is supposed to call:
///
/// ```compile_fail,E0277
/// use keylink::ExternalActorService;
/// keylink::service! { service Ledger; rpc Count(value: u64) -> u64; }
/// fn require<T: ExternalActorService>() {}
/// require::<ledger::RegistryClient>();
/// ```
///
/// # `send` and `tell` are absent, and that is not an oversight
///
/// [`METHODS`](Self::METHODS) lists `rpc`, `recv` and `blob` only. A keyed
/// `send` would stream a body to an actor that four separate refusals can
/// reject after the bytes are already on the wire — a malformed id, an id
/// nothing is mounted under, a registry at capacity, and an expired queue
/// budget — and a keyed `tell` has no reply to carry any of them. Neither has
/// a generated `ActorRef` method, so there is nothing here to call even if the
/// inventory named them.
pub trait ExternalActorService: Send + Sync + 'static {
    /// The service name declared in `service!`.
    const SERVICE: &'static str;
    /// Enabled declarations a keyed actor can serve, in declaration order. A
    /// strict subset of [`ExternalService::METHODS`], folded from the same
    /// declaration list rather than written a second time.
    const METHODS: &'static [ExternalMethod];
    /// The generated connected registry client.
    type Client: Clone + Send + Sync + 'static;

    // A service declaring only `send` and `tell` still implements this trait,
    // with an empty `METHODS` and an `invoke_at` that answers `UnknownMethod`
    // to everything. So the bound alone does not say a service can serve
    // anything keyed — a surface that routes to it must refuse an empty
    // inventory at configuration time rather than answering `404` per request.

    /// Decode arguments, invoke the exact generated `at(actor)` method, and
    /// encode its return value.
    ///
    /// The actor is addressed through the generated `RegistryClient::at`, never
    /// by constructing an `Addressed<Request>` or reaching through a private
    /// field — an adapter that built the envelope itself would be a second
    /// implementation of the routing rules, free to disagree with the first.
    fn invoke_at<C: ServiceCodec>(
        client: &Self::Client,
        actor: &crate::ActorId,
        method: &str,
        body: &[u8],
        call: ExternalCall,
        codec: C,
    ) -> ExternalFuture<C>;
}

/// Generated conversion seam for one singleton service.
///
/// Addressed actor requests do not acquire the singleton external surface:
///
/// ```compile_fail
/// use keylink::{Addressed, ExternalService, ServiceCodec};
/// #[derive(Clone)] struct Codec;
/// impl ServiceCodec for Codec {
///     type Error = std::io::Error;
///     type Output = Vec<u8>;
///     fn decode<T: serde::de::DeserializeOwned>(&self, _: &[u8]) -> Result<T, Self::Error> { unreachable!() }
///     fn encode<T: serde::Serialize>(&self, _: &T) -> Result<Vec<u8>, Self::Error> { unreachable!() }
/// }
/// keylink::service! { service Inventory; rpc Count(value: u64) -> u64; }
/// fn require<T: ExternalService>() {}
/// require::<Addressed<inventory::Request>>();
/// ```
///
/// Implementing [`crate::Rpc`] manually does not opt a type into generated conversion:
///
/// ```compile_fail
/// use keylink::{ExternalService, Rpc, ServiceCodec};
/// #[derive(Clone)] struct Codec;
/// impl ServiceCodec for Codec {
///     type Error = std::io::Error;
///     type Output = Vec<u8>;
///     fn decode<T: serde::de::DeserializeOwned>(&self, _: &[u8]) -> Result<T, Self::Error> { unreachable!() }
///     fn encode<T: serde::Serialize>(&self, _: &T) -> Result<Vec<u8>, Self::Error> { unreachable!() }
/// }
/// struct Manual;
/// impl Rpc for Manual {
///     const SERVICE: &'static str = "Manual";
///     const METHODS: &'static [&'static str] = &["Run"];
///     fn method(&self) -> &'static str { "Run" }
/// }
/// fn require<T: ExternalService>() {}
/// require::<Manual>();
/// ```
pub trait ExternalService: Send + Sync + 'static {
    /// The service name declared in `service!`.
    const SERVICE: &'static str;
    /// Enabled externally adaptable declarations, in declaration order.
    const METHODS: &'static [ExternalMethod];
    /// The generated connected client.
    type Client: Clone + Send + Sync + 'static;

    /// Decode arguments, invoke the exact generated client method, and encode its return value.
    fn invoke<C: ServiceCodec>(
        client: &Self::Client,
        method: &str,
        body: &[u8],
        call: ExternalCall,
        codec: C,
    ) -> ExternalFuture<C>;
}
