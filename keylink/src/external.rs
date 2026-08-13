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
