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
    /// The method is not an enabled plain `rpc` declaration.
    #[error("unknown external rpc method")]
    UnknownMethod,
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
                    <C as ServiceCodec>::Output,
                    ExternalError<<C as ServiceCodec>::Error>,
                >,
            > + Send,
    >,
>;

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
    /// Enabled plain `rpc` declarations, in declaration order.
    const RPC_METHODS: &'static [&'static str];
    /// The generated connected client.
    type Client: Clone + Send + Sync + 'static;

    /// Decode arguments, invoke the exact generated client method, and encode its return value.
    fn invoke<C: ServiceCodec>(
        client: &Self::Client,
        method: &str,
        body: &[u8],
        key: Option<IdempotencyKey>,
        codec: C,
    ) -> ExternalFuture<C>;
}
