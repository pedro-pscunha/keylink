//! Bounded JSON codec for the generated external service seam.

use keylink::ServiceCodec;
use serde::{de::DeserializeOwned, Serialize};

use crate::limits::BoundedWriter;

/// Internal bounded JSON conversion failure.
#[doc(hidden)]
#[derive(Debug, thiserror::Error)]
pub enum JsonError {
    /// The argument object was malformed or had the wrong shape.
    #[error("invalid JSON arguments")]
    InvalidJson,
    /// The declared return value crossed the configured success ceiling.
    #[error("successful response exceeds its JSON limit")]
    ResponseTooLarge,
    /// The declared return value could not be serialized.
    #[error("return value could not be encoded as JSON")]
    Codec,
}

/// Internal bounded JSON codec selected by [`crate::Gateway`].
#[doc(hidden)]
#[derive(Clone)]
pub struct JsonCodec {
    response_limit: usize,
}

impl JsonCodec {
    pub(crate) fn new(response_limit: usize) -> Self {
        Self { response_limit }
    }
}

impl ServiceCodec for JsonCodec {
    type Error = JsonError;
    type Output = Vec<u8>;

    fn decode<T: DeserializeOwned>(&self, bytes: &[u8]) -> Result<T, Self::Error> {
        serde_json::from_slice(bytes).map_err(|_| JsonError::InvalidJson)
    }

    fn encode<T: Serialize>(&self, value: &T) -> Result<Self::Output, Self::Error> {
        let mut writer = BoundedWriter::new(self.response_limit);
        match serde_json::to_writer(&mut writer, value) {
            Ok(()) => Ok(writer.into_inner()),
            Err(_) if writer.overflowed() => Err(JsonError::ResponseTooLarge),
            Err(_) => Err(JsonError::Codec),
        }
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::expect_used)]

    use iroh::{endpoint::presets, Endpoint};
    use keylink::{Allowlist, Context, ExternalService, Node};
    use postcard_schema::Schema;
    use serde::{Deserialize, Serialize};

    use super::JsonCodec;

    #[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Schema)]
    pub struct Item {
        #[serde(rename = "displayName")]
        display_name: String,
    }

    keylink::service! {
        service Conversion;

        rpc Empty() -> ();
        rpc One(value: u64) -> u64;
        rpc Many(name: String, count: u16) -> String;
        #[serde(rename = "wire-name")]
        rpc Nested(item: Item) -> Item;
        tell Ignored(value: u64) -> ();
        #[cfg(any())]
        rpc Disabled(value: u64) -> u64;
    }

    struct Handler;

    impl conversion::Handler for Handler {
        async fn empty(&self, _cx: &Context) {}

        async fn one(&self, _cx: &Context, value: u64) -> u64 {
            value + 1
        }

        async fn many(&self, _cx: &Context, name: String, count: u16) -> String {
            format!("{name}-{count}")
        }

        async fn nested(&self, _cx: &Context, item: Item) -> Item {
            item
        }

        async fn ignored(&self, _cx: &Context, _value: u64) {}
    }

    async fn local_node() -> Node {
        Node::from_endpoint(
            Endpoint::bind(presets::Minimal)
                .await
                .expect("a local endpoint binds"),
        )
    }

    /// Watched red with `scripts/break-once.sh` by replacing
    /// `                                    codec.encode(&value).map_err($crate::ExternalError::Codec)`
    /// with unit encoding. The exact async test terminated and failed on the one-argument return.
    #[tokio::test]
    async fn json_conversion_uses_declared_arguments_and_return_values() {
        let server_node = local_node().await;
        let client_node = local_node().await;
        let _server =
            conversion::serve(&server_node, Handler, Allowlist::anyone()).expect("service starts");
        let client = conversion::Client::connect(&client_node, server_node.endpoint().addr())
            .await
            .expect("client connects");
        let codec = JsonCodec::new(1024);

        let empty = conversion::External::invoke(&client, "Empty", b"{}", None, codec.clone())
            .await
            .expect("empty call");
        assert_eq!(empty, b"null");

        let one =
            conversion::External::invoke(&client, "One", br#"{"value":4}"#, None, codec.clone())
                .await
                .expect("one argument");
        assert_eq!(one, b"5");

        let many = conversion::External::invoke(
            &client,
            "Many",
            br#"{"name":"box","count":2}"#,
            None,
            codec.clone(),
        )
        .await
        .expect("many arguments");
        assert_eq!(many, br#""box-2""#);

        let nested = conversion::External::invoke(
            &client,
            "Nested",
            br#"{"item":{"displayName":"crate"}}"#,
            None,
            codec,
        )
        .await
        .expect("nested serde metadata");
        assert_eq!(nested, br#"{"displayName":"crate"}"#);
    }

    #[tokio::test]
    async fn reused_key_for_a_different_rpc_rejects_the_wrong_response_variant() {
        let server_node = local_node().await;
        let client_node = local_node().await;
        let _server =
            conversion::serve(&server_node, Handler, Allowlist::anyone()).expect("service starts");
        let client = conversion::Client::connect(&client_node, server_node.endpoint().addr())
            .await
            .expect("client connects");
        let key = keylink::IdempotencyKey::new("same-logical-call").expect("valid key");
        let codec = JsonCodec::new(1024);

        conversion::External::invoke(
            &client,
            "One",
            br#"{"value":4}"#,
            Some(key.clone()),
            codec.clone(),
        )
        .await
        .expect("first call records a One response");

        let error = conversion::External::invoke(
            &client,
            "Many",
            br#"{"name":"box","count":2}"#,
            Some(key),
            codec,
        )
        .await
        .expect_err("a One response cannot become a successful Many response");

        assert!(matches!(
            error,
            keylink::ExternalError::Client(keylink::Error::UnexpectedResponse {
                expected: "Many",
                actual: "One"
            })
        ));
    }
}
