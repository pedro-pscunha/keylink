//! Bounded JSON codec for the generated external service seam.

use keylink::ServiceCodec;
use serde::{de::DeserializeOwned, Serialize};

use crate::limits::BoundedWriter;

/// Internal bounded JSON conversion failure.
#[derive(Debug, thiserror::Error)]
pub(crate) enum JsonError {
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
#[derive(Clone)]
pub(crate) struct JsonCodec {
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
    use keylink::{Allowlist, Context, ExternalCall, ExternalOutput, ExternalService, Node};
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
        rpc Optional(r#type: Option<u64>) -> Option<u64>;
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

        async fn optional(&self, _cx: &Context, r#type: Option<u64>) -> Option<u64> {
            r#type
        }

        async fn nested(&self, _cx: &Context, item: Item) -> Item {
            item
        }

        async fn ignored(&self, _cx: &Context, _value: u64) {}
    }

    fn json(output: ExternalOutput<Vec<u8>>) -> Vec<u8> {
        match output {
            ExternalOutput::Json(json) => json,
            ExternalOutput::Recv { .. } | ExternalOutput::Blob { .. } => {
                panic!("unary RPC returned a streaming output")
            }
        }
    }

    async fn local_node() -> Node {
        Node::from_endpoint(
            Endpoint::bind(presets::Minimal)
                .await
                .expect("a local endpoint binds"),
        )
    }

    /// Watched red with `scripts/break-once.sh` by replacing
    /// `                }
    ///                 .map_err($crate::ExternalError::Client)?;
    ///                 $codec.encode(&value)
    ///                     .map($crate::ExternalOutput::Json)`
    /// with the same typed output block encoding `()`. The exact async test selected
    /// one guard and failed with `null` instead of the declared return value.
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

        let empty = conversion::External::invoke(
            &client,
            "Empty",
            b"{}",
            ExternalCall::Rpc { key: None },
            codec.clone(),
        )
        .await
        .expect("empty call");
        let empty = json(empty);
        assert_eq!(empty, b"null");

        let one = conversion::External::invoke(
            &client,
            "One",
            br#"{"value":4}"#,
            ExternalCall::Rpc { key: None },
            codec.clone(),
        )
        .await
        .expect("one argument");
        let one = json(one);
        assert_eq!(one, b"5");

        let many = conversion::External::invoke(
            &client,
            "Many",
            br#"{"name":"box","count":2}"#,
            ExternalCall::Rpc { key: None },
            codec.clone(),
        )
        .await
        .expect("many arguments");
        let many = json(many);
        assert_eq!(many, br#""box-2""#);

        let nested = conversion::External::invoke(
            &client,
            "Nested",
            br#"{"item":{"displayName":"crate"}}"#,
            ExternalCall::Rpc { key: None },
            codec,
        )
        .await
        .expect("nested serde metadata");
        let nested = json(nested);
        assert_eq!(nested, br#"{"displayName":"crate"}"#);

        for body in [b"{}".as_slice(), br#"{"type":null}"#.as_slice()] {
            let optional = conversion::External::invoke(
                &client,
                "Optional",
                body,
                ExternalCall::Rpc { key: None },
                JsonCodec::new(1024),
            )
            .await
            .expect("missing and null option");
            assert_eq!(json(optional), b"null");
        }
        let optional = conversion::External::invoke(
            &client,
            "Optional",
            br#"{"type":7}"#,
            ExternalCall::Rpc { key: None },
            JsonCodec::new(1024),
        )
        .await
        .expect("raw identifier uses Serde spelling");
        assert_eq!(json(optional), b"7");

        for (method, body) in [
            ("Empty", br#"{"unexpected":1}"#.as_slice()),
            ("One", br#"{"value":4,"unexpected":1}"#.as_slice()),
            (
                "Many",
                br#"{"name":"box","count":2,"unexpected":1}"#.as_slice(),
            ),
        ] {
            let error = conversion::External::invoke(
                &client,
                method,
                body,
                ExternalCall::Rpc { key: None },
                JsonCodec::new(1024),
            )
            .await
            .expect_err("undeclared argument fields are rejected before dispatch");
            assert!(matches!(
                error,
                keylink::ExternalError::Codec(super::JsonError::InvalidJson)
            ));
        }
    }

    keylink::service! {
        service Transport;

        rpc Sum(value: u64) -> u64;
        send Upload(prefix: u64) -> u64;
        recv Download(value: u64) -> u64;
        blob Asset() -> u64;
        tell IgnoredTransport(value: u64) -> ();
    }

    #[derive(Clone)]
    struct TransportHandler {
        id: keylink::BlobId,
    }

    impl transport::Handler for TransportHandler {
        async fn sum(&self, _cx: &Context, value: u64) -> u64 {
            value + 1
        }

        async fn upload(&self, _cx: &Context, prefix: u64, body: keylink::BodyReader) -> u64 {
            let bytes = body.read_to_bytes(32).await.expect("finite upload");
            prefix + bytes.len() as u64
        }

        async fn download(&self, _cx: &Context, value: u64) -> (u64, keylink::Body) {
            (value + 1, keylink::Body::from_bytes(b"download".as_slice()))
        }

        async fn asset(&self, _cx: &Context) -> (u64, keylink::Grant) {
            (7, self.id.into())
        }

        async fn ignored_transport(&self, _cx: &Context, _value: u64) {}
    }

    /// A keyed actor serves `rpc`, `recv` and `blob`, and no more than those.
    ///
    /// The actor inventory is folded from the SAME declaration list as the
    /// singleton one, so the two cannot drift by a method — only by a kind,
    /// deliberately. `send` is absent because a keyed upload would stream a
    /// body to an actor that four separate refusals can reject after the bytes
    /// are already on the wire, and `tell` is absent from both because it has
    /// no reply to carry any of them.
    ///
    /// The control comes first: the singleton inventory must actually contain a
    /// `send`, or the assertion below is satisfied by an empty list.
    ///
    /// **NO ANCHOR, because the compiler is the guard.** Letting a `send` into
    /// the actor accumulator in `keylink/src/macros.rs` does not produce a
    /// failing assertion — it produces a compile error, twice:
    ///
    /// ```text
    /// error[E0599]: no method named `upload` found for struct `transport::ActorRef<'a>`
    /// error[E0599]: no method named `upload` found for struct `cancellation::ActorRef<'a>`
    /// ```
    ///
    /// `break-once.sh` reports that as `── exit=101 ── selected=?`, and the
    /// `?` is the tell: nothing ran. A `send` cannot reach the actor inventory
    /// because there is no generated `ActorRef` method for it to call, so the
    /// exclusion is a property of the generated code rather than of this
    /// assertion.
    ///
    /// **And the guard is ONE-WAY.** `E0599` fires when the inventory names a
    /// kind whose `ActorRef` method is absent. The other direction is silent:
    /// adding a `send` arm to `__keylink_actor_client_method!` would leave the
    /// inventory without it and nothing would fail, and the symptom is a
    /// missing route, which reads as an unimplemented feature rather than a
    /// defect. The two macros are 4000 lines apart in the same file.
    ///
    /// The row's other two assertions ARE runtime-checkable, and one of them
    /// is watched. Row `HK28`: dropping `recv` from the actor accumulator by
    /// replacing
    /// `@actor [ $( $actor )* { [ $( #[$cfg] )* ] recv $method ( $( $arg : $ty ),* ) -> $ret } ]`
    /// in `keylink/src/macros.rs` with `@actor [ $( $actor )* ]` compiles and
    /// reports `left: [("Sum", Rpc), ("Asset", Blob)]`.
    ///
    /// The CONTROL is the third: the singleton list must really carry a
    /// `send`, or the comparison is satisfied by two empty lists.
    #[test]
    fn the_actor_inventory_is_the_singleton_one_without_send() {
        use keylink::{ExternalActorService, ExternalKind, ExternalService};

        let singleton: Vec<_> = <transport::External as ExternalService>::METHODS
            .iter()
            .map(|method| (method.name(), method.kind()))
            .collect();
        let actor: Vec<_> = <transport::External as ExternalActorService>::METHODS
            .iter()
            .map(|method| (method.name(), method.kind()))
            .collect();

        assert_eq!(
            singleton,
            [
                ("Sum", ExternalKind::Rpc),
                ("Upload", ExternalKind::Send),
                ("Download", ExternalKind::Recv),
                ("Asset", ExternalKind::Blob),
            ],
            "control: the singleton surface really does carry a `send`"
        );
        assert_eq!(
            actor,
            [
                ("Sum", ExternalKind::Rpc),
                ("Download", ExternalKind::Recv),
                ("Asset", ExternalKind::Blob),
            ],
            "and the actor surface is that list with the `send` removed"
        );
        assert_eq!(
            <transport::External as ExternalActorService>::SERVICE,
            <transport::External as ExternalService>::SERVICE,
            "one service, two address spaces"
        );
    }

    // Its own declaration, because `Transport` carries a `send` and keylink
    // refuses to serve such a service as an actor AT ALL — `ServableAsActor`
    // is a const assertion whose message is four lines long. That is the real
    // guard behind "no keyed `send`", and it fires long before any inventory.
    keylink::service! {
        service Keyed;

        rpc Sum(value: u64) -> u64;
        recv Download(value: u64) -> u64;
        blob Asset() -> u64;
    }

    #[derive(Clone)]
    struct KeyedHandler {
        id: keylink::BlobId,
    }

    impl keyed::Actor for KeyedHandler {
        async fn sum(&mut self, _cx: keylink::Caller<'_>, value: u64) -> u64 {
            value + 1
        }

        async fn download(&mut self, _cx: keylink::Caller<'_>, value: u64) -> (u64, keylink::Body) {
            (value + 1, keylink::Body::from_bytes(b"download".as_slice()))
        }

        async fn asset(&mut self, _cx: keylink::Caller<'_>) -> (u64, keylink::Grant) {
            (7, self.id.into())
        }
    }

    /// The keyed seam runs, against a real registry, once per kind it serves.
    ///
    /// The twin below proves the singleton path. Without this one the whole
    /// `invoke_at` body — the `@seed`/`@to` split, the `WithIdempotency`
    /// rebinding, both arms of `match key`, and the error mapping — shipped on
    /// compilation alone, and a macro is exactly where "it compiles" sits
    /// furthest from "it works".
    ///
    /// It also pins the refusal that has no `ActorRef` method behind it: a
    /// `send` reaches `invoke_at` as an unknown method rather than as a
    /// mis-typed call, because the actor inventory never named it.
    ///
    /// Watched red with `scripts/break-once.sh` (row `HK29`) by replacing
    /// `$actor.clone()`
    /// in `keylink/src/macros.rs` with a constant id, which reports
    /// `keyed rpc: Client(Remote(NoSuchActor { id: "elsewhere" }))`. Dropping
    /// the addressing entirely is NOT a red — `&RegistryClient` has no
    /// per-method surface, so it is `error[E0599] no method named 'sum'` with
    /// `selected=?`, and the `?` is the tell that nothing ran. The seam cannot
    /// silently degrade to a singleton call; it can only address the wrong
    /// actor, which is what this row watches.
    #[tokio::test]
    async fn generated_actor_invocation_addresses_one_actor_per_kind() {
        use keylink::prelude::*;
        use keylink::ExternalActorService;

        let server_node = local_node().await;
        let client_node = local_node().await;
        let blobs = keylink::Blobs::memory();
        let id = blobs
            .add_bytes(b"asset".as_slice())
            .await
            .expect("asset stores");

        let system: keyed::Registry = keyed::Registry::builder(keylink::Eviction::Never).build();
        system
            .mount(
                keylink::ActorId::new("one").expect("a valid id"),
                keyed::spawn(KeyedHandler { id }),
            )
            .expect("mounts");
        let _running = keylink::Server::builder(&server_node)
            .service(
                system
                    .service(Allowlist::anyone())
                    .with_blobs(&blobs)
                    .build(),
            )
            .spawn()
            .expect("a keyed system that serves content");
        let client = keyed::RegistryClient::connect(&client_node, server_node.endpoint().addr())
            .await
            .expect("dials the keyed system");
        let actor = keylink::ActorId::new("one").expect("a valid id");
        let codec = JsonCodec::new(1024);

        let rpc = keyed::External::invoke_at(
            &client,
            &actor,
            "Sum",
            br#"{"value":4}"#,
            ExternalCall::Rpc { key: None },
            codec.clone(),
        )
        .await
        .expect("keyed rpc");
        assert_eq!(json(rpc), b"5");

        // The idempotency branch of `match key`, which the singleton twin also
        // does not reach. Both arms of that match are now executed.
        let keyed = keyed::External::invoke_at(
            &client,
            &actor,
            "Sum",
            br#"{"value":9}"#,
            ExternalCall::Rpc {
                key: Some(keylink::IdempotencyKey::new("k1").expect("a valid key")),
            },
            codec.clone(),
        )
        .await
        .expect("keyed rpc with a replay key");
        assert_eq!(json(keyed), b"10");

        let recv = keyed::External::invoke_at(
            &client,
            &actor,
            "Download",
            br#"{"value":1}"#,
            ExternalCall::Recv,
            codec.clone(),
        )
        .await
        .expect("keyed recv");
        let keylink::ExternalOutput::Recv { value, body } = recv else {
            panic!("a recv answers with a value and a body");
        };
        assert_eq!(value, b"2");
        assert_eq!(
            body.read_to_bytes(64)
                .await
                .expect("a finite body")
                .as_ref(),
            b"download"
        );

        let blob = keyed::External::invoke_at(
            &client,
            &actor,
            "Asset",
            b"{}",
            ExternalCall::Blob,
            codec.clone(),
        )
        .await
        .expect("keyed blob");
        let keylink::ExternalOutput::Blob { value, handle } = blob else {
            panic!("a blob answers with a value and a handle");
        };
        assert_eq!(value, b"7");
        assert_eq!(handle.id(), id);

        // No `ActorRef::upload` exists, and the inventory never named `Upload`.
        let refused = keyed::External::invoke_at(
            &client,
            &actor,
            "Upload",
            br#"{"prefix":3}"#,
            ExternalCall::Rpc { key: None },
            codec,
        )
        .await;
        assert!(
            matches!(refused, Err(keylink::ExternalError::UnknownMethod)),
            "a `send` is unknown on the keyed surface, not mis-typed: {refused:?}"
        );
    }

    #[tokio::test]
    async fn generated_external_invocation_uses_exact_kind_and_output() {
        let server_node = local_node().await;
        let client_node = local_node().await;
        let blobs = keylink::Blobs::memory();
        let id = blobs
            .add_bytes(b"asset".as_slice())
            .await
            .expect("asset stores");
        let server = keylink::Server::builder(&server_node)
            .service(
                transport::service(TransportHandler { id }, Allowlist::anyone())
                    .with_blobs(&blobs)
                    .build(),
            )
            .spawn()
            .expect("service starts");
        let client = transport::Client::connect(&client_node, server_node.endpoint().addr())
            .await
            .expect("client connects");
        let codec = JsonCodec::new(1024);

        let rpc = transport::External::invoke(
            &client,
            "Sum",
            br#"{"value":4}"#,
            ExternalCall::Rpc { key: None },
            codec.clone(),
        )
        .await
        .expect("rpc");
        assert_eq!(json(rpc), b"5");

        let send = transport::External::invoke(
            &client,
            "Upload",
            br#"{"prefix":3}"#,
            ExternalCall::Send {
                body: keylink::Body::from_bytes(b"body".as_slice()),
            },
            codec.clone(),
        )
        .await
        .expect("send");
        assert_eq!(json(send), b"7");

        let recv = transport::External::invoke(
            &client,
            "Download",
            br#"{"value":8}"#,
            ExternalCall::Recv,
            codec.clone(),
        )
        .await
        .expect("recv");
        let ExternalOutput::Recv { value, body } = recv else {
            panic!("recv returned the wrong output kind");
        };
        assert_eq!(value, b"9");
        assert_eq!(
            body.read_to_bytes(32).await.expect("download"),
            b"download"[..]
        );

        let blob =
            transport::External::invoke(&client, "Asset", b"{}", ExternalCall::Blob, codec.clone())
                .await
                .expect("blob");
        let ExternalOutput::Blob { value, handle } = blob else {
            panic!("blob returned the wrong output kind");
        };
        assert_eq!(value, b"7");
        assert_eq!(handle.id(), id);

        let wrong = transport::External::invoke(
            &client,
            "Upload",
            br#"{"prefix":3}"#,
            ExternalCall::Rpc { key: None },
            codec,
        )
        .await
        .expect_err("kind mismatch");
        assert!(matches!(
            wrong,
            keylink::ExternalError::WrongKind {
                expected: keylink::ExternalKind::Send,
                actual: keylink::ExternalKind::Rpc,
            }
        ));
        drop(server);
    }

    #[test]
    fn ordinary_generated_serde_and_schema_keep_the_wire_shape() {
        use postcard_schema::schema::{DataModelType, DataModelVariant};

        let request = conversion::Request::Nested {
            item: Item {
                display_name: "crate".into(),
            },
        };
        assert_eq!(
            serde_json::to_string(&request).expect("request JSON"),
            r#"{"wire-name":{"item":{"displayName":"crate"}}}"#
        );
        assert_eq!(
            serde_json::to_string(&conversion::Response::Nested(Item {
                display_name: "crate".into(),
            }))
            .expect("response JSON"),
            r#"{"Nested":{"displayName":"crate"}}"#
        );

        let DataModelType::Enum(variants) = conversion::Request::SCHEMA.ty else {
            panic!("generated request remains an enum");
        };
        let names: Vec<_> = variants.iter().map(|variant| variant.name).collect();
        assert_eq!(
            names,
            [
                "Empty",
                "One",
                "Many",
                "Optional",
                "wire-name",
                "Ignored",
                "__Status"
            ]
        );
        let nested = variants
            .iter()
            .find(|variant| variant.name == "wire-name")
            .expect("renamed wire variant");
        let DataModelVariant::StructVariant(fields) = nested.ty else {
            panic!("Nested remains a struct variant");
        };
        assert_eq!(
            fields.iter().map(|field| field.name).collect::<Vec<_>>(),
            ["item"]
        );
    }

    #[test]
    fn success_encoding_uses_the_capacity_enforcing_writer_in_production() {
        let source = include_str!("service.rs");
        assert!(source.contains("let mut writer = BoundedWriter::new(self.response_limit);"));
        assert!(source.contains("serde_json::to_writer(&mut writer, value)"));
        assert!(!source.contains(&["serde_json::", "to_vec(value)"].concat()));
    }

    /// A key is bound to its original request. The ledger refuses a different
    /// RPC before replay, rather than handing the adapter a wrong response variant.
    #[tokio::test]
    async fn reused_key_for_a_different_rpc_is_refused_before_replay() {
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
            ExternalCall::Rpc {
                key: Some(key.clone()),
            },
            codec.clone(),
        )
        .await
        .expect("first call records a One response");

        let error = conversion::External::invoke(
            &client,
            "Many",
            br#"{"name":"box","count":2}"#,
            ExternalCall::Rpc { key: Some(key) },
            codec,
        )
        .await
        .expect_err("the key belongs to One, not the new Many request");

        assert!(matches!(
            error,
            keylink::ExternalError::Client(keylink::Error::Remote(
                keylink::RemoteError::Idempotency(keylink::IdempotencyRefusal::RequestMismatch)
            ))
        ));
    }
}
