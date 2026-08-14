//! Where `serde` and `schemars` are asked the same question.
//!
//! The runtime decodes a request with `serde`. The document describes it with
//! `schemars`. Every serde attribute is a place the two can answer differently,
//! and a document that describes a request the gateway refuses — or accepts one
//! it rejects — is worse than no document, because a generator believes it.
//!
//! Each row here is a CYCLE: a JSON value, fed to `serde` to see what the
//! runtime does with it, and the same contract's schema, read to see what a
//! caller would be told. The assertion is that they agree.

#![allow(clippy::expect_used)]

use keylink_http::{Addressing, HttpSurface};
use keylink_openapi::{document, DocumentConfig, Security};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

#[derive(
    Serialize, Deserialize, postcard_schema::Schema, schemars::JsonSchema, Clone, Debug, PartialEq,
)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Renamed {
    pub unit_price: u64,
    #[serde(rename = "sku")]
    pub stock_keeping_unit: String,
    #[serde(default)]
    pub note: String,
    #[serde(skip)]
    pub internal: u64,
}

#[derive(
    Serialize, Deserialize, postcard_schema::Schema, schemars::JsonSchema, Clone, Debug, PartialEq,
)]
#[serde(tag = "kind")]
pub enum Tagged {
    Ready { at: u64 },
    Refused { reason: String },
}

#[derive(
    Serialize, Deserialize, postcard_schema::Schema, schemars::JsonSchema, Clone, Debug, PartialEq,
)]
#[serde(untagged)]
pub enum Untagged {
    Count(u64),
    Label(String),
}

keylink::service! {
    service Parity;
    rpc Rename(item: Renamed) -> u64;
    rpc Tag(item: Tagged) -> u64;
    rpc Untag(item: Untagged) -> u64;
}

keylink_openapi::document_service!(parity);

fn rendered() -> Value {
    let surface = HttpSurface::new("/v1", Addressing::Singleton).expect("valid surface");
    let config = DocumentConfig::new(
        "Parity",
        "1.0.0",
        Security::External {
            description: "Ahead of the gateway.".to_owned(),
        },
    );
    document::<parity::External>(&surface, &config)
        .expect("a document")
        .value()
        .clone()
}

/// The `item` property's own schema, resolved the way a generator resolves it:
/// through `#/components/schemas`, which is the only ref space an OpenAPI
/// document has. Resolving here through `$defs` would test a shape the document
/// does not publish.
fn argument(method: &str) -> Value {
    let document = rendered();
    let item = document["paths"][format!("/v1/rpc/{method}")]["post"]["requestBody"]["content"]
        ["application/json"]["schema"]["properties"]["item"]
        .clone();
    let Some(reference) = item.get("$ref").and_then(Value::as_str) else {
        return item;
    };
    let name = reference
        .strip_prefix("#/components/schemas/")
        .expect("an OpenAPI document references its own components");
    document["components"]["schemas"][name].clone()
}

/// `rename_all`, `rename`, `default` and `skip`, all at once and all visible.
#[test]
fn a_renamed_defaulted_and_skipped_contract_describes_what_serde_accepts() {
    let schema = argument("Rename");
    let properties = schema["properties"].as_object().expect("an object");

    // `rename_all = "camelCase"` and an explicit `rename` both reach the wire.
    assert!(properties.contains_key("unitPrice"), "{properties:?}");
    assert!(properties.contains_key("sku"), "{properties:?}");
    assert!(!properties.contains_key("unit_price"));
    assert!(!properties.contains_key("stock_keeping_unit"));

    // `skip` means the field is not on the wire in either direction.
    assert!(!properties.contains_key("internal"));

    let required: Vec<&str> = schema["required"]
        .as_array()
        .map(|values| values.iter().filter_map(Value::as_str).collect())
        .unwrap_or_default();
    assert!(required.contains(&"unitPrice"));
    assert!(required.contains(&"sku"));
    // `default` means serde accepts the field missing, so the schema must not
    // demand it.
    assert!(!required.contains(&"note"), "required = {required:?}");

    assert_eq!(
        schema["additionalProperties"], false,
        "deny_unknown_fields reaches the document"
    );

    // The cycle: exactly the document the schema describes, through serde.
    let accepted = json!({ "unitPrice": 5, "sku": "abc" });
    let decoded: Renamed = serde_json::from_value(accepted).expect("serde accepts it");
    assert_eq!(decoded.note, "", "the default filled in");
    assert_eq!(decoded.internal, 0, "a skipped field is never on the wire");

    // And the one the schema forbids.
    let refused = json!({ "unitPrice": 5, "sku": "abc", "surprise": 1 });
    assert!(
        serde_json::from_value::<Renamed>(refused).is_err(),
        "deny_unknown_fields refuses, exactly as additionalProperties says"
    );
}

/// An internally tagged enum keeps its tag in the document.
#[test]
fn a_tagged_enum_describes_the_tag_serde_reads() {
    let schema = argument("Tag");
    let rendered = serde_json::to_string(&schema).expect("renderable");
    assert!(
        rendered.contains("kind"),
        "the tag is described: {rendered}"
    );

    let value = json!({ "kind": "Ready", "at": 7 });
    let decoded: Tagged = serde_json::from_value(value.clone()).expect("serde accepts it");
    assert_eq!(decoded, Tagged::Ready { at: 7 });
    assert_eq!(
        serde_json::to_value(&decoded).expect("serializable"),
        value,
        "the round trip is the same document the schema describes"
    );
}

/// An untagged enum describes alternatives rather than a tag.
#[test]
fn an_untagged_enum_describes_alternatives() {
    let schema = argument("Untag");
    let rendered = serde_json::to_string(&schema).expect("renderable");
    assert!(
        rendered.contains("anyOf") || rendered.contains("oneOf"),
        "an untagged enum is a choice, not an object: {rendered}"
    );
    assert!(
        !rendered.contains("\"kind\""),
        "and it carries no tag: {rendered}"
    );

    let decoded: Untagged = serde_json::from_value(json!(4)).expect("serde accepts a number");
    assert_eq!(decoded, Untagged::Count(4));
    let decoded: Untagged = serde_json::from_value(json!("x")).expect("serde accepts a string");
    assert_eq!(decoded, Untagged::Label("x".to_owned()));
}

/// A recursive DTO cannot reach this crate at all, and the refusal is one layer
/// down.
///
/// A review reported that `hoist` mishandles self-reference — correctly, as a
/// property of that function, measured on hand-fed `schemars` output. The
/// scenario it drew from that ("any service that declares a tree cannot be
/// documented") does not hold: `postcard_schema::Schema` is a REQUIRED derive on
/// every service argument, and deriving it for `struct Node { children: Vec<Node> }`
/// is a const-evaluation cycle, `E0391`. The wire schema refuses the type before
/// a document is involved.
///
/// Recorded rather than tested, because the test cannot be written: the fixture
/// does not compile. `hoist` is fixed anyway — an unreachable defect is still a
/// defect, and the thing keeping it unreachable is a derive in another crate.
#[test]
fn a_recursive_dto_is_refused_by_the_wire_schema_not_by_this_crate() {
    // Nothing to assert at runtime. The measurement is that the commented
    // fixture below does not compile, and the record above says why.
    //
    // #[derive(postcard_schema::Schema)]
    // pub struct Node { pub label: String, pub children: Vec<Node> }
    //   error[E0391]: cycle detected when elaborating drops for `<impl>::SCHEMA`
}

#[derive(
    Serialize, Deserialize, postcard_schema::Schema, schemars::JsonSchema, Clone, Debug, PartialEq,
)]
pub struct Asymmetric {
    pub both: u64,
    /// Sent by the server, never accepted from a caller.
    #[serde(skip_deserializing)]
    pub server_only: u64,
    /// Accepted from a caller, never sent back.
    #[serde(skip_serializing)]
    pub client_only: u64,
}

keylink::service! {
    service Asym;
    rpc Round(item: Asymmetric) -> Asymmetric;
}

keylink_openapi::document_service!(asym);

/// A request is described by the DESERIALIZE contract and a response by the
/// SERIALIZE one, because for an asymmetric DTO they are different documents.
///
/// Built from one contract, a response promises a field the runtime never sends
/// and omits one it always does. A generated client's response struct then
/// fails to decode a real response — and the document says `writeOnly: true`
/// inside a response body, which contradicts itself on its face.
///
/// Watched red with `scripts/break-once.sh` (row `HL5`) by replacing
/// `                                    .for_serialize()`
/// with `.for_deserialize()`. Terminated on
/// `assertion failed: response_properties.contains_key("server_only")` — the
/// response stopped describing the field the runtime always sends.
#[test]
fn a_request_and_a_response_are_described_by_their_own_contracts() {
    let surface = HttpSurface::new("/v1", Addressing::Singleton).expect("valid surface");
    let config = DocumentConfig::new(
        "Asym",
        "1.0.0",
        Security::External {
            description: "Ahead of the gateway.".to_owned(),
        },
    );
    let document = document::<asym::External>(&surface, &config).expect("a document");
    let operation = &document.value()["paths"]["/v1/rpc/Round"]["post"];

    let resolve = |value: &Value| -> Value {
        value.get("$ref").and_then(Value::as_str).map_or_else(
            || value.clone(),
            |reference| {
                let name = reference
                    .strip_prefix("#/components/schemas/")
                    .expect("an OpenAPI reference");
                document.value()["components"]["schemas"][name].clone()
            },
        )
    };

    let request = resolve(
        &operation["requestBody"]["content"]["application/json"]["schema"]["properties"]["item"],
    );
    let response = resolve(&operation["responses"]["200"]["content"]["application/json"]["schema"]);

    let request_properties = request["properties"].as_object().expect("an object");
    let response_properties = response["properties"].as_object().expect("an object");

    assert!(request_properties.contains_key("client_only"));
    assert!(
        !request_properties.contains_key("server_only"),
        "a skip_deserializing field is not part of a request: {:?}",
        request_properties.keys().collect::<Vec<_>>()
    );
    assert!(response_properties.contains_key("server_only"));
    assert!(
        !response_properties.contains_key("client_only"),
        "a skip_serializing field is not part of a response: {:?}",
        response_properties.keys().collect::<Vec<_>>()
    );

    // The cycle: serde agrees, on the same values.
    let sent: Asymmetric =
        serde_json::from_value(json!({ "both": 1, "client_only": 2 })).expect("serde accepts it");
    assert_eq!(sent.server_only, 0, "a request cannot set it");
    let back = serde_json::to_value(&sent).expect("serializable");
    assert!(back.get("client_only").is_none(), "a response omits it");
    assert!(back.get("server_only").is_some());
}

#[derive(
    Serialize, Deserialize, postcard_schema::Schema, schemars::JsonSchema, Clone, Debug, PartialEq,
)]
pub struct Inner {
    pub depth: u64,
}

#[derive(
    Serialize, Deserialize, postcard_schema::Schema, schemars::JsonSchema, Clone, Debug, PartialEq,
)]
pub struct Flattened {
    pub name: String,
    #[serde(flatten)]
    pub inner: Inner,
}

keylink::service! {
    service Flat;
    rpc Take(item: Flattened) -> u64;
}

keylink_openapi::document_service!(flat);

/// `flatten` puts the inner fields on the outer object, in both readings.
///
/// serde writes `{"name":"x","depth":1}` and reads it back. A document that
/// described `inner` as a nested object would tell a generator to write
/// `{"name":"x","inner":{"depth":1}}`, which serde refuses.
#[test]
fn a_flattened_field_is_described_where_serde_writes_it() {
    let surface = HttpSurface::new("/v1", Addressing::Singleton).expect("valid surface");
    let config = DocumentConfig::new(
        "Flat",
        "1.0.0",
        Security::External {
            description: "Ahead of the gateway.".to_owned(),
        },
    );
    let document = document::<flat::External>(&surface, &config).expect("a document");
    let item = &document.value()["paths"]["/v1/rpc/Take"]["post"]["requestBody"]["content"]
        ["application/json"]["schema"]["properties"]["item"];
    let schema = item.get("$ref").and_then(Value::as_str).map_or_else(
        || item.clone(),
        |reference| {
            let name = reference
                .strip_prefix("#/components/schemas/")
                .expect("an OpenAPI reference");
            document.value()["components"]["schemas"][name].clone()
        },
    );
    let rendered = serde_json::to_string(&schema).expect("renderable");
    assert!(
        !rendered.contains("\"inner\""),
        "a flattened field has no name of its own on the wire: {rendered}"
    );
    assert!(rendered.contains("depth"), "{rendered}");

    let wire = json!({ "name": "x", "depth": 1 });
    let decoded: Flattened = serde_json::from_value(wire.clone()).expect("serde accepts it");
    assert_eq!(decoded.inner.depth, 1);
    assert_eq!(serde_json::to_value(&decoded).expect("serializable"), wire);
}

/// Every route family maps to its own operation-id segment.
///
/// `kind_name` has a `_ => "unknown"` arm over a `#[non_exhaustive]` enum, so a
/// family added upstream would silently produce `Service.singleton.unknown.M` —
/// and two such families would collide into one id. This row is what turns that
/// from a silent rename into a failing test.
#[test]
fn every_route_family_has_its_own_name() {
    use std::collections::BTreeSet;

    // Through `HttpSurface`, not through the private table: a test that needed
    // that table public would widen `keylink-http`'s surface in order to be
    // tested. Two surfaces with streaming enumerate every family the inventory
    // has.
    let limits = keylink_http::StreamingLimits::new(32, 64, 64, 4).expect("positive limits");
    let mut names = BTreeSet::new();
    let mut kinds = BTreeSet::new();
    for addressing in [Addressing::Singleton, Addressing::Actors] {
        let surface = HttpSurface::new("/v1", addressing)
            .expect("valid surface")
            .with_streaming(limits);
        for route in surface.routes() {
            kinds.insert(format!("{:?}", route.kind()));
            names.insert(keylink_openapi::__kind_name(route.kind()).to_owned());
        }
    }
    assert!(
        !names.contains("unknown"),
        "a route family this crate cannot name: {names:?}"
    );
    assert_eq!(
        names.len(),
        kinds.len(),
        "two families share one operation-id segment: {names:?} for {kinds:?}"
    );
}
