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
