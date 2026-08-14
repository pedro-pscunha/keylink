//! The document, against the same surface the gateway is mounted with.

#![allow(clippy::expect_used)]

use keylink_http::{Addressing, HttpSurface, StreamingLimits};
use keylink_openapi::{document, schemas, DocumentConfig, Security};

keylink::service! {
    service Inventory;
    rpc Count(value: u64) -> u64;
    rpc Echo(value: String) -> String;
    send Upload(name: String) -> u64;
    recv Fetch(sku: String) -> u32;
    blob Take(sku: String) -> u64;
    tell Ignored(value: u64) -> ();
}

keylink_openapi::document_service!(inventory);

fn config() -> DocumentConfig {
    DocumentConfig::new(
        "Inventory",
        "1.0.0",
        Security::External {
            description: "The deployment authenticates ahead of this gateway.".to_owned(),
        },
    )
}

fn streaming() -> StreamingLimits {
    StreamingLimits::new(32, 64, 64, 4).expect("positive limits")
}

#[test]
fn a_singleton_streaming_surface_documents_every_implemented_route() {
    let surface = HttpSurface::new("/v1", Addressing::Singleton)
        .expect("valid surface")
        .with_streaming(streaming());
    let document = document::<inventory::External>(&surface, &config()).expect("a document");
    let paths = document.value()["paths"]
        .as_object()
        .expect("an object")
        .keys()
        .cloned()
        .collect::<Vec<_>>();

    assert!(paths.contains(&"/v1/rpc/Count".to_owned()));
    assert!(paths.contains(&"/v1/send/Upload".to_owned()));
    assert!(paths.contains(&"/v1/recv/Fetch".to_owned()));
    assert!(paths.contains(&"/v1/blob/Take".to_owned()));
    assert!(paths.contains(&"/v1/blob/Take/{id}".to_owned()));
    assert!(
        !paths.iter().any(|path| path.contains("Ignored")),
        "a tell has no external route and no documentation"
    );
    assert_eq!(document.value()["openapi"], "3.1.0");
}

#[test]
fn an_actor_surface_documents_the_keyed_intersection_and_no_send() {
    let surface = HttpSurface::new("/v1", Addressing::Actors)
        .expect("valid surface")
        .with_streaming(streaming());
    let document = document::<inventory::External>(&surface, &config()).expect("a document");
    let paths = document.value()["paths"]
        .as_object()
        .expect("an object")
        .keys()
        .cloned()
        .collect::<Vec<_>>();

    assert!(paths.contains(&"/v1/{actor}/rpc/Count".to_owned()));
    assert!(paths.contains(&"/v1/{actor}/recv/Fetch".to_owned()));
    assert!(paths.contains(&"/v1/{actor}/blob/Take".to_owned()));
    assert!(
        !paths.iter().any(|path| path.contains("Upload")),
        "a keyed surface serves no send, so it documents none"
    );
}

#[test]
fn a_request_contract_refuses_unknown_fields_exactly_as_the_runtime_does() {
    let surface = HttpSurface::new("/v1", Addressing::Singleton).expect("valid surface");
    let document = document::<inventory::External>(&surface, &config()).expect("a document");
    let schema = &document.value()["paths"]["/v1/rpc/Count"]["post"]["requestBody"]["content"]
        ["application/json"]["schema"];
    assert_eq!(schema["additionalProperties"], false);
    assert_eq!(schema["properties"]["value"]["type"], "integer");
    assert_eq!(schema["required"], serde_json::json!(["value"]));
}

#[test]
fn generating_twice_produces_identical_bytes() {
    let surface = HttpSurface::new("/v1", Addressing::Singleton)
        .expect("valid surface")
        .with_streaming(streaming());
    let first = document::<inventory::External>(&surface, &config()).expect("a document");
    let second = document::<inventory::External>(&surface, &config()).expect("a document");
    assert_eq!(first.bytes(), second.bytes());

    let one = schemas::<inventory::External>(&surface).expect("schemas");
    let two = schemas::<inventory::External>(&surface).expect("schemas");
    assert_eq!(one.bytes(), two.bytes());
}
