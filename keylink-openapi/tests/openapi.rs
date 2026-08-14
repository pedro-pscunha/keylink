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

keylink::service! {
    service Awkward;
    // A raw identifier, so the wire name must lose its `r#`.
    rpc Reserve(r#type: String, qty: Option<u32>) -> Option<u64>;
}

keylink_openapi::document_service!(awkward);

/// `Option<T>` is optional AND nullable, and a raw identifier loses its `r#`.
///
/// The runtime decodes with `serde`, which treats a missing `Option` field as
/// `None` AND an explicit `null` as `None`. A schema that marked the field
/// required would describe a request the gateway accepts and a caller cannot
/// send; one that omitted the null would reject a body the gateway takes.
///
/// `r#type` is the Rust spelling of a field whose wire name is `type`. A schema
/// carrying the `r#` would name a property no encoder ever writes.
#[test]
fn optional_fields_are_nullable_and_raw_identifiers_lose_their_prefix() {
    let surface = HttpSurface::new("/v1", Addressing::Singleton).expect("valid surface");
    let document = document::<awkward::External>(&surface, &config()).expect("a document");
    let schema = &document.value()["paths"]["/v1/rpc/Reserve"]["post"]["requestBody"]["content"]
        ["application/json"]["schema"];

    let properties = schema["properties"].as_object().expect("an object");
    assert!(
        properties.contains_key("type"),
        "a raw identifier is named by its wire name, not its Rust spelling: {:?}",
        properties.keys().collect::<Vec<_>>()
    );
    assert!(!properties.contains_key("r#type"));

    let required = schema["required"].as_array().expect("an array");
    assert!(
        required.iter().any(|value| value == "type"),
        "a non-Option argument is required"
    );
    assert!(
        !required.iter().any(|value| value == "qty"),
        "an Option argument is not required, because serde accepts it missing"
    );
    let qty = &properties["qty"];
    let rendered = serde_json::to_string(qty).expect("renderable");
    assert!(
        rendered.contains("null"),
        "an Option argument accepts an explicit null, as serde does: {rendered}"
    );
}

/// The component digest is pinned to a known vector.
///
/// Six BLAKE3 bytes over `keylink-openapi/component/v1\0`, the id's length as a
/// big-endian `u64`, and its UTF-8 bytes. The vector is the point: a change to
/// the domain, the length prefix or the truncation renames every component in
/// every published document, and a rename that nobody noticed is a broken
/// `$ref` in somebody's generator.
#[test]
fn the_component_digest_matches_its_recorded_vector() {
    assert_eq!(
        keylink_openapi::component_name("Inventory.Count.request"),
        // Read off the implementation, not computed by hand — a vector I
        // predicted would pin my arithmetic rather than the digest.
        "a134f0bdce20"
    );
    // The length prefix, measured. Without it these two ids concatenate the
    // same way and would name one component.
    assert_ne!(
        keylink_openapi::component_name("ab.c"),
        keylink_openapi::component_name("a.bc")
    );
}

/// Every documented endpoint serves its exact media type, and HEAD says the
/// same thing without a body.
#[tokio::test]
async fn the_document_endpoints_serve_their_exact_media_types() {
    use axum::body::Body;
    use http::Request;
    use tower::ServiceExt as _;

    let surface = HttpSurface::new("/v1", Addressing::Singleton)
        .expect("valid surface")
        .with_streaming(streaming());
    let openapi = document::<inventory::External>(&surface, &config()).expect("a document");
    let schema = schemas::<inventory::External>(&surface).expect("schemas");
    let router: axum::Router<()> =
        keylink_openapi::router(openapi.bytes().to_vec(), schema.bytes().to_vec());

    for (path, media, len) in [
        (
            "/openapi.json",
            "application/vnd.oai.openapi+json;version=3.1",
            openapi.bytes().len(),
        ),
        (
            "/schema.json",
            "application/schema+json",
            schema.bytes().len(),
        ),
    ] {
        let got = router
            .clone()
            .oneshot(
                Request::builder()
                    .uri(path)
                    .body(Body::empty())
                    .expect("a request"),
            )
            .await
            .expect("a response");
        assert_eq!(got.headers()[http::header::CONTENT_TYPE], media, "{path}");
        let body = axum::body::to_bytes(got.into_body(), usize::MAX)
            .await
            .expect("a body");
        assert_eq!(body.len(), len, "{path}");

        let head = router
            .clone()
            .oneshot(
                Request::builder()
                    .method(http::Method::HEAD)
                    .uri(path)
                    .body(Body::empty())
                    .expect("a request"),
            )
            .await
            .expect("a response");
        assert_eq!(head.headers()[http::header::CONTENT_TYPE], media, "{path}");
        assert_eq!(
            head.headers()[http::header::CONTENT_LENGTH],
            len.to_string(),
            "{path}: HEAD carries the length its GET would"
        );
        let head_body = axum::body::to_bytes(head.into_body(), usize::MAX)
            .await
            .expect("a body");
        assert!(head_body.is_empty(), "{path}: HEAD sends no body");
    }
}

/// The four canonical surfaces, byte for byte.
///
/// Snapshots rather than field assertions, because what a document publishes IS
/// its bytes: a caller's generator reads all of them, so a change to any is a
/// change to the contract. Set `KEYLINK_OPENAPI_BLESS=1` to rewrite them, and
/// read the diff before you do.
#[test]
fn the_canonical_surfaces_render_byte_for_byte() {
    let cases: [(&str, HttpSurface); 4] = [
        (
            "singleton-rpc",
            HttpSurface::new("/v1", Addressing::Singleton).expect("valid surface"),
        ),
        (
            "singleton-streaming",
            HttpSurface::new("/v1", Addressing::Singleton)
                .expect("valid surface")
                .with_streaming(streaming()),
        ),
        (
            "actor-rpc",
            HttpSurface::new("/v1", Addressing::Actors).expect("valid surface"),
        ),
        (
            "actor-streaming",
            HttpSurface::new("/v1", Addressing::Actors)
                .expect("valid surface")
                .with_streaming(streaming()),
        ),
    ];

    for (name, surface) in cases {
        let openapi = document::<inventory::External>(&surface, &config()).expect("a document");
        compare(&format!("{name}.openapi.json"), openapi.bytes());
        let schema = schemas::<inventory::External>(&surface).expect("schemas");
        compare(&format!("{name}.schema.json"), schema.bytes());
    }
}

fn compare(name: &str, bytes: &[u8]) {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/snapshots")
        .join(name);
    if std::env::var_os("KEYLINK_OPENAPI_BLESS").is_some() {
        std::fs::write(&path, bytes).expect("the snapshot is writable");
        return;
    }
    let expected = std::fs::read(&path)
        .unwrap_or_else(|error| panic!("{name} has no snapshot ({error}); bless it deliberately"));
    assert_eq!(
        String::from_utf8_lossy(bytes),
        String::from_utf8_lossy(&expected),
        "{name} changed"
    );
}

/// The bytes are the same in a FRESH PROCESS.
///
/// Two calls in one process share a hash seed, an allocator and a warmed set of
/// iteration orders, so agreeing there is weak evidence. A child process shares
/// none of them, which is what makes this a determinism test rather than a
/// repetition one.
#[test]
fn a_fresh_process_renders_identical_bytes() {
    const MARKER: &str = "KEYLINK_OPENAPI_CHILD";

    let surface = HttpSurface::new("/v1", Addressing::Singleton)
        .expect("valid surface")
        .with_streaming(streaming());
    let here = document::<inventory::External>(&surface, &config()).expect("a document");

    if std::env::var_os(MARKER).is_some() {
        print!("{}", fingerprint(here.bytes()));
        return;
    }

    let exe = std::env::current_exe().expect("this test binary has a path");
    let output = std::process::Command::new(exe)
        .args([
            "a_fresh_process_renders_identical_bytes",
            "--exact",
            "--nocapture",
        ])
        .env(MARKER, "1")
        .output()
        .expect("the child runs");
    let printed = String::from_utf8_lossy(&output.stdout);
    let expected = fingerprint(here.bytes());
    assert!(
        printed.contains(&expected),
        "the child rendered different bytes.\nexpected {expected}\nsaw {printed}"
    );
}

fn fingerprint(bytes: &[u8]) -> String {
    blake3::hash(bytes).to_hex().to_string()
}
