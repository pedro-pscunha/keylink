//! A keyed gateway over a service defined in ANOTHER crate.
//!
//! Every other `service!` in this workspace sits in the crate that consumes it,
//! so nothing measured whether the generated actor seam survives a crate
//! boundary. Three failures live only here: an item the macro emits without
//! `pub`, a type it names by a path valid only where it was written, and an
//! `ExternalActorService` impl on a marker a consumer cannot reach.
//!
//! This file consumes `imported_ledger::ledger`'s generated `External`,
//! `RegistryClient`, `Registry`, `Actor` and `spawn` and drives all three keyed
//! route families over a real socket.

#![allow(clippy::expect_used)]

use axum::{middleware, response::Response, Router};
use http::Request;
use imported_ledger::ledger;
use iroh::{endpoint::presets, Endpoint};
use keylink::prelude::*;
use keylink::{Allowlist, BlobId, Blobs, Caller, Node};
use keylink_http::{
    Access, AccessError, ActorGateway, Addressing, Call, Challenge, HttpSurface, Principal,
    StreamingLimits,
};

#[derive(Clone)]
struct Book {
    name: String,
    blob: (BlobId, u64),
}

impl ledger::Actor for Book {
    async fn count(&mut self, _cx: Caller<'_>, value: u64) -> u64 {
        value + 1
    }

    async fn whoami(&mut self, _cx: Caller<'_>) -> String {
        self.name.clone()
    }

    async fn read(&mut self, _cx: Caller<'_>, chunks: u64) -> (String, keylink::Body) {
        let bytes = self
            .name
            .as_bytes()
            .repeat(usize::try_from(chunks).expect("test-sized body"));
        let declared = Some(bytes.len() as u64);
        (
            self.name.clone(),
            keylink::Body::from_reader(std::io::Cursor::new(bytes), declared),
        )
    }

    async fn take(&mut self, _cx: Caller<'_>) -> (u64, keylink::Grant) {
        (self.blob.1, self.blob.0.into())
    }
}

#[derive(Clone)]
struct Anyone;

impl Access for Anyone {
    async fn authorize(&self, _call: Call<'_>) -> Result<(), AccessError> {
        Ok(())
    }
}

async fn authenticate(mut request: Request<axum::body::Body>, next: middleware::Next) -> Response {
    request
        .extensions_mut()
        .insert(Principal::new(b"tenant-a").expect("valid principal"));
    next.run(request).await
}

async fn local_node() -> Node {
    Node::from_endpoint(
        Endpoint::bind(presets::Minimal)
            .await
            .expect("local endpoint binds"),
    )
}

/// A gateway built over another crate's generated seam serves every keyed
/// route family.
///
/// One row rather than four, because what is under test is the BOUNDARY and not
/// the routing — the routing has its own file. If any generated item were
/// unreachable from here this would not compile, which is the first half of the
/// measurement; the second half is that each family answers with content
/// derived from the actor the path named.
#[tokio::test]
async fn a_gateway_over_an_imported_service_serves_every_keyed_route() {
    let server_node = local_node().await;
    let client_node = local_node().await;
    let blobs = Blobs::memory();
    let id = blobs
        .add_bytes(b"bytes of imported".to_vec())
        .await
        .expect("stores");

    let system: ledger::Registry = ledger::Registry::builder(keylink::Eviction::Never).build();
    system
        .mount(
            keylink::ActorId::new("imported").expect("a valid id"),
            ledger::spawn(Book {
                name: "imported".to_owned(),
                blob: (id, 17),
            }),
        )
        .expect("mounts");
    let _server = keylink::Server::builder(&server_node)
        .service(
            system
                .service(Allowlist::anyone())
                .with_blobs(&blobs)
                .build(),
        )
        .spawn()
        .expect("a keyed system over an imported service");
    let client = ledger::RegistryClient::connect(&client_node, server_node.endpoint().addr())
        .await
        .expect("dials the keyed system");

    let gateway = ActorGateway::<ledger::External, _>::new(
        client,
        Anyone,
        Challenge::new(http::HeaderValue::from_static("Bearer realm=keylink"))
            .expect("valid challenge"),
    )
    .router::<()>(
        HttpSurface::new("/v1", Addressing::Actors)
            .expect("valid actor surface")
            .with_streaming(StreamingLimits::new(32, 64, 64, 1).expect("positive limits")),
    )
    .expect("actor surface builds over an imported External");
    let app: Router<()> = Router::new()
        .merge(gateway)
        .layer(middleware::from_fn(authenticate));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("listener binds");
    let address = listener.local_addr().expect("listener address");
    let served = tokio::spawn(async move { axum::serve(listener, app).await });

    let post = |path: String, body: &'static str| {
        reqwest::Client::new()
            .post(format!("http://{address}{path}"))
            .header(http::header::CONTENT_TYPE, "application/json")
            .body(body)
            .send()
    };

    // rpc
    let counted = post("/v1/imported/rpc/Count".to_owned(), r#"{"value":41}"#)
        .await
        .expect("the request completes");
    assert_eq!(counted.status(), http::StatusCode::OK);
    assert_eq!(counted.text().await.expect("a body"), "42");

    // recv
    let read = post("/v1/imported/recv/Read".to_owned(), r#"{"chunks":2}"#)
        .await
        .expect("the request completes");
    assert_eq!(read.status(), http::StatusCode::OK);
    assert_eq!(
        read.text().await.expect("a body"),
        "importedimported",
        "the streamed body came from the actor the path named"
    );

    // blob lookup, then the bytes it addresses
    let minted = post("/v1/imported/blob/Take".to_owned(), "{}")
        .await
        .expect("the request completes");
    assert_eq!(minted.status(), http::StatusCode::CREATED);
    let location = minted
        .headers()
        .get(http::header::LOCATION)
        .expect("a lookup says where the bytes live")
        .to_str()
        .expect("a header-safe path")
        .to_owned();
    let bytes = reqwest::Client::new()
        .get(format!("http://{address}{location}"))
        .send()
        .await
        .expect("the request completes");
    assert_eq!(bytes.status(), http::StatusCode::OK);
    assert_eq!(bytes.text().await.expect("a body"), "bytes of imported");

    served.abort();
}
