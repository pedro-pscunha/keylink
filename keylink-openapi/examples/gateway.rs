//! Every surface this workspace serves, on one process, for the smoke to drive.
//!
//! The singleton example in `keylink-http` is the small one — one `rpc` surface
//! and an access policy. This is the whole thing: streaming, blobs, a keyed
//! actor registry, and the two document endpoints, mounted the way a deployment
//! would mount them.
//!
//! It prints one line: the base URL. `mise run http-smoke` reads that line and
//! drives every route against it.

use std::sync::Arc;

use axum::{
    middleware::{self, Next},
    response::Response,
    Router,
};
use http::Request;
use keylink::prelude::*;
use keylink::{Allowlist, Blobs, Caller, Context, Node};
use keylink_http::{
    Access, AccessError, ActorGateway, Addressing, Call, Challenge, Gateway, HttpSurface,
    JsonLimits, Principal, StreamingLimits,
};
use keylink_openapi::{DocumentConfig, Security};

// TWO services, and the split is forced rather than chosen. A service that
// declares `send` cannot be served as an actor at all — the generated
// `ServableAsActor` check refuses it, because a keyed `send` would stream a
// body to an actor that four separate refusals can reject after the bytes are
// already on the wire. So the singleton surface keeps `send` and the keyed one
// is a second declaration without it.
keylink::service! {
    service Inventory;
    rpc Add(left: u64, right: u64) -> u64;
    rpc Count(value: u64) -> u64;
    send Upload(name: String) -> u64;
    recv Fetch(sku: String) -> u32;
    blob Datasheet(sku: String) -> u64;
    tell Audit(line: String) -> ();
}

keylink::service! {
    service Bays;
    rpc Whoami() -> String;
    recv Fetch(sku: String) -> u32;
    blob Datasheet(sku: String) -> u64;
}

keylink_openapi::document_service!(inventory);

const PAYLOAD: &[u8] = b"0123456789abcdefghijklmnopqrstuvwxyz";

#[derive(Clone)]
struct Warehouse {
    runs: Arc<std::sync::atomic::AtomicU64>,
    sheet: (keylink::BlobId, u64),
}

impl inventory::Handler for Warehouse {
    async fn add(&self, _cx: &Context, left: u64, right: u64) -> u64 {
        left + right
    }

    async fn count(&self, _cx: &Context, _value: u64) -> u64 {
        self.runs.fetch_add(1, std::sync::atomic::Ordering::SeqCst) + 1
    }

    async fn upload(&self, _cx: &Context, name: String, mut body: keylink::BodyReader) -> u64 {
        let mut seen = 0u64;
        while let Ok(Some(chunk)) = body.next_chunk().await {
            seen += chunk.len() as u64;
        }
        seen + name.len() as u64
    }

    async fn fetch(&self, _cx: &Context, sku: String) -> (u32, keylink::Body) {
        let bytes = sku.into_bytes().repeat(3);
        let declared = Some(bytes.len() as u64);
        (
            bytes.len() as u32,
            keylink::Body::from_reader(std::io::Cursor::new(bytes), declared),
        )
    }

    async fn datasheet(&self, _cx: &Context, _sku: String) -> (u64, keylink::BlobId) {
        (self.sheet.1, self.sheet.0)
    }

    async fn audit(&self, _cx: &Context, _line: String) {}
}

/// The keyed half. One actor per warehouse bay, each answering for itself.
#[derive(Clone)]
struct Bay {
    name: String,
    sheet: (keylink::BlobId, u64),
}

impl bays::Actor for Bay {
    async fn whoami(&mut self, _cx: Caller<'_>) -> String {
        self.name.clone()
    }

    async fn fetch(&mut self, _cx: Caller<'_>, _sku: String) -> (u32, keylink::Body) {
        let bytes = self.name.clone().into_bytes();
        let declared = Some(bytes.len() as u64);
        (
            bytes.len() as u32,
            keylink::Body::from_reader(std::io::Cursor::new(bytes), declared),
        )
    }

    async fn datasheet(&mut self, _cx: Caller<'_>, _sku: String) -> (u64, keylink::BlobId) {
        (self.sheet.1, self.sheet.0)
    }
}

/// What the authentication layer proved, not what the caller claimed.
#[derive(Clone, Copy)]
struct VerifiedClaims {
    may_call: bool,
}

#[derive(Clone)]
struct ExampleAccess;

impl Access for ExampleAccess {
    async fn authorize(&self, call: Call<'_>) -> Result<(), AccessError> {
        match call.extensions.get::<VerifiedClaims>() {
            Some(claims) if claims.may_call => Ok(()),
            _ => Err(AccessError::Forbidden),
        }
    }
}

/// A stand-in for a real authentication layer.
///
/// The principal comes from what THIS layer verified and never from a header
/// the caller can set to anything. A deployment replaces the token comparison
/// with its own verification; what must not change is that the `Principal` is
/// derived from the result of that verification.
async fn authenticate(mut request: Request<axum::body::Body>, next: Next) -> Response {
    let claims = match request.headers().get(http::header::AUTHORIZATION) {
        Some(value) if value == "Bearer allow" => Some(VerifiedClaims { may_call: true }),
        Some(value) if value == "Bearer deny" => Some(VerifiedClaims { may_call: false }),
        _ => None,
    };
    if let Some(claims) = claims {
        if let Ok(principal) = Principal::new(b"tenant-a") {
            request.extensions_mut().insert(principal);
            request.extensions_mut().insert(claims);
        }
    }
    next.run(request).await
}

fn limits() -> Result<StreamingLimits, Box<dyn std::error::Error>> {
    Ok(StreamingLimits::new(4096, 1 << 20, 1 << 20, 16)?)
}

fn challenge() -> Result<Challenge, Box<dyn std::error::Error>> {
    Ok(Challenge::new(http::HeaderValue::from_static(
        "Bearer realm=keylink",
    ))?)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let server_node = Node::bind_local().await?;
    let client_node = Node::bind_local().await?;
    let blobs = Blobs::memory();
    let id = blobs.add_bytes(PAYLOAD.to_vec()).await?;
    let sheet = (id, PAYLOAD.len() as u64);

    let warehouse = Warehouse {
        runs: Arc::new(std::sync::atomic::AtomicU64::new(0)),
        sheet,
    };
    let registry: bays::Registry = bays::Registry::builder(keylink::Eviction::Never).build();
    for name in ["bay-1", "bay/2"] {
        registry.mount(
            keylink::ActorId::new(name)?,
            bays::spawn(Bay {
                name: name.to_owned(),
                sheet,
            }),
        )?;
    }

    let _keylink = keylink::Server::builder(&server_node)
        .service(
            inventory::service(warehouse, Allowlist::anyone())
                .with_blobs(&blobs)
                .build(),
        )
        .service(
            registry
                .service(Allowlist::anyone())
                .with_blobs(&blobs)
                .build(),
        )
        .spawn()?;

    let singleton_surface =
        HttpSurface::new("/inventory", Addressing::Singleton)?.with_streaming(limits()?);
    // The SECOND gateway on this router serves no fallback: axum permits one
    // per router, and merging two that both have one panics.
    let actor_surface = HttpSurface::new("/bays", Addressing::Actors)?
        .with_streaming(limits()?)
        .without_fallback();

    let singleton = Gateway::<inventory::External, _>::new(
        inventory::Client::connect(&client_node, server_node.endpoint().addr()).await?,
        ExampleAccess,
        challenge()?,
    )
    .limits(JsonLimits::new(64 * 1024, 64 * 1024)?)
    .router::<()>(singleton_surface.clone())?;

    let keyed = ActorGateway::<bays::External, _>::new(
        bays::RegistryClient::connect(&client_node, server_node.endpoint().addr()).await?,
        ExampleAccess,
        challenge()?,
    )
    .limits(JsonLimits::new(64 * 1024, 64 * 1024)?)
    .router::<()>(actor_surface)?;

    // The documents describe the SINGLETON surface, built from the same
    // `HttpSurface` that gateway is mounted with. Mounted explicitly, under a
    // prefix this example chose — nothing auto-installs them.
    let config = DocumentConfig::new(
        "Inventory",
        env!("CARGO_PKG_VERSION"),
        Security::External {
            description: "A deployment authenticates ahead of this gateway and \
                          inserts a verified Principal."
                .to_owned(),
        },
    );
    let openapi = keylink_openapi::document::<inventory::External>(&singleton_surface, &config)?;
    let schema = keylink_openapi::schemas::<inventory::External>(&singleton_surface)?;
    let spec: Router<()> =
        keylink_openapi::router(openapi.bytes().to_vec(), schema.bytes().to_vec());

    let app: Router<()> = Router::new()
        .merge(singleton)
        .merge(keyed)
        .merge(spec)
        .layer(middleware::from_fn(authenticate));

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let server = tokio::spawn(async move { axum::serve(listener, app).await });

    println!("http://{address}");
    server.await??;
    Ok(())
}
