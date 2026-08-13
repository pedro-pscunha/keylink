use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};

use axum::{
    middleware::{self, Next},
    response::Response,
    Router,
};
use http::Request;
use keylink::{Allowlist, Context, Node};
use keylink_http::{Access, AccessError, Call, Challenge, Gateway, JsonLimits, Principal};

keylink::service! {
    service Inventory;
    rpc Add(left: u64, right: u64) -> u64;
    rpc Count(value: u64) -> u64;
    tell Audit(line: String) -> ();
}

#[derive(Clone, Default)]
struct Inventory {
    runs: Arc<AtomicU64>,
}

impl inventory::Handler for Inventory {
    async fn add(&self, _cx: &Context, left: u64, right: u64) -> u64 {
        left + right
    }

    async fn count(&self, _cx: &Context, _value: u64) -> u64 {
        self.runs.fetch_add(1, Ordering::SeqCst) + 1
    }

    async fn audit(&self, _cx: &Context, _line: String) {}
}

#[derive(Clone, Copy)]
struct VerifiedClaims {
    can_call_inventory: bool,
}

#[derive(Clone)]
struct ExampleAccess;

impl Access for ExampleAccess {
    async fn authorize(&self, call: Call<'_>) -> Result<(), AccessError> {
        match call.extensions.get::<VerifiedClaims>() {
            Some(claims) if claims.can_call_inventory => Ok(()),
            _ => Err(AccessError::Forbidden),
        }
    }
}

async fn authenticate(mut request: Request<axum::body::Body>, next: Next) -> Response {
    let claims = match request.headers().get(http::header::AUTHORIZATION) {
        Some(value) if value == "Bearer allow" => Some(VerifiedClaims {
            can_call_inventory: true,
        }),
        Some(value) if value == "Bearer deny" => Some(VerifiedClaims {
            can_call_inventory: false,
        }),
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

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let server_node = Node::bind_local().await?;
    let client_node = Node::bind_local().await?;
    let _keylink = inventory::serve(&server_node, Inventory::default(), Allowlist::anyone())?;
    let client = inventory::Client::connect(&client_node, server_node.endpoint().addr()).await?;
    let challenge = Challenge::new(http::HeaderValue::from_static("Bearer realm=keylink"))?;
    let gateway = Gateway::<inventory::External, _>::new(client, ExampleAccess, challenge)
        .limits(JsonLimits::new(64 * 1024, 64 * 1024)?)
        .router::<()>(keylink_http::HttpSurface::new(
            "/inventory",
            keylink_http::Addressing::Singleton,
        )?)?;
    let app: Router<()> = Router::new()
        .merge(gateway)
        .layer(middleware::from_fn(authenticate));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let server = tokio::spawn(async move { axum::serve(listener, app).await });

    println!("http://{address}/inventory");
    server.await??;
    Ok(())
}
