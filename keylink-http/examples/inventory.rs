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

#[derive(Clone)]
struct ExampleAccess;

impl Access for ExampleAccess {
    async fn authorize(&self, call: Call<'_>) -> Result<(), AccessError> {
        match call.headers.get(http::header::AUTHORIZATION) {
            Some(value) if value == "Bearer allow" => Ok(()),
            _ => Err(AccessError::Forbidden),
        }
    }
}

async fn authenticate(mut request: Request<axum::body::Body>, next: Next) -> Response {
    let verified = request
        .headers()
        .get(http::header::AUTHORIZATION)
        .filter(|value| *value == "Bearer allow" || *value == "Bearer deny")
        .cloned();
    if let Some(verified) = verified {
        if let Ok(principal) = Principal::new(verified.as_bytes()) {
            request.extensions_mut().insert(principal);
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
        .router::<()>();
    let app: Router<()> = Router::new()
        .nest("/inventory", gateway)
        .layer(middleware::from_fn(authenticate));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let server = tokio::spawn(async move { axum::serve(listener, app).await });

    if std::env::args().any(|arg| arg == "--self-test") {
        self_test(address).await?;
        server.abort();
        return Ok(());
    }

    println!("http://{address}/inventory");
    server.await??;
    Ok(())
}

async fn self_test(address: std::net::SocketAddr) -> Result<(), Box<dyn std::error::Error>> {
    let client = reqwest::Client::new();
    let base = format!("http://{address}/inventory/rpc");
    let normal = client
        .post(format!("{base}/Add"))
        .header("authorization", "Bearer allow")
        .header("content-type", "application/json")
        .body(r#"{"left":2,"right":3}"#)
        .send()
        .await?;
    require(
        normal.status() == http::StatusCode::OK,
        "normal call failed",
    )?;

    for expected in ["1", "1"] {
        let retry = client
            .post(format!("{base}/Count"))
            .header("authorization", "Bearer allow")
            .header("content-type", "application/json")
            .header("idempotency-key", "example-operation")
            .body(r#"{"value":7}"#)
            .send()
            .await?;
        require(retry.text().await? == expected, "idempotent retry failed")?;
    }

    for (authorization, path, body, status) in [
        (
            "Bearer deny",
            "Add",
            r#"{"left":2,"right":3}"#,
            http::StatusCode::FORBIDDEN,
        ),
        ("Bearer allow", "Add", "{", http::StatusCode::BAD_REQUEST),
        (
            "Bearer allow",
            "Audit",
            r#"{"line":"x"}"#,
            http::StatusCode::NOT_FOUND,
        ),
    ] {
        let response = client
            .post(format!("{base}/{path}"))
            .header("authorization", authorization)
            .header("content-type", "application/json")
            .body(body)
            .send()
            .await?;
        require(response.status() == status, "example failure smoke failed")?;
    }
    Ok(())
}

fn require(condition: bool, message: &'static str) -> Result<(), std::io::Error> {
    if condition {
        Ok(())
    } else {
        Err(std::io::Error::other(message))
    }
}
