#![allow(clippy::expect_used)]

use std::sync::Arc;

use axum::{
    middleware::{self, Next},
    response::Response,
    Router,
};
use http::Request;
use iroh::{endpoint::presets, Endpoint};
use keylink::{Allowlist, Context, ExternalFuture, ExternalService, Node};
use keylink_http::{Access, AccessError, Call, Challenge, Gateway, JsonCodec, Principal};
use tokio::io::AsyncWriteExt;

keylink::service! {
    service Cancellation;
    rpc Wait(token: u64) -> u64;
}

struct Handler {
    entered: tokio::sync::mpsc::UnboundedSender<()>,
    release: Arc<tokio::sync::Notify>,
}

impl cancellation::Handler for Handler {
    async fn wait(&self, _cx: &Context, token: u64) -> u64 {
        self.entered.send(()).expect("test receiver stays open");
        self.release.notified().await;
        token
    }
}

#[derive(Clone)]
struct Allow;

impl Access for Allow {
    async fn authorize(&self, _call: Call<'_>) -> Result<(), AccessError> {
        Ok(())
    }
}

#[derive(Clone)]
struct TrackedClient {
    inner: cancellation::Client,
    dropped: tokio::sync::watch::Sender<bool>,
}

struct Tracked;

impl ExternalService<JsonCodec> for Tracked {
    const SERVICE: &'static str = <cancellation::External as ExternalService<JsonCodec>>::SERVICE;
    const RPC_METHODS: &'static [&'static str] =
        <cancellation::External as ExternalService<JsonCodec>>::RPC_METHODS;
    type Client = TrackedClient;

    fn invoke(
        client: &Self::Client,
        method: &str,
        body: &[u8],
        key: Option<keylink::IdempotencyKey>,
        codec: JsonCodec,
    ) -> ExternalFuture<JsonCodec> {
        let future = cancellation::External::invoke(&client.inner, method, body, key, codec);
        let sentinel = DropSentinel(client.dropped.clone());
        Box::pin(async move {
            let _sentinel = sentinel;
            future.await
        })
    }
}

struct DropSentinel(tokio::sync::watch::Sender<bool>);

impl Drop for DropSentinel {
    fn drop(&mut self) {
        let _ = self.0.send(true);
    }
}

async fn authenticate(mut request: Request<axum::body::Body>, next: Next) -> Response {
    request
        .extensions_mut()
        .insert(Principal::new(b"tenant-a").expect("valid principal"));
    next.run(request).await
}

async fn local_node() -> Node {
    Node::from_endpoint(
        Endpoint::bind(presets::Minimal)
            .await
            .expect("a local endpoint binds"),
    )
}

#[tokio::test]
async fn tcp_disconnect_drops_the_adapter_owned_invoke_future() {
    let server_node = local_node().await;
    let client_node = local_node().await;
    let (entered_tx, mut entered_rx) = tokio::sync::mpsc::unbounded_channel();
    let release = Arc::new(tokio::sync::Notify::new());
    let _keylink = cancellation::serve(
        &server_node,
        Handler {
            entered: entered_tx,
            release: Arc::clone(&release),
        },
        Allowlist::anyone(),
    )
    .expect("service starts");
    let client = cancellation::Client::connect(&client_node, server_node.endpoint().addr())
        .await
        .expect("client connects");
    let (dropped_tx, mut dropped_rx) = tokio::sync::watch::channel(false);
    let challenge = Challenge::new(http::HeaderValue::from_static("Bearer realm=keylink"))
        .expect("valid challenge");
    let gateway = Gateway::<Tracked, _>::new(
        TrackedClient {
            inner: client,
            dropped: dropped_tx,
        },
        Allow,
        challenge,
    )
    .router::<()>();
    let app: Router<()> = Router::new()
        .nest("/v1", gateway)
        .layer(middleware::from_fn(authenticate));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("listener binds");
    let address = listener.local_addr().expect("listener address");
    let server = tokio::spawn(async move { axum::serve(listener, app).await });

    let mut socket = tokio::net::TcpStream::connect(address)
        .await
        .expect("raw client connects");
    socket
        .write_all(
            b"POST /v1/rpc/Wait HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/json\r\nContent-Length: 11\r\n\r\n{\"token\":7}",
        )
        .await
        .expect("request writes");
    entered_rx.recv().await.expect("backend call starts");
    drop(socket);

    dropped_rx
        .changed()
        .await
        .expect("invoke future drop is reported");
    assert!(*dropped_rx.borrow());
    release.notify_waiters();
    server.abort();
}
