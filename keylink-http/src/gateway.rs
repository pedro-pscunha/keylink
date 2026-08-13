//! Ordered state-generic Axum routing.

use std::{error::Error as _, marker::PhantomData, sync::Arc};

use axum::{body::Body, routing::any, Router};
use http::{header, HeaderMap, Method, Request, Response};
use keylink::ExternalService;

use crate::{
    error::GatewayError, service::JsonCodec, Access, AccessError, Call, Challenge, JsonLimits,
    Principal,
};

const IDEMPOTENCY_HEADER: &str = "idempotency-key";
const KEY_DOMAIN: &[u8] = b"keylink-http/idempotency/v1\0";

/// HTTP/JSON adapter for one already-connected generated singleton client.
pub struct Gateway<S, A>
where
    S: ExternalService,
    A: Access,
{
    client: S::Client,
    access: A,
    challenge: Challenge,
    limits: JsonLimits,
    service: PhantomData<fn() -> S>,
}

impl<S, A> Gateway<S, A>
where
    S: ExternalService,
    A: Access,
{
    /// Build a gateway with mandatory access and a validated cached challenge.
    #[must_use]
    pub fn new(client: S::Client, access: A, challenge: Challenge) -> Self {
        Self {
            client,
            access,
            challenge,
            limits: JsonLimits::default(),
            service: PhantomData,
        }
    }

    /// Replace the finite inclusive JSON byte ceilings.
    #[must_use]
    pub fn limits(mut self, limits: JsonLimits) -> Self {
        self.limits = limits;
        self
    }

    /// Build a router that can be nested into an application router with any state.
    pub fn router<State>(self) -> Router<State>
    where
        State: Clone + Send + Sync + 'static,
    {
        let gateway = Arc::new(self);
        Router::new()
            .route(
                "/rpc/{method}",
                any(move |request: Request<Body>| {
                    let gateway = Arc::clone(&gateway);
                    async move { gateway.handle(request).await }
                }),
            )
            .fallback(|| async { GatewayError::MethodNotFound.response(None) })
    }

    async fn handle(&self, request: Request<Body>) -> Response<Body> {
        let decoded = match decode_method(request.uri().path()) {
            Ok(method) => method,
            Err(error) => return error.response(None),
        };
        let Some(method) = S::RPC_METHODS
            .iter()
            .copied()
            .find(|candidate| *candidate == decoded)
        else {
            return GatewayError::MethodNotFound.response(None);
        };
        if request.method() != Method::POST {
            return GatewayError::MethodNotAllowed.response(None);
        }
        let Some(principal) = request.extensions().get::<Principal>().cloned() else {
            return GatewayError::Unauthenticated.response(Some(self.challenge.header()));
        };
        match self
            .access
            .authorize(Call {
                extensions: request.extensions(),
                headers: request.headers(),
                principal: &principal,
                service: S::SERVICE,
                method,
            })
            .await
        {
            Ok(()) => {}
            Err(AccessError::Unauthenticated) => {
                return GatewayError::Unauthenticated.response(Some(self.challenge.header()));
            }
            Err(AccessError::Forbidden) => return GatewayError::Forbidden.response(None),
        }
        if !is_json(request.headers()) {
            return GatewayError::UnsupportedMediaType.response(None);
        }
        let key = match derived_key(request.headers(), &principal) {
            Ok(key) => key,
            Err(error) => return error.response(None),
        };
        let (_, body) = request.into_parts();
        let request_limit = self.limits.request.get();
        let body = match axum::body::to_bytes(body, request_limit.saturating_add(1)).await {
            Ok(body) if body.len() <= request_limit => body,
            Ok(_) => return GatewayError::RequestTooLarge.response(None),
            Err(error) if is_length_limit(&error) => {
                return GatewayError::RequestTooLarge.response(None);
            }
            Err(_) => return GatewayError::InvalidRequest.response(None),
        };
        let invocation = S::invoke(
            &self.client,
            method,
            &body,
            key,
            JsonCodec::new(self.limits.response.get()),
        );
        match invocation.await {
            Ok(json) => json_response(json),
            Err(error) => GatewayError::from_external(error).response(None),
        }
    }
}

fn json_response(json: Vec<u8>) -> Response<Body> {
    let mut response = Response::new(Body::from(json));
    response.headers_mut().insert(
        header::CONTENT_TYPE,
        http::HeaderValue::from_static("application/json"),
    );
    response
}

fn decode_method(path: &str) -> Result<String, GatewayError> {
    let encoded = path
        .strip_prefix("/rpc/")
        .ok_or(GatewayError::MethodNotFound)?;
    if encoded.is_empty() || encoded.contains('/') {
        return Err(GatewayError::MethodNotFound);
    }
    let bytes = encoded.as_bytes();
    let mut decoded = Vec::with_capacity(bytes.len());
    let mut index = 0;
    while index < bytes.len() {
        if bytes[index] == b'%' {
            let Some(pair) = bytes.get(index + 1..index + 3) else {
                return Err(GatewayError::InvalidRequest);
            };
            let Some(high) = hex(pair[0]) else {
                return Err(GatewayError::InvalidRequest);
            };
            let Some(low) = hex(pair[1]) else {
                return Err(GatewayError::InvalidRequest);
            };
            decoded.push((high << 4) | low);
            index += 3;
        } else {
            decoded.push(bytes[index]);
            index += 1;
        }
    }
    String::from_utf8(decoded).map_err(|_| GatewayError::InvalidRequest)
}

fn hex(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}

fn is_json(headers: &HeaderMap) -> bool {
    let mut values = headers.get_all(header::CONTENT_TYPE).iter();
    let Some(value) = values.next() else {
        return false;
    };
    if values.next().is_some() {
        return false;
    }
    let Ok(value) = value.to_str() else {
        return false;
    };
    let Ok(media) = value.parse::<mime::Mime>() else {
        return false;
    };
    media.type_() == mime::APPLICATION
        && (media.subtype() == mime::JSON || media.suffix() == Some(mime::JSON))
}

fn is_length_limit(error: &axum::Error) -> bool {
    let mut source = error.source();
    while let Some(error) = source {
        if error.is::<http_body_util::LengthLimitError>() {
            return true;
        }
        source = error.source();
    }
    false
}

fn derived_key(
    headers: &HeaderMap,
    principal: &Principal,
) -> Result<Option<keylink::IdempotencyKey>, GatewayError> {
    let mut values = headers.get_all(IDEMPOTENCY_HEADER).iter();
    let Some(value) = values.next() else {
        return Ok(None);
    };
    if values.next().is_some() {
        return Err(GatewayError::InvalidIdempotencyKey);
    }
    let value = value
        .to_str()
        .map_err(|_| GatewayError::InvalidIdempotencyKey)?;
    if value.is_empty()
        || value.len() > keylink::MAX_KEY_LEN
        || !value.bytes().all(|byte| (0x21..=0x7e).contains(&byte))
    {
        return Err(GatewayError::InvalidIdempotencyKey);
    }
    let principal = principal.as_bytes();
    let mut hasher = blake3::Hasher::new();
    hasher.update(KEY_DOMAIN);
    hasher.update(&(principal.len() as u64).to_be_bytes());
    hasher.update(principal);
    hasher.update(&(value.len() as u64).to_be_bytes());
    hasher.update(value.as_bytes());
    let encoded = hasher.finalize().to_hex().to_string();
    keylink::IdempotencyKey::new(encoded)
        .map(Some)
        .map_err(|_| GatewayError::InvalidIdempotencyKey)
}

#[cfg(test)]
mod tests {
    #![allow(clippy::expect_used)]

    use std::{
        convert::Infallible,
        future::{pending, Future},
        pin::Pin,
        sync::{
            atomic::{AtomicBool, AtomicUsize, Ordering},
            Arc, Mutex,
        },
        task::{Context, Poll, Waker},
    };

    use axum::{body::Body, Router};
    use bytes::Bytes;
    use http::{HeaderValue, Request};
    use http_body::{Body as HttpBody, Frame, SizeHint};
    use iroh::{endpoint::presets, Endpoint};
    use keylink::{
        Allowlist, Context as KeylinkContext, ExternalError, ExternalFuture, ExternalService, Node,
    };
    use tokio::io::AsyncWriteExt;

    use super::Gateway;
    use crate::{Access, AccessError, Call, Challenge, Principal};

    #[derive(Clone)]
    struct TestClient {
        invoked: Arc<AtomicUsize>,
        dropped: Arc<AtomicBool>,
        pending: bool,
    }

    struct TestService;

    impl ExternalService for TestService {
        const SERVICE: &'static str = "CapturedService";
        const RPC_METHODS: &'static [&'static str] = &["Run"];
        type Client = TestClient;

        fn invoke<C: keylink::ServiceCodec>(
            client: &Self::Client,
            _method: &str,
            _body: &[u8],
            _key: Option<keylink::IdempotencyKey>,
            codec: C,
        ) -> ExternalFuture<C> {
            client.invoked.fetch_add(1, Ordering::SeqCst);
            let sentinel = DropSentinel(Arc::clone(&client.dropped));
            if client.pending {
                Box::pin(async move {
                    let _sentinel = sentinel;
                    pending::<Result<C::Output, ExternalError<C::Error>>>().await
                })
            } else {
                drop(sentinel);
                Box::pin(async move { codec.encode(&()).map_err(ExternalError::Codec) })
            }
        }
    }

    struct DropSentinel(Arc<AtomicBool>);

    impl Drop for DropSentinel {
        fn drop(&mut self) {
            self.0.store(true, Ordering::SeqCst);
        }
    }

    struct RecordingBody {
        polls: Arc<AtomicUsize>,
        bytes: Option<Bytes>,
    }
    struct ErrorBody;

    impl HttpBody for ErrorBody {
        type Data = Bytes;
        type Error = std::io::Error;

        fn poll_frame(
            self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
        ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
            Poll::Ready(Some(Err(std::io::Error::other("short body failed"))))
        }
    }

    impl RecordingBody {
        fn new(polls: Arc<AtomicUsize>) -> Self {
            Self {
                polls,
                bytes: Some(Bytes::from_static(b"{")),
            }
        }
    }

    impl HttpBody for RecordingBody {
        type Data = Bytes;
        type Error = Infallible;

        fn poll_frame(
            mut self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
        ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
            self.polls.fetch_add(1, Ordering::SeqCst);
            Poll::Ready(self.bytes.take().map(Frame::data).map(Ok))
        }

        fn size_hint(&self) -> SizeHint {
            SizeHint::with_exact(self.bytes.as_ref().map_or(0, Bytes::len) as u64)
        }
    }

    #[derive(Clone)]
    struct PanicAccess;

    impl Access for PanicAccess {
        async fn authorize(&self, _call: Call<'_>) -> Result<(), AccessError> {
            panic!("unknown methods must bypass access")
        }
    }

    #[derive(Clone)]
    struct Deny;

    impl Access for Deny {
        async fn authorize(&self, _call: Call<'_>) -> Result<(), AccessError> {
            Err(AccessError::Forbidden)
        }
    }

    #[derive(Debug, PartialEq, Eq)]
    struct Captured {
        extension: Option<u64>,
        header: Option<HeaderValue>,
        principal: Principal,
        service: &'static str,
        method: &'static str,
    }

    #[derive(Clone)]
    struct Capture(Arc<Mutex<Option<Captured>>>);

    impl Access for Capture {
        async fn authorize(&self, call: Call<'_>) -> Result<(), AccessError> {
            *self.0.lock().expect("capture mutex") = Some(Captured {
                extension: call.extensions.get::<u64>().copied(),
                header: call.headers.get("x-capture").cloned(),
                principal: call.principal.clone(),
                service: call.service,
                method: call.method,
            });
            Err(AccessError::Forbidden)
        }
    }

    fn client() -> TestClient {
        TestClient {
            invoked: Arc::new(AtomicUsize::new(0)),
            dropped: Arc::new(AtomicBool::new(false)),
            pending: true,
        }
    }

    fn ready_client() -> TestClient {
        TestClient {
            invoked: Arc::new(AtomicUsize::new(0)),
            dropped: Arc::new(AtomicBool::new(false)),
            pending: false,
        }
    }

    fn gateway<A: Access>(client: TestClient, access: A) -> Gateway<TestService, A> {
        Gateway::new(
            client,
            access,
            Challenge::new(HeaderValue::from_static("Bearer realm=keylink"))
                .expect("valid challenge"),
        )
    }

    fn request(path: &str, body: Body) -> Request<Body> {
        Request::builder()
            .method("POST")
            .uri(path)
            .header("content-type", "application/json")
            .extension(Principal::new(b"tenant-a").expect("valid principal"))
            .body(body)
            .expect("request")
    }

    #[test]
    fn malformed_percent_encoding_is_an_invalid_adapter_request() {
        assert!(matches!(
            super::decode_method("/rpc/%ZZ"),
            Err(crate::error::GatewayError::InvalidRequest)
        ));
        assert!(matches!(
            super::decode_method("/rpc/%FF"),
            Err(crate::error::GatewayError::InvalidRequest)
        ));
    }

    #[tokio::test]
    async fn unknown_method_never_polls_the_body_or_dispatches() {
        let client = client();
        let polls = Arc::new(AtomicUsize::new(0));
        let response = gateway(client.clone(), PanicAccess)
            .handle(request(
                "/rpc/Missing",
                Body::new(RecordingBody::new(Arc::clone(&polls))),
            ))
            .await;

        assert_eq!(response.status(), http::StatusCode::NOT_FOUND);
        assert_eq!(polls.load(Ordering::SeqCst), 0);
        assert_eq!(client.invoked.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn denied_method_never_polls_the_body_or_dispatches() {
        let client = client();
        let polls = Arc::new(AtomicUsize::new(0));
        let response = gateway(client.clone(), Deny)
            .handle(request(
                "/rpc/Run",
                Body::new(RecordingBody::new(Arc::clone(&polls))),
            ))
            .await;

        assert_eq!(response.status(), http::StatusCode::FORBIDDEN);
        assert_eq!(polls.load(Ordering::SeqCst), 0);
        assert_eq!(client.invoked.load(Ordering::SeqCst), 0);
    }

    /// Watched red with `scripts/break-once.sh` by replacing
    /// `    if values.next().is_some() {
    ///         return false;
    ///     }` with an always-false branch.
    /// The exact test terminated with HTTP 200 instead of 415.
    #[tokio::test]
    async fn duplicate_content_types_are_rejected() {
        let client = ready_client();
        let mut request = request("/rpc/Run", Body::from("{}"));
        request.headers_mut().remove(http::header::CONTENT_TYPE);
        request.headers_mut().append(
            http::header::CONTENT_TYPE,
            HeaderValue::from_static("application/json"),
        );
        request.headers_mut().append(
            http::header::CONTENT_TYPE,
            HeaderValue::from_static("text/plain"),
        );

        let response = gateway(client.clone(), Denyless).handle(request).await;
        assert_eq!(response.status(), http::StatusCode::UNSUPPORTED_MEDIA_TYPE);
        assert_eq!(client.invoked.load(Ordering::SeqCst), 0);
    }

    /// Watched red with `scripts/break-once.sh` by replacing
    /// `    let Ok(media) = value.parse::<mime::Mime>() else {
    ///         return false;
    ///     };` with parsing of only the value before the first semicolon.
    /// The exact test terminated with HTTP 200 instead of 415.
    #[tokio::test]
    async fn malformed_content_types_are_rejected() {
        for value in ["application/json; charset", "application/json; =utf-8"] {
            let client = ready_client();
            let mut request = request("/rpc/Run", Body::from("{}"));
            request
                .headers_mut()
                .insert(http::header::CONTENT_TYPE, HeaderValue::from_static(value));

            let response = gateway(client.clone(), Denyless).handle(request).await;
            assert_eq!(
                response.status(),
                http::StatusCode::UNSUPPORTED_MEDIA_TYPE,
                "{value}"
            );
            assert_eq!(client.invoked.load(Ordering::SeqCst), 0, "{value}");
        }
    }

    /// Watched red with `scripts/break-once.sh` by replacing
    /// `            Err(_) => return GatewayError::InvalidRequest.response(None),`
    /// with `RequestTooLarge`. The exact test terminated with HTTP 413 instead of 400.
    #[tokio::test]
    async fn short_body_collection_failure_is_not_reported_as_too_large() {
        let client = client();
        let response = gateway(client.clone(), Denyless)
            .handle(request("/rpc/Run", Body::new(ErrorBody)))
            .await;
        assert_eq!(response.status(), http::StatusCode::BAD_REQUEST);
        assert_eq!(client.invoked.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn opaque_idempotency_header_is_rejected_before_body_poll_or_dispatch() {
        let client = client();
        let polls = Arc::new(AtomicUsize::new(0));
        let mut request = request(
            "/rpc/Run",
            Body::new(RecordingBody::new(Arc::clone(&polls))),
        );
        request.headers_mut().insert(
            "idempotency-key",
            HeaderValue::from_bytes(&[0xff]).expect("opaque header value"),
        );

        let response = gateway(client.clone(), Denyless).handle(request).await;

        assert_eq!(response.status(), http::StatusCode::BAD_REQUEST);
        assert_eq!(polls.load(Ordering::SeqCst), 0);
        assert_eq!(client.invoked.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn gateway_forwards_the_complete_verified_call_to_access() {
        let client = client();
        let captured = Arc::new(Mutex::new(None));
        let polls = Arc::new(AtomicUsize::new(0));
        let mut request = request(
            "/rpc/Run",
            Body::new(RecordingBody::new(Arc::clone(&polls))),
        );
        request
            .headers_mut()
            .insert("x-capture", HeaderValue::from_static("header-value"));
        request.extensions_mut().insert(7_u64);

        let response = gateway(client.clone(), Capture(Arc::clone(&captured)))
            .handle(request)
            .await;

        assert_eq!(response.status(), http::StatusCode::FORBIDDEN);
        assert_eq!(polls.load(Ordering::SeqCst), 0);
        assert_eq!(client.invoked.load(Ordering::SeqCst), 0);
        assert_eq!(
            captured.lock().expect("capture mutex").take(),
            Some(Captured {
                extension: Some(7),
                header: Some(HeaderValue::from_static("header-value")),
                principal: Principal::new(b"tenant-a").expect("valid principal"),
                service: "CapturedService",
                method: "Run",
            })
        );
    }

    #[test]
    fn concurrent_handlers_poll_independent_invocations_without_waiting() {
        let client = client();
        let gateway = gateway(client.clone(), Denyless);
        let mut first = Box::pin(gateway.handle(request("/rpc/Run", Body::from("{}"))));
        let mut second = Box::pin(gateway.handle(request("/rpc/Run", Body::from("{}"))));
        let waker = Waker::noop();
        let mut context = Context::from_waker(waker);

        assert!(Future::poll(first.as_mut(), &mut context).is_pending());
        assert!(Future::poll(second.as_mut(), &mut context).is_pending());
        assert_eq!(client.invoked.load(Ordering::SeqCst), 2);
    }

    #[test]
    fn dropping_handler_drops_the_adapter_owned_invocation_future() {
        let client = client();
        let gateway = gateway(client.clone(), Denyless);
        let mut handler = Box::pin(gateway.handle(request("/rpc/Run", Body::from("{}"))));
        let waker = Waker::noop();
        let mut context = Context::from_waker(waker);

        assert!(Future::poll(handler.as_mut(), &mut context).is_pending());
        assert_eq!(client.invoked.load(Ordering::SeqCst), 1);
        drop(handler);
        assert!(client.dropped.load(Ordering::SeqCst));
    }

    keylink::service! {
        service Cancellation;
        rpc Wait(token: u64) -> u64;
    }

    struct CancellationHandler {
        entered: tokio::sync::mpsc::UnboundedSender<()>,
        release: Arc<tokio::sync::Notify>,
    }

    impl cancellation::Handler for CancellationHandler {
        async fn wait(&self, _cx: &KeylinkContext, token: u64) -> u64 {
            self.entered.send(()).expect("test receiver stays open");
            self.release.notified().await;
            token
        }
    }

    #[derive(Clone)]
    struct TrackedClient {
        inner: cancellation::Client,
        dropped: tokio::sync::watch::Sender<bool>,
    }

    struct Tracked;

    impl ExternalService for Tracked {
        const SERVICE: &'static str = <cancellation::External as ExternalService>::SERVICE;
        const RPC_METHODS: &'static [&'static str] =
            <cancellation::External as ExternalService>::RPC_METHODS;
        type Client = TrackedClient;

        fn invoke<C: keylink::ServiceCodec>(
            client: &Self::Client,
            method: &str,
            body: &[u8],
            key: Option<keylink::IdempotencyKey>,
            codec: C,
        ) -> ExternalFuture<C> {
            let future = cancellation::External::invoke(&client.inner, method, body, key, codec);
            let dropped = client.dropped.clone();
            Box::pin(async move {
                struct SignalDrop {
                    signal: tokio::sync::watch::Sender<bool>,
                }
                impl Drop for SignalDrop {
                    fn drop(&mut self) {
                        let _ = self.signal.send(true);
                    }
                }
                let _signal = SignalDrop { signal: dropped };
                future.await
            })
        }
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
        let source = include_str!("gateway.rs");
        let production = source
            .split("#[cfg(test)]")
            .next()
            .expect("production precedes tests");
        assert!(production.contains("let invocation = S::invoke("));
        assert!(production.contains("match invocation.await"));
        assert!(!production.contains("tokio::spawn(invocation)"));

        let server_node = local_node().await;
        let client_node = local_node().await;
        let (entered_tx, mut entered_rx) = tokio::sync::mpsc::unbounded_channel();
        let release = Arc::new(tokio::sync::Notify::new());
        let _keylink = cancellation::serve(
            &server_node,
            CancellationHandler {
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
        let gateway = Gateway::<Tracked, _>::new(
            TrackedClient {
                inner: client,
                dropped: dropped_tx,
            },
            Denyless,
            Challenge::new(HeaderValue::from_static("Bearer realm=keylink"))
                .expect("valid challenge"),
        )
        .router::<()>();
        let app: Router<()> = Router::new()
            .nest("/v1", gateway)
            .layer(axum::middleware::from_fn(
                |mut request: Request<Body>, next: axum::middleware::Next| async move {
                    request
                        .extensions_mut()
                        .insert(Principal::new(b"tenant-a").expect("valid principal"));
                    next.run(request).await
                },
            ));
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

    #[derive(Clone)]
    struct Denyless;

    impl Access for Denyless {
        async fn authorize(&self, _call: Call<'_>) -> Result<(), AccessError> {
            Ok(())
        }
    }
}
