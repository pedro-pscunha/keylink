//! Ordered state-generic Axum routing.

use std::{error::Error as _, marker::PhantomData, sync::Arc};

use axum::{body::Body, routing::any, Router};
use http::{header, HeaderMap, Request, Response};
use keylink::ExternalService;

use crate::{
    __contract::{Route, RouteKind},
    error::GatewayError,
    service::JsonCodec,
    Access, AccessError, Addressing, Call, Challenge, ConfigError, HttpSurface, JsonLimits,
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

    /// Build the configured absolute routes for merging into an application router.
    ///
    /// # Errors
    ///
    /// Returns [`ConfigError::WrongAddressing`] when this singleton gateway receives
    /// an actor surface, or [`ConfigError::MissingRuntimeRoute`] when the selected
    /// surface has no implemented singleton RPC route.
    pub fn router<State>(self, surface: HttpSurface) -> Result<Router<State>, ConfigError>
    where
        State: Clone + Send + Sync + 'static,
    {
        if surface.addressing() != Addressing::Singleton {
            return Err(ConfigError::WrongAddressing);
        }
        let Some(rpc) = surface
            .routes()
            .find(|route| route.kind() == RouteKind::Rpc)
        else {
            return Err(ConfigError::MissingRuntimeRoute);
        };
        let rpc_path = format!("{}{}", surface.prefix(), rpc.template());
        let handler_path = rpc_path.clone();
        let gateway = Arc::new(self);
        let rpc_gateway = Arc::clone(&gateway);
        let mut router = Router::new().route(
            &rpc_path,
            any(move |request: Request<Body>| {
                let gateway = Arc::clone(&rpc_gateway);
                let path = handler_path.clone();
                async move { gateway.handle_rpc(request, rpc, &path).await }
            }),
        );
        if let (Some(limits), Some(send)) = (
            surface.streaming(),
            surface
                .routes()
                .find(|route| route.kind() == RouteKind::Send),
        ) {
            let path = format!("{}{}", surface.prefix(), send.template());
            let handler_path = path.clone();
            router = router.route(
                &path,
                any(move |request: Request<Body>| {
                    let gateway = Arc::clone(&gateway);
                    let path = handler_path.clone();
                    async move { gateway.handle_send(request, send, &path, limits).await }
                }),
            );
        }
        Ok(router.fallback(|| async { GatewayError::MethodNotFound.response(None) }))
    }

    async fn handle_rpc(
        &self,
        request: Request<Body>,
        route: &'static Route,
        route_path: &str,
    ) -> Response<Body> {
        let decoded = match decode_method::<S>(
            request.uri().path(),
            route_path,
            keylink::ExternalKind::Rpc,
        ) {
            Ok(method) => method,
            Err(error) => return error.response(None),
        };
        let Some(method) = S::METHODS
            .iter()
            .copied()
            .find(|candidate| {
                candidate.name() == decoded && candidate.kind() == keylink::ExternalKind::Rpc
            })
            .map(keylink::ExternalMethod::name)
        else {
            return GatewayError::MethodNotFound.response(None);
        };
        if request.method() != route.method() {
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
        if !is_media(request.headers(), route.request_media()) {
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
            keylink::ExternalCall::Rpc { key },
            JsonCodec::new(self.limits.response.get()),
        );
        match invocation.await {
            Ok(keylink::ExternalOutput::Json(json)) => json_response(json, route),
            Ok(_) => GatewayError::UpstreamError.response(None),
            Err(error) => GatewayError::from_external(error).response(None),
        }
    }

    async fn handle_send(
        &self,
        request: Request<Body>,
        route: &'static Route,
        route_path: &str,
        limits: crate::StreamingLimits,
    ) -> Response<Body> {
        let decoded = match decode_method::<S>(
            request.uri().path(),
            route_path,
            keylink::ExternalKind::Send,
        ) {
            Ok(method) => method,
            Err(error) => return error.response(None),
        };
        let Some(method) = S::METHODS
            .iter()
            .copied()
            .find(|candidate| {
                candidate.name() == decoded && candidate.kind() == keylink::ExternalKind::Send
            })
            .map(keylink::ExternalMethod::name)
        else {
            return GatewayError::MethodNotFound.response(None);
        };
        if request.method() != route.method() {
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
        if request.headers().get(IDEMPOTENCY_HEADER).is_some() {
            return GatewayError::IdempotencyNotSupported.response(None);
        }
        if request.headers().get(header::RANGE).is_some()
            || request.headers().get(header::IF_RANGE).is_some()
        {
            return GatewayError::RangeNotSupported.response(None);
        }
        if !crate::stream::is_octet_stream(request.headers()) {
            return GatewayError::UnsupportedMediaType.response(None);
        }
        let arguments =
            match crate::stream::arguments(request.headers(), limits.metadata_json_bytes()) {
                Ok(arguments) => arguments,
                Err(_) => return GatewayError::InvalidStreamMetadata.response(None),
            };
        let declared_len = request
            .headers()
            .get(header::CONTENT_LENGTH)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.parse::<u64>().ok());
        if declared_len.is_some_and(|len| len > limits.max_upload_bytes()) {
            return GatewayError::RequestStreamTooLarge.response(None);
        }
        let (_, body) = request.into_parts();
        let invocation = S::invoke(
            &self.client,
            method,
            &arguments,
            keylink::ExternalCall::Send {
                body: crate::stream::upload_body(body, declared_len, limits.max_upload_bytes()),
            },
            JsonCodec::new(self.limits.response.get()),
        );
        match invocation.await {
            Ok(keylink::ExternalOutput::Json(json)) => json_response(json, route),
            Ok(_) => GatewayError::UpstreamError.response(None),
            Err(error) => GatewayError::from_external(error).response(None),
        }
    }
}

#[cfg(test)]
const SINGLETON_RPC_PATH: &str = "/rpc/{method}";

#[cfg(test)]
fn singleton_rpc_route() -> &'static Route {
    crate::__contract::singleton_rpc_route()
}

fn json_response(json: Vec<u8>, route: &Route) -> Response<Body> {
    let mut response = Response::new(Body::from(json));
    *response.status_mut() = route.success_status();
    if let Some(media) = route.response_media() {
        response
            .headers_mut()
            .insert(header::CONTENT_TYPE, http::HeaderValue::from_static(media));
    }
    response
}

fn decode_method<S: ExternalService>(
    path: &str,
    route_path: &str,
    kind: keylink::ExternalKind,
) -> Result<String, GatewayError> {
    let prefix = route_path
        .strip_suffix("{method}")
        .ok_or(GatewayError::MethodNotFound)?;
    let encoded = path
        .strip_prefix(prefix)
        .ok_or(GatewayError::MethodNotFound)?;
    if encoded.is_empty() || encoded.contains('/') {
        return Err(GatewayError::MethodNotFound);
    }
    let max_bytes = S::METHODS
        .iter()
        .filter(|method| method.kind() == kind)
        .map(|method| method.name().len())
        .max()
        .unwrap_or(0);
    crate::raw_path::parse(encoded, crate::raw_path::Shape::Segment { max_bytes })
        .map_err(|_| GatewayError::InvalidRequest)
}

fn is_media(headers: &HeaderMap, expected: Option<&str>) -> bool {
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
    match expected {
        Some("application/json") => {
            media.type_() == mime::APPLICATION
                && (media.subtype() == mime::JSON || media.suffix() == Some(mime::JSON))
        }
        Some("application/octet-stream") => {
            media.type_() == mime::APPLICATION
                && media.subtype() == mime::OCTET_STREAM
                && media.params().next().is_none()
        }
        _ => false,
    }
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

    use std::sync::atomic::AtomicU64;
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
        const METHODS: &'static [keylink::ExternalMethod] = &[keylink::ExternalMethod::new(
            "Run",
            keylink::ExternalKind::Rpc,
        )];
        type Client = TestClient;

        fn invoke<C: keylink::ServiceCodec>(
            client: &Self::Client,
            _method: &str,
            _body: &[u8],
            _call: keylink::ExternalCall,
            codec: C,
        ) -> ExternalFuture<C> {
            client.invoked.fetch_add(1, Ordering::SeqCst);
            let sentinel = DropSentinel(Arc::clone(&client.dropped));
            if client.pending {
                Box::pin(async move {
                    let _sentinel = sentinel;
                    pending::<Result<keylink::ExternalOutput<C::Output>, ExternalError<C::Error>>>()
                        .await
                })
            } else {
                drop(sentinel);
                Box::pin(async move {
                    codec
                        .encode(&())
                        .map(keylink::ExternalOutput::Json)
                        .map_err(ExternalError::Codec)
                })
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
    fn singleton_gateway_rejects_an_actor_surface() {
        let surface =
            crate::HttpSurface::new("/v1", crate::Addressing::Actors).expect("valid actor surface");
        let error = gateway(ready_client(), Denyless)
            .router::<()>(surface)
            .expect_err("singleton client cannot serve actors");
        assert_eq!(error, crate::ConfigError::WrongAddressing);
    }

    #[test]
    fn malformed_percent_encoding_is_an_invalid_adapter_request() {
        assert!(matches!(
            super::decode_method::<TestService>(
                "/rpc/%ZZ",
                super::SINGLETON_RPC_PATH,
                keylink::ExternalKind::Rpc,
            ),
            Err(crate::error::GatewayError::InvalidRequest)
        ));
        assert!(matches!(
            super::decode_method::<TestService>(
                "/rpc/%FF",
                super::SINGLETON_RPC_PATH,
                keylink::ExternalKind::Rpc,
            ),
            Err(crate::error::GatewayError::InvalidRequest)
        ));
    }

    #[derive(Clone)]
    struct SendClient {
        invoked: Arc<Mutex<Vec<keylink::ExternalKind>>>,
    }

    struct SendService;

    impl ExternalService for SendService {
        const SERVICE: &'static str = "SendService";
        const METHODS: &'static [keylink::ExternalMethod] = &[
            keylink::ExternalMethod::new("Run", keylink::ExternalKind::Rpc),
            keylink::ExternalMethod::new("Upload", keylink::ExternalKind::Send),
        ];
        type Client = SendClient;

        fn invoke<C: keylink::ServiceCodec>(
            client: &Self::Client,
            _method: &str,
            _arguments: &[u8],
            call: keylink::ExternalCall,
            codec: C,
        ) -> ExternalFuture<C> {
            client.invoked.lock().expect("test mutex").push(call.kind());
            Box::pin(async move {
                codec
                    .encode(&7_u64)
                    .map(keylink::ExternalOutput::Json)
                    .map_err(ExternalError::Codec)
            })
        }
    }

    fn send_gateway(
        invoked: Arc<Mutex<Vec<keylink::ExternalKind>>>,
    ) -> Gateway<SendService, Denyless> {
        Gateway::<SendService, _>::new(
            SendClient { invoked },
            Denyless,
            Challenge::new(HeaderValue::from_static("Bearer realm=keylink"))
                .expect("valid challenge"),
        )
        .limits(crate::JsonLimits::new(32, 32).expect("positive JSON limits"))
    }

    /// One refusal case: content type, arguments header, one extra header,
    /// declared `Content-Length`, and the status all four must produce.
    type Refusal = (
        Option<&'static str>,
        Option<&'static str>,
        Option<(&'static str, &'static str)>,
        Option<&'static str>,
        http::StatusCode,
    );

    /// Everything a `send` refuses on its headers alone, and the one property
    /// they share: the upload never starts. Refusing after the body has been
    /// read costs the bytes it was refusing.
    async fn refused_before_any_poll(cases: &[Refusal]) {
        for &(media, arguments, extra, length, status) in cases {
            let invoked = Arc::new(Mutex::new(Vec::new()));
            let polls = Arc::new(AtomicUsize::new(0));
            let limits = crate::StreamingLimits::new(2, 4, 4, 1).expect("positive limits");
            let route = crate::HttpSurface::new("", crate::Addressing::Singleton)
                .expect("valid surface")
                .with_streaming(limits)
                .routes()
                .find(|route| route.kind() == crate::__contract::RouteKind::Send)
                .expect("send route");
            let mut request = Request::builder()
                .method("POST")
                .uri("/send/Upload")
                .extension(Principal::new(b"tenant-a").expect("valid principal"));
            if let Some(media) = media {
                request = request.header("content-type", media);
            }
            if let Some(arguments) = arguments {
                request = request.header("keylink-arguments", arguments);
            }
            if let Some((name, value)) = extra {
                request = request.header(name, value);
            }
            if let Some(length) = length {
                request = request.header("content-length", length);
            }
            let response = send_gateway(Arc::clone(&invoked))
                .handle_send(
                    request
                        .body(Body::new(RecordingBody::new(Arc::clone(&polls))))
                        .expect("request"),
                    route,
                    "/send/{method}",
                    limits,
                )
                .await;
            assert_eq!(
                response.status(),
                status,
                "{media:?} {arguments:?} {extra:?} {length:?}"
            );
            assert_eq!(polls.load(Ordering::SeqCst), 0);
            assert!(invoked.lock().expect("test mutex").is_empty());
        }
    }

    #[tokio::test]
    async fn send_rejects_malformed_metadata_before_body_poll() {
        refused_before_any_poll(&[
            (
                None,
                None,
                None,
                None,
                http::StatusCode::UNSUPPORTED_MEDIA_TYPE,
            ),
            (
                Some("application/json"),
                Some("e30"),
                None,
                None,
                http::StatusCode::UNSUPPORTED_MEDIA_TYPE,
            ),
            (
                Some("application/octet-stream"),
                None,
                None,
                None,
                http::StatusCode::BAD_REQUEST,
            ),
            (
                Some("application/octet-stream"),
                Some("e30="),
                None,
                None,
                http::StatusCode::BAD_REQUEST,
            ),
        ])
        .await;
    }

    /// A `send` has no reply to replay and no range to serve, so both headers
    /// are refused rather than quietly ignored — and refused before the upload
    /// they would otherwise have consumed.
    #[tokio::test]
    async fn send_rejects_idempotency_and_range_before_body_poll() {
        refused_before_any_poll(&[
            (
                Some("application/octet-stream"),
                Some("e30"),
                Some(("idempotency-key", "x")),
                None,
                http::StatusCode::BAD_REQUEST,
            ),
            (
                Some("application/octet-stream"),
                Some("e30"),
                Some(("range", "bytes=0-1")),
                None,
                http::StatusCode::BAD_REQUEST,
            ),
            (
                Some("application/octet-stream"),
                Some("e30"),
                Some(("if-range", "\"etag\"")),
                None,
                http::StatusCode::BAD_REQUEST,
            ),
        ])
        .await;
    }

    /// A sender that announces more than the ceiling is refused on the
    /// announcement. Discovering it byte by byte would mean accepting the bytes.
    #[tokio::test]
    async fn send_rejects_declared_oversize_before_body_poll() {
        refused_before_any_poll(&[(
            Some("application/octet-stream"),
            Some("e30"),
            None,
            Some("5"),
            http::StatusCode::PAYLOAD_TOO_LARGE,
        )])
        .await;
    }

    #[tokio::test]
    async fn send_route_selects_only_enabled_send_methods() {
        let invoked = Arc::new(Mutex::new(Vec::new()));
        let gateway = Gateway::<SendService, _>::new(
            SendClient {
                invoked: Arc::clone(&invoked),
            },
            Denyless,
            Challenge::new(HeaderValue::from_static("Bearer realm=keylink"))
                .expect("valid challenge"),
        );
        let limits = crate::StreamingLimits::new(32, 32, 32, 1).expect("positive limits");
        let router = gateway
            .router::<()>(
                crate::HttpSurface::new("", crate::Addressing::Singleton)
                    .expect("valid surface")
                    .with_streaming(limits),
            )
            .expect("surface builds");
        let app = router.layer(axum::middleware::from_fn(
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
        let client = reqwest::Client::new();

        let upload = client
            .post(format!("http://{address}/send/Upload"))
            .header("content-type", "application/octet-stream")
            .header("keylink-arguments", "e30")
            .body("bytes")
            .send()
            .await
            .expect("HTTP response");
        assert_eq!(upload.status(), http::StatusCode::OK);
        assert_eq!(
            *invoked.lock().expect("test mutex"),
            [keylink::ExternalKind::Send]
        );

        let wrong_kind = client
            .post(format!("http://{address}/send/Run"))
            .header("content-type", "application/octet-stream")
            .header("keylink-arguments", "e30")
            .body("bytes")
            .send()
            .await
            .expect("HTTP response");
        assert_eq!(wrong_kind.status(), http::StatusCode::NOT_FOUND);
        assert_eq!(invoked.lock().expect("test mutex").len(), 1);
        server.abort();
    }

    #[derive(Clone)]
    struct WrongOutputClient(Arc<Mutex<Option<keylink::BlobHandle>>>);

    struct WrongOutputService;

    impl ExternalService for WrongOutputService {
        const SERVICE: &'static str = "WrongOutput";
        const METHODS: &'static [keylink::ExternalMethod] = &[keylink::ExternalMethod::new(
            "Run",
            keylink::ExternalKind::Rpc,
        )];
        type Client = WrongOutputClient;

        fn invoke<C: keylink::ServiceCodec>(
            client: &Self::Client,
            _method: &str,
            _body: &[u8],
            _call: keylink::ExternalCall,
            codec: C,
        ) -> ExternalFuture<C> {
            let handle = client
                .0
                .lock()
                .expect("test mutex")
                .take()
                .expect("one call");
            Box::pin(async move {
                let value = codec.encode(&()).map_err(ExternalError::Codec)?;
                Ok(keylink::ExternalOutput::Blob { value, handle })
            })
        }
    }

    #[tokio::test]
    async fn unary_gateway_rejects_the_wrong_generated_output_variant() {
        let node = local_node().await;
        let blobs = keylink::Blobs::memory();
        let id = blobs
            .add_bytes(b"wrong-output".as_slice())
            .await
            .expect("blob stores");
        let client = WrongOutputClient(Arc::new(Mutex::new(Some(keylink::BlobHandle::new(
            &node,
            node.endpoint().addr(),
            id,
        )))));
        let gateway = Gateway::<WrongOutputService, _>::new(
            client,
            Denyless,
            Challenge::new(HeaderValue::from_static("Bearer realm=keylink"))
                .expect("valid challenge"),
        );
        let response = gateway
            .handle_rpc(
                request("/rpc/Run", Body::from("{}")),
                super::singleton_rpc_route(),
                super::SINGLETON_RPC_PATH,
            )
            .await;

        assert_eq!(response.status(), http::StatusCode::BAD_GATEWAY);
    }

    #[tokio::test]
    async fn singleton_routes_use_bounded_raw_parser() {
        let client = client();
        let polls = Arc::new(AtomicUsize::new(0));
        let response = gateway(client.clone(), PanicAccess)
            .handle_rpc(
                request(
                    "/rpc/%61%61%61%61",
                    Body::new(RecordingBody::new(Arc::clone(&polls))),
                ),
                super::singleton_rpc_route(),
                super::SINGLETON_RPC_PATH,
            )
            .await;

        assert_eq!(response.status(), http::StatusCode::BAD_REQUEST);
        assert_eq!(polls.load(Ordering::SeqCst), 0);
        assert_eq!(client.invoked.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn unknown_method_never_polls_the_body_or_dispatches() {
        let client = client();
        let polls = Arc::new(AtomicUsize::new(0));
        let response = gateway(client.clone(), PanicAccess)
            .handle_rpc(
                request(
                    "/rpc/Bad",
                    Body::new(RecordingBody::new(Arc::clone(&polls))),
                ),
                super::singleton_rpc_route(),
                super::SINGLETON_RPC_PATH,
            )
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
            .handle_rpc(
                request(
                    "/rpc/Run",
                    Body::new(RecordingBody::new(Arc::clone(&polls))),
                ),
                super::singleton_rpc_route(),
                super::SINGLETON_RPC_PATH,
            )
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

        let response = gateway(client.clone(), Denyless)
            .handle_rpc(
                request,
                super::singleton_rpc_route(),
                super::SINGLETON_RPC_PATH,
            )
            .await;
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

            let response = gateway(client.clone(), Denyless)
                .handle_rpc(
                    request,
                    super::singleton_rpc_route(),
                    super::SINGLETON_RPC_PATH,
                )
                .await;
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
            .handle_rpc(
                request("/rpc/Run", Body::new(ErrorBody)),
                super::singleton_rpc_route(),
                super::SINGLETON_RPC_PATH,
            )
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

        let response = gateway(client.clone(), Denyless)
            .handle_rpc(
                request,
                super::singleton_rpc_route(),
                super::SINGLETON_RPC_PATH,
            )
            .await;

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
            .handle_rpc(
                request,
                super::singleton_rpc_route(),
                super::SINGLETON_RPC_PATH,
            )
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
        let mut first = Box::pin(gateway.handle_rpc(
            request("/rpc/Run", Body::from("{}")),
            super::singleton_rpc_route(),
            super::SINGLETON_RPC_PATH,
        ));
        let mut second = Box::pin(gateway.handle_rpc(
            request("/rpc/Run", Body::from("{}")),
            super::singleton_rpc_route(),
            super::SINGLETON_RPC_PATH,
        ));
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
        let mut handler = Box::pin(gateway.handle_rpc(
            request("/rpc/Run", Body::from("{}")),
            super::singleton_rpc_route(),
            super::SINGLETON_RPC_PATH,
        ));
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
        send Upload(prefix: u64) -> u64;
    }

    /// The serving endpoint's per-stream receive window for the backpressure
    /// test. Pinned rather than inherited, so the bound below is measured
    /// against a number this file owns instead of a dependency's default.
    const RECEIVE_WINDOW: u32 = 16 * 1024;

    /// An upload the gateway polls directly, counting what it hands over.
    ///
    /// Directly is the point. Behind a real socket this same measurement reads
    /// the kernel's buffers rather than the adapter's behaviour, and a megabyte
    /// disappears into them before Keylink is involved at all.
    struct CountingBody {
        chunks: u64,
        chunk_len: usize,
        produced: Arc<AtomicU64>,
        first: Option<tokio::sync::oneshot::Sender<()>>,
    }

    impl HttpBody for CountingBody {
        type Data = Bytes;
        type Error = Infallible;

        fn poll_frame(
            mut self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
        ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
            if self.chunks == 0 {
                return Poll::Ready(None);
            }
            self.chunks -= 1;
            let chunk_len = self.chunk_len;
            self.produced.fetch_add(chunk_len as u64, Ordering::SeqCst);
            if let Some(first) = self.first.take() {
                let _ = first.send(());
            }
            Poll::Ready(Some(Ok(Frame::data(Bytes::from(vec![b'k'; chunk_len])))))
        }
    }

    /// The HTTP body is pulled by the Keylink writer, not drained into it. While
    /// the receiver reads nothing its flow-control window closes, the gateway's
    /// write parks, and the body stops being polled — so peak memory is a
    /// bounded amount of transport buffer however long the upload is.
    ///
    /// # Why this counts bytes rather than polls
    ///
    /// The first shape of this test asserted "no second poll after the first"
    /// and was wrong: keylink's frame reader pulls a whole transport read to
    /// find the request header, so a receiver legitimately absorbs more than one
    /// chunk before the handler asks for anything. The window is not the only
    /// slack, and pinning the assertion to one chunk pinned it to an accident.
    ///
    /// So the claim is measured as what it is: an upper bound on what a
    /// *non-reading* receiver can pull through. Half the offered payload is
    /// eight times the slack expected here and half of what an adapter that
    /// drained the body would show, so nothing in between is a near miss. Load
    /// can only make the body produce less, so the bound cannot redden under
    /// load — only a real regression moves it.
    ///
    /// Watched red with `scripts/break-once.sh` (row `HJ11`) by replacing
    /// `tokio::io::copy_buf(&mut reader, stream)
    /// .await
    /// .map(drop)
    /// .map_err(Error::from)`
    /// in `keylink/src/body.rs` with one that drains the reader into a `Vec`
    /// and writes that afterwards, which is the regression this bound exists
    /// for. The exact test terminated with `pulled 1048576 of 1048576 offered
    /// bytes` — every byte pulled through, with the receiver reading none.
    #[tokio::test]
    async fn send_backpressure_stops_http_body_polling() {
        let chunks = 64_u64;
        let offered = u64::from(RECEIVE_WINDOW) * chunks;
        let limits = crate::StreamingLimits::new(32, offered, 64, 1).expect("positive limits");
        let route = crate::HttpSurface::new("", crate::Addressing::Singleton)
            .expect("valid surface")
            .with_streaming(limits)
            .routes()
            .find(|route| route.kind() == crate::__contract::RouteKind::Send)
            .expect("send route");

        let server_node = Node::from_endpoint(
            Endpoint::builder(presets::Minimal)
                .transport_config(
                    iroh::endpoint::QuicTransportConfig::builder()
                        .stream_receive_window(RECEIVE_WINDOW.into())
                        .build(),
                )
                .bind()
                .await
                .expect("a local endpoint binds"),
        );
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
        let gateway = Gateway::<cancellation::External, _>::new(
            client,
            Denyless,
            Challenge::new(HeaderValue::from_static("Bearer realm=keylink"))
                .expect("valid challenge"),
        );

        let produced = Arc::new(AtomicU64::new(0));
        let (first_tx, first) = tokio::sync::oneshot::channel();
        let request = Request::builder()
            .method("POST")
            .uri("/send/Upload")
            .header("content-type", "application/octet-stream")
            .header("keylink-arguments", {
                use base64::Engine as _;

                base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(r#"{"prefix":3}"#)
            })
            .extension(Principal::new(b"tenant-a").expect("valid principal"))
            .body(Body::new(CountingBody {
                chunks,
                chunk_len: RECEIVE_WINDOW as usize,
                produced: Arc::clone(&produced),
                first: Some(first_tx),
            }))
            .expect("request");

        let responding = gateway.handle_send(request, route, "/send/{method}", limits);
        let watching = async {
            entered_rx.recv().await.expect("the handler is dispatched");
            first.await.expect("the body is polled at least once");
            tokio::task::yield_now().await;
            let stalled = produced.load(Ordering::SeqCst);
            assert!(
                stalled <= offered / 2,
                "a receiver that reads nothing pulled {stalled} of {offered} offered bytes"
            );
            release.notify_waiters();
            stalled
        };
        let (response, stalled) = tokio::join!(responding, watching);

        assert_eq!(response.status(), http::StatusCode::OK);
        assert_eq!(
            produced.load(Ordering::SeqCst),
            offered,
            "and once the receiver reads, the whole upload still arrives"
        );
        assert!(
            stalled < offered,
            "a reader that resumes is what let the rest through"
        );
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

        /// Reports that it holds the request, then reads nothing until released.
        /// That gap is the window the backpressure test measures across.
        async fn upload(
            &self,
            _cx: &KeylinkContext,
            prefix: u64,
            body: keylink::BodyReader,
        ) -> u64 {
            self.entered.send(()).expect("test receiver stays open");
            self.release.notified().await;
            prefix + body.drain().await.expect("the upload arrives whole")
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
        const METHODS: &'static [keylink::ExternalMethod] =
            <cancellation::External as ExternalService>::METHODS;
        type Client = TrackedClient;

        fn invoke<C: keylink::ServiceCodec>(
            client: &Self::Client,
            method: &str,
            body: &[u8],
            call: keylink::ExternalCall,
            codec: C,
        ) -> ExternalFuture<C> {
            let future = cancellation::External::invoke(&client.inner, method, body, call, codec);
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
        .router::<()>(
            crate::HttpSurface::new("/v1", crate::Addressing::Singleton)
                .expect("valid singleton surface"),
        )
        .expect("surface matches singleton gateway");
        let app: Router<()> = Router::new()
            .merge(gateway)
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
