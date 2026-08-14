//! Immutable configured HTTP surface shared by runtime routing and documents.

use crate::{ConfigError, StreamingLimits};

/// Which generated service address space the HTTP surface selects.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum Addressing {
    /// The generated singleton client.
    Singleton,
    /// The generated keyed-actor client.
    Actors,
}

/// One validated immutable selection of runtime routes and public limits.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HttpSurface {
    prefix: String,
    addressing: Addressing,
    streaming: Option<StreamingLimits>,
}

impl HttpSurface {
    /// Build a surface with streaming routes disabled.
    ///
    /// An empty prefix installs routes at the application root. A non-empty prefix
    /// must be an absolute path without a query, fragment, or trailing slash.
    ///
    /// # Errors
    ///
    /// Returns [`ConfigError::InvalidSurfacePrefix`] for a non-canonical prefix.
    pub fn new(prefix: impl Into<String>, addressing: Addressing) -> Result<Self, ConfigError> {
        let prefix = prefix.into();
        let valid = prefix.is_empty()
            || (prefix.starts_with('/')
                && prefix != "/"
                && !prefix.ends_with('/')
                && !prefix.contains(['?', '#']));
        if !valid {
            return Err(ConfigError::InvalidSurfacePrefix);
        }
        Ok(Self {
            prefix,
            addressing,
            streaming: None,
        })
    }

    /// Enable the bounded streaming and blob route families.
    #[must_use]
    pub fn with_streaming(mut self, limits: StreamingLimits) -> Self {
        self.streaming = Some(limits);
        self
    }

    /// The configured application prefix, or empty for root routes.
    #[must_use]
    pub fn prefix(&self) -> &str {
        &self.prefix
    }

    /// The selected generated address space.
    #[must_use]
    pub const fn addressing(&self) -> Addressing {
        self.addressing
    }

    /// Explicit streaming limits, when streaming routes are enabled.
    #[must_use]
    pub const fn streaming(&self) -> Option<StreamingLimits> {
        self.streaming
    }
}

/// One runtime route family.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum RouteKind {
    /// Bounded JSON request and response.
    Rpc,
    /// Raw request stream with bounded JSON metadata.
    Send,
    /// Bounded JSON request with a raw response stream.
    Recv,
    /// Blob method invocation that creates a byte-resource ticket.
    BlobLookup,
    /// Blob byte-resource retrieval.
    BlobGet,
    /// Blob byte-resource metadata.
    BlobHead,
}

/// One schema-free route descriptor used by runtime routing and documents.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Route {
    kind: RouteKind,
    addressing: Addressing,
    template: &'static str,
    method: http::Method,
    success: http::StatusCode,
    request_media: Option<&'static str>,
    response_media: Option<&'static str>,
    streaming: bool,
}

impl Route {
    /// The route family.
    #[must_use]
    pub const fn kind(&self) -> RouteKind {
        self.kind
    }
    /// The address space selected by the route.
    #[must_use]
    pub const fn addressing(&self) -> Addressing {
        self.addressing
    }
    /// The Axum-compatible path template, relative to the configured prefix.
    #[must_use]
    pub const fn template(&self) -> &'static str {
        self.template
    }
    /// The one accepted HTTP method.
    #[must_use]
    pub fn method(&self) -> &http::Method {
        &self.method
    }
    /// The successful HTTP status.
    #[must_use]
    pub const fn success_status(&self) -> http::StatusCode {
        self.success
    }
    /// The required request media type, when the route has one.
    #[must_use]
    pub const fn request_media(&self) -> Option<&'static str> {
        self.request_media
    }
    /// The successful response media type, when the route has one.
    #[must_use]
    pub const fn response_media(&self) -> Option<&'static str> {
        self.response_media
    }
}

const ROUTES: &[Route] = &[
    Route {
        kind: RouteKind::Rpc,
        addressing: Addressing::Singleton,
        template: "/rpc/{method}",
        method: http::Method::POST,
        success: http::StatusCode::OK,
        request_media: Some("application/json"),
        response_media: Some("application/json"),
        streaming: false,
    },
    Route {
        kind: RouteKind::Send,
        addressing: Addressing::Singleton,
        template: "/send/{method}",
        method: http::Method::POST,
        success: http::StatusCode::OK,
        request_media: Some("application/octet-stream"),
        response_media: Some("application/json"),
        streaming: true,
    },
    Route {
        kind: RouteKind::Recv,
        addressing: Addressing::Singleton,
        template: "/recv/{method}",
        method: http::Method::POST,
        success: http::StatusCode::OK,
        request_media: Some("application/json"),
        // The declared return value travels in `Keylink-Result`, because the
        // body is the payload and a response has only one of those.
        response_media: Some("application/octet-stream"),
        streaming: true,
    },
    Route {
        kind: RouteKind::BlobLookup,
        addressing: Addressing::Singleton,
        template: "/blob/{method}",
        method: http::Method::POST,
        success: http::StatusCode::CREATED,
        request_media: Some("application/json"),
        response_media: Some("application/json"),
        streaming: true,
    },
    // One template, two entries. The verb is what decides whether a byte
    // reader is opened at all, so a route that answered to both would have no
    // single answer for `method()` and no single answer for what it does.
    Route {
        kind: RouteKind::BlobGet,
        addressing: Addressing::Singleton,
        template: "/blob/{method}/{id}",
        method: http::Method::GET,
        success: http::StatusCode::OK,
        // Not `application/json` and not absent-by-oversight: a byte route
        // accepts no request body of any type, which is a different fact from
        // "accepts any type".
        request_media: None,
        response_media: Some("application/octet-stream"),
        streaming: true,
    },
    Route {
        kind: RouteKind::BlobHead,
        addressing: Addressing::Singleton,
        template: "/blob/{method}/{id}",
        method: http::Method::HEAD,
        success: http::StatusCode::OK,
        request_media: None,
        response_media: Some("application/octet-stream"),
        streaming: true,
    },
    // The actor surface. The same families, addressed per actor, minus `send`:
    // a keyed upload would stream a body to an actor that four refusals can
    // reject after the bytes are on the wire, and no generated `ActorRef`
    // method exists for it either — so there is nothing here to route to.
    //
    // The actor segment LEADS. `/{actor}/rpc/{method}` cannot be shadowed by a
    // singleton prefix, and the two address spaces never share a router
    // because `HttpSurface` serves one or the other.
    Route {
        kind: RouteKind::Rpc,
        addressing: Addressing::Actors,
        template: "/{actor}/rpc/{method}",
        method: http::Method::POST,
        success: http::StatusCode::OK,
        request_media: Some("application/json"),
        response_media: Some("application/json"),
        streaming: false,
    },
    Route {
        kind: RouteKind::Recv,
        addressing: Addressing::Actors,
        template: "/{actor}/recv/{method}",
        method: http::Method::POST,
        success: http::StatusCode::OK,
        request_media: Some("application/json"),
        response_media: Some("application/octet-stream"),
        streaming: true,
    },
    Route {
        kind: RouteKind::BlobLookup,
        addressing: Addressing::Actors,
        template: "/{actor}/blob/{method}",
        method: http::Method::POST,
        success: http::StatusCode::CREATED,
        request_media: Some("application/json"),
        response_media: Some("application/json"),
        streaming: true,
    },
    Route {
        kind: RouteKind::BlobGet,
        addressing: Addressing::Actors,
        template: "/{actor}/blob/{method}/{id}",
        method: http::Method::GET,
        success: http::StatusCode::OK,
        request_media: None,
        response_media: Some("application/octet-stream"),
        streaming: true,
    },
    Route {
        kind: RouteKind::BlobHead,
        addressing: Addressing::Actors,
        template: "/{actor}/blob/{method}/{id}",
        method: http::Method::HEAD,
        success: http::StatusCode::OK,
        request_media: None,
        response_media: Some("application/octet-stream"),
        streaming: true,
    },
];

#[cfg(test)]
pub(crate) const fn singleton_rpc_route() -> &'static Route {
    &ROUTES[0]
}

impl HttpSurface {
    /// Iterate the routes implemented for this configured surface.
    #[doc(hidden)]
    pub fn routes(&self) -> impl Iterator<Item = &'static Route> + '_ {
        ROUTES.iter().filter(|route| {
            route.addressing == self.addressing && (!route.streaming || self.streaming.is_some())
        })
    }
}

/// One stable adapter-owned error category.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ErrorKind {
    /// Request JSON or a path segment is invalid.
    InvalidRequest,
    /// The external idempotency key is malformed.
    InvalidIdempotencyKey,
    /// The selected operation cannot promise idempotent replay.
    IdempotencyNotSupported,
    /// Streaming metadata is missing, malformed, noncanonical, or over limit.
    InvalidStreamMetadata,
    /// A Range or If-Range header was supplied where ranges are not supported.
    RangeNotSupported,
    /// A declared or observed upload exceeds its configured limit.
    RequestStreamTooLarge,
    /// A declared download exceeds its configured limit.
    ResponseStreamTooLarge,
    /// A verified principal is absent or no longer accepted.
    Unauthenticated,
    /// The verified principal may not invoke the operation.
    Forbidden,
    /// The generated method does not exist for this route family.
    MethodNotFound,
    /// The addressed actor does not exist.
    ActorNotFound,
    /// The HTTP method is not accepted by this route.
    MethodNotAllowed,
    /// The idempotency key cannot be replayed for this call.
    IdempotencyConflict,
    /// The bounded request exceeds its configured limit.
    RequestTooLarge,
    /// The required request media type is absent or invalid.
    UnsupportedMediaType,
    /// Upstream idempotency state has reached its finite capacity.
    IdempotencyCapacity,
    /// The upstream call failed without a more specific safe category.
    UpstreamError,
    /// The upstream service cannot currently be reached.
    UpstreamUnavailable,
    /// The upstream operation exceeded its deadline.
    UpstreamTimeout,
    /// A successful response exceeds its configured metadata limit.
    ResponseTooLarge,
    /// No byte resource of this identity is available to this caller.
    ///
    /// One shape for every reason: a guessed identity, another principal's,
    /// another method's, one evicted from the finite table, and one that never
    /// existed. Telling them apart is an existence oracle.
    BlobNotFound,
    /// The byte resource existed for this caller and the holder no longer
    /// serves it.
    BlobGone,
    /// A syntactically valid range that the representation cannot satisfy.
    RangeNotSatisfiable,
}

impl ErrorKind {
    /// Every stable adapter-owned error category.
    pub const ALL: &'static [Self] = &[
        Self::InvalidRequest,
        Self::InvalidIdempotencyKey,
        Self::IdempotencyNotSupported,
        Self::InvalidStreamMetadata,
        Self::RangeNotSupported,
        Self::RequestStreamTooLarge,
        Self::ResponseStreamTooLarge,
        Self::Unauthenticated,
        Self::Forbidden,
        Self::MethodNotFound,
        Self::ActorNotFound,
        Self::MethodNotAllowed,
        Self::IdempotencyConflict,
        Self::RequestTooLarge,
        Self::UnsupportedMediaType,
        Self::IdempotencyCapacity,
        Self::UpstreamError,
        Self::UpstreamUnavailable,
        Self::UpstreamTimeout,
        Self::ResponseTooLarge,
        Self::BlobNotFound,
        Self::BlobGone,
        Self::RangeNotSatisfiable,
    ];

    /// The stable HTTP representation of this category.
    #[must_use]
    pub const fn spec(self) -> &'static ErrorSpec {
        match self {
            Self::InvalidRequest => &INVALID_REQUEST,
            Self::InvalidIdempotencyKey => &INVALID_IDEMPOTENCY_KEY,
            Self::IdempotencyNotSupported => &IDEMPOTENCY_NOT_SUPPORTED,
            Self::InvalidStreamMetadata => &INVALID_STREAM_METADATA,
            Self::RangeNotSupported => &RANGE_NOT_SUPPORTED,
            Self::RequestStreamTooLarge => &REQUEST_STREAM_TOO_LARGE,
            Self::ResponseStreamTooLarge => &RESPONSE_STREAM_TOO_LARGE,
            Self::Unauthenticated => &UNAUTHENTICATED,
            Self::Forbidden => &FORBIDDEN,
            Self::MethodNotFound => &METHOD_NOT_FOUND,
            Self::ActorNotFound => &ACTOR_NOT_FOUND,
            Self::MethodNotAllowed => &METHOD_NOT_ALLOWED,
            Self::IdempotencyConflict => &IDEMPOTENCY_CONFLICT,
            Self::RequestTooLarge => &REQUEST_TOO_LARGE,
            Self::UnsupportedMediaType => &UNSUPPORTED_MEDIA_TYPE,
            Self::IdempotencyCapacity => &IDEMPOTENCY_CAPACITY,
            Self::UpstreamError => &UPSTREAM_ERROR,
            Self::UpstreamUnavailable => &UPSTREAM_UNAVAILABLE,
            Self::UpstreamTimeout => &UPSTREAM_TIMEOUT,
            Self::ResponseTooLarge => &RESPONSE_TOO_LARGE,
            Self::BlobNotFound => &BLOB_NOT_FOUND,
            Self::BlobGone => &BLOB_GONE,
            Self::RangeNotSatisfiable => &RANGE_NOT_SATISFIABLE,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ErrorSpec {
    kind: ErrorKind,
    status: http::StatusCode,
    code: &'static str,
    message: &'static str,
    allow: Option<&'static str>,
    challenge: bool,
}

impl ErrorSpec {
    /// The stable adapter error category.
    #[must_use]
    pub const fn kind(self) -> ErrorKind {
        self.kind
    }
    /// The HTTP response status.
    #[must_use]
    pub const fn status(self) -> http::StatusCode {
        self.status
    }
    /// The stable JSON error code.
    #[must_use]
    pub const fn code(self) -> &'static str {
        self.code
    }
    /// The stable redacted JSON error message.
    #[must_use]
    pub const fn message(self) -> &'static str {
        self.message
    }
    /// The `Allow` header value, when required.
    #[must_use]
    pub const fn allow(self) -> Option<&'static str> {
        self.allow
    }
    /// Whether this category can carry the configured authentication challenge.
    #[must_use]
    pub const fn challenge(self) -> bool {
        self.challenge
    }
}

macro_rules! error_spec {
    ($name:ident, $kind:ident, $status:ident, $code:literal, $message:literal) => {
        const $name: ErrorSpec = ErrorSpec {
            kind: ErrorKind::$kind,
            status: http::StatusCode::$status,
            code: $code,
            message: $message,
            allow: None,
            challenge: false,
        };
    };
}

error_spec!(
    INVALID_REQUEST,
    InvalidRequest,
    BAD_REQUEST,
    "invalid_request",
    "the request JSON or path is invalid"
);
error_spec!(
    INVALID_IDEMPOTENCY_KEY,
    InvalidIdempotencyKey,
    BAD_REQUEST,
    "invalid_idempotency_key",
    "the idempotency key is invalid"
);
error_spec!(
    IDEMPOTENCY_NOT_SUPPORTED,
    IdempotencyNotSupported,
    BAD_REQUEST,
    "idempotency_not_supported",
    "this method does not accept idempotency keys"
);
error_spec!(
    INVALID_STREAM_METADATA,
    InvalidStreamMetadata,
    BAD_REQUEST,
    "invalid_stream_metadata",
    "the streaming metadata header is invalid"
);
error_spec!(
    RANGE_NOT_SUPPORTED,
    RangeNotSupported,
    BAD_REQUEST,
    "range_not_supported",
    "this streaming route does not accept byte ranges"
);
error_spec!(
    REQUEST_STREAM_TOO_LARGE,
    RequestStreamTooLarge,
    PAYLOAD_TOO_LARGE,
    "request_stream_too_large",
    "the request stream exceeds its limit"
);
error_spec!(
    RESPONSE_STREAM_TOO_LARGE,
    ResponseStreamTooLarge,
    BAD_GATEWAY,
    "response_stream_too_large",
    "the response stream exceeds its limit"
);
const UNAUTHENTICATED: ErrorSpec = ErrorSpec {
    kind: ErrorKind::Unauthenticated,
    status: http::StatusCode::UNAUTHORIZED,
    code: "unauthenticated",
    message: "authentication is required",
    allow: None,
    challenge: true,
};
error_spec!(
    FORBIDDEN,
    Forbidden,
    FORBIDDEN,
    "forbidden",
    "this caller may not invoke this method"
);
error_spec!(
    METHOD_NOT_FOUND,
    MethodNotFound,
    NOT_FOUND,
    "method_not_found",
    "the RPC method was not found"
);
error_spec!(
    ACTOR_NOT_FOUND,
    ActorNotFound,
    NOT_FOUND,
    "actor_not_found",
    "the addressed actor was not found"
);
const METHOD_NOT_ALLOWED: ErrorSpec = ErrorSpec {
    kind: ErrorKind::MethodNotAllowed,
    status: http::StatusCode::METHOD_NOT_ALLOWED,
    code: "method_not_allowed",
    message: "this route accepts POST only",
    allow: Some("POST"),
    challenge: false,
};
error_spec!(
    BLOB_NOT_FOUND,
    BlobNotFound,
    NOT_FOUND,
    "blob_not_found",
    "no such byte resource"
);
error_spec!(
    BLOB_GONE,
    BlobGone,
    GONE,
    "blob_gone",
    "the byte resource is no longer available"
);
error_spec!(
    RANGE_NOT_SATISFIABLE,
    RangeNotSatisfiable,
    RANGE_NOT_SATISFIABLE,
    "range_not_satisfiable",
    "the requested range cannot be satisfied"
);
error_spec!(
    IDEMPOTENCY_CONFLICT,
    IdempotencyConflict,
    CONFLICT,
    "idempotency_conflict",
    "the idempotency key cannot be replayed"
);
error_spec!(
    REQUEST_TOO_LARGE,
    RequestTooLarge,
    PAYLOAD_TOO_LARGE,
    "request_too_large",
    "the request JSON exceeds its limit"
);
error_spec!(
    UNSUPPORTED_MEDIA_TYPE,
    UnsupportedMediaType,
    UNSUPPORTED_MEDIA_TYPE,
    "unsupported_media_type",
    "a JSON content type is required"
);
error_spec!(
    IDEMPOTENCY_CAPACITY,
    IdempotencyCapacity,
    TOO_MANY_REQUESTS,
    "idempotency_capacity",
    "the upstream idempotency capacity is exhausted"
);
error_spec!(
    UPSTREAM_ERROR,
    UpstreamError,
    BAD_GATEWAY,
    "upstream_error",
    "the upstream call failed"
);
error_spec!(
    UPSTREAM_UNAVAILABLE,
    UpstreamUnavailable,
    SERVICE_UNAVAILABLE,
    "upstream_unavailable",
    "the upstream service is unavailable"
);
error_spec!(
    UPSTREAM_TIMEOUT,
    UpstreamTimeout,
    GATEWAY_TIMEOUT,
    "upstream_timeout",
    "the upstream call timed out"
);
error_spec!(
    RESPONSE_TOO_LARGE,
    ResponseTooLarge,
    INTERNAL_SERVER_ERROR,
    "response_too_large",
    "the successful response JSON exceeds its limit"
);

#[cfg(test)]
mod tests {
    #![allow(clippy::expect_used)]

    use super::{Addressing, ErrorKind, HttpSurface, Route, RouteKind};

    /// One inventory, two address spaces, and a surface serves exactly one.
    ///
    /// Watched red with `scripts/break-once.sh` (row `HK27`) by replacing
    /// `route.addressing == self.addressing && (!route.streaming || self.streaming.is_some())`
    /// with `!route.streaming || self.streaming.is_some()`, which lets an
    /// actor route onto a singleton surface: `left: 2, right: 1`. A gateway
    /// that mounted both would answer `/rpc/{method}` and
    /// `/{actor}/rpc/{method}` from one router, with one `Access` policy that
    /// was written for whichever the deployment chose.
    #[test]
    fn configured_surface_filters_the_single_runtime_route_inventory() {
        let singleton =
            HttpSurface::new("/v1", Addressing::Singleton).expect("valid singleton surface");
        let routes: Vec<_> = singleton.routes().collect();
        assert_eq!(routes.len(), 1);
        assert_eq!(routes[0].kind(), RouteKind::Rpc);
        assert_eq!(routes[0].template(), "/rpc/{method}");
        assert_eq!(routes[0].request_media(), Some("application/json"));
        assert_eq!(routes[0].response_media(), Some("application/json"));

        let limits = crate::StreamingLimits::new(1, 1, 1, 1).expect("positive limits");
        let streaming_singleton = HttpSurface::new("/v1", Addressing::Singleton)
            .expect("valid singleton surface")
            .with_streaming(limits);
        assert_eq!(
            streaming_singleton
                .routes()
                .map(Route::kind)
                .collect::<Vec<_>>(),
            [
                RouteKind::Rpc,
                RouteKind::Send,
                RouteKind::Recv,
                RouteKind::BlobLookup,
                RouteKind::BlobGet,
                RouteKind::BlobHead,
            ]
        );
        // The two byte routes share one template and differ only in verb, so
        // one `Route` per verb is what keeps `method()` a single answer.
        let blob: Vec<_> = streaming_singleton
            .routes()
            .filter(|route| route.template() == "/blob/{method}/{id}")
            .collect();
        assert_eq!(blob.len(), 2);
        assert_eq!(blob[0].method(), http::Method::GET);
        assert_eq!(blob[1].method(), http::Method::HEAD);
        assert_eq!(
            blob[0].request_media(),
            None,
            "a byte route takes no request body at all"
        );
        // The actor surface is the singleton one addressed per actor, minus
        // `send`: a keyed upload would stream a body to an actor that four
        // refusals can reject after the bytes are on the wire, and there is no
        // generated `ActorRef` method for it either.
        let actors_only = HttpSurface::new("/v1", Addressing::Actors).expect("valid actor surface");
        assert_eq!(
            actors_only.routes().map(Route::kind).collect::<Vec<_>>(),
            [RouteKind::Rpc],
            "without streaming limits, an actor surface serves rpc alone"
        );
        assert_eq!(
            actors_only.routes().next().map(Route::template),
            Some("/{actor}/rpc/{method}")
        );

        let streaming_actors = HttpSurface::new("/v1", Addressing::Actors)
            .expect("valid actor surface")
            .with_streaming(limits);
        assert_eq!(
            streaming_actors
                .routes()
                .map(Route::kind)
                .collect::<Vec<_>>(),
            [
                RouteKind::Rpc,
                RouteKind::Recv,
                RouteKind::BlobLookup,
                RouteKind::BlobGet,
                RouteKind::BlobHead,
            ],
            "and no `Send` route exists on an actor surface at all"
        );
        assert_eq!(
            streaming_actors
                .routes()
                .map(Route::template)
                .collect::<Vec<_>>(),
            [
                "/{actor}/rpc/{method}",
                "/{actor}/recv/{method}",
                "/{actor}/blob/{method}",
                "/{actor}/blob/{method}/{id}",
                "/{actor}/blob/{method}/{id}",
            ],
            "the actor segment leads, so one prefix cannot shadow another"
        );
    }

    #[test]
    fn every_error_kind_resolves_through_one_stable_inventory() {
        for kind in ErrorKind::ALL {
            let spec = kind.spec();
            assert_eq!(spec.kind(), *kind);
            assert!(!spec.code().is_empty());
            assert!(!spec.message().is_empty());
        }
    }
}
