//! Adapter-owned configuration and HTTP failures.

/// Invalid gateway configuration.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
#[non_exhaustive]
pub enum ConfigError {
    /// A principal namespace was empty.
    #[error("principal namespace must not be empty")]
    EmptyPrincipal,
    /// A principal namespace exceeded the finite cap.
    #[error("principal namespace exceeds the {max} byte cap")]
    PrincipalTooLong {
        /// Maximum accepted principal namespace length in bytes.
        max: usize,
    },
    /// The request JSON limit was zero.
    #[error("request JSON limit must be positive")]
    ZeroRequestLimit,
    /// The successful response JSON limit was zero.
    #[error("response JSON limit must be positive")]
    ZeroResponseLimit,
    /// The authentication challenge was empty or not a legal header value.
    #[error("authentication challenge must be a non-empty header value")]
    InvalidChallenge,
}

use axum::{body::Body, response::Response};
use http::{header, HeaderValue, StatusCode};
use serde::Serialize;

use crate::JsonError;

const MAX_ERROR_BYTES: usize = 512;
const FALLBACK_ERROR: &[u8] =
    br#"{"error":{"code":"upstream_error","message":"the upstream call failed"}}"#;

#[derive(Clone, Copy)]
pub(crate) enum GatewayError {
    InvalidRequest,
    InvalidIdempotencyKey,
    IdempotencyNotSupported,
    Unauthenticated,
    Forbidden,
    MethodNotFound,
    ActorNotFound,
    MethodNotAllowed,
    IdempotencyConflict,
    RequestTooLarge,
    UnsupportedMediaType,
    IdempotencyCapacity,
    UpstreamError,
    UpstreamUnavailable,
    UpstreamTimeout,
    ResponseTooLarge,
}

#[derive(Serialize)]
struct ErrorEnvelope {
    error: ErrorBody,
}

#[derive(Serialize)]
struct ErrorBody {
    code: &'static str,
    message: &'static str,
}

impl GatewayError {
    fn status(self) -> StatusCode {
        match self {
            Self::InvalidRequest | Self::InvalidIdempotencyKey | Self::IdempotencyNotSupported => {
                StatusCode::BAD_REQUEST
            }
            Self::Unauthenticated => StatusCode::UNAUTHORIZED,
            Self::Forbidden => StatusCode::FORBIDDEN,
            Self::MethodNotFound | Self::ActorNotFound => StatusCode::NOT_FOUND,
            Self::MethodNotAllowed => StatusCode::METHOD_NOT_ALLOWED,
            Self::IdempotencyConflict => StatusCode::CONFLICT,
            Self::RequestTooLarge => StatusCode::PAYLOAD_TOO_LARGE,
            Self::UnsupportedMediaType => StatusCode::UNSUPPORTED_MEDIA_TYPE,
            Self::IdempotencyCapacity => StatusCode::TOO_MANY_REQUESTS,
            Self::UpstreamError => StatusCode::BAD_GATEWAY,
            Self::ResponseTooLarge => StatusCode::INTERNAL_SERVER_ERROR,
            Self::UpstreamUnavailable => StatusCode::SERVICE_UNAVAILABLE,
            Self::UpstreamTimeout => StatusCode::GATEWAY_TIMEOUT,
        }
    }

    fn body(self) -> ErrorBody {
        match self {
            Self::InvalidRequest => ErrorBody {
                code: "invalid_request",
                message: "the request JSON or path is invalid",
            },
            Self::InvalidIdempotencyKey => ErrorBody {
                code: "invalid_idempotency_key",
                message: "the idempotency key is invalid",
            },
            Self::IdempotencyNotSupported => ErrorBody {
                code: "idempotency_not_supported",
                message: "this method does not accept idempotency keys",
            },
            Self::Unauthenticated => ErrorBody {
                code: "unauthenticated",
                message: "authentication is required",
            },
            Self::Forbidden => ErrorBody {
                code: "forbidden",
                message: "this caller may not invoke this method",
            },
            Self::MethodNotFound => ErrorBody {
                code: "method_not_found",
                message: "the RPC method was not found",
            },
            Self::ActorNotFound => ErrorBody {
                code: "actor_not_found",
                message: "the addressed actor was not found",
            },
            Self::MethodNotAllowed => ErrorBody {
                code: "method_not_allowed",
                message: "this route accepts POST only",
            },
            Self::IdempotencyConflict => ErrorBody {
                code: "idempotency_conflict",
                message: "the idempotency key cannot be replayed",
            },
            Self::RequestTooLarge => ErrorBody {
                code: "request_too_large",
                message: "the request JSON exceeds its limit",
            },
            Self::UnsupportedMediaType => ErrorBody {
                code: "unsupported_media_type",
                message: "a JSON content type is required",
            },
            Self::IdempotencyCapacity => ErrorBody {
                code: "idempotency_capacity",
                message: "the upstream idempotency capacity is exhausted",
            },
            Self::UpstreamError => ErrorBody {
                code: "upstream_error",
                message: "the upstream call failed",
            },
            Self::UpstreamUnavailable => ErrorBody {
                code: "upstream_unavailable",
                message: "the upstream service is unavailable",
            },
            Self::UpstreamTimeout => ErrorBody {
                code: "upstream_timeout",
                message: "the upstream call timed out",
            },
            Self::ResponseTooLarge => ErrorBody {
                code: "response_too_large",
                message: "the successful response JSON exceeds its limit",
            },
        }
    }

    pub(crate) fn response(self, challenge: Option<&HeaderValue>) -> Response<Body> {
        let encoded = serde_json::to_vec(&ErrorEnvelope { error: self.body() })
            .unwrap_or_else(|_| FALLBACK_ERROR.to_vec());
        let encoded = if encoded.len() <= MAX_ERROR_BYTES {
            encoded
        } else {
            FALLBACK_ERROR.to_vec()
        };
        let mut response = Response::new(Body::from(encoded));
        *response.status_mut() = self.status();
        response.headers_mut().insert(
            header::CONTENT_TYPE,
            HeaderValue::from_static("application/json"),
        );
        if let Some(challenge) = challenge {
            response
                .headers_mut()
                .insert(header::WWW_AUTHENTICATE, challenge.clone());
        }
        if matches!(self, Self::MethodNotAllowed) {
            response
                .headers_mut()
                .insert(header::ALLOW, HeaderValue::from_static("POST"));
        }
        response
    }

    pub(crate) fn from_external(error: keylink::ExternalError<JsonError>) -> Self {
        match error {
            keylink::ExternalError::UnknownMethod => Self::MethodNotFound,
            keylink::ExternalError::Codec(JsonError::InvalidJson) => Self::InvalidRequest,
            keylink::ExternalError::Codec(JsonError::ResponseTooLarge) => Self::ResponseTooLarge,
            keylink::ExternalError::Codec(JsonError::Codec) => Self::UpstreamError,
            keylink::ExternalError::Client(error) => Self::from_keylink(error),
        }
    }

    fn from_keylink(error: keylink::Error) -> Self {
        match error {
            keylink::Error::Transport(_) | keylink::Error::Closed => Self::UpstreamUnavailable,
            keylink::Error::Remote(remote) => match remote {
                keylink::RemoteError::Unauthorized(_) => Self::Forbidden,
                keylink::RemoteError::TimedOut { .. } => Self::UpstreamTimeout,
                keylink::RemoteError::NoSuchActor { .. } => Self::ActorNotFound,
                keylink::RemoteError::AtCapacity { .. } | keylink::RemoteError::MailboxFull => {
                    Self::UpstreamUnavailable
                }
                keylink::RemoteError::Idempotency(refusal) => match refusal {
                    keylink::IdempotencyRefusal::Malformed(_) => Self::InvalidIdempotencyKey,
                    keylink::IdempotencyRefusal::NotKeyable { .. } => Self::IdempotencyNotSupported,
                    keylink::IdempotencyRefusal::TooManyRunning { .. } => Self::IdempotencyCapacity,
                    keylink::IdempotencyRefusal::InFlight
                    | keylink::IdempotencyRefusal::Interrupted
                    | keylink::IdempotencyRefusal::Unrecordable
                    | keylink::IdempotencyRefusal::Forgotten => Self::IdempotencyConflict,
                    _ => Self::UpstreamError,
                },
                _ => Self::UpstreamError,
            },
            _ => Self::UpstreamError,
        }
    }
}
