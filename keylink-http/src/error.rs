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

use crate::service::JsonError;

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

#[cfg(test)]
mod tests {
    #![allow(clippy::expect_used)]

    use axum::body::to_bytes;
    use http::{header, HeaderValue, StatusCode};
    use keylink::{Error, IdempotencyRefusal, RemoteError};

    use super::{GatewayError, MAX_ERROR_BYTES};

    async fn rendered(error: GatewayError) -> (StatusCode, http::HeaderMap, String) {
        let response = error.response(None);
        let status = response.status();
        let headers = response.headers().clone();
        let bytes = to_bytes(response.into_body(), MAX_ERROR_BYTES)
            .await
            .expect("adapter error body");
        (
            status,
            headers,
            String::from_utf8(bytes.to_vec()).expect("JSON is UTF-8"),
        )
    }

    #[tokio::test]
    async fn every_adapter_error_has_an_exact_stable_http_representation() {
        let cases = [
            (
                GatewayError::InvalidRequest,
                StatusCode::BAD_REQUEST,
                "invalid_request",
                "the request JSON or path is invalid",
            ),
            (
                GatewayError::InvalidIdempotencyKey,
                StatusCode::BAD_REQUEST,
                "invalid_idempotency_key",
                "the idempotency key is invalid",
            ),
            (
                GatewayError::IdempotencyNotSupported,
                StatusCode::BAD_REQUEST,
                "idempotency_not_supported",
                "this method does not accept idempotency keys",
            ),
            (
                GatewayError::Unauthenticated,
                StatusCode::UNAUTHORIZED,
                "unauthenticated",
                "authentication is required",
            ),
            (
                GatewayError::Forbidden,
                StatusCode::FORBIDDEN,
                "forbidden",
                "this caller may not invoke this method",
            ),
            (
                GatewayError::MethodNotFound,
                StatusCode::NOT_FOUND,
                "method_not_found",
                "the RPC method was not found",
            ),
            (
                GatewayError::ActorNotFound,
                StatusCode::NOT_FOUND,
                "actor_not_found",
                "the addressed actor was not found",
            ),
            (
                GatewayError::MethodNotAllowed,
                StatusCode::METHOD_NOT_ALLOWED,
                "method_not_allowed",
                "this route accepts POST only",
            ),
            (
                GatewayError::IdempotencyConflict,
                StatusCode::CONFLICT,
                "idempotency_conflict",
                "the idempotency key cannot be replayed",
            ),
            (
                GatewayError::RequestTooLarge,
                StatusCode::PAYLOAD_TOO_LARGE,
                "request_too_large",
                "the request JSON exceeds its limit",
            ),
            (
                GatewayError::UnsupportedMediaType,
                StatusCode::UNSUPPORTED_MEDIA_TYPE,
                "unsupported_media_type",
                "a JSON content type is required",
            ),
            (
                GatewayError::IdempotencyCapacity,
                StatusCode::TOO_MANY_REQUESTS,
                "idempotency_capacity",
                "the upstream idempotency capacity is exhausted",
            ),
            (
                GatewayError::UpstreamError,
                StatusCode::BAD_GATEWAY,
                "upstream_error",
                "the upstream call failed",
            ),
            (
                GatewayError::UpstreamUnavailable,
                StatusCode::SERVICE_UNAVAILABLE,
                "upstream_unavailable",
                "the upstream service is unavailable",
            ),
            (
                GatewayError::UpstreamTimeout,
                StatusCode::GATEWAY_TIMEOUT,
                "upstream_timeout",
                "the upstream call timed out",
            ),
            (
                GatewayError::ResponseTooLarge,
                StatusCode::INTERNAL_SERVER_ERROR,
                "response_too_large",
                "the successful response JSON exceeds its limit",
            ),
        ];

        for (error, status, code, message) in cases {
            let (actual_status, headers, body) = rendered(error).await;
            assert_eq!(actual_status, status, "{code}");
            assert_eq!(headers[header::CONTENT_TYPE], "application/json", "{code}");
            assert_eq!(
                body,
                format!(r#"{{"error":{{"code":"{code}","message":"{message}"}}}}"#),
                "{code}"
            );
            assert_eq!(
                headers.get(header::ALLOW),
                (code == "method_not_allowed")
                    .then(|| HeaderValue::from_static("POST"))
                    .as_ref(),
                "{code}"
            );
        }

        let challenge = HeaderValue::from_static("Bearer realm=stable");
        let response = GatewayError::Unauthenticated.response(Some(&challenge));
        assert_eq!(response.headers()[header::WWW_AUTHENTICATE], challenge);
    }

    #[tokio::test]
    async fn keylink_failures_map_completely_without_upstream_text_leakage() {
        let secret = "principal=tenant-secret key=raw-secret upstream=private";
        let cases = [
            (Error::Closed, GatewayError::UpstreamUnavailable),
            (
                Error::Remote(RemoteError::Unauthorized(secret.into())),
                GatewayError::Forbidden,
            ),
            (
                Error::Remote(RemoteError::TimedOut { millis: 9 }),
                GatewayError::UpstreamTimeout,
            ),
            (
                Error::Remote(RemoteError::NoSuchActor { id: secret.into() }),
                GatewayError::ActorNotFound,
            ),
            (
                Error::Remote(RemoteError::AtCapacity { capacity: 7 }),
                GatewayError::UpstreamUnavailable,
            ),
            (
                Error::Remote(RemoteError::Idempotency(IdempotencyRefusal::Malformed(
                    secret.into(),
                ))),
                GatewayError::InvalidIdempotencyKey,
            ),
            (
                Error::Remote(RemoteError::Idempotency(IdempotencyRefusal::NotKeyable {
                    method: secret.into(),
                })),
                GatewayError::IdempotencyNotSupported,
            ),
            (
                Error::Remote(RemoteError::Idempotency(
                    IdempotencyRefusal::TooManyRunning { limit: 1 },
                )),
                GatewayError::IdempotencyCapacity,
            ),
            (
                Error::Remote(RemoteError::Idempotency(IdempotencyRefusal::InFlight)),
                GatewayError::IdempotencyConflict,
            ),
            (
                Error::Remote(RemoteError::Idempotency(IdempotencyRefusal::Interrupted)),
                GatewayError::IdempotencyConflict,
            ),
            (
                Error::Remote(RemoteError::Idempotency(IdempotencyRefusal::Unrecordable)),
                GatewayError::IdempotencyConflict,
            ),
            (
                Error::Remote(RemoteError::Idempotency(IdempotencyRefusal::Forgotten)),
                GatewayError::IdempotencyConflict,
            ),
            (
                Error::Remote(RemoteError::Failed(secret.into())),
                GatewayError::UpstreamError,
            ),
            (
                Error::HandlerPanic(secret.into()),
                GatewayError::UpstreamError,
            ),
        ];

        for (source, expected) in cases {
            let mapped = GatewayError::from_keylink(source);
            assert_eq!(mapped.status(), expected.status());
            assert_eq!(mapped.body().code, expected.body().code);
            let (_, _, body) = rendered(mapped).await;
            for forbidden in [
                "tenant-secret",
                "raw-secret",
                "private",
                "principal=",
                "key=",
            ] {
                assert!(!body.contains(forbidden), "{forbidden} leaked in {body}");
            }
        }
    }
}
