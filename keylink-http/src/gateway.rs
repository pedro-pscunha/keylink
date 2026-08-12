//! Ordered state-generic Axum routing.

use std::{marker::PhantomData, sync::Arc};

use axum::{body::Body, routing::any, Router};
use http::{header, HeaderMap, Method, Request, Response};
use keylink::ExternalService;

use crate::{
    error::GatewayError, Access, AccessError, Call, Challenge, JsonCodec, JsonLimits, Principal,
};

const IDEMPOTENCY_HEADER: &str = "idempotency-key";
const KEY_DOMAIN: &[u8] = b"keylink-http/idempotency/v1\0";

/// HTTP/JSON adapter for one already-connected generated singleton client.
pub struct Gateway<S, A>
where
    S: ExternalService<JsonCodec>,
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
    S: ExternalService<JsonCodec>,
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
            Ok(_) | Err(_) => return GatewayError::RequestTooLarge.response(None),
        };
        match S::invoke(
            &self.client,
            method,
            &body,
            key,
            JsonCodec::new(self.limits.response.get()),
        )
        .await
        {
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
    let Some(value) = headers.get(header::CONTENT_TYPE) else {
        return false;
    };
    let Ok(value) = value.to_str() else {
        return false;
    };
    let media = value
        .split(';')
        .next()
        .map(str::trim)
        .unwrap_or_default()
        .to_ascii_lowercase();
    media == "application/json" || (media.starts_with("application/") && media.ends_with("+json"))
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
