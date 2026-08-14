//! An immutable router for the two document endpoints.
//!
//! Never mounted for you. A deployment decides whether its contract is public,
//! and a crate that installed the route on its own would make that decision by
//! being linked.

use std::sync::Arc;

use axum::{body::Body, routing::get, Router};
use http::{header, Response, StatusCode};

/// The media type OpenAPI 3.1 names for its own document.
const OPENAPI_MEDIA: &str = "application/vnd.oai.openapi+json;version=3.1";
/// The media type RFC 8927 names for a JSON Schema document.
const SCHEMA_MEDIA: &str = "application/schema+json";

/// `GET` and `HEAD` on `/openapi.json` and `/schema.json`, and nothing else.
///
/// The bytes are captured when the router is built, so the two endpoints cannot
/// drift from each other or from the document a test snapshotted.
pub fn router<State>(openapi: Vec<u8>, schema: Vec<u8>) -> Router<State>
where
    State: Clone + Send + Sync + 'static,
{
    // One buffer each, shared. The streaming document is tens of kilobytes and
    // a clone per request would copy all of it to answer a `HEAD`.
    let openapi: Arc<[u8]> = Arc::from(openapi);
    let schema: Arc<[u8]> = Arc::from(schema);
    let openapi_head = Arc::clone(&openapi);
    let schema_head = Arc::clone(&schema);
    Router::new()
        .route(
            "/openapi.json",
            get(move || {
                let bytes = Arc::clone(&openapi);
                async move { serve(bytes, OPENAPI_MEDIA, true) }
            })
            .head(move || {
                let bytes = Arc::clone(&openapi_head);
                async move { serve(bytes, OPENAPI_MEDIA, false) }
            }),
        )
        .route(
            "/schema.json",
            get(move || {
                let bytes = Arc::clone(&schema);
                async move { serve(bytes, SCHEMA_MEDIA, true) }
            })
            .head(move || {
                let bytes = Arc::clone(&schema_head);
                async move { serve(bytes, SCHEMA_MEDIA, false) }
            }),
        )
}

/// One builder for both verbs, because RFC 9110 §9.3.2 asks a HEAD to carry the
/// fields its GET would — including `Content-Length`, which is why the length
/// is taken before the body is dropped.
fn serve(bytes: Arc<[u8]>, media: &'static str, with_body: bool) -> Response<Body> {
    let length = bytes.len();
    let body = if with_body {
        Body::from(bytes.to_vec())
    } else {
        Body::empty()
    };
    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, media)
        .header(header::CONTENT_LENGTH, length.to_string())
        .header(header::CACHE_CONTROL, "public, max-age=0, must-revalidate")
        .body(body)
        .unwrap_or_else(|_| {
            // Unreachable: every header value above is a literal or a decimal
            // length. Answered rather than panicked, because a document
            // endpoint that killed the process would take the gateway with it.
            let mut fallback = Response::new(Body::empty());
            *fallback.status_mut() = StatusCode::INTERNAL_SERVER_ERROR;
            fallback
        })
}
