# keylink-http

`keylink-http` is a bounded HTTP/JSON gateway for generated Keylink services.
`Gateway` serves one singleton service; `ActorGateway` serves one keyed actor
registry. Both call the existing generated Keylink client and neither creates a
second server pipeline.

| Declaration | Singleton | Keyed |
|---|---|---|
| `rpc` | `POST /rpc/{method}` | `POST /{actor}/rpc/{method}` |
| `send` | `POST /send/{method}` | — |
| `recv` | `POST /recv/{method}` | `POST /{actor}/recv/{method}` |
| `blob` | `POST /blob/{method}` | `POST /{actor}/blob/{method}` |
| blob bytes | `GET`/`HEAD` `/blob/{method}/{id}` | `GET`/`HEAD` `/{actor}/blob/{method}/{id}` |

`tell` has no route in either space, and a keyed surface serves no `send`. The
streaming families are installed only when `HttpSurface::with_streaming` is
called.

A byte body cannot carry a declared value, so one rides a header as base64url:
`Keylink-Arguments` on a `send` request, `Keylink-Result` on a `recv` response.

`keylink-openapi` is a separate opt-in package that builds an OpenAPI 3.1
document and a standalone JSON Schema from the same `HttpSurface`. Its schema
dependency does not reach this package.

The crate documentation is the complete reference: limits, the error envelope,
idempotency, cancellation, blob resume, actor ids, and what to configure on a
proxy in front of it.

## Start a gateway

Outer middleware must authenticate the HTTP caller.
The middleware must insert a verified `Principal` in the request extensions.
Use a stable, non-secret principal namespace.
Store verified authorization claims in a separate extension.

`Access` is mandatory.
It checks the principal, claims, service, and method before the gateway reads the request body.
The Keylink server sees the gateway node as its peer.
It does not see the HTTP principal as an `EndpointId`.

```rust,ignore
let gateway = keylink_http::Gateway::<inventory::External, _>::new(
    client,
    access,
    keylink_http::Challenge::new("Bearer realm=keylink".parse()?)?,
)
.limits(keylink_http::JsonLimits::new(64 * 1024, 64 * 1024)?)
.router::<AppState>(keylink_http::HttpSurface::new(
    "/v1/inventory",
    keylink_http::Addressing::Singleton,
)?)?;

let app: axum::Router<AppState> = axum::Router::new()
    .merge(gateway)
    .with_state(app_state);
```

Run the complete example from the workspace root:

```sh
cargo run -p keylink-http --example inventory
```

The example prints its loopback URL.
The repository gate runs `.mise/tasks/http-smoke` against this URL with `curl`.

## HTTP contract

A route has the exact form `POST <prefix>/rpc/<DeclaredRustMethodName>`.
The route has no trailing-slash alias.
An unknown, disabled, non-RPC, or internal name within the generated method byte cap returns `method_not_found`.
A malformed encoded method segment returns `invalid_request` before
authorization. An over-cap one returns `method_not_found`: a segment too long to
be any method names no method, and answering `400` there reported the longest
served name's length to a caller who had not authenticated.
All other HTTP methods return `method_not_allowed` and `Allow: POST`.

The request must contain exactly one valid JSON media type.
The gateway accepts `application/json` and valid `application/*+json` media types.
Parameters are valid when the MIME parser accepts them.
Missing, duplicate, malformed, or non-JSON content types return `unsupported_media_type`.

The request body is one JSON object with the declared Rust argument names.
Unknown fields are invalid.
A zero-argument RPC uses `{}`.
Nested domain types keep their Serde behavior.
The success body contains only the declared return value.
A unit return becomes `null`.

Set positive request and success limits with `JsonLimits`.
Both byte limits are inclusive.
The gateway stops request collection and success encoding at these limits.
Error bodies have a separate fixed limit.
JavaScript clients must use safe integers or JSON-safe domain types.

## Idempotency

`Idempotency-Key` is optional for unary RPC calls.
Use one key for one logical call.
The gateway binds the key to the verified principal before Keylink receives it.
Two principals cannot replay each other’s results.
Do not reuse a key for different methods or arguments.

Replay records live for the Keylink server process lifetime.
They do not provide durable exactly-once execution.

## Errors

Errors use this JSON shape:

```json
{"error":{"code":"forbidden","message":"this caller may not invoke this method"}}
```

The gateway does not include credentials, principal bytes, authorization details, transport errors, or upstream failure text.

| Status | Code | Meaning |
|---:|---|---|
| 400 | `invalid_request` | The path or JSON argument object is invalid. |
| 400 | `invalid_idempotency_key` | The idempotency key is invalid. |
| 400 | `idempotency_not_supported` | The route does not accept idempotency keys. Only `rpc` does. |
| 400 | `invalid_stream_metadata` | The `Keylink-Arguments` header is missing or undecodable. |
| 400 | `range_not_supported` | `Range` or `If-Range` on a route that serves no ranges. |
| 401 | `unauthenticated` | Authentication is required. |
| 403 | `forbidden` | The verified caller cannot invoke this method. |
| 404 | `method_not_found` | The RPC route does not exist. |
| 404 | `actor_not_found` | The addressed actor does not exist, or the policy refused it — the two answer alike. |
| 405 | `method_not_allowed` | The route does not serve this verb. Call routes take `POST`; blob byte routes take `GET` and `HEAD`. |
| 409 | `idempotency_conflict` | The key cannot be replayed. |
| 404 | `blob_not_found` | No grant of this caller's names these bytes. |
| 410 | `blob_gone` | The grant existed and the holder no longer serves it. |
| 413 | `request_too_large` | The request exceeds its byte limit. |
| 413 | `request_stream_too_large` | The upload exceeds its byte limit. |
| 416 | `range_not_satisfiable` | The requested range lies outside the content. |
| 415 | `unsupported_media_type` | One valid JSON media type is required. |
| 429 | `idempotency_capacity` | The upstream idempotency capacity is exhausted. |
| 500 | `response_too_large` | The success JSON exceeds its byte limit. |
| 500 | `response_stream_too_large` | The download exceeds its byte limit. |
| 502 | `upstream_error` | The upstream call failed. |
| 503 | `upstream_unavailable` | The upstream service is unavailable. |
| 504 | `upstream_timeout` | The upstream call timed out. |

A `401` response includes the configured `WWW-Authenticate` challenge.
A `405` response includes `Allow` naming the verbs that route does serve.
All error responses use `application/json` and `Cache-Control: private, no-store`.

This table is the complete inventory and a test says so:
`the_readme_documents_every_error_code` compares it against `ErrorKind::ALL`.
Seven codes were missing from it before that test existed.

## Cancellation and unsupported capabilities

A client disconnect drops the gateway-owned Keylink call future.
Backend work that already started can continue.
The gateway does not promise rollback.
An idempotent call can settle as `Interrupted`.

This gateway does not expose `tell`, feeds, `__Status`, WebSockets, actor
lifecycle, actor discovery, Keylink administration, or CORS. `send`, `recv`,
blobs, keyed actors and OpenAPI all exist — see the route table above.

## Every surface at once

The example above is the small one. For a process serving a singleton surface,
a keyed surface and the two document endpoints together:

```sh
cargo run -p keylink-openapi --example gateway
mise run http-smoke-full
```

The smoke drives 45 checks against it with real curl, including ranges,
conditional GETs, encoded actor ids and both document media types.

Two gateways merged into one axum router need
`HttpSurface::without_fallback()` on the second: axum permits one fallback per
router.
