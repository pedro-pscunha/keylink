# keylink-http

`keylink-http` is a bounded HTTP/JSON gateway for generated Keylink services.
It exposes enabled singleton `rpc` declarations only.
The gateway calls the existing generated Keylink client.
It does not create a second server pipeline.

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
.router::<AppState>();

let app: axum::Router<AppState> = axum::Router::new()
    .nest("/v1/inventory", gateway)
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
Unknown, disabled, non-RPC, and internal methods return `method_not_found`.
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
| 400 | `idempotency_not_supported` | The RPC does not accept idempotency keys. |
| 401 | `unauthenticated` | Authentication is required. |
| 403 | `forbidden` | The verified caller cannot invoke this method. |
| 404 | `method_not_found` | The RPC route does not exist. |
| 404 | `actor_not_found` | The addressed actor does not exist. This code is reserved for actor HTTP support. |
| 405 | `method_not_allowed` | The route accepts `POST` only. |
| 409 | `idempotency_conflict` | The key cannot be replayed. |
| 413 | `request_too_large` | The request exceeds its byte limit. |
| 415 | `unsupported_media_type` | One valid JSON media type is required. |
| 429 | `idempotency_capacity` | The upstream idempotency capacity is exhausted. |
| 500 | `response_too_large` | The success JSON exceeds its byte limit. |
| 502 | `upstream_error` | The upstream call failed. |
| 503 | `upstream_unavailable` | The upstream service is unavailable. |
| 504 | `upstream_timeout` | The upstream call timed out. |

A `401` response includes the configured `WWW-Authenticate` challenge.
A `405` response includes `Allow: POST`.
All error responses use `application/json`.

## Cancellation and unsupported capabilities

A client disconnect drops the gateway-owned Keylink call future.
Backend work that already started can continue.
The gateway does not promise rollback.
An idempotent call can settle as `Interrupted`.

This version does not expose `tell`, `send`, `recv`, blobs, feeds, actors, `__Status`, WebSockets, streaming HTTP, OpenAPI, or administration.
