# keylink-http

`keylink-http` is a bounded HTTP/JSON gateway for generated Keylink services.
`Gateway` serves one singleton service; `ActorGateway` serves one keyed actor
registry. Both call the existing generated Keylink client and neither creates a
second server pipeline.

Part of the [keylink workspace](../README.md). Use this package when a caller
cannot speak Rust or the Keylink protocol.

```toml
[dependencies]
keylink = { git = "https://github.com/pedro-pscunha/keylink" }
keylink-http = { git = "https://github.com/pedro-pscunha/keylink" }
axum = "0.8"
```

Not published to crates.io yet, so a git dependency is the only way in. Requires
Rust 1.91 or later. You bring the axum server; this package produces a `Router`
to merge into it.

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
It checks the principal, claims, service, method and addressed actor before the gateway reads the request body.
The actor is `None` on a singleton surface, and it is the whole difference an `Access` implementation sees between the two.
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

The example prints its loopback URL, and mounts its surface at `/inventory`
rather than the `/v1/inventory` of the snippet above — the prefix is yours to
choose, so read it off the printed URL rather than off this page. One call
against it:

```sh
curl -i -X POST "$URL/rpc/Add" \
  -H 'Authorization: Bearer allow' \
  -H 'Content-Type: application/json' \
  --data '{"left":2,"right":3}'
```

```
HTTP/1.1 200 OK
content-type: application/json

5
```

The success body is the declared return value alone, never a Keylink `Response`
envelope. Send `Bearer deny` instead and the same call answers `403` with the
error envelope below. The repository gate runs `.mise/tasks/http-smoke` against
this URL with `curl`.

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
Parameters are valid when the MIME parser accepts them, so `application/json; charset=utf-8` is a JSON media type.
A `send` route is the exception: it takes `application/octet-stream` with no parameters at all.
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
A gateway built without `.limits(..)` uses the `JsonLimits` default of 64 KiB each way, so it is bounded before you configure anything.
`StreamingLimits` has no default, which is what keeps the streaming and blob route families opt-in.
Error bodies have a separate fixed limit of 512 bytes.
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
| 400 | `idempotency_not_supported` | This method does not accept idempotency keys. The gateway refuses the header on `send`, `recv` and every blob route; Keylink refuses it for an `rpc` method its service does not key. |
| 400 | `invalid_stream_metadata` | The `Keylink-Arguments` header is missing, duplicated, not canonical base64url, or over its metadata limit. |
| 400 | `range_not_supported` | `Range` or `If-Range` on a route that serves no ranges. |
| 401 | `unauthenticated` | Authentication is required. |
| 403 | `forbidden` | The verified caller cannot invoke this method. |
| 404 | `method_not_found` | The RPC route does not exist. |
| 404 | `actor_not_found` | No actor of this id is mounted. |
| 404 | `blob_not_found` | No byte resource of this id is available to this caller. A guessed id, another caller's, another method's, and one evicted from the finite table all answer alike. |
| 405 | `method_not_allowed` | The route does not serve this verb. Call routes take `POST`; blob byte routes take `GET` and `HEAD`. |
| 409 | `idempotency_conflict` | The key cannot be replayed. |
| 410 | `blob_gone` | The byte resource existed for this caller and the holder no longer serves it. |
| 413 | `request_too_large` | The request exceeds its byte limit. |
| 413 | `request_stream_too_large` | The upload exceeds its byte limit. |
| 415 | `unsupported_media_type` | One valid JSON media type is required. |
| 416 | `range_not_satisfiable` | The requested range lies outside the content. |
| 429 | `idempotency_capacity` | The upstream idempotency capacity is exhausted. |
| 500 | `response_too_large` | The success JSON exceeds its byte limit. |
| 502 | `response_stream_too_large` | The download exceeds its byte limit. |
| 502 | `upstream_error` | The upstream call failed. |
| 503 | `upstream_unavailable` | The upstream service is unavailable. |
| 502 | `upstream_indeterminate` | The upstream ran the work and cannot say whether it committed. Reconcile before retrying: an idempotency key does not make this call safe to replay, because the server settles the key as interrupted and refuses the replay. |
| 504 | `upstream_timeout` | The upstream call timed out. |

A `401` response includes the configured `WWW-Authenticate` challenge.
A `405` response includes `Allow` naming the verbs that route does serve.
All error responses use `application/json` and `Cache-Control: private, no-store`.

**A refusal never says whether the actor exists.** A caller the policy refuses
gets `403` byte for byte whether the actor is mounted or was never mounted. Only
a caller the policy *admits* can reach `404 actor_not_found`, which is the
registry answering. Read `403` as "not for you" and never as "not there".

This table names every code the gateway can answer with, and
`the_readme_documents_every_error_code` in `keylink-http/tests/api.rs` fails when
a new one is missing from either README. It compares the **codes only** — no test
checks the status or the meaning beside one, so those two columns are reviewed by
hand. Seven codes were missing before that test existed.

## Cancellation and unsupported capabilities

A client disconnect drops the gateway-owned Keylink call future.
Backend work that already started can continue.
The gateway does not promise rollback.
An idempotent call can settle as `Interrupted`.

This gateway does not expose `tell`, feeds, `__Status`, WebSockets, actor
lifecycle, actor discovery, Keylink administration, or CORS. `send`, `recv`,
blobs and keyed actors all exist — see the route table above. OpenAPI documents
are not a gateway route: they come from [`keylink-openapi`](../keylink-openapi),
which installs its own two endpoints and is never mounted for you.

## Every surface at once

The example above is the small one. For a process serving a singleton surface,
a keyed surface and the two document endpoints together:

```sh
cargo run -p keylink-openapi --example gateway
mise run http-smoke-full
```

The smoke drives every route family in the table above with real curl, including
ranges, conditional GETs, encoded actor ids and both document media types. It
also fails when a family stops being driven, so the coverage claim is checked
rather than asserted. No count is written here: it would rot on the next check
added, and the script is the inventory.

The keyed half differs from the singleton in three things — the marker type, the
client, and `Addressing::Actors`:

```rust,ignore
let keyed = keylink_http::ActorGateway::<bays::External, _>::new(
    bays::RegistryClient::connect(&node, addr).await?,
    access,
    challenge,
)
.limits(keylink_http::JsonLimits::new(64 * 1024, 64 * 1024)?)
.router::<AppState>(
    keylink_http::HttpSurface::new("/bays", keylink_http::Addressing::Actors)?
        .with_streaming(streaming_limits)
        .without_fallback(),
)?;
```

Two gateways merged into one axum router need
`HttpSurface::without_fallback()` on the second, as above: axum permits one
fallback per router, and merging two that both have one panics.

## License

MIT or Apache-2.0, at your option. See [LICENSE-MIT](../LICENSE-MIT) and
[LICENSE-APACHE](../LICENSE-APACHE) at the workspace root.
