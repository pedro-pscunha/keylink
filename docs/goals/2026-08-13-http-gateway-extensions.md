# Goal: complete the HTTP gateway surface

## Intent

Extend the authenticated HTTP/JSON gateway so non-Rust clients can use Keylink's
streaming methods, blobs, and keyed actors. Generate accurate OpenAPI 3.1 and
JSON Schema documents from the same `service!` declaration and runtime route
inventory.

This goal replaces every unary requirement that makes the generated inventory
`rpc`-only or requires streaming, keyed actors, OpenAPI, or JSON Schema to stay
absent. This replacement includes the unary success criteria, definition of
done, and completion condition. All positive unary behavior, trust, bounds,
error, cancellation, and dependency guarantees remain in force. The gateway
remains a client of the existing Keylink pipeline. It does not become a second
dispatcher or a Keylink peer.

## Context

The work lives in `/Users/pedrocunha/repos/keylink`. It continues from the
reviewed unary gateway work in
`docs/goals/2026-08-12-http-json-gateway.md` and its implementation plan.

The extension uses these existing seams:

- generated `Client`, `IdempotentClient`, `RegistryClient`, and `ActorRef` calls;
- `Body` and `BodyReader` for upload and one-shot download streams;
- `BlobHandle` and the existing peer-bound blob grant;
- deployment authentication through `Principal` and mandatory async `Access`;
- the dependency-neutral generated `ExternalService` marker with codec-generic invocation.

The current blob interface cannot provide honest HTTP byte ranges. This goal
therefore includes a verified, cancellable blob-reader seam in `keylink`.

## Skills

Load the matching skill before work in its area:

- `ponytail`: keep one generated method inventory and avoid speculative routes.
- `rust-best-practices`: design owned streams, precise errors, and bounded types.
- `codebase-design`: keep the generated and HTTP modules deep and dependency-neutral.
- `test-driven-development`: run each new test red, then green, before broad filters.
- `systematic-debugging`: diagnose unexpected failures before changing code.
- `simple-english`: write public documentation and error guidance clearly.
- `verification-before-completion`: require current command evidence for every completion claim.
- `brooks-review` and `brooks-test`: run the plan and merge-boundary review gates.
- `requesting-code-review` and `receiving-code-review`: request and apply findings separately.

Never use fable without Pedro's direct authorization. GitHub Actions is disabled.
The local hooks and pre-push gate are authoritative.

## Required surface

Applications select their own Axum prefix. Paths below are relative to it.
`Method` is the declared CamelCase method name.

### Singleton routes

| Declaration | Route | Request | Success |
|---|---|---|---|
| `rpc` | `POST /rpc/{Method}` | bounded JSON arguments | `200` bounded JSON return value |
| `send` | `POST /send/{Method}` | raw byte stream plus bounded JSON argument metadata | `200` bounded JSON return value |
| `recv` | `POST /recv/{Method}` | bounded JSON arguments | raw byte stream plus bounded JSON result metadata |
| `blob` | `POST /blob/{Method}` | bounded JSON arguments | `201` bounded JSON return value and principal-bound `Location` |
| blob bytes | `GET /blob/{Method}/{blob_id}` | no body | full or single-range verified bytes |
| blob metadata | `HEAD /blob/{Method}/{blob_id}` | no body | fresh full-representation metadata; Range is ignored |

### Keyed-actor routes

| Declaration | Route |
|---|---|
| `rpc` | `POST /{actor_id}/rpc/{Method}` |
| `recv` | `POST /{actor_id}/recv/{Method}` |
| `blob` | `POST /{actor_id}/blob/{Method}` |
| actor blob bytes | `GET /{actor_id}/blob/{Method}/{blob_id}` |
| actor blob metadata | `HEAD /{actor_id}/blob/{Method}/{blob_id}` |

Actor `send` is absent because generated `ActorRef` deliberately has no honest
upload method. Actor `tell`, feeds, internal status, lifecycle, and registry
administration are also absent. Unknown, disabled, and unsupported methods return
the same adapter-owned method-not-found response before `Access` or dispatch.

The actor ID occupies one percent-encoded UTF-8 path segment. Decode it once,
apply finite encoded and decoded limits, then construct `ActorId`. A literal
slash is not an alias for `%2F`.

## Streaming contract

`send` and `recv` use raw `application/octet-stream` bodies. Bounded unpadded
base64url JSON headers carry the second typed value:

- `Keylink-Arguments` carries the argument object for `send`.
- `Keylink-Result` carries the declared return value for `recv`.

The adapter validates header size before decoding and validates decoded JSON
against an explicit limit. It rejects duplicate or malformed metadata.
`StreamingLimits` is explicit and finite. It bounds metadata, uploads,
downloads, and blob-ticket count. Enabling streaming requires an operator to
supply these limits.

The adapter does not collect a stream into one buffer. Backpressure runs from
the HTTP body through Keylink flow control. Dropping the HTTP handler or response
drops the owned Keylink stream. No detached pump task survives cancellation.
Late stream errors terminate the HTTP body because response headers can no longer
change.

`send` and `recv` reject `Idempotency-Key` before body polling. Current Keylink
replay can answer before it consumes a retry upload, while the caller sends the
complete body before it reads that answer. Therefore streamed upload replay is
not payload-size independent. Blob lookup idempotency covers only the lookup
call, not the later byte transfer.

## Blob range and resume contract

Add a cloneable `BlobHandle` and an owned `BlobReader` interface that can:

- obtain a verified total length;
- open a verified full stream;
- open one verified half-open byte range;
- return bounded verified chunks without a detached copy task.

`open` and `open_range` complete the verified-size preflight before they return.
They compare the data response size with that verified value. Thus
`BlobReader::total_len` is safe for HTTP headers. An out-of-bounds range can
start the size preflight, but it must fail before the data-range request.

The implementation accepts only the existing root blob grant. It must not widen
admission to child or hash-sequence selection. It verifies Bao data, contiguous
offsets, exact selected length, and unaligned edge slicing. Dropping a reader
cancels that read.

A successful blob method stores only its returned handle in a finite process-local
ticket table. The key includes the verified principal, method, optional actor,
and blob ID. Each insertion has a generation. A stale read can remove only the
generation that it used. Replacement moves one live entry to the newest FIFO
position. A path blob ID carries no authority. Every `GET` and `HEAD` request
runs `Access` before ticket lookup.

Blob GET supports one RFC byte range, `If-Range`, `If-None-Match`, strong
content-address ETags, `206`, `304`, and `416`. Multiple ranges and multipart
responses are out of scope. After access and an exact live ticket, evaluate the
full `If-None-Match` list before Range. Then evaluate Range and strong
`If-Range`. HEAD performs a fresh verified availability check, reports the full
representation, and ignores Range and `If-Range`. Do not emit
`Accept-Ranges: bytes`, `Content-Range`, or a verified `Content-Length` until
the range seam passes its watched-red tests.

Resume means a later Range GET can continue while all of these remain valid:

- the gateway process and bounded ticket entry;
- the same verified HTTP principal and access decision;
- the upstream Keylink blob grant and content.

There is no send or recv resume. A restart invalidates blob locations. The
adapter does not promise durable URLs, a local blob store, or CDN publishing.

## Trust and ordering invariants

For each matched route, preserve this order:

1. Parse the route shape with bounded raw-segment decoders and select one enabled method of the required kind.
2. Validate the HTTP verb.
3. For actor routes, decode once and validate the `ActorId`.
4. Extract the verified `Principal`.
5. Run mandatory async `Access` with service, method, and optional actor.
6. Validate content type, headers, conditions, range, and idempotency inputs.
7. Poll or collect the request body only after authorization succeeds.
8. Invoke the exact generated client method.
9. Encode or stream the declared result through finite limits.

A malformed actor on a wrong-verb request returns the method error because verb
validation precedes actor decoding. Actor existence must not be checked before
access or before the actual generated call. Denied calls must not poll a
body, claim idempotency, mount an actor, inspect a ticket, or invoke Keylink.

HTTP principals are never converted to `EndpointId`. Backend authorization still
sees the gateway's real Keylink endpoint. HTTP access is an additional edge
policy, not a replacement.

For every route that accepts idempotency, derive the Keylink key from the
canonical HTTP call identity and the external key. The identity contains the
verified principal, generated service, generated method, addressing-mode tag,
and actor ID when present. Arguments remain outside the identity, so one key
still names one logical call at that route. This binding prevents a call that
passes current `Access` for one method or actor from replaying another method or
actor. Clients must use one key per logical operation.

## Keyed-actor privacy

The gateway calls the generated `RegistryClient::at(actor)` path. It never emits
or accepts `Addressed<Request>` JSON and never reaches through private registry
fields.

No HTTP route lists actors or performs a status preflight. Missing and stopped
actors preserve Keylink's public distinctions only through stable, redacted HTTP
categories. Responses never disclose an actor ID that the caller did not supply, registry
capacity, lifecycle text, panic text, endpoint IDs, or raw upstream errors. An
actor blob `Location` can canonically reflect the caller-supplied encoded actor
segment because that segment is required to use the resource. Error bodies and
logs never contain actor IDs.

A blob ticket for an actor includes the actor context. The byte route repeats
`Access` with that context. A bare blob ID must not cross actors or principals.

## OpenAPI and JSON Schema

Generate OpenAPI 3.1 and JSON Schema draft 2020-12 from an optional
`keylink-openapi` workspace crate. `keylink` and normal `keylink-http` consumers
must not gain schema-generation dependencies.

The existing generated `External` marker remains the service identity. An
opt-in companion macro or equivalent one-declaration mechanism names the original
argument and return types. Applications must not repeat the service declaration.
Types included in documents must implement `schemars::JsonSchema` in addition to
the existing Serde bounds.

Introduce the dependency-neutral runtime route/error inventory before any new
route. Runtime routing, error rendering, and documentation consume it. Gateway
configuration produces one immutable `HttpSurface` value that records addressing
mode, enabled bounded streaming/blob routes, prefix, and finite public limits.
Route installation and document construction consume the same value. Actor
`recv` and `blob` remain absent until streaming limits are supplied.

The document generator consumes the final generated method inventory and this
configured runtime surface. It must not maintain a second handwritten route
list. It documents only implemented singleton and actor intersections. Every
raw octet stream uses the same OpenAPI 3.1 schema: `type: string` with
`contentMediaType: application/octet-stream`.

The build-time document interface is primary. `DocumentConfig::new` requires a
`Security` value, so an undocumented authentication state is unrepresentable.
An immutable `GET/HEAD /openapi.json` and `GET/HEAD /schema.json` router is
explicit and opt-in. The
gateway does not auto-mount documentation, Swagger UI, YAML, CORS, or credentials.
Authentication configuration must describe deployment behavior without inventing
a Bearer or API-key mechanism.

Generated argument schemas match the runtime deserializer. With ordinary Serde,
a missing `Option<T>` argument is accepted as `None`; its property is optional
and nullable. Explicit `null` is also accepted. Raw Rust identifiers use the
Serde JSON spelling without the `r#` prefix. Actor path schemas describe the
decoded logical value, percent-encoded once by URI serialization, and state the
128-byte UTF-8 limit without pretending that JSON Schema `maxLength` counts
bytes.

Documents have deterministic ordering and stable operation IDs. Component
collision suffixes use the first six BLAKE3 bytes as lowercase hex over the
domain `keylink-openapi/component/v1\0`, a big-endian `u64` input length, and
the UTF-8 schema ID. Construction fails on path, operation, component, or
incompatible-schema collisions. The renderer resolves logical schema IDs to
`#/components/schemas/...` for OpenAPI and `#/$defs/...` for the standalone
catalog. Canonical JSON snapshots and full `$ref` resolution are compatibility
evidence. OpenAPI output does not enter the
postcard contract or ALPN.

## Bounds and errors

All request metadata, JSON, path segments, streams, ticket state, and successful
response metadata have finite validated limits. Raw encoded path segments are
capped before allocation. Method limits derive from the longest enabled generated
name, actor limits derive from `MAX_KEY_LEN`, and blob IDs have one exact finite
shape. Encoded stream metadata is capped before base64 decoding, then decoded
JSON is checked separately. Memory use is independent of payload length except
for bounded transport chunks.

Adapter-owned JSON error envelopes use stable codes and redact untrusted or
upstream text. The runtime renderer and document generator consume one error
inventory. Errors found after stream headers terminate the stream and cannot
become a JSON envelope.

## Non-goals

- WebSockets, SSE, feeds, subscriptions, or push.
- HTTP `tell`, actor `send`, actor status, lifecycle, discovery, or administration.
- Multipart requests, multiple byte ranges, compression, or content transformation.
- Durable or signed blob URLs, a persistent ticket table, TTL tasks, or a CDN.
- Upload or one-shot download resume.
- Idempotent `send` or `recv` unless a later goal changes the core RPC body/replay protocol.
- Generated REST SDKs, Swagger UI, YAML, GraphQL, or gRPC.
- A second dispatcher, server middleware path, ledger, authorization system, or blob grant.
- TLS, CORS, credential verification, rate limits, or deployment manifests.
- Publishing crates or changing the postcard wire, ALPN, or existing generated Rust calls.

## Blast radius

The work can modify:

- `keylink/src/external.rs`, `keylink/src/macros.rs`, and focused exports/tests;
- `keylink/src/blobs.rs` and focused blob tests for verified range reads only;
- `keylink-http/**` for runtime routes, streams, tickets, errors, tests, and examples;
- a new optional `keylink-openapi/**` crate;
- workspace manifests, focused docs, examples, and anchor tasks.

Do not refactor unrelated serving, actor, persistence, authorization, middleware,
wire, or idempotency code. If a required capability needs a wider change, stop
and present the exact widening before implementation.

## Success criteria and evidence

1. Real loopback HTTP tests invoke each supported singleton route and prove exact typed values and bytes.
2. Real keyed tests cover mounted, lazy, missing, stopped, denied, and capacity cases without an existence leak.
3. Individually terminating tests prove access-before-body, end-to-end backpressure, direct-await concurrency, and drop cancellation.
4. Blob tests prove verified full reads, aligned and unaligned ranges, conditions, interruption, and later range resume within the stated lifetime.
5. Cross-principal, cross-method, and cross-actor blob-ticket attempts return the same redacted absence category.
6. Generated-surface tests prove one cfg-aware method inventory, exact calls, unsupported-kind absence, and unchanged wire contract structure.
7. OpenAPI and schema snapshots match every implemented route, header, status, schema, actor intersection, and authentication representation.
8. Dependency inspection proves that `keylink` has no HTTP/schema dependency and `keylink-http` has no schema dependency.
9. Each new behavioral guard has an individually observed red mutation and a registered replayable anchor. Every liveness red proves its event-driven terminator before a broad filter runs.
10. Brooks merge, test-quality, and invariant reviews contain no unresolved Critical or Major finding.
11. The authoritative pre-push gate passes on the exact pushed HEAD. The transcript records the complete log path and exact local/remote SHA match.

No literal contract-hash update or ALPN cost accounting is part of this goal.

## Execution interaction policy

Execute autonomously. Resolve code-answerable questions from current source and
tests. Record any unavoidable product assumption in the goal or plan before code.
Ask Pedro only when a required change widens the stated trust model or blast radius.

## Completion condition

Complete this goal and every task in
`docs/superpowers/plans/2026-08-13-http-gateway-extensions.md`. Surface focused
red/green evidence, watched-red anchors, review outcomes, the authoritative gate
log, and exact pushed SHA. Do not claim completion while a Critical or Major
finding remains, while any supported route is undocumented, or while a range,
actor, schema, authorization, bound, backpressure, cancellation, or privacy
invariant lacks direct evidence.
