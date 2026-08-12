# Goal: an HTTP/JSON gateway for non-Rust Keylink clients

## Intent

Ship a small, separately packaged HTTP adapter that lets any HTTP-capable
language invoke acknowledged Keylink commands without implementing Keylink's
postcard/QUIC protocol. Preserve the existing `service!` declaration and every
generated Rust caller surface: HTTP enters a bounded JSON edge, and the adapter
uses the existing typed `RpcClient` to reach the one Keylink serving pipeline.

Version one is deliberately narrow: generated singleton services and declared
unary `rpc` methods only. It adds no WebSockets, remote feeds, actor addressing,
streaming, generated REST client, or HTTP dependency to the `keylink` crate.

## Context

Repository: `/Users/pedrocunha/repos/keylink`. The execution branch must start
from current `origin/main`; this goal was written against
`841c09596d2aa18fc012351f2ecb2ee419d0386b`.

The useful seams already exist:

- `service!` generates public `Request` and `Response` enums with Serde support.
- `RpcClient<Req, Resp>` already exposes generic calls and idempotent calls.
- Keylink already distinguishes transport transmission, queue admission, handler
  completion, and replay. The adapter must preserve those distinctions.

The serving pipeline becomes an erased iroh `DynProtocolHandler`. Reusing it as
HTTP ingress would expose or duplicate admission, required authorization,
middleware, idempotency, actor routing, and blob grants. Do not do that. The HTTP
adapter is a gateway client: HTTP → generated conversion seam → `RpcClient` →
existing Keylink server. Execution semantics retain one source of truth.

The backend authenticates the gateway's `EndpointId`, not the HTTP caller.
Deployment-owned HTTP middleware authenticates a caller and inserts a stable,
non-secret principal namespace into request extensions. Mandatory gateway access
then authorizes that verified principal for the selected service/method before
reading the body. The adapter never fabricates an `EndpointId`.

Keylink's ledger scopes a key by peer, and every HTTP caller shares the gateway
peer. Therefore a raw external `Idempotency-Key` must never become a Keylink key.
The gateway derives a bounded opaque Keylink key from `(principal namespace,
external key)` with a domain-separated collision-resistant digest. Same-principal
reuse deliberately retains Keylink's existing one-call key semantics: a key is
for one logical call, not merely one method or argument set. Reuse for a different
call can replay the first response; the generated conversion seam must reject a
wrong response variant rather than return it as HTTP 200. Documentation must tell
clients to generate one key per logical operation.

## Skills (load the matching one BEFORE working in its area — every job on this matter)

- `ponytail` — mandatory: reuse Serde and `RpcClient`; reject generated routing,
  OpenAPI, WebSockets, and speculative serving seams.
- `rust-best-practices` — precise errors, bounded data, ownership, public Rust
  interfaces, and type-driven illegal-state prevention.
- `codebase-design` — keep one narrow generated conversion interface at the
  existing client seam; do not leak Keylink transport internals.
- `test-driven-development` — red → green → refactor; watch each new guard fail.
- `systematic-debugging` — use before changing code in response to an unexpected
  failure.
- `verification-before-completion` — evidence before completion claims.
- `brooks-review` — mandatory adversarial plan and merge-boundary reviews.
- `brooks-sweep` — discretionary quality sweep inside this blast radius only.
- `brooks-test` — review real HTTP-boundary tests for false confidence.
- `superpowers:requesting-code-review` and
  `superpowers:receiving-code-review` — request and triage reviews; reviewers do
  not apply their own findings.
- `superpowers:writing-plans`, `superpowers:subagent-driven-development`, and
  `worktrunk:worktrunk` — a reviewed implementation plan is required and runs in
  a `wt` worktree.

Caveats:

- Historical RFCs/audits are orientation, not current capability evidence.
- GitHub Actions is disabled by decision. The pre-push gate is authoritative;
  read `AGENTS.md` and `CONTRIBUTING.md` before work.
- Never run fable; Pedro has not authorized it. Prime/RLM subagents inherit the
  parent Codex model at max effort.

## Scope

### In scope

- Add a publishable workspace package named `keylink-http`. It depends on
  `keylink` and the minimum Axum/HTTP/JSON stack. Those dependencies must not
  enter the `keylink` package's dependency graph.
- Expose one state-generic Axum router for one already-connected generated
  singleton `Client`. Applications choose the prefix with normal Axum nesting
  and run it with normal `axum::serve`.
- Expose `POST /rpc/{CamelCaseMethod}`. The body is the JSON object of declared
  arguments; a successful body is only the declared return value.
- Keep `service!` syntax and generated Rust calling methods unchanged. No HTTP
  attributes or route annotations enter service declarations.
- Add dependency-neutral `ServiceCodec` and generated `ExternalService<C>`
  traits to `keylink`. `ServiceCodec` has generic Serde decode/encode operations
  but no HTTP or JSON type. Generated code implements `ExternalService<C>` where
  its local service types satisfy coherence; `keylink-http` supplies the bounded
  JSON codec. `Addressed<Request>`/registry clients and manual `Rpc` types do not
  implement this generated seam.
- The generated implementation owns four facts in one match over the declaration
  list: which methods are plain `rpc`; how codec-decoded argument fields invoke
  the exact generated Client method; how only that method's declared return value
  reaches codec encoding; and how an optional idempotency key selects the
  generated `IdempotentClient`. It honors cfg without depending on enum ordinals,
  variant order, externally tagged JSON names, or ordinary wire JSON.
- HTTP argument field names are the Rust argument identifiers from `service!`.
  Method-level Serde attributes remain wire-variant metadata and are not replayed
  onto HTTP argument structs. Nested argument/return domain types retain their
  own Serde behavior. Routes always use declared CamelCase method identity.
- `tell`, `send`, `recv`, `blob`, feeds, and internal `__Status` generate no HTTP
  method and no conversion arm. The generated HTTP metadata/conversion is not a
  wire field and must not participate in Serde/Schema-derived contract types.
- Require verified principal identity and access for every matched call:
  deployment middleware inserts `Principal` in `http::Extensions`; `Access`
  receives extensions, headers, service, and validated method, then allows or
  returns unauthenticated/forbidden. a validated `Challenge` supplied to `Gateway::new` provides the single
  `WWW-Authenticate` value for missing-principal and access-denied 401s.
  `Access` may be async and returns only allow/unauthenticated/forbidden.
- Select and authorize a known rpc method before polling/reading the request body.
  Unknown/non-rpc/internal methods do not invoke `Access` or Keylink.
- Support optional `Idempotency-Key`: validate it as visible ASCII within
  Keylink's external key length rules, then derive a fixed-size Keylink key from
  the verified principal namespace and external value. Never log either input.
- Bound incoming and successful outgoing JSON with finite, documented, validated
  defaults. Ceilings are inclusive (`encoded_len <= limit`). Adapter-owned error
  envelopes use a separate fixed compile-time maximum and are not charged to the
  configured successful-response limit. Serialize successes through a writer
  that stops at the ceiling; never allocate the entire over-limit value first.
- Return stable adapter-owned JSON error envelopes and deterministic HTTP status
  categories without raw transport errors, authorization internals,
  `RemoteError::Failed` text, secrets, or debug output.
- Document and test the adapter-owned HTTP surface, including wrong methods,
  content type, paths, trailing slash, malformed path encoding, and fallbacks.
- Provide an end-to-end example and curl smoke for normal, idempotent, denied,
  malformed, and unsupported calls.
- Update the ignored architecture HTML only if it exists in the execution
  checkout. Never track it.

### Out of scope

- WebSockets, SSE, long polling, push, subscriptions, or remote feeds.
- Actor registry clients and `Addressed<Request>`; actor identity routes are not
  part of v1. Applications can expose an explicit domain RPC on a singleton
  gateway service if they need actor mediation now.
- `tell`, `send`, `recv`, `blob`, `__Status`, `key_state`, registry
  administration, inspect, or console over HTTP.
- Streaming HTTP request/response bodies. Oversized JSON is rejected.
- HTTP ingress into `Dispatch`, `ServiceBuilder`, mailboxes, middleware, the
  ledger, or serving-side authorization.
- Widening the existing `Rpc` trait; ordinal-based/custom enum deserializers;
  reliance on generated enum variant order. The separate dependency-neutral
  codec/service traits are intentionally in scope.
- Generated per-method route functions or SDKs, OpenAPI/JSON Schema, GraphQL,
  gRPC, reflection, or discovery routes.
- TLS, CORS policy, rate limiting, tracing backend, credential verification/API
  key storage, or deployment manifests. Outer application middleware owns them;
  the verified `Principal` handoff and gateway authorization remain mandatory.
- Changing the postcard wire, ordinary Request/Response Serde shape, ALPN,
  generated calling signatures, or cryptographic peer identity.
- Compatibility aliases, deprecated wrappers, or parallel legacy paths.
- Publishing to crates.io. This goal produces a publishable workspace crate; an
  external release is a separate explicitly authorized action.

### Blast radius

May modify:

- Root `Cargo.toml` and `Cargo.lock` for workspace/dependencies.
- New `keylink-http/**` package, tests, examples, and docs.
- `keylink/src/macros.rs` to emit the generated dependency-neutral external
  service implementation.
- `keylink/src/lib.rs` and one focused new `keylink/src/external.rs` module for
  the codec/service traits. Default builds acquire no HTTP/JSON dependency.
- Focused `keylink/tests/**` for generated-surface, cfg, Serde-rename,
  representability, schema, and downstream compatibility guards.
- `README.md`, `CONTRIBUTING.md`, `docs/**`, and minimum mise/anchor files needed
  to document and gate the package.

Must not modify:

- Keylink serving, actor, compose, persistence, blob, body, peer, admission,
  authorization, idempotency, error, middleware, or wire implementations unless
  an accepted review proves a criterion impossible. Widening requires Pedro's
  approval.
- The existing public `Rpc` trait or production `Addressed<T>` implementation.
  The new external codec traits remain separate and have no blanket impl.
- Historical documents to rewrite history; GitHub Actions; or tracked `.tmp`.

## Public interface

Names can change during the reviewed plan only if the replacement is smaller and
preserves every capability decision. The interface must remain equivalent to:

```rust
use std::future::Future;
use http::{Extensions, HeaderMap, HeaderValue};

/// Stable, non-secret namespace from verified deployment authentication.
/// Private bytes, finite constructor, redacted Debug, no Display.
pub struct Principal { /* private */ }
impl Principal {
    pub fn new(namespace: impl AsRef<[u8]>) -> Result<Self, ConfigError>;
}

pub struct Call<'a> {
    pub extensions: &'a Extensions,
    pub headers: &'a HeaderMap,
    pub principal: &'a Principal,
    pub service: &'static str,
    pub method: &'static str,
}

pub enum AccessError {
    Unauthenticated,
    Forbidden,
}

pub struct Challenge { /* private validated non-empty HeaderValue */ }
impl Challenge {
    pub fn new(value: HeaderValue) -> Result<Self, ConfigError>;
}

pub trait Access: Clone + Send + Sync + 'static {
    fn authorize(
        &self,
        call: Call<'_>,
    ) -> impl Future<Output = Result<(), AccessError>> + Send;
}

#[non_exhaustive]
pub enum ConfigError {
    EmptyPrincipal,
    PrincipalTooLong { max: usize },
    ZeroRequestLimit,
    ZeroResponseLimit,
    InvalidChallenge,
}

pub struct JsonLimits { /* private positive inclusive ceilings */ }
impl JsonLimits {
    pub fn new(request_bytes: usize, response_bytes: usize)
        -> Result<Self, ConfigError>;
}
impl Default for JsonLimits { /* finite documented defaults */ }

// Owned by `keylink`; contains no HTTP/JSON type.
pub trait ServiceCodec {
    type Error: std::error::Error + Send + Sync + 'static;
    type Output: Send + 'static;
    fn decode<T: serde::de::DeserializeOwned>(&self, bytes: &[u8])
        -> Result<T, Self::Error>;
    fn encode<T: serde::Serialize>(&self, value: &T)
        -> Result<Self::Output, Self::Error>;
}

// Implemented by generated plain singleton service marker only.
pub trait ExternalService<C: ServiceCodec>: Send + Sync + 'static {
    const SERVICE: &'static str;
    const RPC_METHODS: &'static [&'static str];
    type Client: Clone + Send + Sync + 'static;
    fn invoke(/* client, method, bounded bytes, optional key, codec */)
        -> impl Future<Output = Result<C::Output, ExternalError<C::Error>>> + Send;
}

pub struct Gateway<S, A> { /* private generated client/access/limits */ }
impl<S, A> Gateway<S, A>
where
    S: keylink::ExternalService<JsonCodec>,
    A: Access,
{
    pub fn new(client: S::Client, access: A, challenge: Challenge) -> Self;
    pub fn limits(self, limits: JsonLimits) -> Self;
    pub fn router<State>(self) -> axum::Router<State>
    where
        State: Clone + Send + Sync + 'static;
}
```

The macro generates a marker and dependency-neutral implementation, not a new
calling surface:

```rust
// Generated in `inventory`; exact reserved name may differ.
pub struct External;
impl<C: keylink::ServiceCodec> keylink::ExternalService<C> for External {
    /* one cfg-aware rpc match */
}
```

Usage in an application with existing Axum state:

```rust
let client = inventory::Client::connect(&node, peer).await?;
let inventory = Gateway::<inventory::External, _>::new(client, access, challenge)
    .limits(JsonLimits::new(64 * 1024, 64 * 1024)?)
    .router::<AppState>();

let app: axum::Router<AppState> = axum::Router::new()
    .nest("/v1/inventory", inventory);
let app = app.with_state(app_state);
let listener = tokio::net::TcpListener::bind("127.0.0.1:3000").await?;
axum::serve(listener, app).await?;
```

Authentication middleware must insert a verified principal before the gateway:

```rust
request.extensions_mut().insert(Principal::new(tenant_id.as_bytes())?);
```

For:

```rust
keylink::service! {
    service Inventory;
    rpc GetItem(id: u32) -> Option<Item>;
    rpc Reserve(sku: String, qty: u32) -> Receipt;
    tell Audit(line: String) -> ();
}
```

external callers send argument objects, not wire enum JSON:

```http
POST /v1/inventory/rpc/GetItem
Authorization: Bearer …
Content-Type: application/json

{"id":7}
```

and receive only the return value. `POST .../rpc/Audit` is not found and never
calls access or Keylink.

Failures use:

```json
{"error":{"code":"forbidden","message":"this caller may not invoke this method"}}
```

`code` is a documented snake-case set; `message` is stable adapter-owned prose.
For requests that match the gateway prefix, adapter-owned responses are:

| HTTP | code | producer |
|---:|---|---|
| 400 | `invalid_request` | malformed JSON, wrong argument shape, malformed path encoding |
| 400 | `invalid_idempotency_key` | malformed `Idempotency-Key` |
| 400 | `idempotency_not_supported` | upstream `NotKeyable` |
| 401 | `unauthenticated` | missing `Principal` or access denial; cached validated Challenge required |
| 403 | `forbidden` | edge denial or upstream `Unauthorized` |
| 404 | `method_not_found` | unknown/internal/non-rpc method, missing method segment, unmatched nested path, or trailing slash |
| 404 | `actor_not_found` | upstream `NoSuchActor` if a singleton domain RPC reports it |
| 405 | `method_not_allowed` | a known gateway route called with a non-POST verb; include `Allow: POST` |
| 409 | `idempotency_conflict` | `InFlight`, `Interrupted`, `Unrecordable`, `Forgotten` |
| 413 | `request_too_large` | request exceeds inclusive configured ceiling |
| 415 | `unsupported_media_type` | body-bearing call without JSON-compatible Content-Type |
| 429 | `idempotency_capacity` | `TooManyRunning` |
| 502 | `upstream_error` | codec, unexpected/wrong variant, withheld/failed, other unmapped upstream failure |
| 503 | `upstream_unavailable` | transport/closed connection or capacity |
| 504 | `upstream_timeout` | upstream `TimedOut` |
| 500 | `response_too_large` | success JSON exceeds inclusive response ceiling |

HTTP parser failures before Axum creates a request (invalid request line/header)
are outside this adapter contract. Every response produced after the gateway
prefix matches uses JSON, including 404/405/415 fallbacks. `application/json` and
`application/*+json`, with optional parameters, are accepted; other or absent
Content-Type values on a body-bearing rpc are 415. No trailing-slash alias exists.

## Success criteria

- [ ] `keylink-http` is a separate package; `cargo tree -p keylink` has no new
  Axum/Hyper/Tower HTTP/serde_json edge, while `cargo tree -p keylink-http` shows
  them only at the adapter — captured manifests and tree outputs prove it.
- [ ] Existing `service!` syntax, Rust caller methods, ordinary Request/Response
  Serde shape, manual `Rpc` implementations, and singleton/registry ALPNs are
  unchanged — existing tests plus focused downstream compile and schema-property
  probes pass. Structural review confirms HTTP metadata is outside every wire
  `Serialize`/`Deserialize`/`Schema` type; no literal ALPN pin is introduced.
- [ ] A mixed, cfg-conditional service exposes exactly its enabled plain `rpc`
  methods through its generated `External` marker; excludes tell/send/recv/blob/feed/
  `__Status`; and does not implement `ExternalService<JsonCodec>` for `Addressed<Request>` or a
  manual `Rpc` — focused runtime and compile-fail probes prove each absence.
- [ ] Arguments-only JSON works for zero/one/multiple arguments, `{}` for zero
  arguments, unit returns as `null`, nested domain types' own Serde metadata, and
  cfg-disabled declarations. Method-level `#[serde(rename)]` remains wire-only;
  route/method identity stays declared CamelCase — focused conversion/e2e tests.
- [ ] A deliberately wrong response variant becomes stable `502 upstream_error`,
  never HTTP 200 — focused conversion test watches the guard fail when variant
  checking is broken.
- [ ] The generic router nests into `Router<AppState>`, then accepts state and
  serves — downstream compile probe plus real listener integration test.
- [ ] A real in-process Keylink server and loopback TCP HTTP listener accepts a
  valid authenticated JSON call from an ordinary HTTP client and returns only
  the declared value with 200/application-json.
- [ ] Outer middleware inserts `Principal`; `Access` consumes it and extensions
  asynchronously without reverifying credentials. Missing principal fails 401
  using the Gateway's cached validated Challenge.
  Unknown/non-rpc/internal methods invoke neither `Access` nor Keylink.
- [ ] Method selection and authorization precede body polling: denied requests
  return 401/403 even with malformed/oversized/never-yielding bodies — focused
  ordering test uses a body stream that records whether it was polled.
- [ ] `Access` and `Principal` are mandatory; no zero-argument, allow-all, or
  default construction compiles. Missing-principal and access-denied 401s use
  the cached validated `Challenge` exactly; 403 has no challenge — compile-fail/HTTP tests.
- [ ] Adapter `ConfigError` has the exact non-exhaustive variants in the public
  sketch; its Debug/Display never contains principal bytes — unit/rustdoc tests.
- [ ] Limits are positive, inclusive, documented, and enforced at `limit-1`,
  `limit`, `limit+1`. Input over limit never dispatches. Success output uses a
  capacity-enforcing writer, emits no partial success, and returns bounded JSON
  `response_too_large`. Tiny positive limits are valid because adapter error
  envelopes use their separate finite cap.
- [ ] Two principals using the same external `Idempotency-Key` derive different
  Keylink keys and cannot replay each other's result. The same principal/key
  repeated for one completed call replays without a second execution. Different
  logical-call reuse never returns a wrong variant as 200. Real ledger tests
  prove all three; no derived/raw key or principal appears in logs/errors.
- [ ] Malformed idempotency headers and each documented idempotency refusal map
  correctly. Real-ledger tests produce reachable states; direct narrow mapping
  tests are allowed only for states impractical to produce deterministically.
- [ ] All tabled HTTP/error mappings, JSON Content-Type variants, missing segment,
  unmatched paths, malformed encoding, wrong verbs/Allow header, and trailing
  slash behavior are proven through a real listener. No raw upstream text leaks.
- [ ] No adapter mutex serializes concurrent calls — a synchronization-based
  test proves two HTTP calls enter independent `RpcClient` calls without a
  wall-clock upper bound.
- [ ] Cancellation contract: when Hyper drops the handler future after a
  response-side client disconnect, no adapter task retains/detaches the Keylink
  call future. Already-started backend work may continue and an idempotent call
  may become `Interrupted`; the adapter promises no rollback. A real TCP
  disconnect synchronized with call-future drop plus source inspection proves
  this exact claim.
- [ ] README/example documents startup, outer auth handoff, identity distinction,
  principal-namespaced idempotency and one-key-per-logical-call rule, finite
  limits, JSON shape, JS integer precision, errors, cancellation, and unsupported
  capabilities. Compile examples where practical.
- [ ] Every new guard has a registered break-once anchor and an individually
  observed red run; `mise run anchors` reports every row registered.
- [ ] A curl smoke against the example captures normal, idempotent retry, denied,
  malformed, and unsupported-tell responses without any Rust client.
- [ ] Focused tests, format, Clippy, docs, features, package, browser/wasm guards,
  MSRV, anchors, and authoritative pre-push gate pass in complete logs with
  `EXIT=0` on final HEAD.

## Invariants

### Universal

- Minimum implementation: one HTTP adapter, one generated conversion match, one
  serving pipeline, one error mapping. No speculative transport surface.
- Precisely typed/documented public items. Domain states use enums/newtypes and
  private fields, never bool/string configuration soup.
- Raw headers, extensions, and JSON stay at the adapter edge. Core work uses
  generated requests/responses and clients; no `serde_json::Value` or raw map is
  exposed as structured public interface.
- No `Dispatch`, Context, mailbox, ledger, connection, or raw upstream error in
  the public HTTP interface.
- Fail closed and in order: known method → verified principal → access → bounded
  body → typed request → Keylink. No permissive default or silent fallback.
- Preserve phase truth: HTTP 200 means a Keylink reply was received and boundedly
  encoded, not durable domain application. Replay is process-lifetime Keylink
  replay, not durable exactly-once.
- External idempotency keys are always principal-namespaced before Keylink. One
  key means one logical call; wrong response variants fail loudly.
- No compatibility aliases, duplicate serving implementation, unrelated refactor,
  unbounded queue/body/allocation/error, raw secret logging, or detached task.
- Strict red → green → refactor at real HTTP→Keylink boundaries. Do not mock
  `RpcClient`, handler dispatch, mailbox, or ledger.
- No clock-fragile upper bounds; synchronize or pause time.
- HTTP routes/JSON/status/error codes/limit behavior are compatibility surface.

### Context-dependent decisions

- Test strategy: hermetic in-process Keylink node/server plus real loopback TCP
  Axum server/client; no external network, database, recordings, containers, or
  core mocks.
- Gates: Rust fmt; focused tests; all-target/all-feature Clippy; docs; feature/
  browser/wasm; package; MSRV; anchors; authoritative pre-push gate.
- New package: `keylink-http`, with only Gateway, Principal, Call, Access,
  AccessError, Challenge, JsonLimits, and ConfigError public; HTTP error DTOs stay private.
- Ports/adapters: existing generated `Client` is outbound seam;
  `Gateway<External, Access>` is HTTP adapter; dependency-neutral
  `ServiceCodec`/`ExternalService` is the narrow bridge. No serving-side port.
- Type boundary: bounded argument JSON becomes an exact generated Request inside
  the generated conversion implementation; exact expected Response becomes
  bounded JSON only at the response edge.
- Authentication: outer middleware verifies and inserts `Principal`; mandatory
  async `Access` authorizes it before body polling. Backend sees gateway peer.
- JSON compatibility: document JavaScript integer precision and recommend
  JSON-safe domain DTOs; never silently stringify arbitrary numbers.

## Execution interaction policy

Questions are allowed only for a true structural blocker or material scope
widening. Read current source/tests/docs first. For ordinary ambiguity choose the
smallest option consistent with this goal, record it, and continue. Never widen
the blast radius silently.

## Process

A reviewed plan is required. Use `wt` from fetched/fast-forwarded main, focused
atomic commits, and local hooks. Do not duplicate a hook-owned full gate.

1. Explore and write the plan against this goal.
2. One adversarial plan gate with `brooks-review` and
   `requesting-code-review`; apply valuable findings via `receiving-code-review`.
   Re-review only for Critical/structural revision.
3. Execute strict TDD. Run each intended-red async/liveness test individually and
   prove it terminates before aggregate filters.
4. Register break-once anchors for every guard.
5. Focused verification while iterating; hooks own aggregate gates.
6. Before merge run one adversarial merge review and a distinct invariant review
   grading every invariant. Fix all valuable findings and every invariant miss.
7. PR to `main` with session id, goal, evidence, and deviations. Merge only after
   complete local gate green on exact HEAD.
8. Prove merge; remove worktree and local/remote branch with `wt`; verify absent.

Long commands write complete output plus `EXIT=` to `/tmp`, never filter on write,
and are never interrupted. GitHub Actions remains disabled.

## Definition of done

- [ ] Every success criterion has final-HEAD evidence and the final response
  prints exact commands/status/key assertions, not “tests pass.”
- [ ] Diff stays in blast radius with no WebSocket, actor HTTP, streaming,
  ordinal-Serde machinery, generated REST methods, tracked artifact, or unrelated
  refactor.
- [ ] Existing service UI, Rust callers, wire/ALPN, authorization/idempotency, and
  phase guarantees remain intact.
- [ ] All focused/stack gates and complete pre-push gate are green with `EXIT=0`
  on exact pushed HEAD.
- [ ] Plan gate, merge gate, and invariant review have no open Critical/Major;
  final report includes counts and per-invariant verdicts.
- [ ] Plan is entirely fulfilled or Pedro explicitly descoped an item.
- [ ] PR merged; local/remote main match; merged worktree and branches removed.
- [ ] Re-read this goal end to end against evidence before declaring completion.

## /goal completion condition

Satisfy every success criterion and Definition-of-done item in
`docs/goals/2026-08-12-http-json-gateway.md`. Preserve `service!`, generated Rust
callers, wire/ALPN, and the one serving pipeline; ship only the separate bounded,
principal-authenticated HTTP/JSON singleton unary-rpc gateway described there,
with no WebSockets, actors, streaming, ordinals, generated REST surface, or raw
HTTP idempotency keys. As the final step print one proof block: final/remote/merge
SHAs; blast-radius diff; conversion/cfg/Serde-rename/wrong-variant and compatibility
guards; real HTTP→Keylink, auth/order/limit/principal-idempotency/error/concurrency/
cancellation tests; watched-red anchors; cargo trees; curl smoke; fmt/Clippy/docs/
features/package/wasm/MSRV; full pre-push `EXIT=0` on exact HEAD; review counts;
per-invariant verdict; PR/merge URL; and verified cleanup. Stop when all pass or
after 30 turns. If a structural blocker requires widening scope, stop and report
evidence and the exact decision required rather than widening it.

## Deliverable

A merged PR to `main` from an isolated worktree, a publishable (but not externally published) `keylink-http`
workspace package, complete documentation/example, green local gates, and
verified post-merge cleanup. External registry publication is not included.
