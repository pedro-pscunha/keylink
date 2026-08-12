# HTTP/JSON Gateway Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a bounded, authenticated HTTP/JSON adapter for generated singleton unary RPC services without changing Keylink's existing Rust caller or wire interfaces.

**Architecture:** A new `keylink-http` workspace crate owns Axum, JSON, access control, limits, error mapping, and the generic gateway router. `keylink` owns dependency-neutral `ServiceCodec` and generated `ExternalService<C>` traits. `service!` implements `ExternalService<C>` for a generated local marker, which satisfies coherence even when another crate imports the contract; `keylink-http` supplies the bounded JSON codec. The generated implementation invokes exact Client/IdempotentClient methods and encodes only their declared values, without a second wire/dispatch path.

**Tech Stack:** Rust 1.91, `macro_rules!`, Keylink generated clients, Axum 0.8, Hyper/HTTP 1, Serde/serde_json, Tokio, BLAKE3, bytes, thiserror, reqwest dev tests.

---

## File map

- Modify `Cargo.toml`, `Cargo.lock`: add `keylink-http` workspace member and shared adapter dependencies only.
- Create `keylink/src/external.rs`: dependency-neutral `ServiceCodec`, `ExternalService<C>`, and precise generated-seam errors.
- Modify `keylink/src/lib.rs`: curated exports for that seam.
- Modify `keylink/src/macros.rs`: generate an `External` marker and cfg-aware `ExternalService<C>` implementation; no HTTP/JSON crate path.
- Modify `keylink/tests/api.rs`: prove existing syntax/surfaces, marker availability, cfg/Serde metadata replay, and non-HTTP builds.
- Modify `keylink/tests/protocol.rs`: structural/property guard that HTTP marker/replay metadata is outside Contract and KeyedContract; never pin a literal hash.
- Create `keylink-http/Cargo.toml`: publishable adapter manifest.
- Create `keylink-http/src/lib.rs`: curated adapter exports only.
- Create `keylink-http/src/access.rs`: Principal, Call, Access, AccessError.
- Create `keylink-http/src/limits.rs`: validated inclusive JSON ceilings and bounded writer.
- Create `keylink-http/src/error.rs`: stable adapter error codes/envelopes and Keylink mapping.
- Create `keylink-http/src/service.rs`: bounded `JsonCodec` implementation and adapter invocation helpers.
- Create `keylink-http/src/gateway.rs`: state-generic Axum router and ordered request pipeline.
- Create `keylink-http/tests/compile.rs` plus `tests/ui/**`: downstream compile-pass/fail representability.
- Create `keylink-http/tests/http.rs`: real loopback HTTP→Keylink behavior.
- Create `keylink-http/tests/cancellation.rs`: real TCP disconnect/future-drop behavior, individually terminating.
- Create `keylink-http/examples/inventory.rs`: runnable curl target.
- Modify `README.md`, `CONTRIBUTING.md`: usage, guarantees, unsupported surface, gate/package notes.
- Modify `.mise/tasks/anchors` and `.mise/tasks/replay-anchors` only for guards actually watched red.

### Task 1: Prove and add the dependency-neutral codec/service seam

**Files:**
- Create: `keylink/src/external.rs`
- Modify: `keylink/src/lib.rs`, `keylink/src/macros.rs`
- Modify: `keylink/tests/api.rs`, `keylink/tests/protocol.rs`

- [ ] **Step 1: Write behavior-specific failing tests**

Add a mixed service with rpc/tell/send/recv/blob/feed, cfg-disabled rpc, and a
method-level `#[serde(rename = "wire-name")]`. Tests require generated
`inventory::External`, exact enabled `RPC_METHODS`, generic invocation through a
test codec, and exclusion of every non-rpc/internal declaration. A separate
fixture crate defines a service; a downstream fixture imports it and successfully
uses its External marker, proving coherence across crate boundaries. Existing
manual `Rpc` and `Addressed<Request>` types must not satisfy ExternalService.
A structural/property test confirms Contract/KeyedContract fields remain only
request/response/kinds and no literal ALPN pin is introduced.

- [ ] **Step 2: Run each test individually and observe the correct red**

Expected reds are missing `External`, `ServiceCodec`, and `ExternalService`, not
syntax/package failures. Run each exact test/probe alone and capture the failure.

- [ ] **Step 3: Implement the minimal dependency-neutral traits**

```rust
pub trait ServiceCodec: Clone + Send + Sync + 'static {
    type Error: std::error::Error + Send + Sync + 'static;
    type Output: Send + 'static;
    fn decode<T: DeserializeOwned>(&self, bytes: &[u8]) -> Result<T, Self::Error>;
    fn encode<T: Serialize>(&self, value: &T) -> Result<Self::Output, Self::Error>;
}

pub trait ExternalService<C: ServiceCodec>: Send + Sync + 'static {
    const SERVICE: &'static str;
    const RPC_METHODS: &'static [&'static str];
    type Client: Clone + Send + Sync + 'static;
    fn invoke(client: &Self::Client, method: &str, body: &[u8], key: Option<IdempotencyKey>, codec: C)
      -> Pin<Box<dyn Future<Output = Result<C::Output, ExternalError<C::Error>>> + Send>>;
}
```

`service!` emits local `pub struct External` and its generic impl. Per rpc it
emits a private argument struct from plain Rust argument identifiers, decodes it,
and calls the exact generated Client/IdempotentClient method. Method-level Serde
attrs stay only on the wire Request variant; nested argument/return types keep
their own Serde behavior. Non-rpc declarations emit no arms. Unknown method and
client error remain distinct. No blanket impl for Rpc/Addressed exists.

- [ ] **Step 4: Run individual green tests and affected existing suites**

Run exact new tests, fixture compile probes, then existing API/protocol tests.

- [ ] **Step 5: Watch and register every new guard, then commit**

Inventory each new guard, assign a concrete break mutation, observe it red alone,
restore it, and add a checked/replayed anchor row. One mutation may cover multiple
assertions only when it proves one coherent guard. Run `mise run anchors`, then
commit the seam.

### Task 2: Create the adapter package and public security/limit types

**Files:**
- Modify: `Cargo.toml`, `Cargo.lock`
- Create: `keylink-http/Cargo.toml`
- Create: `keylink-http/src/lib.rs`
- Create: `keylink-http/src/access.rs`
- Create: `keylink-http/src/limits.rs`
- Create: `keylink-http/src/error.rs`
- Test: `keylink-http/tests/compile.rs`, `keylink-http/tests/ui/**`, unit tests in new modules

- [ ] **Step 1: Bootstrap only the manifest/empty library, then write compile and unit tests one at a time**

Create the publishable manifest and minimal compiling `lib.rs` with `#![warn(missing_docs)]` as test scaffolding; this is not counted as a behavioral red. Then add tests one at a time requiring non-empty bounded `Principal`, redacted Debug/no Display, positive inclusive `JsonLimits`, mandatory Access, no Default/allow-all Gateway path, async Access over headers/extensions/principal, stable private snake-case HTTP error codes, and bounded writer behavior at `limit-1/limit/limit+1` without allocating past the cap.

Public shape:

```rust
pub struct Principal(SecretBytes);
pub struct Call<'a> { /* extensions, headers, principal, service, method */ }
pub trait Access: Clone + Send + Sync + 'static {
    fn authorize(&self, call: Call<'_>)
        -> impl Future<Output = Result<(), AccessError>> + Send;
}
pub struct JsonLimits { request: NonZeroUsize, response: NonZeroUsize }
#[non_exhaustive]
pub enum ConfigError { EmptyPrincipal, PrincipalTooLong { max: usize }, ZeroRequestLimit, ZeroResponseLimit, InvalidChallenge }
pub struct Challenge(/* private validated HeaderValue */);
impl Challenge { pub fn new(value: HeaderValue) -> Result<Self, ConfigError>; }
// ErrorCode/ErrorEnvelope stay private; only their HTTP representation is public.
```

- [ ] **Step 2: Run each new test individually and observe expected red**

Use exact `cargo test -p keylink-http <name> -- --exact` commands. Each test runs after the scaffold exists and fails for its missing API/behavior, never because Cargo cannot find the package.

- [ ] **Step 3: Add the package and minimal implementations**

Add Axum/HTTP/serde_json/BLAKE3 only to `keylink-http`. `Principal::new` rejects empty/over-cap namespaces, stores bytes privately, redacts Debug, and never implements Display/Serialize. `JsonLimits` uses `NonZeroUsize`; `len <= limit` is accepted. Adapter-owned error envelopes have a separate fixed maximum. `ConfigError` is adapter-owned, non-exhaustive, precisely documented, and never carries principal bytes. A validated `Challenge` is constructed once and passed to Gateway; it supplies missing-principal and access-denied 401s. `AccessError::Unauthenticated` is a unit variant, so no second challenge source exists. Implement a `Write` sink that errors immediately when a successful JSON document crosses the configured response ceiling.

- [ ] **Step 4: Run focused package tests and dependency checks**

```bash
mise exec -- cargo test -p keylink-http --lib
mise exec -- cargo test -p keylink-http --test compile
mise exec -- cargo tree -p keylink
mise exec -- cargo tree -p keylink-http
```

Expected: tests pass; `keylink` has no new Axum/Hyper/Tower HTTP/serde_json dependency edge caused by this change.

- [ ] **Step 5: Watch guards red, register anchors, commit**

Inventory every new guard (including missing-doc lint, mandatory access, Challenge validation/caching, constructor errors, async access, private stable error mapping, zero limits, redaction, writer ceiling); break each coherent guard individually, restore, register all rows, and commit.

### Task 3: Generate exact unary-RPC conversion and invocation

**Files:**
- Create: `keylink-http/src/service.rs`
- Modify: `keylink-http/src/lib.rs`
- Modify: `keylink/src/macros.rs`
- Create/Modify: focused `keylink-http` unit and compile tests

- [ ] **Step 1: Write failing conversion tests**

Use the generated `inventory::External` through `keylink::ExternalService<JsonCodec>`. Prove RPC_METHODS includes only enabled rpc methods. Prove arguments-only JSON for zero/one/multiple args, unit response null, cfg-disabled exclusion, nested domain types with their own Serde-renamed fields/return values, and wrong method. Compile-fail proves `Addressed<Request>` and manual Rpc types cannot implement/use the generated service marker.

- [ ] **Step 2: Observe every conversion test red individually**

Run each exact test. Expected: missing/incomplete `ExternalService<JsonCodec>`, then behavior-specific failures as slices are added. Do not run a broad filter until each async test terminates alone.

- [ ] **Step 3: Implement the generated typed call path**

The generated `ExternalService<C>` implementation from Task 1 receives `JsonCodec`. For each rpc its private argument struct uses declared Rust argument identifiers; method-level Serde attrs are deliberately not replayed. It invokes `Client::$method` or `IdempotentClient::$method`, and hands the concrete return value to the bounded codec. Non-rpc declarations have no arm. No Value, enum ordinal, ordinary Request JSON, or app-side trait impl is used.

Retain the single Task 1 `keylink::ExternalService<C>` signature. Decode and method selection happen before boxing; clone the cheap generated client and move decoded args plus `JsonCodec` into the future; encode the concrete return after await. Map `ExternalError<JsonError>` into private GatewayError only inside `keylink-http`.

- [ ] **Step 4: Run all focused conversion/compile tests green**

Also run existing keylink API/protocol tests to prove caller/wire behavior remains.

- [ ] **Step 5: Inventory and register every conversion guard; break wrong-method replay, kind exclusion, argument shapes, cfg, and nested Serde behavior individually, then commit**

A deliberately reused idempotency key across two different RPC methods must produce `upstream_error`, never serialize another variant/value as 200. Commit the conversion seam.

### Task 4: Implement ordered state-generic HTTP routing and stable failures

**Files:**
- Create: `keylink-http/src/gateway.rs`
- Modify: `keylink-http/src/error.rs`, `keylink-http/src/lib.rs`
- Create: `keylink-http/tests/http.rs`

- [ ] **Step 1: Write failing real-listener tests**

Start an in-process Keylink server and loopback Axum listener. Use an ordinary HTTP client. Cover valid call, AppState nesting, missing principal using cached Challenge, async Access using Extensions, access-denied 401 using the same cached challenge, invalid/empty Challenge construction, 403 no challenge, known-method authorization before a body that records polling, malformed/oversized JSON, unknown/non-rpc/internal method bypass, content types, missing method, malformed encoding, unmatched nested paths, trailing slash, wrong verb/Allow, and every error-table category reachable with real components.

- [ ] **Step 2: Run each async/liveness test alone and observe red termination**

Capture every exact command and expected assertion. Never start with the aggregate `http` test binary filter.

- [ ] **Step 3: Implement the request pipeline in this exact order**

```text
route/prefix match
→ exact RPC method membership
→ extract verified Principal from Extensions
→ async Access
→ validate Content-Type and Idempotency-Key
→ bounded body collection
→ generated typed invoke
→ bounded success JSON
→ stable adapter-owned response
```

Build `Gateway::router<State>() -> Router<State>` with state captured in handler closures, not Axum missing state. Install adapter-owned method/fallback handlers under the nested router so all promised post-prefix failures are JSON. Do not poll the body before access.

Derive the Keylink key as domain-separated BLAKE3 over length-delimited principal bytes and external key; encode a fixed safe textual digest accepted by `IdempotencyKey`. Never log inputs/digest. Map exact Keylink variants to the goal table; raw messages never cross the edge.

- [ ] **Step 4: Run each focused test green, then the full test binary**

Use synchronization rather than wall-clock upper bounds. Run Clippy for `keylink-http` after tests.

- [ ] **Step 5: Watch ordering/key/error guards red, anchor, commit**

Inventory every new router/error guard; observe each coherent break red alone (including access/body order, challenge construction/caching, key derivation, fallbacks, content types, limits and raw-message suppression), restore, register all anchors, commit.

### Task 5: Prove concurrency and disconnect cancellation

**Files:**
- Create: `keylink-http/tests/cancellation.rs`
- Modify: `keylink-http/tests/http.rs` only for shared test support if necessary

- [ ] **Step 1: Write individually terminating tests**

Concurrency test gates two backend calls on barriers and proves both enter before either is released. Cancellation test opens raw TCP HTTP/1, waits until the Keylink call future/stream is active, closes the client before response, and uses a drop sentinel around the adapter-owned invoke future to prove Hyper drops it and no adapter task retains it. Assert only the documented claim: already-started backend work may continue/settle Interrupted; no rollback.

- [ ] **Step 2: Run each alone and observe red without timeout/kill**

Tests own bounded protocol events/channels and explicit terminators. Do not assert a wall-clock completion ceiling.

- [ ] **Step 3: Make the minimum implementation adjustment**

Handlers await invoke directly. No `tokio::spawn`, detached JoinHandle, client mutex, or shared serialization lock is added.

- [ ] **Step 4: Run individual tests green, then package tests**

- [ ] **Step 5: Register every concurrency/cancellation guard after individual direct-await/no-mutex/drop-sentinel breaks, then commit**

### Task 6: Documentation, example, curl smoke, and package integration

**Files:**
- Create: `keylink-http/examples/inventory.rs`
- Modify: `keylink-http/src/lib.rs`, `README.md`, `CONTRIBUTING.md`
- Modify: `mise.toml` unconditionally so docs and examples cover `keylink-http`
- Update ignored local `.tmp/keylink-architecture/index.html` only if present

- [ ] **Step 1: Write failing doctest/example/package checks**

Document exact startup, generated `External` marker use, outer middleware Principal handoff, Access, limits, calls, error envelope, identity distinction, namespaced idempotency and one-key-per-logical-call, process-lifetime replay, cancellation, JS integer precision, and unsupported capabilities.

- [ ] **Step 2: Observe docs/example red, then add minimal runnable example**

The example serves a real Inventory service and prints its loopback address. It supports deterministic curl normal/retry/denied/malformed/unsupported-tell smoke.

- [ ] **Step 3: Update `mise.toml`: add `-p keylink-http` to docs (with `#![warn(missing_docs)]` already enabled) and a deterministic terminating `keylink-http` example self-test to `tasks.examples`. Then run focused docs/example/package checks and real curl smoke**

Capture server/client output in `/tmp` with terminators. Verify no Rust client is used for smoke calls.

- [ ] **Step 4: Update the ignored architecture artifact locally if present**

Add HTTP gateway as an adapter/client path, not direct ingress. Verify it remains ignored/untracked.

- [ ] **Step 5: Commit docs/example**

### Task 7: Integrated verification and review/improve waves

**Files:** all files changed above; no new scope.

- [ ] **Step 1: Run focused integrated verification**

Run changed package tests, existing affected keylink tests, `mise run anchors`, focused Clippy/docs, and dependency trees. Capture complete logs with `EXIT=`. Do not run the full hook-owned gate immediately before commit/push.

- [ ] **Step 2: Commit any integration-only fixes**

Use TDD for every behavior fix and focused verification.

- [ ] **Step 3: Run Brooks merge review plus requesting-code-review**

Review `origin/main..HEAD` against the goal. Report all Critical/Major/Minor findings. Apply valuable findings through receiving-code-review one by one with tests. Re-review only for Critical/structural fixes.

- [ ] **Step 4: Run Brooks test-quality and invariant-compliance waves**

Test review checks real boundaries, no mocked core, liveness, brittleness, and coverage illusions. Invariant reviewer grades every goal invariant with machine evidence. Fix every invariant miss; repeat only when Critical/structural.

- [ ] **Step 5: Push through authoritative pre-push gate**

Capture the complete hook log with `env -u FORCE_COLOR`, never interrupt it, and verify remote SHA exactly equals HEAD. Use no bypass except repository-documented transport exception after a fully green same-HEAD gate.

- [ ] **Step 6: Open, review, and merge PR**

PR targets `main`, includes session id, goal/plan paths, review counts, red/green evidence, curl output, dependency trees, and gate log. Merge only with zero open Critical/Major and exact green HEAD.

- [ ] **Step 7: Verify merge and clean up**

Fast-forward root main, prove PR merged and commit ancestry, remove worktree with `wt`, delete merged local/remote branch, prune, and verify all absent. Preserve only the ignored local architecture artifact.
