# HTTP Gateway Extensions Implementation Plan

> Execute this plan sequentially. The tasks share generated macros and gateway
> internals, so parallel implementation in one checkout is unsafe.

**Goal:** Complete the HTTP gateway with bounded streaming, verified blob ranges,
keyed actors, and optional OpenAPI 3.1 and JSON Schema generation.

**Architecture:** One cfg-aware generated method inventory drives exact client
calls. `keylink-http` adapts that inventory to authenticated HTTP routes.
`keylink-openapi` consumes the final runtime inventory without entering the
runtime dependency graph. `BlobReader` provides the missing verified range seam.

**Required rules:** Read `AGENTS.md`, `CONTRIBUTING.md`, and the tracked
extension goal. The local `.tmp` audits are background evidence, not normative
inputs. This tracked plan contains every required ruling and proof.

Use this micro-cycle for each checkbox behavior:

1. Add only the named test or finite harness.
2. Run its exact test name and capture the expected red with an `EXIT=` marker.
3. For liveness tests, prove the channel, barrier, protocol event, or drop-sentinel terminator in that individual run.
4. Add the minimum implementation.
5. Run the exact test green.
6. Break the guard once, run that exact test red, restore, and register its anchor.
7. Only then add the next behavior.

Never add a group of nonexistent-API tests whose first compile error masks the
rest. Never run a broad async filter until every new test in it terminates alone.
Use focused checks while iterating. Do not run a duplicate full suite immediately
before a commit or push because hooks own that gate. Do not price, schedule, or
report ALPN movement.

## Task 0: finish the unary remediation gate

**Modify only the current unary review scope.**

- [ ] Return `403` immediately for forbidden access. Prove zero body polls and zero dispatch.
- [ ] Make bounded JSON serialization allocate incrementally, not with `Vec::with_capacity(limit)`.
- [ ] Generate argument objects with `deny_unknown_fields`. Pin missing versus explicit-null `Option<T>` behavior.
- [ ] Keep `JsonCodec` and its error private to the adapter implementation.
- [ ] Parse exactly one valid JSON content type. Reject duplicates and malformed values.
- [ ] Distinguish body collection failure from observed size overflow in stable error mapping.
- [ ] Bound method path segments before allocation, including limit−1, limit, and limit+1 encoded cases.
- [ ] Remove the unused direct `bytes` dependency.
- [ ] Derive the example principal only from verified middleware state.
- [ ] Add the package README and deterministic real curl smoke.
- [ ] Expand anchor population scanning to include `keylink-http`.
- [ ] Complete access metadata, HTTP compatibility, error redaction, bounds, generated-surface, concurrency, and cancellation tests.
- [ ] Run every new unary test individually, then focused package and affected `keylink` tests.
- [ ] Complete each watched-red mutation and update the red-green journal.
- [ ] Request focused Brooks test, invariant, and merge rereviews.
- [ ] Resolve every Critical and Major finding, then commit the unary remediation.

**Commit:** `fix(http): close unary gateway review findings`

Do not begin Task 1 while the unary tree is uncommitted or has an unresolved
Critical or Major finding.

## Task 1: establish the runtime contract and unified generated seam

**Files:**

- Modify `keylink/src/external.rs`, `keylink/src/macros.rs`, `keylink/src/lib.rs`.
- Modify focused tests in `keylink/tests/api.rs` and `keylink/tests/protocol.rs`.
- Add `keylink-http/src/contract.rs` and one private bounded path parser.
- Migrate unary routing and error rendering in `keylink-http/src/gateway.rs` and `src/error.rs`.
- Modify `keylink-http/src/access.rs`, `src/limits.rs`, `src/service.rs`, and focused tests.

Use the shared micro-cycle. In order:

- [ ] Add one cfg-aware `ExternalMethod` descriptor with declared name and `ExternalKind::{Rpc, Send, Recv, Blob}`.
- [ ] Add one generated invocation seam with JSON, upload, one-shot download, and blob output variants.
- [ ] Prove exact client calls, exact response extraction, wrong-output failure, and absence of tell, feeds, and status.
- [ ] Prove method-level Serde rename stays wire-only. Prove raw argument identifiers use the Serde spelling without `r#`.
- [ ] Pin runtime argument behavior: unknown fields fail; a missing `Option<T>` and explicit `null` both decode as `None`.
- [ ] Preserve the existing generated `External` marker and unary calling behavior. Do not use enum ordinals or Request JSON.
- [ ] Add a schema-free hidden route/error inventory. Migrate unary route installation, `Allow`, media types, error codes, statuses, messages, and response headers to it now.
- [ ] Add `HttpSurface`, an immutable validated selection with prefix, `Addressing::{Singleton, Actors}`, and optional `StreamingLimits`.
- [ ] Make runtime router construction consume `HttpSurface`. Singleton and actor surfaces expose only rpc until streaming limits exist.
- [ ] Add one bounded raw-segment decoder. Cap encoded input before allocation. Derive method caps from the longest enabled method, actor caps from `MAX_KEY_LEN`, and blob IDs from their exact fixed shape.
- [ ] Run `raw_segment_parser_enforces_each_shape_before_allocation` as one synchronous table: method encoded limit−1/limit/limit+1; actor encoded limit−1/limit/limit+1; actor decoded 127/128/129 UTF-8 bytes including multibyte values; blob ID 63/64/65 bytes, uppercase, nonhex, and percent-encoded input. Record whether allocation/decoding began.
- [ ] Run one route integration cycle per family: `singleton_routes_use_bounded_raw_parser`, `actor_routes_use_bounded_raw_parser`, and `blob_byte_routes_use_fixed_shape_parser`. In each, replace only that family's shared-parser call with the old unbounded decoder and observe the exact test red.
- [ ] Add runtime/contract parity tests and configured enabled/disabled surface tests.
- [ ] Inspect `cargo tree -p keylink` to prove no HTTP dependency entered the core.
- [ ] Watch and anchor wrong kind, omitted cfg, included tell/status, wrong response extraction, wire-name leakage, runtime/contract drift, unbounded preallocation, and streaming without limits.

**Commit:** `feat(http): define the generated and runtime surface contract`

Every later route and stable error must enter through this settled contract.
Task 7 consumes it and must not refactor runtime routes.

## Task 2: add singleton `send` streaming

**Files:**

- Modify `keylink-http/Cargo.toml`, `src/lib.rs`, `src/limits.rs`, `src/service.rs`, `src/gateway.rs`, and `src/error.rs`.
- Prefer one focused private `src/stream.rs` for metadata and body adapters.
- Modify `keylink-http/tests/api.rs`.
- Add or modify `keylink-http/tests/streaming.rs` and `tests/cancellation.rs`.

- [ ] Add red constructor tests for explicit finite `StreamingLimits` and opt-in route installation.
- [ ] Add red tests for `POST /send/{Method}`, exact method-kind selection, `application/octet-stream`, and required `Keylink-Arguments`.
- [ ] Define `Keylink-Arguments` as one bounded, unpadded base64url JSON object. Reject missing, duplicate, malformed, and over-limit values.
- [ ] Pin canonical metadata grammar: no padding, standard alphabet, whitespace, noncanonical trailing bits, or non-UTF-8 decoded bytes. Check the encoded ceiling before decode with checked `4 * ceil(decoded/3)` arithmetic, then check decoded limit−1/limit/limit+1.
- [ ] Require exactly one `application/octet-stream` value. Match its token case-insensitively and reject parameters, duplicates, malformed fields, and suffix types.
- [ ] Reject a declared `Content-Length` above the upload limit before polling the body.
- [ ] Count unknown-length uploads and stop at the inclusive configured limit.
- [ ] Adapt the owned HTTP body directly to `AsyncRead`. Do not spawn a forwarding task or aggregate bytes.
- [ ] Pass the HTTP content length to Keylink `Body` when present.
- [ ] Await the exact generated `send` client call and return only its bounded JSON result.
- [ ] Reject `Idempotency-Key`, `Range`, and `If-Range` before body polling. A core RPC protocol change is out of scope.
- [ ] Prove backpressure with gates or channels, not wall-clock ceilings.
- [ ] Prove client disconnect drops the adapter-owned invocation and body reader.
- [ ] Watch and anchor aggregation, missing observed-byte cap, detached invocation, wrong body length, accepted idempotency, accepted Range, and missing encoded-header precheck.


### Task 2 exact test-cycle map

Each row is one complete red → green → watched-red cycle before the next row.
Table-driven grammar cases can share one synchronous test because they have no
liveness state.

| Exact test name | Proof and finite terminator |
|---|---|
| `streaming_limits_reject_each_zero_field` | synchronous constructor table |
| `streaming_routes_require_explicit_limits` | synchronous router inventory assertion |
| `send_route_selects_only_enabled_send_methods` | one request completes through a ready fake invocation |
| `send_requires_one_exact_octet_stream_content_type` | synchronous request table; zero body polls |
| `send_arguments_header_enforces_canonical_base64url` | synchronous header table; zero body polls |
| `send_arguments_header_checks_encoded_limit_before_decode` | allocation probe records no decoder entry |
| `send_arguments_json_accepts_limit_and_rejects_limit_plus_one` | synchronous decoded-size table |
| `send_rejects_idempotency_and_range_before_body_poll` | recording body reports zero polls |
| `send_rejects_declared_oversize_before_body_poll` | recording body reports zero polls |
| `send_streams_declared_length_and_exact_result` | handler sends `entered`; test releases `read_body`; handler sends `finished` |
| `send_streams_unknown_length_to_inclusive_limit` | source sends `last_chunk`; handler sends `finished` |
| `send_unknown_length_aborts_at_limit_plus_one` | source sends `overflow_chunk`; handler reports reader error and releases request |
| `send_backpressure_stops_http_body_polling` | body reports first poll; handler closes `read_gate`; test observes no second poll, then opens gate |
| `send_disconnect_drops_invocation_and_reader` | invocation sends `entered`; raw socket closes; drop sentinel sends `dropped` |
| `send_body_error_reaches_keylink_reader` | source sends error frame; handler reports reader error and releases request |

**Commit:** `feat(http): stream generated send uploads`

Actor `send` remains absent in all later tasks.

## Task 3: add one-shot `recv` streaming

**Files:** Same runtime and focused test files as Task 2.

- [ ] Add red tests for `POST /recv/{Method}` with bounded JSON arguments.
- [ ] Stream `BodyReader` directly as `application/octet-stream`.
- [ ] Encode the declared return value in one bounded `Keylink-Result` base64url JSON header.
- [ ] Prove the encoded response-header ceiling before headers at decoded limit−1/limit/limit+1 with the same canonical base64url codec.
- [ ] Use `BodyReader::declared_len()` for exact `Content-Length` when present.
- [ ] Reject a declared download length over the limit before response headers.
- [ ] Count unknown-length bytes and terminate with a body error when the limit is crossed.
- [ ] Reject `Idempotency-Key`, `Range`, and `If-Range` before request-body polling.
- [ ] Do not claim replay or resume for `recv`.
- [ ] Prove one-chunk polling, downstream backpressure, late-error propagation, and response-drop cancellation.
- [ ] Watch and anchor body aggregation, swallowed late errors, detached pumps, false idempotency, and missing length checks.


### Task 3 exact test-cycle map

Each row is one complete cycle.

| Exact test name | Proof and finite terminator |
|---|---|
| `recv_route_selects_only_enabled_recv_methods` | ready fake invocation completes the request |
| `recv_rejects_idempotency_and_ranges_before_argument_poll` | recording argument body reports zero polls |
| `recv_result_header_enforces_encoded_ceiling` | ready body plus synchronous boundary table |
| `recv_streams_known_length_and_exact_result_header` | handler sends `invoked`; test releases `emit_chunks`; reader sends `eof` |
| `recv_streams_unknown_length_to_inclusive_limit` | reader sends `last_chunk` then `eof` |
| `recv_rejects_declared_oversize_before_headers` | invocation returns reader; response-start probe remains unset |
| `recv_unknown_length_aborts_at_limit_plus_one` | reader sends overflow chunk; HTTP body reports error; reader drop sentinel fires |
| `recv_propagates_truncation_as_body_error` | reader sends declared-short EOF; HTTP body reports error and closes |
| `recv_backpressure_stops_keylink_reader_polling` | HTTP client holds `read_gate`; reader reports first poll and no second poll; test opens gate |
| `recv_disconnect_drops_body_reader` | reader sends `first_chunk`; raw client closes; reader drop sentinel sends `dropped` |

**Commit:** `feat(http): stream generated recv responses`

The generated output model from this task must be reusable by keyed actors.

## Task 4: add verified core blob readers and byte ranges

**Files:**

- Modify `keylink/src/blobs.rs` and curated exports in `keylink/src/lib.rs`.
- Modify focused blob tests, normally `keylink/tests/blobs.rs`.
- Append current evidence to blob documentation only where needed. Do not rewrite history.

Use the shared micro-cycle in this order:

- [ ] Prove that a returned `BlobHandle` can clone and open independent reads.
- [ ] Make `open` and `open_range` complete `get_verified_size`, compare the data-response size, and only then return `BlobReader`.
- [ ] Add a deterministic fake iroh-blobs protocol peer that accepts the real `GetRequest` and sends malicious size, corrupt Bao, and truncated Bao responses to the real client FSM. Give each case a protocol-event terminator.
- [ ] Prove that no reader or HTTP-safe length exists after a malicious size mismatch.
- [ ] Prove verified full reads and exact half-open aligned, unaligned, first-byte, last-byte, and single-byte ranges.
- [ ] Cover an empty blob. Full read returns zero bytes; every nonempty range is unsatisfiable without underflow.
- [ ] Reject empty, reversed, overflowing, and out-of-bounds ranges after size preflight but before the data-range request.
- [ ] Verify contiguous offsets, exact selected length, and first/last leaf slicing.
- [ ] Keep root-only admission. Prove arbitrary finite root chunk ranges pass while child and hash-sequence selection fail.
- [ ] Make `BlobReader` own the connection and get state. Dropping it cancels that read.
- [ ] Return one verified `Bytes` chunk at a time and refactor whole-blob `copy_to` through the same implementation.
- [ ] Keep `fetch_into` unchanged and add the missing real interrupted-`fetch_into` resume characterization test. If it fails, narrow its rustdoc instead of repeating an unproved promise.
- [ ] Watch and anchor missing verified-size comparison, widened admission, unaligned slicing, offset gaps, short EOF, and detached copying. Treat fake-peer Bao corruption as dependency-path evidence, not a Keylink-owned literal anchor.


### Task 4 exact test-cycle map

Each row is one complete cycle. The fake peer sends `request_read` after it
parses the real request. The test sends `write_response` only after that event,
so every malicious-peer case has a finite release path.

| Exact test name | Proof and finite terminator |
|---|---|
| `blob_handle_clone_opens_independent_full_reads` | each peer request sends `request_read`; peer sends complete response and `closed` |
| `blob_open_rejects_data_size_different_from_verified_size` | peer completes size preflight, sends mismatched data header, then `closed` |
| `blob_open_rejects_corrupt_bao_from_real_fsm` | peer sends one mutated Bao frame and `closed`; client reports verification error |
| `blob_open_rejects_truncated_bao_from_real_fsm` | peer sends prefix then QUIC finish and `closed`; client reports truncation |
| `blob_reader_reads_verified_full_content` | peer sends complete response; reader emits exact length then EOF |
| `blob_reader_reads_aligned_range` | peer sends selected chunks then `closed` |
| `blob_reader_slices_unaligned_first_and_last_chunks` | peer sends covering chunks then `closed` |
| `blob_reader_reads_first_last_and_single_byte_ranges` | synchronous subcases each use a complete peer response and EOF |
| `blob_reader_handles_empty_blob_without_underflow` | verified size returns zero; reader emits EOF |
| `blob_reader_rejects_invalid_ranges_before_data_request` | size preflight sends `closed`; peer data-request counter remains zero |
| `blob_reader_rejects_gap_and_out_of_order_offsets` | injected verified-item harness sends bad offset then `harness_done` |
| `blob_reader_rejects_short_eof` | injected item harness sends prefix then `harness_done` |
| `blob_admission_accepts_only_finite_root_ranges` | provider event callback sends one decision for each synchronous request case |
| `dropping_blob_reader_cancels_get_and_allows_reopen` | first reader sends `opened`; drop closes stream; peer sends `first_closed`; second read reaches EOF |
| `interrupted_fetch_into_resumes_from_store_progress` | first fetch sends `stored_prefix`; transport closes; second fetch sends `finished`; store byte counter proves suffix only |

**Commit:** `feat: expose verified blob range readers`

Do not advertise HTTP ranges before this task is green.

## Task 5: add principal-bound blob HTTP resources

**Files:**

- Modify the settled `keylink-http/src/contract.rs` inventory first for each route/error.
- Modify `keylink-http/src/gateway.rs`, `src/limits.rs`, `src/error.rs`, and `src/lib.rs`.
- Add one private `keylink-http/src/blob.rs` for tickets, ranges, and conditions.
- Add `keylink-http/tests/blobs.rs` and focused cancellation tests.

Route input grammar after method, verb, principal, and Access:

| Route | Body | Idempotency | Range inputs |
|---|---|---|---|
| blob lookup POST | bounded JSON | optional | reject `Range` and `If-Range` |
| blob GET | empty only | reject | one Range plus conditions |
| blob HEAD | empty only | reject | ignore `Range` and `If-Range`; apply `If-None-Match` to full representation |

`Content-Length: 0` is empty. A positive length or transfer-encoded body is not.
Use the shared micro-cycle in this order:

- [ ] Return the exact blob-method JSON value with `201 Created` and a prefix-preserving `Location`. Do not fetch on POST.
- [ ] Insert only the returned `BlobHandle` after invocation and result encoding succeed.
- [ ] Implement a finite FIFO ticket table keyed by principal, method, optional actor, and blob ID.
- [ ] Give each insertion a generation. Replacement leaves one live entry and moves it to newest. Overflow evicts the oldest live entry.
- [ ] Clone `(generation, handle)` under the lock. On refusal, compare-and-remove only that generation so a stale request cannot delete a concurrent refresh.
- [ ] Never construct a handle from a path ID. Run Access before exact ticket lookup.
- [ ] For every GET and HEAD, perform a fresh verified-length availability check. On grant refusal, generation-safe removal returns `410`; a later lookup returns `404`.
- [ ] GET emits strong content-address ETag, `private, no-transform`, verified `Content-Length`, and `Accept-Ranges: bytes`.
- [ ] After an exact available ticket, parse combined `If-None-Match` fields as the full OWS/entity-tag list. Support `*` and weak comparison. A match returns `304` before Range; malformed grammar returns `400`.
- [ ] Then parse one `bytes=` Range. Return exact `206`/`Content-Range`, `416` for valid unsatisfiable ranges, and `400` for malformed, overflow, wrong-unit, duplicate, or multiple ranges.
- [ ] Apply `If-Range` only after a valid Range. Only one exact strong ETag permits `206`; weak, date, malformed, duplicate, or nonmatching values produce full `200`.
- [ ] HEAD performs the same fresh availability and `If-None-Match` checks, returns full-representation metadata, ignores Range/If-Range, and never opens the byte reader.
- [ ] Cover empty content: GET/HEAD `200` with length zero; every valid nonempty Range `416 bytes */0`; matching condition `304`.
- [ ] Prove cross-principal, cross-method, malformed, guessed, and evicted IDs have one redacted `404` shape.
- [ ] Prove concurrent downloads hold no ticket lock across await. A disconnected range read leaves its ticket available while the grant survives.
- [ ] Watch and anchor ID-only keys, access-after-lookup, fabricated handles, stale-generation deletion, stale HEAD, incorrect condition precedence, weak If-Range, lock-across-await, false `200`, and leaked grant errors.


### Task 5 exact test-cycle map

Each row is one complete cycle. Parser matrices are synchronous. Network and
concurrency rows name both the observation event and the release event.

| Exact test name | Proof and finite terminator |
|---|---|
| `blob_lookup_returns_exact_value_location_and_no_fetch` | ready lookup completes; blob peer request counter remains zero |
| `blob_lookup_rejects_range_headers_before_body_poll` | recording body reports zero polls |
| `blob_ticket_fifo_replacement_moves_entry_to_newest` | synchronous ticket-book table |
| `blob_ticket_overflow_evicts_oldest_live_generation` | synchronous ticket-book table |
| `stale_refusal_cannot_delete_refreshed_ticket` | old GET sends `handle_cloned`; POST sends `reinserted`; test releases old refusal; new lookup succeeds |
| `blob_get_and_head_reject_body_and_idempotency_after_access` | Access sends `authorized`; recording body reports zero polls and ticket counter stays zero |
| `blob_access_runs_before_ticket_lookup_and_id_details` | Access gate sends `entered`; ticket probe remains unset; test releases denial |
| `blob_head_rechecks_grant_and_removes_stale_generation` | first HEAD caches length; test evicts grant; second preflight sends refusal; compare-remove sends `removed` |
| `blob_get_serves_full_verified_representation` | reader sends `opened`; peer reaches EOF; HTTP body reaches EOF |
| `blob_if_none_match_parses_lists_and_precedes_range` | synchronous condition matrix after ready availability probe |
| `blob_range_parser_classifies_single_malformed_and_unsatisfiable_ranges` | synchronous parser matrix |
| `blob_if_range_requires_one_matching_strong_etag` | synchronous condition matrix with ready reader |
| `blob_get_serves_exact_unaligned_range` | reader sends `opened_range`; peer reaches EOF; HTTP body reaches EOF |
| `blob_head_ignores_range_and_never_opens_reader` | availability probe sends `verified`; reader-open counter remains zero |
| `empty_blob_get_head_conditions_and_ranges_are_exact` | ready zero-size preflight; synchronous response matrix |
| `blob_ticket_absence_is_identical_across_guess_principal_method_and_eviction` | ready Access and synchronous response comparison |
| `slow_blob_download_does_not_hold_ticket_lock` | first reader sends `blocked`; second lookup sends `completed`; test releases first reader |
| `blob_disconnect_drops_reader_but_keeps_ticket_for_range_resume` | first reader sends `opened`; raw socket closes; drop sentinel sends `dropped`; second range reaches EOF |

**Commit:** `feat(http): proxy principal-bound blob ranges`

## Task 6: add keyed-actor HTTP routing

**Files:**

- Modify `keylink/src/external.rs`, `keylink/src/macros.rs`, and focused generated tests.
- Modify `keylink-http/src/access.rs`, `src/gateway.rs`, `src/error.rs`, and stream/blob helpers.
- Modify `keylink-http/tests/api.rs`, `tests/http.rs`, `tests/streaming.rs`, `tests/blobs.rs`, and `tests/cancellation.rs`.
- Add one focused keyed example only if it improves the existing example.

- [ ] Add generated reds for actor support on `rpc`, `recv`, and `blob` only.
- [ ] Add an imported-service fixture whose defining crate emits the actor seam and whose consumer uses its generated `External` and `RegistryClient`.
- [ ] Add compile-fail probes that `Addressed<Request>`, `Request`, a manual `Rpc`, `ActorRef`, and `RegistryClient` do not themselves implement the external actor trait. Only the generated local `External` marker does.
- [ ] Add one hidden generated actor invocation seam on the existing `External` marker.
- [ ] Invoke the exact generated `RegistryClient::at(actor)` method. Never serialize `Addressed<Request>` or reach through private fields.
- [ ] Before `RegistryClient::at(actor)`, derive an optional Keylink key from length-delimited principal, service, method, addressing-mode tag, actor bytes, and external key. Use a versioned domain separator. Arguments stay outside the identity.
- [ ] Add `Call.actor: Option<&ActorId>` and reuse the mandatory `Access` interface.
- [ ] Install `POST /{actor_id}/rpc/{Method}`, `POST /{actor_id}/recv/{Method}`, `POST /{actor_id}/blob/{Method}`, and actor-context blob byte routes.
- [ ] Parse raw path segments with the Task 1 decoder so `%2F` remains one actor ID. Validate the HTTP verb before actor decode, bound the encoded form, decode once, and construct `ActorId`. Pin wrong-verb plus malformed-actor precedence.
- [ ] Select a supported method before actor validation. Validate actor syntax before access. Check existence only through the authorized generated call.
- [ ] Prove denied access cannot mount a lazy actor or inspect registry state.
- [ ] Map malformed IDs, no-such actor, capacity, mailbox, withheld lifecycle, timeout, backend denial, and transport through stable redacted categories.
- [ ] Preserve the absence of actor `send`, `tell`, status, lifecycle, discovery, and administration.
- [ ] Include actor context in blob tickets and repeat actor-aware `Access` for GET and HEAD. Permit `Location` to reflect only the caller-supplied canonical encoded actor segment; never emit actor IDs in errors or logs.
- [ ] Prove mounted, lazy, missing, stopped, capacity, mailbox, backend auth, concurrency, cancellation, and call-identity-bound idempotency with real Keylink registries.
- [ ] Add a revocation test: record under actor A, deny A, permit actor B, reuse the external key, and prove no A response replays. Repeat with a blob result and prove no B-context ticket reaches A bytes.
- [ ] Prove singleton keys also bind service and method, so cross-method reuse cannot replay another route.
- [ ] Watch and anchor Axum-decoded `%2F`, missing actor access context, preflight existence, denied lazy mount, Addressed construction, omitted actor/method key identity, actor send/status admission, leaking errors, and cross-actor blob lookup.

**Commit:** `feat(http): route keyed actor calls`

## Task 7: generate optional OpenAPI 3.1 and JSON Schema

Start only after Tasks 1 through 6 define the final runtime route inventory.
Consume the Task 1 contract and `HttpSurface`; do not refactor runtime routes or
create another capability table.

**Files:**

- Modify root `Cargo.toml`, `Cargo.lock`, and deny configuration only when required by the actual license check.
- Add `keylink-openapi/Cargo.toml`, `src/lib.rs`, and focused document modules.
- Modify `keylink/src/macros.rs` for an opt-in declaration callback.
- Modify the existing hidden `keylink-http/src/contract.rs` only when a schema-free documentation accessor is required.
- Add `keylink-openapi/tests/openapi.rs` and canonical snapshots.

- [ ] Add compile reds proving ordinary `service!` users need no schema dependency.
- [ ] Settle the macro seam first: each generated service module re-exports one uniquely module-scoped hidden callback; `keylink_openapi::document_service!(module)` is invoked only in the service-defining crate and implements the foreign trait for its local `External`.
- [ ] Add compile proofs before schema work: ordinary service users need no schema crate; two documented services coexist; the defining crate can invoke each callback; downstream crates consume the impl; downstream invocation fails with the expected orphan diagnostic.
- [ ] Do not use a crate-root exported callback, a fallback wrapper, or repeated service declarations.
- [ ] Use `schemars` draft 2020-12 schemas for deserialize request contracts and serialize response contracts.
- [ ] Keep `schemars` and the OpenAPI model dependency in `keylink-openapi` only.
- [ ] Require `JsonSchema` only when document generation is selected.
- [ ] Add `document_service_reports_missing_json_schema_bound` as a compile-fail fixture that names the exposed method and nested DTO lacking `JsonSchema`.
- [ ] Build request schemas for deserialization and response schemas for serialization. Model missing `Option<T>` as optional nullable and explicit null as valid, matching Task 1 runtime tests. Strip `r#` from raw identifiers.
- [ ] Run runtime/schema parity cycles for nested `serde(rename)`, `rename_all`, `default`, `flatten`, `deny_unknown_fields`, tagged and untagged enums, skipped fields, and asymmetric skip-serialize/skip-deserialize DTOs. If schemars cannot represent one used attribute, require an explicit schemars override or manual impl; never emit an unconstrained fallback.
- [ ] Generate OpenAPI `3.1.0`, deterministic paths, stable operation IDs, and one service tag.
- [ ] Document the exact singleton and actor route intersections implemented by Tasks 2 through 6.
- [ ] Model stream bodies as binary. Describe base64url metadata headers with references to their decoded schemas.
- [ ] Document actor and blob path grammar without examples that disclose identities.
- [ ] Make `Security` a required argument of `DocumentConfig::new`, so document construction without an honest auth representation cannot compile or succeed. Represent unknown deployment authentication with explicit extensions, not invented credentials.
- [ ] Consume the existing hidden runtime route/error inventory and the same configured `HttpSurface`. Snapshot rpc-only, streaming-enabled singleton, rpc-only actor, and streaming-enabled actor surfaces with their finite public limits.
- [ ] Build one shared redacted `ErrorEnvelope` schema and per-operation status sets.
- [ ] Reject path, operation ID, component, unresolved reference, and incompatible schema collisions.
- [ ] Support recursive and concrete generic DTOs when `JsonSchema` provides stable unique identities.
- [ ] Keep logical schema IDs internally. Render and resolver-test every OpenAPI ref as `#/components/schemas/...` and every standalone ref as `#/$defs/...`, including direct recursion, mutual recursion, and generic components.
- [ ] Describe actor IDs as decoded logical strings, percent-encoded once by URI serialization. State the UTF-8 byte cap separately from character `maxLength`.
- [ ] Use one raw-octet schema helper for send, recv, and blob: `type: string` plus `contentMediaType: application/octet-stream`.
- [ ] Pin component digest naming to first six BLAKE3 bytes over `keylink-openapi/component/v1\0`, big-endian `u64` input length, and UTF-8 schema ID. Snapshot a known vector.
- [ ] Provide deterministic build-time OpenAPI and standalone schema values and bytes.
- [ ] Add an explicit immutable router for `GET/HEAD /openapi.json` and `/schema.json`. Do not auto-mount it.
- [ ] Prove exact endpoint bytes and media types: `application/vnd.oai.openapi+json;version=3.1` and `application/schema+json`. HEAD returns the same headers without a body. Watch each content type red separately.
- [ ] After the minimal manifest and lockfile change, run focused `mise run deny` and `mise run msrv` before document assembly.
- [ ] Do not add Swagger UI, YAML, CORS, runtime reflection, or generated SDKs.
- [ ] Add canonical singleton/streaming, keyed actor, and standalone schema snapshots.
- [ ] Generate twice and in a subprocess to prove byte determinism.
- [ ] Inspect `cargo tree -p keylink` and `cargo tree -p keylink-http` to prove no schema edge.
- [ ] Watch and anchor missing cfg, tell/status inclusion, wire-name route leakage, wrong request contract, required `Option<T>` or rejected missing `Option<T>`, recursion inlining, collision overwrite, actor send docs, binary-as-JSON, invented Bearer auth, runtime/doc error drift, nondeterministic sorting, auto-mounted docs, and each documentation endpoint content type.

**Commit:** `feat: generate HTTP OpenAPI and JSON Schema`

Use the smallest typed OpenAPI model that preserves every selected schemars
keyword. If conversion through `utoipa` is not lossless, use adapter-owned Serde
DTOs with `Value` only at schema leaves. Never erase unsupported schema keywords.

## Task 8: integrated documentation and examples

**Files:**

- Modify `keylink-http/src/lib.rs`, its package README, root `README.md`, and `CONTRIBUTING.md`.
- Modify the existing inventory example or add one focused extension example.
- Add a deterministic real curl smoke.
- Modify mise tasks only where the documented package or example inventory requires it.

- [ ] Document every route, content type, metadata header, finite limit, stable error, idempotency rule, cancellation guarantee, and unsupported intersection.
- [ ] Document the exact blob resume lifetime and late-stream error limitation.
- [ ] Document actor ID encoding, privacy, per-actor access, and absent actor send/status/admin routes.
- [ ] Document OpenAPI schema bounds, custom Serde limitations, authentication honesty, JS integer precision, and snapshot policy.
- [ ] Document browser upload compatibility, CORS headers, proxy buffering, timeout, and private cache requirements.
- [ ] Use a real principal derivation in the example. Do not derive identity from unverified request data.
- [ ] Make the smoke drive rpc, send, recv, blob lookup, full and range GET, conditional GET, actor rpc/recv/blob, denied calls, malformed inputs, and spec retrieval.
- [ ] Keep the architecture HTML local and ignored. Update it only if it exists in this checkout and the public comparison became inaccurate.
- [ ] Run focused docs, example, package, and browser checks.
- [ ] Watch and anchor the smoke and package-document inventory where a real regression could otherwise pass.

**Commit:** `docs(http): document the complete gateway`

## Task 9: boundary reviews, authoritative gate, and delivery

- [ ] Run the single mandatory Brooks merge gate, plus test-quality and invariant-compliance reviews, against the complete branch.
- [ ] Apply every finding that adds real value. The reviewer must not fix its own findings.
- [ ] Resolve all Critical and Major findings. Re-review only Critical or structural fixes and their blast radius.
- [ ] Run focused verification for review fixes. Do not run a manual full suite immediately before commit or push.
- [ ] Commit the final focused fixes and let the commit hook run its checks.
- [ ] Push once with `env -u FORCE_COLOR` and capture the complete hook output in `/tmp/keylink-http-extensions-pre-push.log` with an `EXIT=` marker.
- [ ] Never interrupt the gate. Read the full captured failure before any retry.
- [ ] Compare `git rev-parse HEAD` with `git ls-remote --heads origin <branch>` and require an exact match.
- [ ] Open the PR with a file-backed body that lists routes, invariants, focused evidence, watched-red anchors, reviews, dependency checks, and gate log.
- [ ] Confirm that the PR HEAD is the reviewed SHA. Run only targeted re-review if a Critical or structural fix changed that SHA. Merge only after the single gate is green, and verify the merge against the default branch.
- [ ] Remove the merged worktree and local/remote branch with `wt` and exact merge proof.

No GitHub Actions result exists. The pre-push gate is the authoritative aggregate
evidence. Do not run or update literal ALPN pin tests.
