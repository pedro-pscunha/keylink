# Delivery record: Tasks 4-9 of the HTTP gateway extensions

Every question, assumption, deviation and blast-radius widening annotated while
delivering `docs/superpowers/plans/2026-08-13-http-gateway-extensions.md` against
`docs/goals/2026-08-13-http-gateway-autonomous-delivery.md`.

The goal's execution-interaction policy is no-questions: annotate as you go and
surface afterwards. This file is that surfacing, kept in the tree because a
commit message is not where the next reader looks.

## Blast-radius widenings

Five, each reported in the commit that made it.

| Widening | Task | Why |
|---|---|---|
| `keylink/src/error.rs` | 4 | A new `Error::BlobUnverified` variant with a `BlobFault` enum. Task 4's file list did not name it, and a verified reader has nothing to return without it. |
| `fixtures/imported-ledger`, root `Cargo.toml` | 6 | The checkbox asks for a DEFINING crate and a CONSUMER crate. That cannot be one crate. |
| `.config/nextest.toml` (new) | 9 | `keylink::drift` races itself; the gate cannot pass without a test group. On no task's file list. |
| `mise.toml` (`package`, `docs`, `gate`) | 7-9 | The new crate joins the docs gate; `package` excludes the fixture; `gate` gains `http-smoke-full`. |
| `keylink/gate-tests/gate.rs` | 9 | A new suite must be registered or the gate refuses. Found by the gate itself. |

## Deviations from the plan

| Deviation | Where recorded |
|---|---|
| Blob lookup IGNORES `Range`/`If-Range` rather than refusing them. RFC 9110 §14.2 requires ignoring a range on a method where range handling is undefined; the plan's route table says reject. The RFC wins. | `gateway.rs`, `handle_blob_lookup` |
| Idempotency on blob lookup is refused, not optional as the plan's table has it. `ExternalCall::Blob` carries no key slot. | same |
| "Select a supported method before actor validation" was shipped inverted for one commit, recorded as unobservable, then implemented literally in `54d1aad`. | `addressed` |
| Task 8's "watch and anchor the smoke and package-document inventory" — the smoke carries its own inventory check, watched red directly rather than through `break-once.sh` (which reports `selected=?` for a shell target). The `package` task and the example list remain unanchored: both are `mise.toml` prose, which the anchors walk does not read. | this file, and the smoke |

## Assumptions

1. **A recursive DTO cannot reach `keylink-openapi`.** `postcard_schema::Schema`
   is a required derive on every service argument and cannot derive for a
   self-referential type (`E0391`). Measured. `hoist` is fixed anyway.
2. **`compile_fail` error codes are not enforced on stable.** Measured on
   `rustc 1.97.1`: a block whose real error is `E0412` passes when pinned to
   `E0277`, and a nonexistent `E0999` passes too. Every probe therefore leans on
   a positive control rather than on its annotation.
3. **`publish = false` was the wrong answer to a manifest error.** An opt-in
   documentation crate a downstream user cannot depend on is not opt-in.
   `keylink-openapi` packages properly instead.
4. **Mailbox depth is not observable through this surface.** keylink parks a
   sender on a full mailbox rather than refusing, so `MailboxFull` is
   unreachable here and pressure is latency, not an error category.
5. **A Keylink allowlist rejection arrives as `503`, not `403`.** The peer loses
   the connection rather than receiving an authorization error, so a permanent
   decision reaches the caller as a temporary one. Core behaviour, outside this
   plan's blast radius, asserted as it is with the consequence written down.

## Claims made in the source that turned out to be false

Kept because each was found by measurement, not by reasoning, and each was the
same shape: a mechanism invented to make a conclusion follow.

1. `compile_fail,E0277` described as a check. It is a label.
2. `schema.rs` cited `the_error_envelope_matches_the_runtime_one`. `rg` returned
   one hit: the sentence. The test exists now.
3. The component digest was said to disambiguate two services' `Item`. It cannot,
   being a pure function of the name.
4. Four rows were committed claiming `HK42` watched them. Measured: two are, one
   carries its own control, and one is green under it — recorded as green.
5. `"InvalidRequest` is kept for `InvalidEncoding` alone… no statement at all
   about the inventory". False for one commit: the length gate ran first.
6. The declaration callback's boundary was attributed to `pub(crate)`. Measured:
   relaxing it to `pub` changes nothing, because a `macro_rules!` without
   `#[macro_export]` is crate-local regardless.

## Open, and not fixed

Reported by the merge-gate wave and left for a later pass. None is a correctness
defect in a served path.

- `keylink-openapi`'s public API is typed by `#[doc(hidden)]` items of
  `keylink-http` (`Route`, `RouteKind`, `ErrorKind`).
- Every blob byte request costs two QUIC dials and two verified-size proofs.
- Two octet-stream predicates exist, already disagreeing, one unreachable.
- `Shape::BlobId` is dead and duplicates the live address check.
- `ErrorKind::ALL` is hand-maintained with nothing forcing completeness.
- The ticket table is O(n) under its own mutex.
- 42 of the 52 break rows added here are resolution-checked rather than
  replayable. Ten carry a `WATCHED REPLAY`, chosen by security weight.
