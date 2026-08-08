//! keylink, executing inside a real browser engine.
//!
//! Everything else this repository knows about the browser target is a build.
//! `mise run wasm` links it, `mise run wasm-web` links it with the `web`
//! feature, and `mise run wasm-guards` proves three features refuse it — three
//! statements about a compiler, none about a browser. The README said "runs in
//! the browser" while `CONTRIBUTING.md` said "nothing has ever run in one", and
//! both were accurate about different things.
//!
//! This file is the other half. `wasm_bindgen_test_configure!(run_in_browser)`
//! makes `wasm-bindgen-test-runner` load the module into headless Chrome and
//! call each function there, so the module INSTANTIATING at all is the first
//! assertion — a wasm binary that links and then traps on a missing import
//! fails here and passes every build task in the gate.
//!
//! # What is in scope, and why it stops where it does
//!
//! **A browser cannot open a UDP socket**, so no test here dials anything. QUIC
//! is out of reach from a page whatever keylink does, and pretending otherwise
//! would be a test that skips rather than a test that passes. What is left is
//! the surface a browser build actually reaches on its own:
//!
//! 1. **The ALPN hash.** `derive_alpn` walks a transitive type graph and folds
//!    it through blake3. Both halves are dependencies with target-specific code
//!    paths, and the number they produce is the one that decides whether a
//!    handshake is refused.
//! 2. **postcard on the wire types.** `WireResponse<Resp>` is what every reply
//!    is encoded as, and postcard encodes an enum variant by declaration index
//!    with no name on the wire.
//! 3. **`Body`'s two browser constructors.** These exist for exactly one target
//!    and had never run on it. `from_web_stream`'s whole reason to return a
//!    `Result` is that `wasm_streams` PANICS through `expect_throw` on a stream
//!    that cannot make a BYOB reader — a JS exception, not an unwind, so under
//!    `panic = "abort"` it ends the module. That claim was untested on the only
//!    target that can evaluate it.
//!
//! Not covered, said here rather than left to be assumed: no `Node`, no
//! `Server`, no `Client`, no connection, no actor turn, and nothing at all from
//! `keylink-persist` — the version frame and the migration chain compile for
//! this target and are not run on it.
//!
//! # Why nothing here pins a hash literal
//!
//! An ALPN string written into this file would be a third copy of a value this
//! file does not own, and it would go red on every unrelated change to the wire
//! graph — the two pins in `tests/protocol.rs` already carry that job and carry
//! it deliberately. What is asked here instead is what only a browser can
//! answer: does the derivation RUN, is it stable across calls, and does it still
//! discriminate between two schemas.
//!
//! # The two breaks, watched
//!
//! Both are DESCRIBED and not quoted. A record in the file it breaks makes its
//! own quote a second occurrence of the literal unless indentation separates the
//! two, and both edits below are at column 0 — which is the same-file case
//! `.mise/tasks/anchors` says to describe. Neither is registered there for that
//! reason.
//!
//! 1. **The browser is not assumed.** Comment out the
//!    `wasm_bindgen_test_configure!` call below. The runner then sends this file
//!    to Node, where **all four tests pass in 0.01s** — the vacuous green this
//!    whole task exists to refuse. `mise run wasm-browser` was red anyway, on
//!    its own check of the runner's announced engine:
//!    `── the tests passed, but NOT in a browser.`
//! 2. **A failure in a page reaches the shell.** Change the expected length in
//!    `a_browser_file_becomes_a_body_that_knows_its_length` from
//!    `Some(contents.len() as u64)` to that expression `+ 1`. The task exits 1
//!    with the assertion's own message,
//!    `the receiver would not be able to refuse this body on size`, and the file
//!    and line it panicked at. A browser panic is a JS exception, so this is not
//!    free: it proves the runner turns one back into a test failure rather than
//!    into a page that stops.

// Empty on every other target. The runner is `wasm-bindgen-test-runner` and the
// harness is a browser, neither of which a native `cargo test` has; the file is
// still compiled by `lint` and by `test`, as a test binary with no tests in it.
#![cfg(all(target_family = "wasm", target_os = "unknown"))]
#![allow(clippy::expect_used, clippy::unwrap_used)]

use keylink::{derive_alpn, Body, Error, Outcome, RemoteError, WireResponse};
use serde::{Deserialize, Serialize};
use wasm_bindgen::JsValue;
use wasm_bindgen_test::{wasm_bindgen_test, wasm_bindgen_test_configure};

wasm_bindgen_test_configure!(run_in_browser);

/// A schema, and below it the same schema with one more field. Local to this
/// file on purpose: the property under test is that the hash MOVES, and a type
/// somebody else owns would make a red here mean two different things.
#[derive(Serialize, Deserialize, postcard_schema::Schema)]
struct V1 {
    id: u64,
    name: String,
}

#[derive(Serialize, Deserialize, postcard_schema::Schema)]
struct V2 {
    id: u64,
    name: String,
    scale: u8,
}

const PROTOCOL: &str = "keylink/browser-smoke";

/// The schema hash runs here, is stable, and still separates two schemas.
///
/// Stability across two calls is not a tautology on this target. blake3 selects
/// an implementation at run time on a native host, and `derive_alpn` formats
/// through `format!`; a hash that came back different on the second call, or
/// that came back zero, would link exactly as well as one that works.
#[wasm_bindgen_test]
fn the_alpn_derivation_runs_in_a_browser() {
    let first = derive_alpn::<V1>(PROTOCOL);
    let again = derive_alpn::<V1>(PROTOCOL);
    assert_eq!(first, again, "the same schema derived two different ALPNs");

    let alpn = String::from_utf8(first).expect("an ALPN is ASCII by construction");
    let hex = alpn
        .strip_prefix(PROTOCOL)
        .and_then(|rest| rest.strip_prefix('/'))
        .unwrap_or_else(|| panic!("{alpn} does not carry its protocol name"));

    assert_eq!(hex.len(), 16, "a u64 is 16 hex digits: {alpn}");
    assert!(
        hex.bytes()
            .all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase()),
        "the hash is lowercase hex: {alpn}"
    );
    // A hash of zero is what a derivation that silently did nothing returns, and
    // it formats to sixteen perfectly valid hex digits.
    assert_ne!(hex, "0000000000000000", "the schema hashed to nothing");

    let moved = derive_alpn::<V2>(PROTOCOL);
    assert_ne!(
        alpn.as_bytes(),
        moved.as_slice(),
        "one more field did not move the ALPN, so drift would not be refused"
    );
}

/// The reply type survives a postcard round trip, in both arms.
///
/// postcard writes an enum variant as its declaration index and writes no name,
/// so a decoder that disagrees about the index returns a different variant
/// rather than an error. Both arms are here because the failure that matters is
/// silent: `Outcome::Err` decoding as `Outcome::Ok` is a refusal read as a
/// success.
#[wasm_bindgen_test]
fn postcard_round_trips_a_reply() {
    let refused: WireResponse<u32> =
        WireResponse::Call(Outcome::Err(RemoteError::TimedOut { millis: 7 }));
    let bytes = postcard::to_stdvec(&refused).expect("a reply serialises");
    let back: WireResponse<u32> = postcard::from_bytes(&bytes).expect("and comes back");
    match back {
        WireResponse::Call(Outcome::Err(RemoteError::TimedOut { millis })) => {
            assert_eq!(millis, 7, "the budget did not survive the round trip");
        }
        other => panic!("a refusal decoded as {other:?}"),
    }

    // A multi-byte varint, so the round trip is not passing on a value that
    // happens to be one byte in either direction.
    let answered: WireResponse<u32> = WireResponse::Call(Outcome::Ok(300_000));
    let bytes = postcard::to_stdvec(&answered).expect("a reply serialises");
    let back: WireResponse<u32> = postcard::from_bytes(&bytes).expect("and comes back");
    match back {
        WireResponse::Call(Outcome::Ok(value)) => assert_eq!(value, 300_000),
        other => panic!("an answer decoded as {other:?}"),
    }
}

/// A browser `File` becomes a `Body` that declares its length.
///
/// This is `Body::from_file`'s counterpart and the reason the `web` feature
/// exists. It reaches `wasm_streams::ReadableStream::try_into_async_read`
/// against a stream the File API produced, which is the call documented as
/// expected to succeed — a sentence that until now nothing had evaluated.
///
/// The length is the assertion worth making: `from_reader(_, None)` would also
/// build a body, and it gives up the property that lets a receiver tell a
/// truncated transfer from a clean close.
#[wasm_bindgen_test]
fn a_browser_file_becomes_a_body_that_knows_its_length() {
    let contents = "keylink in a browser";
    let parts = js_sys::Array::of1(&JsValue::from_str(contents));
    let file = web_sys::File::new_with_str_sequence(&parts, "smoke.txt")
        .expect("the File API builds a File out of one string");

    let body = Body::from_web_file(&file).expect("a File's stream is a byte stream");
    assert_eq!(
        body.declared_len(),
        Some(contents.len() as u64),
        "the receiver would not be able to refuse this body on size"
    );
}

/// A stream that cannot make a BYOB reader is REFUSED, not thrown out of.
///
/// `ReadableStream::new()` with no underlying source is a default stream rather
/// than a byte stream, which is the case `Body::from_web_stream` documents as
/// refused. If the guard were gone, `wasm_streams` would raise a JS exception
/// through `expect_throw` instead of returning, and this test would fail as an
/// uncaught error rather than an assertion — which is the failure being bought
/// off, so it is worth knowing what it would look like.
#[wasm_bindgen_test]
fn a_stream_without_a_byob_reader_is_refused() {
    let stream = web_sys::ReadableStream::new().expect("a default ReadableStream constructs");

    match Body::from_web_stream(stream, None) {
        Err(Error::NotAByteStream) => {}
        Err(other) => panic!("refused, but for the wrong reason: {other}"),
        Ok(_) => panic!("a default ReadableStream was accepted as a byte stream"),
    }
}
