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
//!    and had never run on it. `from_web_stream` refuses a stream that cannot make
//!    a BYOB reader instead of letting a JS exception end the module.
//! 4. **The portable topic timeout.** This target checks all four `Patience`
//!    boundaries without waiting for the accepted maximum, then exercises the
//!    browser timer and confirms timeout removes the subscriber without queuing
//!    the item that exceeded its patience.
//! 5. **Bounded lossy fan-out.** `BroadcastDepth` boundaries and both loss
//!    policies run in Chrome, including in-band loss before the surviving item.
//! 6. **Random event-log identities.** The gate compares the first log minted
//!    in two independent browser instances with Date.now fixed to the same value.
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
//! # The three breaks, watched
//!
//! The first two are described rather than quoted because each edits this file at
//! column zero; quoting either search literal here would make a second occurrence.
//! The third edits `topic.rs`, so its quoted literal is registered in
//! `.mise/tasks/anchors`.
//!
//! 1. **The browser is not assumed.** Comment out the
//!    `wasm_bindgen_test_configure!` call below. The runner then sends this file
//!    to Node, where **all tests pass under Node** — the vacuous green this
//!    whole task exists to refuse. `mise run wasm-browser` was red anyway, on
//!    its own check of the runner's announced engine:
//!    `── the tests passed, but NOT in a browser.`
//! 2. **A failure in a page reaches the shell.** Change the expected length in
//!    `a_browser_file_becomes_a_body_that_knows_its_length` by one. The task exits
//!    1 with the assertion's own message and source line.
//! 3. **The portable timeout and constructor domain are evaluated here.** The
//!    independent fractional and maximum substitutions recorded beside the native
//!    boundary test also fail this Chrome target at their corresponding browser
//!    assertions. Their production search literals are
//!    `let whole_millis = duration.subsec_nanos().is_multiple_of(1_000_000);` and
//!    `let portable = duration.as_millis() <= i32::MAX as u128;`. The timeout
//!    guard was broken separately by replacing `return Poll::Ready(None);` with
//!    `return Poll::Ready(Some(Ok(())));`; Chrome reported one delivery instead
//!    of one drop. The native equality guard separately swaps the poll order.

// Empty on every other target. The runner is `wasm-bindgen-test-runner` and the
// harness is a browser, neither of which a native `cargo test` has; the file is
// still compiled by `lint` and by `test`, as a test binary with no tests in it.
#![cfg(all(target_family = "wasm", target_os = "unknown"))]
#![allow(clippy::expect_used, clippy::unwrap_used)]

use keylink::compose::{
    BroadcastDepth, BroadcastDepthError, Fanout, Lagged, Lossy, LossyTopic, Overflow, Patience,
    Topic,
};
use keylink::{derive_alpn, Body, Error, Outcome, RemoteError, WireResponse};
use n0_future::time::Duration;
use n0_future::StreamExt;
use serde::{Deserialize, Serialize};
use std::num::{NonZeroU64, NonZeroUsize};
use wasm_bindgen::JsValue;
use wasm_bindgen_test::{wasm_bindgen_test, wasm_bindgen_test_configure};

wasm_bindgen_test_configure!(run_in_browser);

/// Two fresh browser runs use the same clock and no shared log-incarnation state.
/// Compare this test's printed identities across those runs: they must differ.
#[wasm_bindgen_test]
fn event_log_identity_comes_from_browser_randomness() {
    let date = js_sys::Reflect::get(&js_sys::global(), &JsValue::from_str("Date")).unwrap();
    let now_name = JsValue::from_str("now");
    let original = js_sys::Reflect::get(&date, &now_name).unwrap();
    let fixed =
        wasm_bindgen::closure::Closure::wrap(Box::new(|| 1234.0) as Box<dyn FnMut() -> f64>);
    js_sys::Reflect::set(&date, &now_name, fixed.as_ref()).unwrap();
    assert_eq!(js_sys::Date::now(), 1234.0);
    let log = keylink::events::EventLog::<u64>::memory(keylink::events::Retention::new(
        NonZeroUsize::new(1).unwrap(),
        NonZeroU64::new(1024).unwrap(),
    ));
    let identity = postcard::to_allocvec(&log.head()).unwrap();
    js_sys::Reflect::set(&date, &now_name, &original).unwrap();
    wasm_bindgen_test::console_log!("event-log identity: {:?}", identity);
}

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

/// All portable constructor boundaries run under the browser clock. The accepted
/// maximum is constructed but never awaited.
#[wasm_bindgen_test]
fn patience_constructor_boundaries_hold_in_a_browser() {
    assert_eq!(
        Patience::try_after(Duration::ZERO),
        Err(keylink::compose::PatienceError)
    );
    assert_eq!(
        Patience::try_after(Duration::from_nanos(1_000_001)),
        Err(keylink::compose::PatienceError),
        "fractional milliseconds are not portable"
    );

    let largest = Duration::from_millis(i32::MAX as u64);
    assert!(Patience::try_after(largest).is_ok());
    assert_eq!(
        Patience::try_after(largest + Duration::from_millis(1)),
        Err(keylink::compose::PatienceError)
    );
}

/// The portable timeout path runs on the browser clock: a full subscriber is
/// removed, keeps only its accepted item, and then ends.
#[wasm_bindgen_test]
async fn a_timed_out_topic_subscriber_is_removed_in_a_browser() {
    let topic = Topic::new(Overflow::BlockThenDrop {
        depth: NonZeroUsize::MIN,
        patience: Patience::try_after(Duration::from_millis(1)).expect("portable patience"),
    });
    let mut slow = topic.subscribe();
    assert_eq!(topic.publish(1_u8).await.delivered, 1);
    assert_eq!(
        topic.publish(2).await,
        Fanout {
            delivered: 0,
            dropped: 1,
            gone: 0,
        }
    );
    assert_eq!(slow.next().await, Some(1));
    assert_eq!(slow.next().await, None);
}

/// Watched in headless Chrome: `scripts/break-once.sh` replaced
/// `        if !depth.get().is_power_of_two() || depth.get() > Self::MAX {` with
/// `        if depth.get() > Self::MAX {`. Chrome saw `Ok(BroadcastDepth(3))`
/// instead of `Err(BroadcastDepthError)`.
#[wasm_bindgen_test]
fn lossy_broadcast_depth_boundaries_hold_in_a_browser() {
    let largest = NonZeroUsize::new(1_usize << (usize::BITS - 2)).unwrap();
    assert_eq!(NonZeroUsize::new(0), None, "zero is unrepresentable");
    assert_eq!(
        BroadcastDepth::try_new(NonZeroUsize::new(3).unwrap()),
        Err(BroadcastDepthError)
    );
    assert_eq!(
        BroadcastDepth::try_new(NonZeroUsize::new(largest.get() * 2).unwrap()),
        Err(BroadcastDepthError)
    );
    assert_eq!(
        BroadcastDepth::try_new(NonZeroUsize::MIN).unwrap().get(),
        NonZeroUsize::MIN
    );
    let two = NonZeroUsize::new(2).unwrap();
    assert_eq!(BroadcastDepth::try_new(two).unwrap().get(), two);
    assert_eq!(BroadcastDepth::try_new(largest).unwrap().get(), largest);
}

/// Watched in headless Chrome before zero lag became an explicit invariant
/// failure: the lag-count conversion was replaced with
/// `NonZeroU64::new(skipped.saturating_add(1))`. Chrome saw `Skipped(2)`
/// instead of `Skipped(1)`.
#[wasm_bindgen_test]
async fn lossy_drop_oldest_runs_in_a_browser() {
    let topic = LossyTopic::new(Lossy::DropOldest(
        BroadcastDepth::try_new(NonZeroUsize::MIN).unwrap(),
    ));
    let mut subscription = topic.subscribe();
    assert_eq!(topic.publish('A').accepted, 1);
    assert_eq!(topic.publish('B').accepted, 1);
    assert_eq!(
        subscription.next().await,
        Some(Lagged::Skipped(NonZeroU64::MIN))
    );
    assert_eq!(subscription.next().await, Some(Lagged::Item('B')));
}

/// Watched in headless Chrome: `scripts/break-once.sh` replaced
/// `                                .map_or(1, |count| count.get().saturating_add(1));` with
/// `                                .map_or(0, |_| 0);`. Chrome saw `Item('C')`
/// instead of `Skipped(1)`.
#[wasm_bindgen_test]
async fn lossy_drop_newest_runs_in_a_browser() {
    let topic = LossyTopic::new(Lossy::DropNewest(NonZeroUsize::MIN));
    let mut subscription = topic.subscribe();
    assert_eq!(topic.publish('A').accepted, 1);
    assert_eq!(topic.publish('B').accepted, 0);
    assert_eq!(subscription.next().await, Some(Lagged::Item('A')));
    assert_eq!(topic.publish('C').accepted, 1);
    assert_eq!(
        subscription.next().await,
        Some(Lagged::Skipped(NonZeroU64::MIN))
    );
    assert_eq!(subscription.next().await, Some(Lagged::Item('C')));
}
