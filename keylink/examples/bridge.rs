//! Driving a keylink service from a wire keylink did not design.
//!
//! ```sh
//! cargo run -p keylink --example bridge
//! ```
//!
//! Two nodes on the loopback interface. `Node::bind_local` binds with no relay
//! and no discovery, so this runs offline.
//!
//! # What this seam is, and what it is not
//!
//! `keylink-http` turns a service into HTTP routes. It does that over a public
//! seam, not a private one, and this is that seam: a generated `External`
//! marker, a table of methods, and one call that takes a **method name and a
//! byte body** rather than typed arguments. Anything that can produce those two
//! can drive a keylink service — a JSON gateway, a gRPC shim, a message-queue
//! consumer, a scripting binding.
//!
//! # One trait is yours, and it is the only one
//!
//! | type | who writes it |
//! |---|---|
//! | [`ServiceCodec`] | **you** |
//! | [`ExternalService`] and its `External` marker | `service!` |
//! | [`ExternalMethod`], [`ExternalKind`] | keylink hands them to you |
//! | [`ExternalCall`] | you construct it, per call |
//! | [`ExternalOutput`], [`ExternalError`] | keylink returns them |
//!
//! **Do not hand-implement [`ExternalService`].** Only the generated marker
//! implements it, and the four hand-written impls in this workspace are all
//! test doubles inside `keylink-http` — one returns a future that never
//! resolves, one declares a method kind it deliberately violates. Writing one
//! by hand is a way to test an adapter, not a way to expose a service.
//!
//! # A `tell` cannot be bridged, and the table says so rather than the docs
//!
//! Five declarations below, four entries in `METHODS`. A `tell` answers proof
//! of transmission and nothing a caller can wait on, so it is not adaptable and
//! asking for it by name is [`ExternalError::UnknownMethod`]. That is act 5,
//! and it is read off the table rather than asserted in prose.
//!
//! # Why JSON here, when keylink's own format is postcard
//!
//! A postcard codec would be three lines shorter and would demonstrate nothing:
//! the point of a codec is that the outside world speaks something else. JSON is
//! the smallest honest choice, and `serde_json` is a dev-dependency for this
//! example alone.

use keylink::{
    Allowlist, Body, BodyReader, Context, ExternalCall, ExternalError, ExternalKind,
    ExternalMethod, ExternalOutput, ExternalService, Node, ServiceCodec,
};
use serde::{de::DeserializeOwned, Serialize};

keylink::service! {
    service Depot;

    /// Bounded in, bounded out. The ordinary shape.
    rpc Count(value: u64) -> u64;

    /// Two declared arguments, which arrive as one JSON object.
    rpc Label(name: String, count: u16) -> String;

    /// A payload streamed up beside the bounded arguments.
    send Upload(prefix: u64) -> u64;

    /// A payload streamed back beside the bounded reply.
    recv Download(value: u64) -> u64;

    /// Deliberately present, and deliberately absent from `METHODS`.
    tell Audit(line: String) -> ();
}

struct Depot;

impl depot::Handler for Depot {
    async fn count(&self, _cx: &Context, value: u64) -> u64 {
        value + 1
    }

    async fn label(&self, _cx: &Context, name: String, count: u16) -> String {
        format!("{name}-{count}")
    }

    async fn upload(&self, _cx: &Context, prefix: u64, body: BodyReader) -> u64 {
        //  A bound, not a `Vec`: `read_to_bytes` refuses past its cap rather
        //  than letting a peer name the memory this handler uses.
        let bytes = body.read_to_bytes(64).await.unwrap_or_default();
        prefix + bytes.len() as u64
    }

    async fn download(&self, _cx: &Context, value: u64) -> (u64, Body) {
        (value + 1, Body::from_bytes(b"payload".as_slice()))
    }

    async fn audit(&self, _cx: &Context, _line: String) {}
}

/// The one trait a bridge author writes.
///
/// It converts bytes from the outside world into a method's declared arguments,
/// and a method's declared return value back into whatever that world reads.
/// keylink ships no public implementation — `keylink-http`'s is `pub(crate)` —
/// so this is genuinely the work, and it is nine lines.
#[derive(Clone)]
struct Json;

impl ServiceCodec for Json {
    type Error = serde_json::Error;
    type Output = Vec<u8>;

    fn decode<T: DeserializeOwned>(&self, bytes: &[u8]) -> Result<T, Self::Error> {
        serde_json::from_slice(bytes)
    }

    fn encode<T: Serialize>(&self, value: &T) -> Result<Self::Output, Self::Error> {
        serde_json::to_vec(value)
    }
}

/// The bounded arm, for the calls that have no stream beside them.
fn bounded(output: ExternalOutput<Vec<u8>>) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
    match output {
        ExternalOutput::Json(bytes) => Ok(bytes),
        //  All three arms, because the variant IS the transport shape. A
        //  wildcard here is what lets a router return a body nobody reads.
        ExternalOutput::Recv { .. } => Err("a Recv output has a body to drain".into()),
        ExternalOutput::Blob { .. } => Err("a Blob output has a handle to fetch".into()),
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let depot_node = Node::bind_local().await?;
    let bridge_node = Node::bind_local().await?;
    let _serving = depot::serve(&depot_node, Depot, Allowlist::anyone())?;
    let client = depot::Client::connect(&bridge_node, depot_node.endpoint().addr()).await?;

    // ── 1. The table a router builds its routes from.
    //
    //  Five declarations above, four entries here. A router reads this rather
    //  than being told what the service has, which is why a method added to the
    //  declaration reaches the bridge without the bridge being edited.
    let methods: &[ExternalMethod] = <depot::External as ExternalService>::METHODS;
    println!(
        "service {} exposes {} adaptable method(s):",
        <depot::External as ExternalService>::SERVICE,
        methods.len()
    );
    for method in methods {
        println!("  {:<9} {:?}", method.name(), method.kind());
    }
    assert_eq!(methods.len(), 4, "five declarations, and a tell is not one");
    assert!(
        !methods.iter().any(|method| method.name() == "Audit"),
        "a tell answers nothing a bridge can wait on"
    );

    // ── 2. A bounded call, by name and by bytes.
    //
    //  Nothing here names `count` the Rust method, or `u64`. The bridge holds a
    //  string and a byte slice, which is exactly what an HTTP router has.
    let counted = bounded(
        depot::External::invoke(
            &client,
            "Count",
            br#"{"value":41}"#,
            ExternalCall::Rpc { key: None },
            Json,
        )
        .await?,
    )?;
    println!("Count  -> {}", String::from_utf8_lossy(&counted));
    assert_eq!(counted, b"42");

    //  Two declared arguments arrive as one object, keyed by the names in the
    //  declaration. That mapping is the codec's whole job.
    let labelled = bounded(
        depot::External::invoke(
            &client,
            "Label",
            br#"{"name":"crate","count":2}"#,
            ExternalCall::Rpc { key: None },
            Json,
        )
        .await?,
    )?;
    println!("Label  -> {}", String::from_utf8_lossy(&labelled));
    assert_eq!(labelled, br#""crate-2""#);

    // ── 3. The two streaming shapes, which is where `ExternalCall` earns its
    //       variants.
    //
    //  A `send` carries a body IN, so the call names one. Peak memory is one
    //  chunk on both sides.
    let uploaded = bounded(
        depot::External::invoke(
            &client,
            "Upload",
            br#"{"prefix":10}"#,
            ExternalCall::Send {
                body: Body::from_bytes(b"1234".as_slice()),
            },
            Json,
        )
        .await?,
    )?;
    println!("Upload -> {}", String::from_utf8_lossy(&uploaded));
    assert_eq!(uploaded, b"14", "ten plus the four bytes the handler read");

    //  A `recv` carries a body OUT, so the OUTPUT is the pair. A bridge that
    //  matched only `Json` here would hand its caller a header whose body never
    //  arrives — which is why `bounded` above refuses this variant loudly.
    let downloaded = depot::External::invoke(
        &client,
        "Download",
        br#"{"value":7}"#,
        ExternalCall::Recv,
        Json,
    )
    .await?;
    match downloaded {
        ExternalOutput::Recv { value, body } => {
            let bytes = body.read_to_bytes(64).await?;
            println!(
                "Download -> {} and {} body byte(s)",
                String::from_utf8_lossy(&value),
                bytes.len()
            );
            assert_eq!(value, b"8");
            assert_eq!(bytes.as_ref(), b"payload");
        }
        other => return Err(format!("a recv answers a value and a reader, got {other:?}").into()),
    }

    // ── 4. Three refusals, and they are three different faults.
    //
    //  A bridge maps each to a different answer — 404, 405 and 400 in HTTP —
    //  so collapsing them into one error string is the failure this taxonomy
    //  exists to prevent.
    let unknown = depot::External::invoke(
        &client,
        "Audit",
        br#"{"line":"x"}"#,
        ExternalCall::Rpc { key: None },
        Json,
    )
    .await;
    assert!(
        matches!(unknown, Err(ExternalError::UnknownMethod)),
        "a tell is not on the table, got {unknown:?}"
    );
    println!("Audit  -> UnknownMethod: the caller asked for a name the table has not got");

    //  A name the table HAS, asked for with the wrong transport shape. The
    //  error carries both kinds, so a bridge can say which verb was wanted.
    let mismatched = depot::External::invoke(
        &client,
        "Count",
        br#"{"value":1}"#,
        ExternalCall::Recv,
        Json,
    )
    .await;
    match mismatched {
        Err(ExternalError::WrongKind { expected, actual }) => {
            assert_eq!(expected, ExternalKind::Rpc);
            assert_eq!(actual, ExternalKind::Recv);
            println!("Count  -> WrongKind: the table says {expected:?}, the call said {actual:?}");
        }
        other => return Err(format!("a shape mismatch names both shapes, got {other:?}").into()),
    }

    //  And a body the codec cannot read. This one is the codec's own error
    //  type, surfaced rather than flattened, so a bridge reports the parse
    //  position its users expect.
    let malformed = depot::External::invoke(
        &client,
        "Count",
        b"not json",
        ExternalCall::Rpc { key: None },
        Json,
    )
    .await;
    match malformed {
        Err(ExternalError::Codec(error)) => {
            println!("Count  -> Codec: {error}");
        }
        other => return Err(format!("a bad body is the codec's error, got {other:?}").into()),
    }

    // ── 5. An idempotency key, which crosses the seam like anything else.
    //
    //  `ExternalCall::Rpc` carries one, so a bridge gives its callers
    //  effectively-once without the service knowing a bridge is there.
    let key = keylink::IdempotencyKey::new("bridge-9001")?;
    let first = bounded(
        depot::External::invoke(
            &client,
            "Count",
            br#"{"value":100}"#,
            ExternalCall::Rpc {
                key: Some(key.clone()),
            },
            Json,
        )
        .await?,
    )?;
    let replayed = bounded(
        depot::External::invoke(
            &client,
            "Count",
            br#"{"value":100}"#,
            ExternalCall::Rpc { key: Some(key) },
            Json,
        )
        .await?,
    )?;
    assert_eq!(first, replayed, "the ledger answers a bridged retry too");
    println!(
        "Count  -> {} twice under one key, from the record the second time",
        String::from_utf8_lossy(&first)
    );

    Ok(())
}
