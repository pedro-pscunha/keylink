//! What moves the contract, what does not, and what a move costs.
//!
//! ```sh
//! cargo run -p keylink --example wire_contract
//! ```
//!
//! Two nodes on the loopback interface. `Node::bind_local` binds with no relay
//! and no discovery, so this runs offline.
//!
//! # The contract is the ALPN
//!
//! keylink derives the QUIC ALPN from the service's *schema*:
//! [`derive_alpn`] hashes the types a service puts on the wire and appends
//! sixteen hex digits to the protocol name. Two peers whose types disagree
//! therefore fail to **handshake**, rather than agreeing to exchange bytes
//! neither can parse. There is no version number to remember to bump.
//!
//! That is a strong property, and it is only useful if you know what counts as
//! a change. This example measures it rather than asserting it.
//!
//! # The rule
//!
//! `postcard_schema` hashes the *shape* of a type and the names of its parts:
//!
//! | change | contract |
//! |---|---|
//! | rename a field | **moves** |
//! | rename a variant | **moves** |
//! | add or remove a field | **moves** |
//! | change a field's type | **moves** |
//! | rename the type itself | stays |
//! | move the type to another module | stays |
//!
//! The two halves are not equally believed, and only one of them is tested
//! anywhere in this crate: `node.rs` covers the renamed *field*. "Renaming the
//! type is free" is the half a reader is most likely to doubt and least likely
//! to find evidence for, so acts 1 and 2 pin both directions side by side.
//!
//! # Why each comparison changes exactly one thing
//!
//! A hash comparison is worthless if the two inputs differ in two ways. So the
//! module-path pair holds the type name constant, the type-name pair holds the
//! module constant, and every raw comparison passes the same [`PROTOCOL`]
//! string — the protocol is part of the hash, and letting it vary would make
//! every pair below differ twice.
//!
//! # What keylink puts in the contract besides your types
//!
//! [`Envelope`] is the frame around every message and [`WireKinds`] names the
//! wire shapes this build can accept. Both are inside the hash, so a keylink
//! that learns a new way to carry a message cannot silently talk to one that
//! has not — act 4 has the story of the bug that closed.
//!
//! [`MAX_HEADER`] is the other half of the framing contract, and it is a cap
//! rather than a hash: a peer announcing a larger header frame is refused
//! before a byte is buffered for it.

use std::error::Error as StdError;

use keylink::{derive_alpn, Allowlist, Context, Envelope, Error, Node, Transport, WireKinds};
use postcard_schema::Schema;
use serde::{Deserialize, Serialize};

/// One protocol string for every raw comparison below.
///
/// The protocol is hashed with the schema, so two comparisons that used
/// different strings would differ twice and measure nothing.
const PROTOCOL: &str = "fleet/Kiln";

fn alpn_of<T: Schema>() -> String {
    String::from_utf8_lossy(&derive_alpn::<T>(PROTOCOL)).into_owned()
}

/// A generated service's ALPN, which is the same function over the generated
/// contract type.
fn text(alpn: &[u8]) -> String {
    String::from_utf8_lossy(alpn).into_owned()
}

/// Print a comparison and answer whether the two agreed.
fn compare(claim: &str, left: &str, right: &str) -> bool {
    let same = left == right;
    println!("  {:5}  {claim}", if same { "SAME" } else { "MOVED" });
    println!("         {left}");
    println!("         {right}");
    same
}

// ── one difference per pair ─────────────────────────────────────────────────

mod here {
    use super::{Deserialize, Schema, Serialize};

    #[derive(Serialize, Deserialize, Schema)]
    pub struct Metric {
        pub id: u64,
    }

    /// Same module, same field, **different type name**.
    #[derive(Serialize, Deserialize, Schema)]
    pub struct Gauge {
        pub id: u64,
    }

    #[derive(Serialize, Deserialize, Schema)]
    pub enum Mode {
        Idle,
        Firing,
    }
}

mod elsewhere {
    use super::{Deserialize, Schema, Serialize};

    /// Byte-identical to `here::Metric`. Only the **module path** differs.
    #[derive(Serialize, Deserialize, Schema)]
    pub struct Metric {
        pub id: u64,
    }

    /// `here::Metric` with the **field** renamed, and nothing else.
    #[derive(Serialize, Deserialize, Schema)]
    pub struct Renamed {
        pub ident: u64,
    }

    /// `here::Mode` with the second **variant** renamed.
    #[derive(Serialize, Deserialize, Schema)]
    pub enum Mode {
        Idle,
        Burning,
    }
}

// ── the same question, asked through `service!` ─────────────────────────────
//
//  A generated protocol string is `concat!(CARGO_PKG_NAME, "/", Service)`, so
//  two blocks that both declare `service Kiln` carry the identical protocol
//  whichever module they sit in. The service name is held constant and only the
//  payload changes.

mod named_reading {
    use super::{Deserialize, Schema, Serialize};

    #[derive(Debug, Clone, Serialize, Deserialize, Schema)]
    pub struct Reading {
        pub celsius: f32,
    }

    keylink::service! {
        service Kiln;

        rpc Read() -> Reading;
    }
}

mod named_sample {
    use super::{Deserialize, Schema, Serialize};

    /// `named_reading::Reading` under another name. Same field, same type.
    #[derive(Debug, Clone, Serialize, Deserialize, Schema)]
    pub struct Sample {
        pub celsius: f32,
    }

    keylink::service! {
        service Kiln;

        rpc Read() -> Sample;
    }
}

mod renamed_field {
    use super::{Deserialize, Schema, Serialize};

    /// `named_reading::Reading` with the field renamed.
    #[derive(Debug, Clone, Serialize, Deserialize, Schema)]
    pub struct Reading {
        pub fahrenheit: f32,
    }

    keylink::service! {
        service Kiln;

        rpc Read() -> Reading;
    }
}

//  Handlers, so act 5 can dial a service that is genuinely serving.
struct Sensor;

impl named_reading::kiln::Handler for Sensor {
    async fn read(&self, _cx: &Context) -> named_reading::Reading {
        named_reading::Reading { celsius: 1183.0 }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn StdError>> {
    // ── 1. The half that is tested elsewhere: a name on a part moves the hash.
    println!("a name that is ON THE WIRE:");
    let baseline = compare(
        "the same type, twice (the control)",
        &alpn_of::<here::Metric>(),
        &alpn_of::<here::Metric>(),
    );
    assert!(baseline, "the hash must be a function of the type alone");

    let field = compare(
        "FIELD renamed: Metric { id } vs Renamed { ident }",
        &alpn_of::<here::Metric>(),
        &alpn_of::<elsewhere::Renamed>(),
    );
    assert!(
        !field,
        "a field name is on the wire and must move the contract"
    );

    let variant = compare(
        "VARIANT renamed: Mode { Idle, Firing } vs Mode { Idle, Burning }",
        &alpn_of::<here::Mode>(),
        &alpn_of::<elsewhere::Mode>(),
    );
    assert!(!variant, "a variant name is on the wire too");

    // ── 2. The half with no test anywhere: a name that is NOT on the wire.
    //
    //  This is the direction a reader doubts, and doubting it has a cost: a
    //  team that believes a rename breaks the fleet will not do one. Both pairs
    //  hold everything else identical.
    println!("\na name that is NOT on the wire:");
    let type_name = compare(
        "TYPE renamed, same module: here::Metric vs here::Gauge",
        &alpn_of::<here::Metric>(),
        &alpn_of::<here::Gauge>(),
    );
    assert!(
        type_name,
        "a type's own name is not serialised, so it cannot move the contract"
    );

    let module = compare(
        "MODULE changed, same type: here::Metric vs elsewhere::Metric",
        &alpn_of::<here::Metric>(),
        &alpn_of::<elsewhere::Metric>(),
    );
    assert!(module, "a module path is not serialised either");

    // ── 3. The same two verdicts through `service!`, which is where it matters.
    //
    //  The raw pairs above measure `postcard_schema`. These measure keylink:
    //  the contract a generated client and server actually agree on.
    println!("\nand through a generated service:");
    let payload_type = compare(
        "payload TYPE renamed (Reading -> Sample)",
        &text(named_reading::kiln::alpn()),
        &text(named_sample::kiln::alpn()),
    );
    assert!(
        payload_type,
        "renaming a payload type is free at the handshake"
    );

    let payload_field = compare(
        "payload FIELD renamed (celsius -> fahrenheit)",
        &text(named_reading::kiln::alpn()),
        &text(renamed_field::kiln::alpn()),
    );
    assert!(
        !payload_field,
        "renaming a payload field is a fleet-wide event"
    );

    // ── 4. keylink's own framing is inside the same hash.
    //
    //  [`Envelope`] wraps every message and [`WireKinds`] names the wire shapes
    //  this build accepts. Neither is a caller's type and both are hashed, which
    //  is what makes a keylink upgrade visible at the handshake.
    //
    //  The bug that bought `WireKinds`: before it existed, two peers with
    //  byte-identical service definitions — one built against a keylink that
    //  accepts unidirectional streams and one built against a keylink that does
    //  not — derived the SAME ALPN. The handshake agreed, the sender opened a
    //  `uni` stream, the older peer never accepted one, and every `tell` was
    //  lost with no error on either side. Naming the wire surface inside the
    //  hash turned that into a refused handshake.
    //
    //  Both types are constructible only inside keylink — every field is
    //  `pub(crate)` — so what an example can do with them is name them in a
    //  type position, which is exactly what the contract does.
    println!("\nkeylink's own framing, hashed alongside yours:");
    let framing = compare(
        "Envelope<Metric> vs Envelope<Gauge>: the payload rename, wrapped",
        &alpn_of::<Envelope<here::Metric>>(),
        &alpn_of::<Envelope<here::Gauge>>(),
    );
    assert!(framing, "wrapping cannot make a free change costly");

    let wrapped = compare(
        "Envelope<Metric> vs Metric: the wrapper is part of the contract",
        &alpn_of::<Envelope<here::Metric>>(),
        &alpn_of::<here::Metric>(),
    );
    assert!(!wrapped, "the frame is hashed, not just the message");

    println!("\n  WireKinds is hashed too: {}", alpn_of::<WireKinds>());
    println!(
        "  MAX_HEADER = {} bytes — a cap, checked before buffering, not a hash",
        keylink::MAX_HEADER
    );

    // ── 5. What a moved contract costs, measured against a live server.
    //
    //  The server offers `named_reading`'s ALPN and nothing else. A client of
    //  `renamed_field` — one field name apart — is refused at the handshake,
    //  because the ALPN it offers is not one the server accepts. No bytes are
    //  ever exchanged, so this cannot arrive as a decode error.
    //
    //  It does not arrive as a bare [`Transport`] failure either. `rpc.rs`
    //  narrows the TLS `no_application_protocol` alert into its own variant,
    //  because sending a reader to look at the network for this is the wrong
    //  layer entirely — nothing is wrong with the network. Anything it does
    //  not recognise stays [`Error::Transport`], which is the arm below.
    println!("\nwhat a moved contract costs at dial time:");
    let kiln = Node::bind_local().await?;
    let floor = Node::bind_local().await?;
    let _serving = named_reading::kiln::serve(&kiln, Sensor, Allowlist::anyone())?;

    let matching = named_reading::kiln::Client::connect(&floor, kiln.endpoint().addr()).await?;
    println!(
        "  matching contract: {} degrees",
        matching.read().await?.celsius
    );

    let refused = renamed_field::kiln::Client::connect(&floor, kiln.endpoint().addr()).await;
    match refused {
        Err(Error::SchemaMismatch { ours }) => {
            println!("  one field renamed:  refused at the handshake");
            println!("                      we offered {ours}");
            //  The refusal names the same string act 3 computed. That is what
            //  ties the table at the top of this file to a running handshake:
            //  the hash a reader can print is the hash the dial actually used.
            assert_eq!(
                ours,
                text(renamed_field::kiln::alpn()),
                "the refusal must name the contract we measured"
            );
            assert_ne!(ours, text(named_reading::kiln::alpn()));
        }
        Err(Error::Transport(cause @ Transport::Connect(_))) => {
            return Err(format!("the mismatch was not narrowed, and stayed {cause}").into())
        }
        Err(other) => return Err(format!("a mismatch is refused at the dial, got {other}").into()),
        Ok(_) => return Err("a client with a different contract must not connect".into()),
    }

    println!("\nno version number was consulted at any point.");
    Ok(())
}
