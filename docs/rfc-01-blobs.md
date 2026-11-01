# RFC-01: iroh-blobs in keylink

Status: proposal. Nothing in this document is implemented.

## 1. Summary

keylink RPC carries a content address. iroh-blobs moves the bytes. The caller
gains verified resume, deduplication and content addressing. The call site names
no transport concept.

The proposal adds eight items of public surface: one macro kind (`blob`), one
value type (`BlobId`), one handle type (`BlobHandle`), one node-level object
(`Blobs`), a `Retention` enum, and three startup errors. It adds no escape hatch,
because `Service::foreign` already landed in commit `2e02489`. It removes
nothing from keylink.

Most of it is envelope-independent and can be built before RFC-04 settles.
Section 11 lists the four items that cannot.

The security model is the load-bearing part. keylink does **not** route the
`Allowlist` into blobs admission. keylink records a grant for every content
address that a handler returns, and admits a blob request only when the peer
holds a grant for that exact hash. The user never mounts the blobs protocol by
hand, so the user cannot forget the policy.

Two things carry that claim, and both are checked rather than asserted. keylink
generates the mount, so `iroh_blobs::ALPN` is reserved against a hand-rolled
replacement (section 4.1). A private `ProvenPeer` type keys the ledger, so an
unidentified peer cannot reach an authorization decision (section 5.3).

One property a user must know before choosing a store, and it is about
deduplication rather than confidentiality:

> **One node has one content store.** Two tenants on one node share a
> deduplication domain: identical content is stored once, and a grant from
> either tenant authorizes those bytes. Distinct content stays isolated. If two
> tenants must not share a store, give them separate nodes.

Section 5.8 tests that, including the case where it could have been a leak and
is not.

### What I ran

Every API claim below was compiled and run against the real crates on this
machine. The probe is at `/tmp/kl-blobs-probe/`. It is scratch code and it is
not part of the repository.

| Item | Value | How I checked it |
|---|---|---|
| `iroh-blobs` | 0.103.0, latest on crates.io | crates.io API, local registry source |
| `iroh-blobs` -> `iroh` | `^1.0.0` | `Cargo.toml` of the vendored crate |
| `iroh-blobs` MSRV | `rust-version = "1.91"` | same file |
| Local toolchain | rustc 1.97.1 | `rustc --version` |
| keylink `iroh` | 1.0.3 | `/Users/pedrocunha/repos/keylink/Cargo.toml` |
| `iroh-gossip` | 0.101.0 | compiled in the probe, as a foreign protocol |
| Vendored source | `~/.cargo/registry/src/index.crates.io-1949cf8c6b5b557f/iroh-blobs-0.103.0` | read directly |

### An accepted risk, recorded on purpose

iroh-blobs 0.103.0 carries this in its own `README.md`, first line after the
title:

> **NOTE: this version of iroh-blobs is not yet considered production quality.
> For now, if you need production quality, use iroh-blobs 0.35**

On 2026-08-05 the owner read that warning and chose 0.103.0 anyway. That advice
was not available in any case: iroh-blobs 0.35.0 requires `iroh ^0.35`, and
keylink is on iroh 1.0.3. This is a knowing decision, not an oversight.

**The immaturity argues for this design rather than against adoption.** A
dependency that ships a dead security field and teaches an unauthenticated mount
in six of eight call sites is exactly the dependency a wrapper must own
outright. On an immature crate the wrapper carrying the safety is worth more,
not less. Sections 4.1 and 4.2 are the evidence.

Three claims in the brief were wrong, and I correct them in place below:

1. The `EventMask` field is `connected`, not `connect`.
2. `browser-blobs` is not a published crate. It is a directory in
   `n0-computer/iroh-examples`, unmaintained. See section 7.
3. A default mount leaks reads **and accepts writes**. See section 4.2.

## 2. The constraint that shapes the API

`iroh_blobs::Hash` does **not** implement `postcard_schema::Schema`. The crate
does not depend on `postcard-schema` at all. I checked with a compile error:

```
error[E0277]: the trait bound `iroh_blobs::Hash: postcard_schema::Schema` is not satisfied
```

The `service!` macro derives `Schema` on the generated `Request` and `Response`
enums. Therefore `iroh_blobs::Hash` cannot appear in a `service!` type. keylink
must own the type.

This is good news, not bad news. A keylink-owned type sits inside the hashed
contract, so it moves the ALPN. Invariant 1 holds without extra work.

```rust
/// A content address: the BLAKE3 hash of a payload.
///
/// keylink owns this type because `iroh_blobs::Hash` does not implement
/// `postcard_schema::Schema`, so it cannot appear in a generated wire type.
/// The postcard encoding is the same 32 raw bytes, so the two types
/// cross-decode.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, postcard_schema::Schema,
)]
pub struct BlobId([u8; 32]);
```

I checked the encoding claim:

```
iroh_blobs::Hash -> 32 bytes
keylink BlobId   -> 32 bytes
wire-identical: true
cross-decodes:  true
```

`iroh_blobs::Hash` writes `self.0.as_bytes()` (a `[u8; 32]`) when the serializer
is not human readable, at `src/hash.rs:175`. postcard is not human readable.

## 3. API shape

### 3.1 The `service!` block

One new kind, `blob`. It reads like `recv`, and it replaces `Body` with
`BlobId`.

```rust
keylink::service! {
    service Inventory;

    rpc  Ping() -> String;
    rpc  GetItem(sku: String) -> Option<Item>;

    recv Fetch(sku: String, kib: u32) -> Datasheet;   // unchanged
    send Upload(name: String) -> UploadOutcome;       // unchanged

    blob Manual(sku: String) -> ManualInfo;           // new
}
```

| kind | handler | client |
|---|---|---|
| `rpc` | `async fn f(&self, a: A) -> R` | `f(a) -> R` |
| `send` | `async fn f(&self, a: A, b: BodyReader) -> R` | `f(a, body: Body) -> R` |
| `recv` | `async fn f(&self, a: A) -> (R, Body)` | `f(a) -> (R, BodyReader)` |
| `blob` | `async fn f(&self, a: A) -> (R, BlobId)` | `f(a) -> (R, BlobHandle)` |

### 3.2 The accept side

```rust
impl inventory::Handler for Warehouse {
    /// Return a header and a content address. keylink records the grant.
    async fn manual(&self, sku: String) -> (ManualInfo, BlobId) {
        let id = self.blobs.add_file(self.manual_path(&sku)).await?;
        (ManualInfo { sku, pages: 42 }, id)
    }
}
```

The handler names no ALPN, no connection and no event channel. It returns a
content address. Returning it **is** the grant, because the generated dispatch
records it:

```rust
// generated by `service!` for a `blob` method
async fn __invoke_manual<H: Handler>(
    handler: &H,
    cx: &Context,
    blobs: &Blobs,
    _body: BodyReader,
    sku: String,
) -> (Response, Body) {
    let (value, id) = handler.manual(sku).await;
    // Takes the `Context`, not a raw id: the peer was proved on this
    // connection's handshake. See section 5.3.
    blobs.grant(cx, id);
    (Response::Manual((value, id)), Body::empty())
}
```

There is no `blobs.grant(...)` call at the call site, and there is no way to
return a `BlobId` without one. That is the answer to "impossible to forget".

### 3.3 The call site

```rust
let (info, manual) = client.manual("ADU-014".into()).await?;
println!("{} pages", info.pages);

let mut file = tokio::fs::File::create("manual.pdf").await?;
manual.copy_to(&mut file).await?;
```

Compare invariant 5 against this snippet. There is no IP, no port, no framing,
no serialization call, no ALPN and no version number. `BlobId` is a content
address, which is a domain concept in the same class as an ETag.

`BlobHandle` opens the second connection internally. Two methods, no more:

```rust
impl BlobHandle {
    /// The content address, for logging or for a later fetch.
    pub fn id(&self) -> BlobId;

    /// Stream the verified bytes into `sink`.
    ///
    /// No local store, so no deduplication and no resume across a restart.
    /// Peak memory is one chunk. This is the direct replacement for
    /// `BodyReader::copy_to`.
    pub async fn copy_to<W: AsyncWrite + Unpin>(self, sink: &mut W) -> Result<u64>;

    /// Fetch into a local store.
    ///
    /// Deduplication and resume come from the store: a partial transfer
    /// continues where it stopped, and a blob already present costs nothing.
    pub async fn fetch_into(self, store: &Blobs) -> Result<u64>;
}
```

`copy_to` builds on `iroh_blobs::get::request::get_blob(connection, hash)`,
which streams verified bytes with no store at all. I read the implementation at
`src/get/request.rs:112` and the worked example at `examples/get-blob.rs`. The
stream yields `GetBlobItem::Item(BaoContentItem::Leaf(leaf))`, and `leaf.data`
is the payload.

> **Unverified here.** The example writes leaves to stdout in arrival order and
> expects correct output, which implies a whole-blob request delivers leaves in
> offset order. I did not read the bao-tree state machine to confirm that in
> general. An implementation must either confirm it or write by `leaf.offset`.

`fetch_into` builds on `store.remote().fetch(conn, hash)`, which I ran end to
end. See section 5.

### 3.4 Rejected alternative

Carrying the `BlobId` in `Envelope` beside `body_len` also works, and it needs
no per-kind response type. I rejected it. Every unary rpc pays a byte for a
field it never sets, and the generated `Response` no longer shows which methods
carry content. The per-kind response type is
`__keylink_response_ty!($kind, $ret)`, which expands to `$ret` for three kinds
and to `($ret, BlobId)` for `blob`. A `macro_rules!` call is legal in type
position, so this needs no new mechanism.

## 4. Coexistence

### 4.1 The escape hatch exists. It is currently the bypass.

My first draft proposed `ServerBuilder::protocol(alpn, handler)`. **That work has
already landed**, under a different name, in commit `2e02489` "fix(keylink): make
foreign protocols actually mountable":

```rust
// keylink/src/rpc.rs, on Service
pub fn foreign(alpn: &'static [u8], handler: impl ProtocolHandler) -> Self;
```

The reasoning in that commit matches what I found independently:
`Server::router()` returns a `&Router`, and iroh 1.0.3 accepts protocols only
through `RouterBuilder`, which `spawn(self)` consumes. I read
`src/protocol.rs:484` and `:501` of iroh 1.0.3 to confirm. So this RFC proposes
**no new escape hatch**. `Service::foreign` is the right shape and the right
name, and the surface table in section 11 drops my `protocol()` proposal.

**But as it stands, `Service::foreign` is exactly the hole this RFC closes.**
Its own doc comment says so:

> A foreign protocol brings its own admission rules, or none. `Allowlist` does
> NOT cover it. `iroh-blobs`, for one, will serve any hash to any peer that asks
> unless you wire its own intercept. Read the protocol's documentation before
> you mount it.

That is an accurate warning and a correct decision for `iroh-gossip`. For
iroh-blobs it is the failure mode the brief rules out: mounting blobs without a
policy must be impossible, not documented as risky. Two things need to change.

**One.** The doc example currently reads:

```rust
let blobs = BlobsProtocol::new(&store, None);
Server::builder(&node)
    .service(inventory::service(handler, allow).build())
    .service(Service::foreign(iroh_blobs::ALPN, blobs))
    .spawn()?;
```

`BlobsProtocol::new(&store, None)` is the mount I demonstrated serving every
hash to every peer **and accepting writes** (section 4.2). Copying that example
produces an unauthenticated store on the same public key as a strictly
allowlisted service. The example must not show blobs.

**Both fixes have since landed**, in commit `483bc08` "fix(keylink): refuse
reserved ALPNs instead of warning about them". `RESERVED_ALPNS` in
`keylink/src/rpc.rs:289` holds `b"/iroh-bytes/4"`, and `spawn` raises
`ConfigError::ReservedAlpn` at `rpc.rs:423`. So `ConfigError::ReservedAlpn` in
the section 12 table is **already implemented**, not proposed. It is kept in the
table because this RFC depends on it.

The rest of this section records why, since the reasoning is what the next
reserved ALPN will need.

**Two.** `spawn` must reserve `iroh_blobs::ALPN`. It already walks every service
and rejects a duplicate ALPN:

```rust
// keylink/src/rpc.rs, as it stands
for service in self.services {
    if !seen.insert(service.alpn) {
        return Err(Error::Config(ConfigError::DuplicateAlpn(/* .. */)));
    }
    builder = builder.accept(service.alpn, service.handler);
}
```

The reservation is one more check in that same loop, raised as
`ConfigError::ReservedAlpn`. keylink mounts blobs itself from the `Blobs` its
services carry (section 4.3), so a hand-rolled mount is never the only way to
get blobs on a node. It is only the way to get blobs **without the ledger**.

This is the whole answer to "show me the types that enforce it". The enforcement
is not a warning in a doc comment. It is a startup error on the one ALPN keylink
owns, plus the `ProvenPeer` type in section 5.3 for the decision the generated
policy makes.

### 4.2 Why keylink mounts blobs itself

I mounted `BlobsProtocol::new(&store, None)` exactly as the crate documentation
shows, and ran two attacks from a peer that no allowlist names.

**Read.** A peer that learns a hash fetches the content:

```
NAIVE mount   -> fetched 20 bytes  <-- this is the hazard
```

**Write.** A peer pushes unsolicited content into the store:

```
push accepted
server store HAS the stranger's blob after 0 polls  <-- WRITE HOLE
   contents: "unsolicited payload from a stranger"
```

The write hole surprised me, because `EventMask::DEFAULT` sets
`push: RequestMode::Disabled`. That field is dead. `EventSender::request` at
`src/provider/events.rs:462` reads `self.mask.get` for **every** request type.
The four call sites in `src/provider.rs:150-178` cover get, get_many, push and
observe, and all four funnel into that one function. `self.mask.get_many`,
`self.mask.push` and `self.mask.observe` are never read anywhere in the crate:

```
$ grep -n "self\.mask\." src/provider/events.rs
421:            match self.mask.connected {
462:            match self.mask.get {
518:        let throttle = match self.mask.throttle {
```

**The wide-open mount is what the documentation teaches.** This is not one
careless example. I listed every `BlobsProtocol::new` call site in the crate:

| Site | Policy |
|---|---|
| `src/net_protocol.rs:24` (module documentation) | `None` |
| `examples/transfer.rs:15` | `None` |
| `examples/custom-protocol.rs:102` | `None` |
| `examples/transfer-collection.rs:43` | `None` |
| `examples/mdns-address-lookup.rs:71` | `None` |
| `examples/compression.rs:196` | `EventSender::DEFAULT`, same thing |
| `examples/limit.rs:361` | `Some(events)` |
| `examples/random_store.rs:241` | `Some(events)` |

Six of eight mount with no policy, including the crate's own module
documentation, which is the first code a reader sees. The two that pass a policy
are `limit.rs`, whose entire subject is limiting, and `random_store.rs`. A user
who copies any introductory example gets an unauthenticated store on the same
public key as a strictly allowlisted keylink service.

I treat the dead mask as a bug in iroh-blobs 0.103.0 and I report it in section
9. Two consequences for this design:

1. A user must never be able to mount blobs with the default event sender.
   keylink mounts it, so a user never can.
2. Setting `get: RequestMode::Intercept` intercepts all four request types with
   one handler. That is convenient today and fragile tomorrow. If the crate
   fixes the dead mask, keylink must set all four fields. The design therefore
   sets all four fields now, and the tests in section 8 pin the behavior.

### 4.3 Mounting

`Blobs` is created once per node and handed to each service that serves content.
`ServerBuilder::spawn` mounts the protocol from what the services carry.

```rust
let blobs = keylink::Blobs::memory();

let server = keylink::Server::builder(&node)
    .service(inventory::service(Warehouse::seeded(), allow, &blobs).build())
    .service(telemetry::service(Tele, tele_allow).build())   // no blobs, no argument
    .spawn()?;
```

`spawn` collects the distinct `Blobs` from every service:

- zero: nothing is mounted, and `iroh_blobs::ALPN` stays free.
- one: keylink mounts `BlobsProtocol` with the ledger policy.
- two or more distinct handles: `ConfigError::ConflictingBlobStores`. One node
  has one blobs ALPN, so it can have one store.

A service declares whether it needs a store at compile time. `macro_rules!` can
fold the kinds into a `const`:

```rust
const DECLARES_BLOBS: bool = false $( | $crate::__keylink_is_blob!($kind) )+;
```

`ServiceBuilder` carries that flag, and `spawn` raises
`ConfigError::MissingBlobStore { service }` when a service declares blob methods
and holds no store. Every keylink configuration error is raised at startup
today, and this keeps that property.

`Server::shutdown` already calls `Router::shutdown`, and iroh calls
`ProtocolHandler::shutdown` on every handler. `BlobsProtocol::shutdown` calls
`store.shutdown()`. So a persistent store flushes to disk with no new code. I
read `src/net_protocol.rs` and `src/store/fs.rs:44-65` to confirm.

### 4.4 What is deliberately absent

Two things an earlier draft had, both cut. Recorded so nobody re-proposes them
without new evidence.

**`Blobs::also_admit(Allowlist)`** widened admission to a whole-store read for
named peers. It existed only so outbound `iroh-docs` sync could work over a
shared store. iroh-docs is out of scope, so the weakening has no justification
left. It was a documented way to defeat the ledger, and the ledger is the best
part of this design.

**`Blobs::store()`** exposed the iroh-blobs store. I argued for keeping it on
consistency: `Node::endpoint()`, `Server::router()` and `blocking::Node::inner()`
all expose the object they wrap. That argument is real and it is not enough. The
only concrete consumer was iroh-docs, and an accessor added on a cancelled
premise is surface carried on speculation. Add it later, on evidence.

Handlers still publish content through `Blobs::add_file` and `Blobs::add_bytes`,
and clients still read it through `BlobHandle`. Nothing in the design needs the
raw store.

## 5. The security wiring

### 5.1 Why the `Allowlist` is the wrong input

The brief proposes routing the `Allowlist` into blobs admission. I built that,
ran it, and found it insufficient. Two reasons.

**A store is one flat namespace.** An allowlisted peer can fetch any hash in the
store, including content it was never offered:

```
B offered hash: 1093df55c3c44ad0171d818555c3aeca658924c0b5e0532a4a68501f8bd44a8e
B private hash: c7be6131d6ec66d4c2519603c8c4716f1c8fa19f4888a37242c4cd4853234878
B allowlisted -> fetched the PRIVATE blob, 25 bytes  <-- store is one flat namespace
```

Guessing a BLAKE3 hash is not feasible, so this is not a remote break on its
own. It is still wrong. It makes every hash a bearer token for the whole store.
Any hash disclosure then becomes a full read of that blob by any admitted
peer.

**An allowlist is per service, and blobs is per node.** keylink already serves
several services on one node, each with its own `Allowlist`. Two services with
different allowlists give one node two answers to "who can connect". There is
one blobs ALPN, so there is one answer. Picking one service's allowlist is
arbitrary. Taking the union is the least safe choice available.

### 5.2 The grant ledger

Admission is the ledger, not an allowlist. Every entry was created by an RPC
that already passed that service's `Allowlist` and its layers, so the ledger
composes correctly across services with different policies.

```rust
/// Which peer may fetch which content. Written by generated dispatch code,
/// read by blobs admission.
///
/// Keyed by [`ProvenPeer`], not by `EndpointId`. Section 5.3 explains why that
/// distinction is the whole guard.
#[derive(Debug, Default)]
struct Grants {
    inner: Mutex<HashMap<ProvenPeer, HashSet<BlobId>>>,
}

impl Grants {
    /// A panicking handler must not wedge admission for every later peer.
    /// Recover the guard rather than propagate poisoning.
    fn lock(&self) -> MutexGuard<'_, HashMap<ProvenPeer, HashSet<BlobId>>> {
        self.inner.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    fn grant(&self, peer: ProvenPeer, id: BlobId) {
        self.lock().entry(peer).or_default().insert(id);
    }

    fn permits(&self, peer: &ProvenPeer, id: &BlobId) -> bool {
        self.lock().get(peer).is_some_and(|set| set.contains(id))
    }

    fn knows(&self, peer: &ProvenPeer) -> bool {
        self.lock().contains_key(peer)
    }
}
```

`.expect()` and `.unwrap()` are denied by the workspace lints, so the guard
recovers from poisoning the way `demo/src/main.rs:193` already does.

### 5.3 The identity trap: `endpoint_id` is optional

`ClientConnected.endpoint_id` is `Option<EndpointId>`, at
`src/provider/events.rs:590`. I looked for when it is `None`.

**In iroh-blobs 0.103.0 it is never `None`.** There are exactly two construction
sites in the whole crate, and both fill it in:

```
$ grep -rn "ClientConnected {" src examples tests
src/provider.rs:295              endpoint_id: Some(connection.remote_id()),
examples/compression.rs:161      endpoint_id: Some(connection.remote_id()),
```

`src/provider.rs:295` is the `BlobsProtocol` path. The example is a third-party
handler built on the public `provider::handle_connection` and
`StreamPair::new`, which shows the intended extension point: anyone can drive
the provider from their own transport, and `Option` is the shape that allows a
transport with no proven identity.

**That is exactly what makes it dangerous.** The `None` arm is unreachable
through `BlobsProtocol` today, so a handler written with `_ => Ok(())` passes
every test anyone writes. The bug appears the day a transport reports no
identity, and it appears as a silent admission rather than an error. No test
catches it, because no test can produce the input.

So the design does not rely on writing the `match` correctly. It removes the
option of writing it wrongly.

```rust
/// A peer iroh proved during the handshake.
///
/// The only constructor takes the `Option<EndpointId>` that iroh-blobs reports
/// and yields nothing when there is no identity. Every authorization function
/// demands one, so no code path can decide about an unidentified peer.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) struct ProvenPeer(EndpointId);

impl ProvenPeer {
    fn proved(reported: Option<EndpointId>) -> Option<Self> {
        reported.map(Self)
    }
}
```

`Grants` is keyed by `ProvenPeer`, and `knows` and `permits` take `&ProvenPeer`.
I checked that the two ways of getting this wrong are compile errors, rather
than asserting it:

```
error[E0308]: mismatched types
   |       ----- ^^^^ expected `&ProvenPeer`, found `&Option<PublicKey>`
error[E0308]: mismatched types
   |       ------- ^^^^^ expected `&ProvenPeer`, found `&PublicKey`
```

A bare `EndpointId` is refused too, not only a `None`. The type says the key was
proved on a connection, so an id parsed from a request body cannot reach the
ledger either.

`ProvenPeer` is `pub(crate)`. The enforcement costs nothing in public surface.

There is a second, independent guard. The connection map is populated **only**
on a successful admit, so a request arriving on a connection keylink never
admitted finds no entry and is refused. The `None` case has to defeat both.

### 5.4 Why not `EventMask::ALL_READONLY`

`EventMask::ALL_READONLY` at `src/provider/events.rs:205` does set
`connected: ConnectMode::Intercept`, so it looks like the right starting point.
I measured it, and it is not.

It also sets `get: RequestMode::InterceptLog` and
`throttle: ThrottleMode::Intercept`. Both are machinery keylink does not use,
and both are load-bearing once switched on.

**`InterceptLog` streams per-request updates that the handler must drain.** My
first measurement used the obvious handler, which answers `msg.tx` and ignores
`msg.rx`. The transfer aborted:

```
Error: io: stream reset by peer: error 3
```

Error 3 is `ERR_INTERNAL`. `RequestMode::InterceptLog` builds
`RequestUpdates::Active(tx)` at `src/provider/events.rs:502`, and dropping the
receiver makes the provider's send fail mid-transfer. `RequestMode::Intercept`
builds `RequestUpdates::Disabled(tx)` instead, which sends nothing.

**`ThrottleMode::Intercept` costs one event-channel round trip per 16 KiB.**
`RequestTracker::transfer_progress` calls `throttle.rpc(...).await??` on every
payload write. Measured over a 64 MiB blob, after fixing the drain:

```
proposed keylink mask          64 MiB in  859.51ms  events=2    updates=0       throttle_rpcs=0
EventMask::ALL_READONLY        64 MiB in  867.70ms  events=4098 updates=4098    throttle_rpcs=4096
EventMask::DEFAULT (no policy) 64 MiB in  807.67ms  events=0    updates=0       throttle_rpcs=0
```

4096 is exactly 64 MiB divided by 16 KiB. The proposed mask needs two messages
for the whole transfer: one connect, one get.

**Honest reading of the timing.** On loopback the wall-clock difference is about
one percent, which is inside the noise of a single run. I do not claim
`ALL_READONLY` is slow. I claim it does four thousand times more work for a
result keylink discards, and that it fails outright unless the handler drains a
stream it has no use for.

**Verdict: build the mask by hand.** Name all six fields, so the dead-field bug
in section 4.2 cannot quietly change the meaning later.

### 5.5 The exact current API

This code compiles and runs against iroh-blobs 0.103.0. The field is
`connected`, not `connect`.

```rust
use iroh_blobs::provider::events::{
    AbortReason, ConnectMode, EventMask, EventSender, ObserveMode, ProviderMessage, RequestMode,
    ThrottleMode,
};

impl From<iroh_blobs::Hash> for BlobId {
    fn from(hash: iroh_blobs::Hash) -> Self {
        Self(*hash.as_bytes())
    }
}

/// Turn the grant ledger into a blobs admission policy.
///
/// Every mask field is named. `EventMask::DEFAULT` is not enough (section 4.2),
/// and `EventMask::ALL_READONLY` is the wrong shape (section 5.4).
fn policy(grants: Arc<Grants>) -> EventSender {
    let mask = EventMask {
        connected: ConnectMode::Intercept,
        get: RequestMode::Intercept,
        get_many: RequestMode::Intercept,
        push: RequestMode::Disabled,
        observe: ObserveMode::Intercept,
        throttle: ThrottleMode::None,
    };
    let (tx, mut rx) = EventSender::channel(32, mask);

    tokio::spawn(async move {
        // `RequestReceived` carries `connection_id`, not `endpoint_id`, so the
        // peer identity has to be remembered from the connect event. Only
        // proven peers are ever recorded, so a request on a connection keylink
        // never admitted finds nothing here.
        let mut proven: HashMap<u64, ProvenPeer> = HashMap::new();

        while let Some(msg) = rx.recv().await {
            match msg {
                ProviderMessage::ClientConnected(msg) => {
                    // No identity, no decision to make. See section 5.3.
                    let Some(peer) = ProvenPeer::proved(msg.endpoint_id) else {
                        msg.tx.send(Err(AbortReason::Permission)).await.ok();
                        continue;
                    };
                    let verdict = if grants.knows(&peer) {
                        proven.insert(msg.connection_id, peer);
                        Ok(())
                    } else {
                        Err(AbortReason::Permission)
                    };
                    msg.tx.send(verdict).await.ok();
                }
                ProviderMessage::ConnectionClosed(msg) => {
                    proven.remove(&msg.connection_id);
                }
                ProviderMessage::GetRequestReceived(msg) => {
                    let id = BlobId::from(msg.request.hash);
                    let verdict = match proven.get(&msg.connection_id) {
                        Some(peer) if grants.permits(peer, &id) => Ok(()),
                        _ => Err(AbortReason::Permission),
                    };
                    msg.tx.send(verdict).await.ok();
                }
                // keylink hands out one raw blob per grant. Nothing else is
                // reachable through a `blob` method, so nothing else is served.
                ProviderMessage::GetManyRequestReceived(msg) => {
                    msg.tx.send(Err(AbortReason::Permission)).await.ok();
                }
                ProviderMessage::PushRequestReceived(msg) => {
                    msg.tx.send(Err(AbortReason::Permission)).await.ok();
                }
                ProviderMessage::ObserveRequestReceived(msg) => {
                    msg.tx.send(Err(AbortReason::Permission)).await.ok();
                }
                _ => {}
            }
        }
    });

    tx
}
```

Then, inside `ServerBuilder::spawn`:

```rust
// `Blobs` keeps its store and ledger private. This is keylink's own
// internal access, not the public accessor that section 4.4 cuts.
builder = builder.accept(
    iroh_blobs::ALPN,
    BlobsProtocol::new(blobs.inner_store(), Some(policy(blobs.grants()))),
);
```

### 5.6 What I ran

Control first, because a deny that always denies proves nothing.

```
granted hash   -> fetched 16 bytes   OK (control)
ungranted hash -> REFUSED :: io: stream reset by peer: error 1   OK
granted again  -> fetched 16 bytes   OK (grant is durable)
```

Error 1 is `ERR_PERMISSION`, at `src/protocol.rs:399`.

Connection-level admission, with the same control:

```
ALLOWED peer  -> fetched 20 bytes  (control: must succeed) OK
DENIED peer   -> REFUSED: fetch: io: connection lost  OK
```

The write hole, closed:

```
   policy saw a PushRequest -> refusing
server store does NOT have it after 1s  -> policy closed the write hole
```

### 5.7 Failure modes

**The drain task is never spawned.** I dropped the receiver and connected
anyway:

```
A dropped-rx  -> REFUSED  (fails closed) :: closed by peer: internal (code 3)
```

`ConnectMode::Intercept` runs `client.rpc(f()).await??` at
`src/provider/events.rs:425`. A dead channel yields
`ProgressError::Internal`, and `handle_connection` closes the connection on any
error. Forgetting the task denies everyone. That is the safe direction, and the
design does not depend on it: keylink spawns the task.

**An unhandled `ProviderMessage` variant.** The `_ => {}` arm drops the message
and its `tx`, and the provider side sees a receive error, which denies. Fail
closed again. The design still answers every variant explicitly, because relying
on drop semantics for an authorization decision is not a property worth having.

**Grant lifetime.** Grants are per process and are never revoked. A peer keeps
a grant until the server restarts. That is a deliberate simplification and it
matches the resume story: a transfer interrupted for an hour must still resume.
It is also unbounded memory: 32 bytes per grant plus map overhead, growing with
distinct (peer, content) pairs. A server that grants at high rate needs a bound.
I did not design one. See section 9.

### 5.8 What a grant means, and what one node shares

The team lead raised this as a live defect: two keylink services already share
one node, so under this design they share one `Blobs` and one flat
`(peer, hash)` ledger. A grant issued by service A then reaches content service
B also serves. I tested it rather than agreeing, and the result changed the
answer.

**Distinct content is isolated. I checked the three ways it could fail.**

```
P asks for A's content it WAS granted
   -> FETCHED "service A public datasheet"

P asks for B's content, granted only to Q (P is on no allowlist of B)
   -> REFUSED: stream reset by peer: error 1

P asks for content in the store that nobody was granted
   -> REFUSED: stream reset by peer: error 1
```

The first line is the control. The ledger keys on the hash, so a grant from A
does not reach different content, whoever holds it and whoever else was granted
it.

**There is no existence oracle.** A hash the node holds but never granted is
indistinguishable from a hash the node does not have:

```
   present-ungranted -> REFUSED: stream reset by peer: error 1
   absent-entirely   -> REFUSED: stream reset by peer: error 1
   distinguishable?  -> false
```

That closes the classic attack on deduplicated storage, where a peer probes for
content by timing or by error code. Both refusals are `ERR_PERMISSION` and both
arrive the same way.

**The residual case is not a leak, because there is nothing to leak.** The only
content a grant from A reaches in B is content that is byte-identical. Content
addressing makes that one object:

```
IDENTICAL CONTENT added twice, as if by two services
   A's hash: 53eb084351b742c475d8c595cf52e9b660e72d0469b16fe0e1b0fd61fa70e5b4
   B's hash: 53eb084351b742c475d8c595cf52e9b660e72d0469b16fe0e1b0fd61fa70e5b4
   one blob or two? -> ONE
```

There is no "B's copy" to reach. A peer that fetches it receives exactly the
bytes A authorized. B holds the same bytes; it does not hold a second secret.

**So a grant authorizes bytes, not a service's copy of them.** That is the whole
property, and it is a consequence of content addressing rather than a gap in
this design.

#### The three mechanisms, and why none of them is worth building

**Scope-salted `BlobId` — self-defeating, as the team lead suspected.** The
blobs wire carries `GetRequest { hash, ranges }` and nothing else, so there is
no field a scope token could ride in. Scoping the id therefore means scoping the
*bytes*, which stores one copy per scope and destroys the deduplication that
motivated adopting blobs. It buys no confidentiality either, because the
underlying content is identical in every scope.

**Single-use grants — breaks resume, and fixes nothing.** A resumed transfer is
a second get request. I checked that a durable grant serves both:

```
   1st fetch -> FETCHED "service A public datasheet"
   2nd fetch -> FETCHED "service A public datasheet"
```

Consuming the grant on first use refuses the second, which is exactly the resume
that is one of the two reasons to adopt blobs. It also does not address the
residual case, which is about identical content rather than repeated access.

**One ALPN per scope — considered, rejected.** `BlobsProtocol` is an ordinary
handler, so keylink could mount one per service on its own ALPN over one shared
store, scoping admission at connect time. It preserves deduplication. It still
does not change the identical-content case, because that is one blob either way.
And it adds one ALPN per scope, which RFC-02 §10.2 measured as an O(N) cost on
every handshake plus a TLS configuration clone per mount. It buys nothing the
ledger does not already give.

#### The limitation a user must read first

Not confidentiality. Deduplication.

> **One node has one content store.** Two tenants on one node share a
> deduplication domain: identical content is stored once and a grant from either
> tenant authorizes those bytes. Distinct content stays isolated. If two tenants
> must not share a store, give them separate nodes.

This design already pushes users that way. `ConfigError::ConflictingBlobStores`
refuses two `Blobs` on one server, because one node has one blobs ALPN. Multiple
tenants therefore means multiple nodes, which is the honest shape.

The sentence above belongs in the `Blobs` type documentation, not in a risks
section, so that a user reads it while choosing a store rather than afterwards.

#### Tests that pin this

```
a_grant_to_one_peer_does_not_admit_another_peer
content_that_nobody_was_granted_is_refused
a_present_but_ungranted_hash_is_indistinguishable_from_an_absent_one
identical_content_added_twice_is_one_blob
a_grant_survives_a_second_fetch_so_resume_works
```

The third is the one to write carefully. It must compare the observable result
of both attempts, not merely assert that each fails, or it passes without
testing the oracle at all.

## 6. Store policy

### 6.1 What I checked about growth

Three separate facts, each one enough to fill a disk.

1. **GC is off by default.** `Options.gc` is `Option<GcConfig>` and the default
   is `None`, at `src/store/fs/options.rs:114` and `:124`.
2. **`.await` on an add creates a permanent tag.** `AddProgress::into_future`
   calls `with_tag()`, at `src/api/blobs.rs:677-685`, which calls
   `tags.create(...)`. A tagged blob survives GC forever. Turning GC on does not
   bound growth on its own.
3. **`add_path` copies the file.** `ImportMode` defaults to `Copy`, at
   `src/api/proto.rs:630-645` and `src/api/blobs.rs:265`. Serving a 4 GiB file
   costs 4 GiB of extra disk, plus the outboard. `TryReference` avoids the copy
   and is unsafe if the file changes after import.

`FsStore` also builds its own multi-thread tokio runtime, at
`src/store/fs.rs:1400`, and owns it. The module documentation at
`src/store/fs.rs:48` states this.

### 6.2 The policy

**The default is memory, and it needs no feature and no path.**

```rust
impl Blobs {
    /// Content lives in memory and is gone when the process exits.
    ///
    /// Bounded by RAM, which is a limit an operator already understands.
    #[must_use]
    pub fn memory() -> Self;
}
```

`MemStore::new()` needs no disk and no feature. keylink depends on iroh-blobs
with `default-features = false`, which drops `fs-store` (redb, reflink-copy) and
`rpc` (noq, irpc/rpc). I compiled that configuration.

**Disk sits behind a feature, and its constructor makes growth a typed choice.**

```rust
/// How long added content stays fetchable.
///
/// There is deliberately no `Default`, for the same reason [`Allowlist`] has
/// none: an unbounded disk must be a decision you type out.
#[derive(Debug, Clone, Copy)]
pub enum Retention {
    /// Content is removed `Duration` after it is added. keylink runs GC.
    Expire(Duration),
    /// Content lives until `Blobs::remove`. The store grows without bound.
    Forever,
}

#[cfg(feature = "blobs-fs")]
impl Blobs {
    /// Content lives on disk under `root`.
    ///
    /// # Errors
    ///
    /// [`Error::Io`] when the store cannot be opened.
    pub async fn persistent(root: impl AsRef<Path>, keep: Retention) -> Result<Self>;
}
```

This mirrors `Allowlist`, which has no `Default` because admitting everyone must
be typed out. `Retention::Forever` is the same shape of decision.

> **Implementation cost, stated plainly.** `Retention::Expire` is not a library
> call. iroh-blobs has no tag expiry API. `examples/expiring-tags.rs` shows the
> pattern: encode the expiry in the tag name, sweep expired tags on a timer, and
> set `Options.gc = Some(GcConfig { .. })`. keylink must implement that pattern.
> It is perhaps 80 lines and a background task. If that cost is unwelcome, ship
> `Forever` only in the first version and keep `Retention` as the enum, so
> adding `Expire` later is not a breaking change.

`add_file` uses `ImportMode::Copy`, the safe default, and the doc comment states
the doubled disk cost. I do not propose exposing `TryReference` in the first
version. It is one more knob and its failure mode is silent corruption.

## 7. WASM

**RFC-03 owns wasm for keylink. This section covers only what blobs adds to it.**

The blobs-specific facts, measured with the `wasm32-unknown-unknown` target
installed:

| Crate | `cargo check --target wasm32-unknown-unknown` |
|---|---|
| `iroh` 1.0.3, `default-features = false` | passes |
| `iroh-blobs` 0.103.0, `default-features = false` | passes |
| `iroh-blobs` 0.103.0, default features | fails |

The failure is the `rpc` feature, which pulls `noq`:

```
error: This wasm target is unsupported by mio. If using Tokio, disable the net feature.
```

`default-features = false` is what this RFC proposes anyway, for the store
reasons in section 6.2. So **iroh-blobs adds no new wasm blocker**. That is the
whole finding.

> **This part moved under me, and I rechecked it.** My first draft said keylink
> fails on wasm for the same mio reason, through
> `tokio = { features = ["full"] }`. Commit `1abfd35`, "target-scoped manifests
> for the browser (RFC-03 step 1)", fixed that. keylink still fails, but the
> cause is now its own source rather than a manifest:
>
> ```
> error[E0433]: cannot find `signal` in `tokio`
> error[E0433]: cannot find `fs` in `tokio`
> error[E0599]: no associated function `new_multi_thread` found for `tokio::runtime::Builder`
> ```
>
> RFC-03 says its source guards land in step 4. Read RFC-03, not this section,
> for the base-crate status. I have not rechecked it since that commit.

**On `browser-blobs`:** it is not a published crate, and it is not a repository.
crates.io returns `crate 'browser-blobs' does not exist`, and a GitHub search of
`n0-computer` returns zero repositories. It is a directory inside
`n0-computer/iroh-examples`, which is why neither check found it. Its wasm cfg
gate is commented out and CI does not cover it, so treat it as unmaintained
example code rather than a dependency. That correction came from the team lead,
and I did not check the directory myself.

What iroh-blobs 0.103.0 actually does for wasm is small and visible. Three
`cfg(wasm_browser)` sites exist in the entire crate:

- `src/store/util.rs:76`, a `SystemTime` conversion.
- `src/store/mem.rs:758`, where `import_path` returns
  `"import_path is not supported in the browser"`.
- `src/store/mem.rs:767`, the native `import_path`.

The alias is defined in `build.rs` as
`all(target_family = "wasm", target_os = "unknown")`.

**Statement for the RFC:** in a browser, `MemStore` compiles and file import
does not work. `FsStore` is unavailable, because `fs-store` pulls redb.
Everything else is untested. keylink must fix its own tokio features before any
of this is reachable.

## 8. Test plan

Names follow the existing convention in `keylink/tests/protocol.rs`: a sentence
describing the behavior. All of these belong in a new
`keylink/tests/blobs.rs`, behind the same two-local-endpoint fixture.

### The security tests

The first is the one the brief asks for. It needs its control beside it, or a
broken fixture passes it.

```
a_peer_with_a_grant_can_fetch_that_content                    (control)
a_peer_without_a_grant_cannot_fetch_a_hash_it_knows
a_granted_peer_cannot_fetch_a_different_hash_in_the_same_store
a_peer_with_no_grants_at_all_is_refused_at_the_connection
a_connection_reporting_no_endpoint_id_is_refused
a_request_on_a_connection_that_was_never_admitted_is_refused
a_grant_to_one_peer_does_not_admit_another_peer
content_that_nobody_was_granted_is_refused
a_present_but_ungranted_hash_is_indistinguishable_from_an_absent_one
identical_content_added_twice_is_one_blob
a_grant_survives_a_second_fetch_so_resume_works
a_push_from_any_peer_is_refused_and_the_store_is_unchanged
an_observe_request_from_any_peer_is_refused
a_get_many_request_from_any_peer_is_refused
```

`a_peer_without_a_grant_cannot_fetch_a_hash_it_knows` must hand the hash to the
client out of band, exactly as my probe did. A test where the client has to
guess the hash proves nothing.

`a_connection_reporting_no_endpoint_id_is_refused` cannot run against
`BlobsProtocol`, because iroh-blobs never reports `None` (section 5.3). It has
to call the policy function directly with a synthetic event. That is a weaker
test than the others, and it is the reason the `ProvenPeer` type exists: the
compiler covers what the test cannot reach.

### Configuration tests

Every one of these is a startup error, matching `ConfigError` today.

```
mounting_iroh_blobs_by_hand_through_protocol_is_refused
a_service_that_declares_blob_methods_without_a_store_is_refused
two_services_with_different_blob_stores_are_refused
a_foreign_protocol_on_a_service_alpn_is_refused
service_foreign_on_the_blobs_alpn_is_refused
a_service_and_a_foreign_protocol_coexist_on_one_node
the_store_accessor_hands_out_the_same_store_the_mount_serves
```

`the_store_accessor_hands_out_the_same_store_the_mount_serves` guards the
escape hatch in section 4.4: content added through the accessor must be
fetchable by a granted peer, or the accessor hands out a different store than
the one being served.

`a_service_and_a_foreign_protocol_coexist_on_one_node` is the regression guard
for the `set_alpns` defect that `two_services_share_one_node_and_both_answer`
already covers for services.

### Contract tests

```
the_blob_id_encodes_identically_to_an_iroh_blobs_hash
adding_a_blob_method_moves_the_alpn
changing_a_blob_methods_header_type_moves_the_alpn
```

The first is `postcard::to_stdvec` on both types, asserting equal bytes. It
fails the day iroh-blobs changes its `Hash` encoding, which is exactly when
someone needs to know.

### Behavior tests

```
a_blob_larger_than_the_header_cap_transfers_intact
a_blob_fetched_twice_into_one_store_costs_one_transfer
an_interrupted_fetch_resumes_from_where_it_stopped
a_blob_call_and_a_unary_call_do_not_block_each_other
corrupted_content_fails_verification_rather_than_landing_short
```

`an_interrupted_fetch_resumes_from_where_it_stopped` is the test that justifies
the whole feature. If it does not pass, `recv` is the better tool, and somebody
must reconsider this RFC.

`corrupted_content_fails_verification_rather_than_landing_short` is the blobs
counterpart of the existing
`a_body_shorter_than_declared_is_an_error_not_a_short_file`. It preserves
invariant 4 in the new path.

### What I could not put in the plan

I did not find a way to test the dead-mask behavior of iroh-blobs from outside
without depending on the bug. `a_push_from_any_peer_is_refused` passes today for
the wrong reason (the `get` mask intercepts pushes) and will pass tomorrow for
the right one (the `push` mask is honored). It is a correct test either way, and
it is not a regression guard for the upstream fix.

## 9. Risks, and what I could not check

### Risk 1: the crate says it is not production quality (accepted)

`README.md` of iroh-blobs 0.103.0, first line after the title:

> **NOTE: this version of iroh-blobs is not yet considered production quality.
> For now, if you need production quality, use iroh-blobs 0.35**

That advice is not available here. iroh-blobs 0.35.0 depends on `iroh ^0.35`,
which I checked through the crates.io dependency API. keylink is on iroh 1.0.3.
The only iroh-1.0-compatible line of iroh-blobs is the one its own authors call
not production quality.

**Decided on 2026-08-05: proceed on 0.103.0.** The risk stays open and named
rather than closed. What it costs us in practice, and what to watch:

- The API moves between minor versions. Pin the exact version, and read the
  changelog on every bump rather than trusting semver.
- Bugs of the class in Risk 2 are likely to exist elsewhere in the crate. The
  design assumes the dependency is wrong until measured, which is why section 8
  tests behavior rather than configuration.
- If a keylink release ever needs an audited blobs implementation, that is a
  fork or an upstream contribution, not a version bump.

### Risk 2: the dead event mask is an upstream bug

Section 4.2 documents it and section 5.6 shows the write landing. Consequences:

- Any keylink user who reaches `BlobsProtocol` directly gets a writable store.
  The reserved-ALPN rule in section 4.1 is what stops that, and it is worth more
  than it first appears.
- keylink's policy sets all four mask fields, so a fix upstream does not change
  keylink behavior. A regression the other way, where `get` stops intercepting
  everything, is caught by the section 8 tests.
- This needs a report to n0-computer. I did not send one.

### Risk 3: grants grow without bound

Section 5.7. A long-lived server that grants content to many peers accumulates
`(EndpointId, BlobId)` pairs and never drops them. I did not design a bound. The
options are a per-peer cap, a time-to-live matched to `Retention`, or dropping
grants for content that GC removed. The third is the most correct and the most
work.

### Risk 4: two connections, two admission paths

A `blob` call authorizes on the keylink connection and transfers on the blobs
connection. The two are separate QUIC connections with separate handshakes. The
grant ledger is what binds them, keyed by `EndpointId`, which iroh proved in both
handshakes. I believe that is sound. I did not model it adversarially, and I did
not consider what happens when a peer changes identity between the two.

### Risk 5: refusal is indistinguishable from a network failure

A denied peer sees `io: connection lost` or `stream reset by peer: error 1`.
keylink solved this for its own ALPN with `Error::SchemaMismatch` and
`classify_connect` at `rpc.rs:137`, precisely so an operator does not go looking
at the network. The blobs path reintroduces the problem. `ERR_PERMISSION` is
VarInt 1 and `ERR_INTERNAL` is VarInt 3, both at `src/protocol.rs:399-403`, so
the information is on the wire. Mapping them to a typed `Error::BlobRefused`
must be part of the work, not a follow-up.

One more wrinkle I saw: `execute_push` reported `Ok` to the client while the
server refused the push and stored nothing. A client cannot trust a blobs
success report to mean the server accepted the write. This does not affect the
proposed design, which never pushes.

### What I could not check

- Whether leaves from `get_blob` arrive in offset order in general. Section 3.3.
- Whether iroh QUIC works in a browser once keylink compiles for wasm.
  Section 7.
- `browser-blobs`. It does not exist, so there was nothing to check. Section 7.
- Real resume behavior across a process restart. I ran resume only within one
  process, and I did not interrupt a transfer mid-flight.
- Performance. The brief reports 8 MB in 409 ms for the existing `recv` path. I
  did not benchmark blobs against it. Blobs must hash the content before it can
  serve it, and `add_path` copies the file, so blobs will lose a first-transfer
  benchmark and win every repeat one. Measure it rather than reason about it.
- Whether `postcard-schema` 0.2 and iroh-blobs share any type that conflicts. I saw none, and the crate does not depend on postcard-schema at all.

## 10. Relationship to the existing body streaming

`send` and `recv` stay. Nothing is deprecated. They are verified to work, at
8 MB in 409 ms with a matching checksum, and this RFC gives no reason to remove
them.

The two paths are not competitors, because one thing separates them cleanly:
**blobs requires the content to exist before it can be served.** Content
addressing means hashing, and hashing means a complete payload. keylink's `recv`
streams whatever an `AsyncRead` produces, including content that does not exist
yet. The demo's `Pattern` reader generates its bytes as they are read, and
`docs`, logs and query results behave the same way. Blobs cannot do that at all.

Pick by asking two questions in order.

**Does the payload exist as bytes before the call?** If no, use `recv`. Blobs is
not an option.

**Will the same payload go to more than one peer, or more than once?** If yes,
use `blob`. Deduplication, resume and content addressing are what you are paying
for. If no, use `recv` and pay nothing.

| | `recv` / `send` | `blob` |
|---|---|---|
| Content must exist first | no | yes |
| Generated or streamed content | yes | no |
| Connections per call | 1 | 2 |
| Extra disk on the serving side | none | one copy of the file |
| Resume after interruption | no | yes |
| Repeat transfer of known content | full cost | free |
| Client streams up to server | `send` | not proposed |
| Works today | yes | no |

The last row matters. `recv` is what a user reaches for on day one, and it will
stay the right answer for most calls. `blob` is for catalogs: a datasheet, a
firmware image, a manual, anything served repeatedly to many peers.

There is no `blob` equivalent of `send`. Pushing content up means accepting
writes into the store, which is the hole section 4.2 documents. A client that
must upload uses `send`. If a push path is ever wanted, it needs its own design
and its own grant direction.

## 11. Dependency on the envelope layout

RFC-02 §10 settled that the actor system rides one ALPN, with the actor id in
the envelope and a `u64` per-actor schema hash over an **opaque** payload that is
decoded only after the hash matches. RFC-04 is still being written. This section
says which parts of this RFC that changes, so the blob path is not built twice.

### 11.1 Envelope-independent: build these whenever

Nothing below touches keylink's wire envelope. They can be implemented and
reviewed before RFC-04 lands.

- **The grant ledger**, `Grants` and `ProvenPeer` (section 5). Keyed on the
  connection identity iroh proved, not on any message shape.
- **Mount ownership** and `ConfigError::ReservedAlpn` (section 4). This is about
  iroh's router and the blobs ALPN, which is the constant `/iroh-bytes/4`.
  keylink's own envelope has no bearing on it.
- **The admission policy and its `EventMask`** (section 5.5). It is a function of
  iroh-blobs types only.
- **Store policy**: `Blobs`, `Retention`, the `blobs-fs` feature, and
  `default-features = false` on iroh-blobs (section 6). No wire contact at all.
- **`BlobHandle::copy_to` and `fetch_into`** (section 3.3). They speak the
  iroh-blobs protocol on its own connection, which keylink does not frame.
- **Every security test** in section 8.

That is most of the value in this RFC, and the whole of the security argument.

### 11.2 Envelope-dependent: reconcile before implementing

**(a) Where `BlobId` sits in the hashed contract.** Today the `blob` kind puts
`(R, BlobId)` in the generated `Response`, and `Contract` hashes
`Envelope<Request>` and `Envelope<Response>`, so a `BlobId` moves the ALPN.
Under RFC-02 §10 the payload is opaque and gated by a per-actor hash instead.
`BlobId` therefore belongs in the **per-actor payload schema**, not the envelope.
RFC-02 §10 already advises putting per-actor variation in the payload, so the two
designs agree — but check it rather than assume it. A `BlobId` is a capability.
If one could arrive under a mismatched schema, a peer could be handed a content
address it was never meant to see. RFC-02's "refuse before the payload is
interpreted" is exactly the property that makes a `BlobId` trustworthy.

**(b) The `blob` codegen.** Section 3 extends `service!` with
`__keylink_response_ty!` and `__keylink_is_blob!`. If RFC-04 forks or replaces
that codegen for actors, the `blob` kind must be added to whichever generator
survives. Mechanical work, but it is the literal "build the blob path twice" risk
if both land independently.

**(c) Which node holds the bytes.** `BlobHandle` opens its second connection to
the peer that answered the call. That is sound while the responder is the holder.
RFC-02 §10 keeps actors in a local registry behind one ALPN, so it holds today.
**If the actor system ever forwards or routes** — an actor not resident on the
dialled node — the responder and the holder differ, and the handle fetches from a
node with no such blob. Content addressing makes this recoverable, because any
holder will do and the hash verifies what arrives. But the handle must then carry
**which** node to ask instead of assuming it is the one that answered. Flagging
it now is cheap. Retrofitting it into a shipped `BlobHandle` is not.

**(d) The ledger has no actor dimension. Resolved in section 5.8 — no change
needed.** I first flagged this as a future problem needing a
`(peer, actor, hash)` key. The team lead corrected me twice: it is live today
between services, and a wider key has nothing to match against, because a blobs
fetch arrives on its own connection carrying only a peer and a hash.

Both corrections are right, and testing them changed the conclusion. Section 5.8
shows that distinct content is already isolated by hash, that there is no
existence oracle, and that the only content a grant reaches across a scope
boundary is byte-identical content, which is one blob rather than two secrets.

So the actor case needs nothing new. An actor system inherits the same property:
a grant authorizes bytes. What it inherits alongside it is the deduplication
domain, which is why section 5.8 tells a multi-tenant user to use separate
nodes. That guidance applies unchanged to multi-tenant actors.

**I did not read RFC-04.** It was still being written. Items (c) and (d) are
questions for whoever does, not conclusions about it.

## 12. Summary of new surface

**Eight items, one of which already shipped.** Everything else is internal. `Service::foreign` already exists,
so this RFC adds no escape hatch — it only reserves one ALPN on the one that
landed in commit `2e02489`. `ProvenPeer` is `pub(crate)` and does not count.

Cut from earlier drafts, and why: `ServerBuilder::protocol` (already landed as
`Service::foreign`), `Blobs::also_admit` (weakened the ledger, existed only for
iroh-docs), `Blobs::store` (only consumer was iroh-docs; add later on evidence).
Section 4.4 records the last two.

| Item | Kind |
|---|---|
| `blob` | `service!` method kind |
| `BlobId` | value type, 32 bytes, `Schema` |
| `BlobHandle` | client handle, 3 methods |
| `Blobs` | node object, `memory()` plus `persistent()` behind a feature |
| `Retention` | enum, 2 variants, no `Default` |
| `ConfigError::ReservedAlpn` | startup error — **already landed**, commit `483bc08` |
| `ConfigError::MissingBlobStore` | startup error |
| `ConfigError::ConflictingBlobStores` | startup error |

Plus a `blobs-fs` cargo feature, and `iroh-blobs` with
`default-features = false`.

## 13. Closure grants, revocation, and the end of `MAX_GRANTS`

Appended. Sections 5.7 and 9 (Risk 3) above record what was believed when they
were written, and they are not edited. This section says what replaced them and why.

### 13.1 What §5.7 and Risk 3 left open

The ledger kept 4096 `(peer, hash)` pairs and evicted the oldest across every
peer. Nothing revoked a grant. That bound was a judgement, not a measurement, and
it failed in two ways. A peer fetching a tree of more than 4096 blobs needed more
grants than the ledger would hold. And an evicted grant took its capability with it
without anybody deciding to withdraw it, so a peer could lose access because
another peer was busy.

### 13.2 Decisions

1. **A grant is `(peer, root, scope)`, and the scope is `Blob` or `Closure`.** A
   closure grant admits any `GetRequest` whose hash is the root, with any ranges.
   That is safe because iroh-blobs serves exactly the members the holder's own copy
   of the root lists, by offset. A requester names offsets, never hashes. Admission
   reads nothing from the store, so it costs the same for 5 members and for 50,000.
2. **A member is reached through its root, by index.** A request for member *i* is
   `GetRequest { hash: root, ranges: [empty, ..., ranges at offset i + 1] }`. A bare
   request for a member by its own hash is refused unless that member was granted on
   its own. Admitting a bare member would need a reverse index from member to roots,
   which grows with the total number of chunks. That is the cost this change removes.
3. **Nested sequences are not followed.** The HashSeq format does not say which
   children are sequences. Treating "parses as a sequence" as intent is the alignment
   hole §5.7 already closed for `Blob` grants: any 32-aligned member would grant every
   hash its bytes spell. A tree with sub-sequences is flattened into one sequence.
4. **An iroh-blobs `Collection` needs no special case.** Its root is a HashSeq whose
   child 0 is the metadata blob, so a closure grant on it covers both. keylink adds no
   `Collection` type, because iroh-blobs calls that format subject to change.
5. **Have/want is computed in the fetcher's own store.** `fetch_into` fetches the
   root alone when the store lacks it, then fetches the sequence, and iroh-blobs asks
   only for the members the store does not hold. Fetching the sequence in one request
   would send the whole closure when the root is absent, because the fetcher cannot
   know what it holds until it holds the list. `Blobs::missing` answers the same
   question without the network. The holder's grant is its claim that it has the
   closure. GetMany and Observe stay refused, because no caller uses either.
6. **Revocation is explicit.** `revoke(peer, root)`, `revoke_peer(peer)` and
   `revoke_everywhere(root)`. The per-root calls refuse the next request and let one
   already admitted run to completion. `revoke_peer` also closes that peer's live
   blobs connections, so a transfer in flight stops. Nothing recalls bytes already
   delivered.
7. **The bound is bytes, and it fails loudly.** Each grant is charged `GRANT_COST`
   and each peer `PEER_COST` against `GRANT_BUDGET` (64 MiB). A grant past the budget
   is answered `RemoteError::GrantsExhausted` and nothing is evicted. Grants do not
   expire: a clock would drop capabilities silently, which is the failure eviction had.
8. **A grant is made in two steps.** The ledger reserves it before the reply is
   encoded, so a full ledger replaces the reply and the caller is never handed an
   address it cannot spend. It commits once the idempotency ledger has settled, so a
   refused record leaves no live grant behind. A generation number on each entry stops
   a reservation from before a revoke from reviving a grant made after it.

### 13.3 Risks and what is not possible

- **A closure authorizes every hash its sequence lists.** If an application builds a
  granted sequence from hashes a peer supplied, that peer can list any hash the store
  holds and read it. `SeqId` stops the accidental form (a closure over uploaded bytes)
  and not this one. The rule is in the `Blobs::add_seq` documentation.
- **The request cap.** iroh-blobs refuses a request over 1 MiB. An estimate, not
  measured: a closure of about 150,000 members with scattered gaps can pass it. The fix,
  if it happens, is to split the second fetch into windows of indices.
- **Fetcher memory.** During the second fetch iroh-blobs keeps about 100 bytes per
  member for the length of the fetch. That is the fetcher's memory, not admission's.
- **`GRANT_COST` and `PEER_COST` are derived from `size_of`, not measured.** A
  counting-allocator test could pin them.
- **Restarts.** Grants die with the process while a durable idempotency record
  survives. A replay after a restart carries an address with no grant, and the fetch is
  refused. That predates this change.
- **Removing a peer from an `Allowlist` or `DynamicAdmission` does not touch its blob
  grants.** The ledger is per node and does not record which service granted what.
  Drop the peer from admission, then call `revoke_peer`.
- **Not possible within these bounds:** admitting a bare member by its own hash with
  constant memory per closure; stopping one admitted request without closing its
  connection (the only per-transfer hook costs a round trip per 16 KiB); recalling
  bytes already delivered.

### 13.4 Rejected

- **A membership index so a bare member is admitted.** See decision 2. It also adds
  state that must be kept in step with revocation, and no have/want caller needs it.
- **A second macro kind `closure F() -> R` with its own handle.** It doubles every
  `blob` arm in the macros, the HTTP gateway and OpenAPI, and the two handles would
  differ in one method's behaviour. The scope belongs to the value a handler returns,
  not to the method's name. One kind returns `Content`.
- **Expiring idle grants.** It drops capabilities silently, it needs a sweep, and with
  explicit revocation and a budget that fails loudly it buys nothing.

### 13.5 Corrections found at the merge gate

Appended. The text above stays as it was written; these are the places where it was wrong.

**13.5.1 `SeqId` did not stop a peer choosing a closure.** §13.3 says `SeqId` "stops the accidental form (a closure over uploaded bytes)". It did not. `SeqId` derived `Deserialize`, so a handler that took a `SeqId` as an argument and returned it granted a closure over whatever bytes the peer had uploaded, with no call to `Blobs::add_seq`. The same was true of `Content`, which is deserializable because the caller reads it off every reply.

The shape that replaced it: `SeqId` has no serde and no `Schema`, and its only constructor is `Blobs::add_seq`. `Content` is the wire value and is `Content::Blob(BlobId)` or `Content::Closure(BlobId)`, where naming a closure grants nothing. A handler returns a `Grant`, which converts from a `BlobId` or a `SeqId` and from nothing else, so the only closures a handler can grant are sequences this node built. The conversions that would reopen it are probed at compile time in `representability.rs`. What it still does not stop is a handler building a sequence from hashes a peer supplied; that is the first risk in §13.3 and is unchanged.

**13.5.2 A holder could make the fetcher plan without end.** §13.3 called fetcher memory "the fetcher's own memory, not admission's" and left it unbounded, and listed the 1 MiB request cap as a risk to fix "if it happens". Both were wrong to leave. The member list is the holder's to make as long as it likes, and `missing` and `fetch_into` downloaded the root unchecked and handed iroh-blobs the whole closure to plan in one request.

Now: a closure lists at most `MAX_CLOSURE_MEMBERS` (262,144) members, and the fetcher checks the root's size, proved by the last chunk, before it downloads or plans anything. `fetch_into` then asks for the members 4096 at a time, so a plan and a request stay small whatever the length, which also removes the request-cap risk. `BlobFault::TooManyMembers` is the refusal.

**13.5.3 A closure grant admitted any offset.** §13.2 decision 1 says admission is O(1) "for any ranges". A request can select nothing until offset `1 << 60` in a few bytes, and iroh-blobs walks the empty offsets one at a time before it looks at the sequence, so one request held a thread of the holder for good. Admission now refuses a request whose encoded runs start a non-empty run past `MAX_CLOSURE_MEMBERS`, reading the runs and never expanding them.

**13.5.4 `fetch_into` accepted a root that was not a sequence.** `missing` refused it and `fetch_into` did not, because upstream derives the member count from the size and a bad list has none. Both now check the root first, whether it is fetched or already held.

**13.5.5 A pending reservation shared the entry's scope, and a revoke missed calls already running.** §13.2 decision 8 says a generation stops a stale reservation. It did not stop a blob commit from making a still-pending closure live, because both wrote one scope field; the entry now holds only what was committed and a permit widens it. And a handler already running when `revoke_peer` landed had no entry for the revoke to remove, so it reserved and committed afterwards; calls now take a fence at dispatch and a reservation made under a fence from before a peer revoke is refused as `GrantsWithdrawn`.

**13.5.6 The budget bounded entries, not memory.** Revoking refunded the charge while the hash tables kept their buckets, so churn could retain several times the budget. Both tables are trimmed whenever their capacity passes twice their length, which keeps the retained buckets inside the charge the constants were derived from.

### 13.6 Corrections after the second review

Appended to §13.5, which stays as it was written. These are the places where §13.5 or the code under it was wrong.

**13.6.1 The fence was taken too late.** §13.5.5 says calls take a fence at dispatch. The fence was taken inside `serve_one`, after the request was read and the idempotency key claimed, and admission is decided once per connection and copied into every call. A call accepted before the peer was removed could still be on its way to its fence when the removal and `revoke_peer` both landed, take the new epoch, and look like a call admitted afterwards. The fence is now taken in the accept loop, in the same step that accepts the stream and before the task is spawned, and the connection is asked whether it is still open only after that. Removing a peer closes its connection first, so a fence taken after `revoke_peer` finds the connection closed, and one taken before carries the old epoch: either way the call cannot grant. A call on a closed connection is dropped unread.

**13.6.2 Trimming on `capacity()` did not bound allocation.** §13.5.6 says both tables are trimmed whenever their capacity passes twice their length. hashbrown reports capacity as live entries plus room before the next growth, and a deleted slot is a tombstone that lowers it without freeing anything, so the rule could stay false while the allocation stayed large, and the test that checked it rebuilt the allocation from the same number. Each table now keeps a high-water mark, the most entries it has held at once since it was last rebuilt. hashbrown grows a table only when its live entries need it, so a table's allocation follows that mark. The ledger charges for the mark, not the length, and rebuilds the table (`shrink_to_fit`, or a fresh one when it is empty) when its length falls under half the mark, which is the only thing that lowers the charge. The same rule applies to the table of calls in flight, which is not charged. The test now counts the bytes the thread allocates and compares them with what the ledger has charged, after fill-and-revoke churn, a generation of peers leaving, and insert-and-delete cycles that churn a table without changing its size.

What the budget bounds is entries times a per-entry cost derived from type sizes. It does not bound the allocator's own overhead, or the smallest peers' table (about 340 bytes), which sit outside it. A consequence worth stating: a revoked peer's slot stays charged until its table is rebuilt, so the charge can run up to twice the live entries.

**13.6.3 Fetch windows were not window-sized.** §13.5.2 says `fetch_into` asks for members 4096 at a time so a plan stays small. It called `Remote::local_for_request` once per window, and that indexes every member of the root before it looks at the window, so the memory was the whole root's and the work was repeated for every window. Each window now reads only its own slice of the root off the store, asks the store which of those members it holds in full, and requests the rest as one run per stretch. A member held only in part is requested whole again; what is already there is verified and rewritten. The planning is linear in the closure across all windows. A 20,000-member closure, and an interrupted fetch that resumes, are tested.

**13.6.4 The size proof's bytes went uncounted.** `fetch_into` returns the payload bytes that crossed the wire, and the root's size proof (the root's last chunk) crossed it before the root did. It is counted now.

**13.6.5 The holder-side check bounds where a run starts, not where it ends.** §13.5.3 and the `MAX_CLOSURE_MEMBERS` documentation said a holder refuses a request that selects a member past the limit. It refuses a non-empty run that starts past the limit, which is what makes the walk to it long. A run that starts below the limit may go on as far as the holder's sequence does, as an open-ended request for a whole closure legitimately does. `Blobs::add_seq` does not refuse a longer sequence; the fetcher will not read one. The documentation says so.

**13.6.6 Stale documentation.** `BlobHandle::new` said a `SeqId` is `Serialize`; the local-calls paragraph in `turns.rs` said a blob call returns `Content`; the `closure_share` header linked `Content::Closure` without importing it; and the `fetch_into` documentation said there is no size preflight for a closure. All four are corrected.

### 13.7 Both limits gone, and the budget is a count

Appended to §13.6. Pedro asked for two limits to be removed completely and simply.

**13.7.1 The byte budget is a count of grants.** §13.2 decision 7, and §13.6.2 after it, charged each table a per-entry cost derived from `size_of` and tracked a high-water mark per table to make the charge follow the allocation. That was a model of hashbrown's allocator, checked against another model of it. The ledger now holds at most `GRANT_LIMIT` grants (524,288) across every peer, `Blobs::set_grant_limit` changes it, and the refusal is `RemoteError::GrantsExhausted { limit }`. `GRANT_BUDGET`, `GRANT_COST`, `PEER_COST` and `set_grant_budget` are deleted. A peer has an entry only while it holds at least one grant, so peers never outnumber grants and one count bounds both tables. The rebuild rule stays (a table is rebuilt when its length falls under half the most entries it has held since it was last rebuilt, and an empty one is dropped), so allocation per live grant stays bounded under churn; the high-water mark is now only that rule's memory and charges nothing.

What it costs is measured, not derived. `keylink/tests/grant_allocation.rs`, an isolated target with a counting global allocator, fills a ledger over eight real peers and prints bytes of heap per grant: 568 for a lone grant, 101.8 at 1,000, 64.2 at 100,000, 100.7 per grant held after revoke-and-regrant churn, and 128.5 for 100,000 held after 300,000 one-for-one trades, on aarch64 macOS. The last is the most: deleted slots are tombstones, and hashbrown answers a table full of them by doubling it, so a table that trades at a steady size sits at about twice the size it needs, and the rebuild rule, which looks at length against high-water mark, does not see that. The default limit is about 64 MiB at that worst figure. The test asserts a ceiling on each run and was watched red with a ceiling less than half what the runs need.

**13.7.2 `MAX_CLOSURE_MEMBERS` is gone.** §13.5.2 and §13.5.3 capped a closure at 262,144 members on both ends. The holder-side half existed to stop a request naming an offset near `u64::MAX`; the cap only moved the line and made a legitimate fetch of a larger closure fail with a refusal that looked like a permission problem (`BlobRefused`: the holder refused the one request whose runs started past the cap). It is replaced by the check the cap stood in for: the holder reads the root's size from the store, once, after the grant has been found, and refuses a request whose furthest non-empty run starts past the members the root lists. Nothing is expanded, and a hash nobody was granted is still refused without touching the store. The fetcher-side half is gone because windowing already bounds it: each window of 4096 members is planned from its own slice of the root, and a window's request has at most 4096 entries, which a compile-time check holds far under `iroh_blobs::protocol::MAX_MESSAGE_SIZE`. `BlobFault::TooManyMembers` is deleted.

A closure of 300,000 tiny members, of which the fetcher holds every other one, is fetched in the suite. Before the change it failed with `BlobRefused { id: BlobId(0eed953e…) }` on the tree that asked for the whole closure in one request.

**13.7.3 No rpc sends a list of what is missing.** `Missing` has no `Serialize`, so a handler that wanted to tell a peer what a store lacks would have to convert it by hand, and the oracle warning on `Blobs::missing` still applies.

### 13.8 Corrections after the last review

Appended to §13.7. The limit changes here, so the figure in §13.7.1 is superseded.

**13.8.1 The revoke recipe needs every connection closed.** §13.6.1 says removing a peer closes its connection first, so a fence taken after `revoke_peer` finds it closed. That holds for each connection admission closes. `DynamicAdmission` keeps one revocation handle per peer, so a second live connection of the same peer survives a `replace`, is still open, and can take a fresh fence after `revoke_peer`. The documentation of `revoke_peer` and the README now say so. The `trust` branch fixes the registry, which already tracks connections by id; admission is not changed here.

**13.8.2 Windowing bounds memory and request size, not work.** §13.6.3 says the planning is linear in the closure across all windows. iroh-blobs 0.104 walks a request's offsets one at a time (`iter_non_empty_infinite`) on both ends, so a window that starts at member `k` costs `k` steps before its first member, and a closure of `N` members costs about `N^2 / 4096`. That is not measured on its own. What is measured: a 300,000-member closure with alternating gaps takes 74 s from a cold start (176 s on a loaded machine) (`mise run closure-300k`, which also adds both stores' members and makes about a million store round trips), and 12,289 members take 8 s, in the gate.

**13.8.3 The sparse shape costs more per grant, and sets the default.** §13.7.1 measured only grants concentrated on a few peers. With one grant per peer each grant also pays for a slot in the peers' table and a table of its own: 386.3 bytes at 1,000 peers and 320.7 at 100,000 (a unit test in `src/blobs.rs`, because real connections cannot supply that many peers). `GRANT_LIMIT` is 172,032, about 64 MiB at 386.3 bytes; at the dense figures the same limit is 11 MB to 22 MB.

**13.8.4 A blob listed twice in one window crossed the wire twice.** Each place a member lacked was requested. Now only the first occurrence of a lacking hash in a window is; the sequence keeps its order and its repeats, and `missing` still lists them.
