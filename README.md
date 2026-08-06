# keylink

Typed peer-to-peer RPC over [iroh](https://iroh.computer). You define a service;
callers get async methods. Everything underneath — NAT traversal, framing,
schema versioning, authorization — is invisible from the call site.

```rust
keylink::service! {
    service Inventory;

    rpc  GetItem(sku: String) -> Option<Item>;
    rpc  Reserve(sku: String, qty: u32) -> ReserveOutcome;

    recv Fetch(sku: String) -> Datasheet;   // a file streams down beside the reply
    send Upload(name: String) -> Stored;    // and up beside the request
}
```

```rust
let node   = keylink::Node::bind().await?;
let client = inventory::Client::connect(&node, peer_id).await?;

let item = client.get_item("ADU-014".into()).await?;   // this is the whole API
```

No IP address, no port, no framing, no serialization call, no ALPN string, no
version number appears in your code.

## What it handles

**Addressing.** Peers are dialed by Ed25519 public key. iroh hole-punches
directly when it can and relays when it cannot; neither side names a location.

**Concurrency.** Every call takes its own QUIC stream, so calls run in parallel
and a slow handler blocks nobody. There is no client mutex, no request-id
correlation and no cancellation bookkeeping — dropping the stream cancels the
call.

**Large payloads.** A body is not framed: its extent is the rest of the stream
and its flow control is QUIC's. A 4 GiB file costs zero framing bytes and one
chunk of peak memory. A body that ends early is an error, never a short file.

**Schema safety.** postcard is not self-describing — enum variants encode by
declaration index, so swapping two variants with identical shapes decodes
cleanly as the wrong one. So the ALPN is a hash of the contract, and a drifted
peer is refused at the handshake:

```
Error: schema mismatch: the peer was built against a different contract
```

Nothing to bump by hand. Change any type reachable from the protocol —
including one three structs deep — and the ALPN moves with it.

**Authorization.** iroh proves *who* a peer is; it never decides whether they
are welcome. `Allowlist` does, and fails closed: it has no `Default`, admitting
everyone must be typed out, and `from_env` errors at startup rather than
bouncing every peer at runtime.

**Composition.** Many services share one node, and a keylink service can sit
beside a foreign iroh protocol via `Server::router()`. Registering two services
on one ALPN is a configuration error, not a last-one-wins race.

**Layers.** A `Middleware` sees the request *typed*, so a per-method rule is a
`match` on the real enum. Make the impl generic over `Req: Rpc` and one layer
covers every service you ever define. `Timeout` ships with the crate.

**Actors, when shared state is the awkward part.** The same `service!` block also
generates an `Actor` trait taking `&mut self`, so state lives on a task instead
of behind a `Mutex`:

```rust
let warehouse = inventory::actor(Warehouse::seeded());  // in this process
warehouse.reserve("ADU-014".into(), 2).await?;          // Err = MailboxClosed

let service = warehouse.service(allow);                 // and to other machines
```

Serving it changes nothing on the wire — peers use the same `inventory::Client`,
on the same ALPN. What changes is that calls no longer overlap, which is the
whole trade: a maintainer can add an `.await` inside a handler that mutates
state. Under the concurrent model that same edit fails the build, because
`clippy::await_holding_lock` is deny-level here and a `std::sync` guard is what
`&self` state needs. (A `tokio::sync::Mutex` is not linted — it is designed to
be held across an await — at the cost of an async lock on every access.)

A turn is not cancellable: dropping the call does not stop the handler, and a
layer such as `Timeout` refuses the caller without ending the turn.

Many keyed instances behind one ALPN, addressed by an id inside the request, are
`System` and `spawn`. That path costs more — see RFC-04 §14.8 — and is worth it
only when there are many.

**Content addressing.** A `blob` method returns an address; iroh-blobs moves the
bytes, with verified resume and deduplication. keylink owns the mount, because
the mount iroh-blobs' own documentation teaches serves every hash to every peer
and accepts writes. Returning an address to a peer records a grant for that
exact `(peer, hash)` pair, and a blobs request is admitted only against a grant
— so every entry was created by an RPC that already passed its service's
allowlist and layers.

**Sync and async.** `keylink::blocking` mirrors the async API for programs with
no runtime. It owns a private multi-threaded runtime — mandatory, because a
current-thread runtime only advances iroh's keepalives during `block_on`, so
the connection would stall between calls. Calling it from inside a runtime
returns `Error::BlockingInAsync` instead of deadlocking.

## Try it

```sh
# machine A
KEYLINK_SECRET=<64-hex> KEYLINK_ALLOWED_IDS=<peer-id> cargo run -p demo -- serve

# machine B
cargo run -p demo -- connect <endpoint-id> [file-to-upload]
cargo run -p demo -- connect-sync <endpoint-id>     # the same, without a runtime
```

`cargo run -p demo --features demo/swap-variants -- connect <id>` builds a peer
whose `Currency` variants are swapped — identical shapes, so postcard would
decode `Brl` as `Usd` silently. The handshake refuses it instead.

## Working on it

```sh
./scripts/install-hooks.sh   # once per clone
mise run gate                # everything CI runs
```

You need [`mise`](https://mise.jdx.dev) and rustup, and nothing else.
`rust-toolchain.toml` asks for the `wasm32-unknown-unknown` target and the
`llvm-tools` component — the second because `ring` links its C through `ar`, and
Apple's writes an archive index `rust-lld` cannot read.

See [CONTRIBUTING.md](CONTRIBUTING.md) for the wire-contract pin, the public API
rules, and when `--no-verify` is legitimate.

## Status

Early. The API will move. Not published to crates.io.

Known gaps, in rough priority order:

- Protocols cannot be added or removed after `spawn`. iroh-examples'
  `custom-router` shows a runtime-mutable alternative worth adopting.
- **Never run in a browser.** The crate compiles and links for
  `wasm32-unknown-unknown` and CI gates that, but no `.wasm` has been loaded in a
  tab. `blocking`, `fs` and `env` refuse the target by `compile_error!` — each
  needs an OS facility a browser does not have.
- `Error::SchemaMismatch` is classified from the rendered error string, because
  the TLS alert code is several private layers down. Narrow and tested, but it
  would break if iroh reworded its message.

## License

MIT or Apache-2.0, at your option.
