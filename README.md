# keylink

Typed peer-to-peer RPC over [iroh](https://iroh.computer). You define a service;
callers get async methods. Everything underneath — NAT traversal, framing,
schema versioning, authorization — is invisible from the call site.

```rust
keylink::service! {
    service Inventory;

    rpc  GetItem(sku: String) -> Option<Item>;
    rpc  Reserve(sku: String, qty: u32) -> ReserveOutcome;
    tell Note(line: String) -> ();          // fire-and-forget, no reply

    recv Fetch(sku: String) -> Datasheet;   // a file streams down beside the reply
    send Upload(name: String) -> Stored;    // and up beside the request
    blob Manual(sku: String) -> Sheet;      // an address; iroh-blobs moves the bytes
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

**Fire-and-forget.** A `tell` travels on a unidirectional stream and returns
`Result<()>`, never the declared type: there is no reply stream to decode one
from. The `Ok` means the bytes reached the transport and nothing more — not
delivery, not handling, and no order against a `call`. It does not accept an
idempotency key either, and the compiler is what says so: `with_idempotency`
hands back an `IdempotentClient`, and a `tell` is not among its methods. The
ledger answers a repeat with the recorded reply and a `tell` has no reply to
record, so a key attached to one could only be discarded. **There is no reliable
`tell`.** Anything that has to arrive is an `rpc`.

**Large payloads.** A body is not framed: its extent is the rest of the stream
and its flow control is QUIC's. A 4 GiB file costs zero framing bytes and one
chunk of peak memory. A body that ends early is an error, never a short file.

**Content addressing.** A `blob` method returns an address; iroh-blobs moves the
bytes, with verified resume and deduplication. keylink owns the mount, because
the mount iroh-blobs' own documentation teaches serves every hash to every peer
and accepts writes. Returning an address to a peer records a grant for that
exact `(peer, hash)` pair, and a blobs request is admitted only against a grant
— so every entry was created by an RPC that already passed its service's
allowlist and layers.

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

**Retry safety.** A call that fails mid-flight leaves the caller unable to say
whether it ran. `client.with_idempotency(key)` removes the choice between a
double booking and no booking: the serving side records the reply under
`(peer, key)` and answers a repeat from that record, byte for byte, without
entering the handler again.

The word that does the work is *ran*. A layer that refuses a call before it
reaches the handler leaves the key claimable, because nothing happened; one that
refuses after leaves `Interrupted`, because what happened is genuinely unknown.
A record that expires or is evicted leaves a tombstone, so a forgotten key is
refused rather than silently run a second time.

```rust
// wait for the call already running under this key instead of being refused
client.with_idempotency(Idempotency::attaching_for(key, Duration::from_secs(5)))
      .reserve("ADU-014".into(), 2).await?;

client.key_state(key).await?   // Fresh is the one answer a retry cannot give you
```

The record is in memory and dies with the process. Composed with a reconnecting
handle it is effectively-once for one `rpc`, which is the recipe below; RFC-05 has
the reasoning.

**Departure.** Every other failure here arrives as the answer to a question you
asked. A dead peer does not — you find out on your next call, however long
after the fact that is. `client.watch()` is a future that resolves when the
connection ends, and it distinguishes the three cases that matter:

```rust
match client.watch().await {
    Departure::Closed  => info!("peer said goodbye"),
    Departure::Local   => {}                  // we shut down; nothing happened
    Departure::Lost(e) => reconnect(e).await, // it broke
}
```

**Reconnecting handles.** Dial-per-message is wasteful and holding one client
forever is fragile. `Peer<C>` connects on first use, reuses the connection, and
reconnects **exactly once** when a call fails:

```rust
let peer: Peer<inventory::Client> = Peer::new(&node, addr);
let item = peer.call(|c| async move { c.get_item(sku).await }).await?;
```

Once, not until it works. Retrying forever turns a peer that is genuinely gone
into a caller that never returns.

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

let server = warehouse.serve(&node, allow)?;            // and to other machines
```

Serving it changes nothing on the wire — peers use the same `inventory::Client`,
on the same ALPN. What changes is that calls no longer overlap, which is the
whole trade: a maintainer can add an `.await` inside a handler that mutates
state. Under the concurrent model that same edit fails the build, because
`clippy::await_holding_lock` is deny-level here and a `std::sync` guard is what
`&self` state needs.

Two costs, both structural. A turn is not cancellable: dropping the call does
not stop the handler, and a layer such as `Timeout` refuses the caller without
ending the turn. And an actor must not call itself — the mailbox holds one
message, so a handler awaiting its own handle waits for a permit or a reply that
only it can produce.

A `send` method cannot be served this way at all, and `serve` stops compiling for
a service declaring one. Its handler reads the upload inside the turn, so a peer
that declares a large body and then stops writing holds the actor until the
connection idle timeout, answering nothing else meanwhile. Bulk transfer goes on
a second service, registered beside the actor on the same node.

**Watching a turn, from outside.** `Middleware` intercepts a message on the wire.
An `Observer` watches a turn against the state — after the method, before the
reply. It sees the state and how the turn went, and never a request or a reply
payload; a layer sees the opposite. Neither grows toward the other.

```rust
let ledger = ledger::actor_with(state, vec![Box::new(saver)]);
ledger.post(1).await?;             // the observer ran before this returned

match ledger.ended().await {
    Ended::Stopped              => {}
    Ended::Observer { name, .. } => error!("{name} gave up; the turn was not committed"),
    _ => {}
}
```

Returning `Err` ends the actor, and `Handle::ended()` says which observer and
why — `MailboxClosed` means "gone" and must not be stretched to mean "gave up on
durability". A refused turn reaches a peer as `Failed("observer refused: ..")`
rather than `NoSuchActor`, which would say "there is nothing here, stop asking"
about a turn that ran.

The actor owns its observers, so nothing is shared and no `Sync` bound appears
anywhere. `after_turn` returns a boxed future rather than being an `async fn`,
which is what lets an implementation finish with the state before it awaits — an
implementation that holds it across an await needs `A: Sync`, and one that does
not, does not.

**A second input.** A mailbox is one way into an actor. A `feed` is the other.
Declare `feed Restocked(item: Restock);` and the generated `Actor` trait grows
`async fn on_restocked(&mut self, item: Restock)` — no `Caller`, no return value,
because nobody called and there is nothing to reply to — and a `Stream` is wired
into the same loop at spawn, one argument per declared feed:

```rust
let warehouse = inventory::actor_wired(
    Warehouse::seeded(),
    vec![],                                 // observers
    inventory::Feeds::new(deliveries),      // one argument per declared feed
);
```

Feed items and mailbox messages are interleaved fairly: with everything ready at
once the mailbox takes half the turns and each of `n` feeds takes `1/2n`. A feed
is **local only and never on the wire**, so declaring one does not move the ALPN.
A feed that ends, and a feed handler that panics, both end the actor — a fault
with no caller to resume it into would otherwise be dropped onto a state the
handler may have left half applied — and `Ended` names which happened.

**Many instances behind one ALPN.** Keyed actors, addressed by an id inside the
request, are `inventory::System`: build one, `mount(id, inventory::spawn(state))`,
and serve it with `inventory::keyed_service(&system, allow)`, which peers reach
through `inventory::KeyedClient`. That path once cost per-method layers,
per-method authorization, `blob` methods and observers, because its payload was
opaque to keylink. It no longer does: a keyed request is `Addressed<Request>` and
its reply is the service's own typed `Response`, and a keyed mount runs the same
turn loop a local `Handle` does — so layers, per-method authorization, `blob`
grants, observers, the panic guard and `Observer::finish` all reach it. The
idempotency ledger was never on that list; what was missing was a client that
could send a key, and `KeyedClient::with_idempotency` is it.

What the keyed path still costs: one QUIC connection per actor *service* — not one
for a node's whole fleet, each with its own handshake, admission decision,
idempotency ledger, keep-alive and congestion window — and no per-instance
`ended()`, so `system.shutdown(&id, budget)` reports whether an instance drained
rather than why its loop exited. The budget is not optional: every caller names
how long it is prepared to wait before the actor is killed instead.

**Sync and async.** `keylink::blocking` mirrors the async API for programs with
no runtime. It owns a private multi-threaded runtime — mandatory, because a
current-thread runtime only advances iroh's keepalives during `block_on`, so
the connection would stall between calls. Calling it from inside a runtime
returns `Error::BlockingInAsync` instead of deadlocking. `Node::enter` lends
that runtime to the calling thread, which is what a synchronous program needs
to construct an actor at all.

**The browser.** The crate compiles and links for `wasm32-unknown-unknown` with
`--no-default-features`. The `web` feature adds `Body::from_web_file` and
`Body::from_web_stream`, so a `<input type=file>` streams up the same way a path
does on a server. `blocking`, `fs` and `env` refuse the target by
`compile_error!` — each needs an OS facility a browser does not have. Nothing
has ever run in a tab; see Status.

## Reliable delivery, and its seven limits

Every piece of effectively-once for an `rpc` is already here. Composed, they are a
recipe rather than a feature:

1. **Put the key on the work, not on the attempt.** `client.with_idempotency(key)`
   records the reply under `(peer, key)`. Every retry of that one piece of work
   carries that one key; different work gets a different key.
2. **Call through `Peer<C>`.** It reconnects once when a call fails and re-issues
   the call against the new connection. The ledger belongs to the served
   `Service`, not to the connection, so the retry meets the same record.
3. **Ask to attach.** `Idempotency::attaching_for(key, budget)` waits for a call
   already in flight under the key instead of being refused with `InFlight` —
   the difference between a retry that helps and a retry that races. **The
   budget is capped at `MAX_ATTACH`, 30 s**, and the cap is the serving side's,
   not yours: the budget arrives from the network and a peer asking to be held
   for a year is asking for a QUIC stream to be held for a year. So a caller
   with a 90-second handler cannot wait it out — it is refused at 30 s with the
   work still running, which is `InFlight` and still the truth. Ask again, or
   ask `key_state`.

`examples/reliable_call.rs` is that recipe, compiled by the gate and run over two
nodes. `tests/reliable.rs::a_retried_call_across_a_reconnect_runs_exactly_once` is
the proof: the work runs, the serving side closes the connection that was carrying
its answer, and the retry — over a second handshake, against the same ledger — is
answered from the record without entering the handler again.

That is effectively-once for one `rpc`. Here is what it does not cover.

**Limits 3 and 5 are now partly the serving side's choice rather than only
keylink's**: `ServiceBuilder::keyable` decides which methods accept a key at all, so
a key on any other is refused as `NotKeyable` instead of filling the ledger, and
`ServiceBuilder::records` can decline to hold the reply at all — reaching limit 5's
`Unrecordable` refusal by configuration rather than by the method's shape, which
keeps at-most-once and gives up the answer on purpose so that a caller's payload is
not held for the 600 s of limit 3. It shortens limit 4's window by about 11%. See
`docs/rfc-08-serving-side-keys.md`.

1. **There is no reliable `tell`.** A `tell` has no reply stream, so nothing can
   ack it, nothing can refuse it, and it does not accept a key — there is no reply
   to record and answer a repeat with. `with_idempotency` returns an
   `IdempotentClient`, which has no `tell` method, so this limit is one the
   compiler enforces rather than one you have to remember. Work that has to
   arrive is an `rpc`.
2. **The ledger is memory.** It dies with the process, and a restarted service
   runs a retried call again. `keylink-persist` makes an actor's *state* durable
   and does nothing for the ledger. keylink has no journal and this is not one.

   **Durable state makes this sharper, not milder.** While nothing survived a
   restart, re-running a retried call was *correct* — a fresh process had a fresh
   state. It is not correct any more. A process that fsyncs turn *n*, records the
   reply in the in-memory ledger, and dies before the frame reaches the peer will,
   on restart, load a state that already contains *n*, find an empty ledger, and
   run *n* again. The state is durable and the record that would have suppressed
   the repeat is not.

   So **effectively-once is a property of one process's lifetime, not of a key.**
   Across a restart the guarantee is at-least-once, and a handler that is not
   idempotent against its own committed state will double-apply. This is the real
   distance to a journalled system, and it is one sentence rather than a caveat
   you can tune away.
3. **A record answers for 600 s.** That is `RECORD_TTL`, alongside volume bounds
   of `MAX_RECORDS` — 1024 entries — and `MAX_RECORDED_BYTES` — 1 MiB of held
   reply. Past any of them the reply is dropped and the retry is *refused* as
   `Forgotten`:
   at-most-once still holds, and the answer does not. Read the result out of your
   own domain; do not re-run the work.
4. **Past `MAX_TOMBSTONES` — 8192, eight times the record bound — a key is
   `Fresh` again and the retry runs.** This is the one edge where the guarantee
   degrades: effectively-once, to at-most-once, to at-least-once. It is also the
   one state a caller cannot detect, because `Fresh` is what "never used" looks
   like too. The wide margin over the record bound is what makes it unlikely
   rather than impossible.
5. **A reply that carries a payload is never recorded.** A `recv` method answers
   with a body beside its reply, and a body is streamed rather than held, so the
   repeat is refused as `Unrecordable` instead of being answered with a header
   whose payload never arrives. So is any reply frame over `MAX_RECORDED_BYTES` —
   a permanent property of the method, not an eviction. A `blob` reply is not
   affected: its bytes move on iroh-blobs and the reply itself is just a header
   carrying an address, so it records and replays like any other.
6. **`Interrupted` is permanent, and it is the answer that needs a decision.** A
   call whose handler was entered and produced no reply — a dead connection, a
   shutdown, a layer that stopped waiting after dispatch — settles the key as
   `Interrupted`. Whether the work took effect is genuinely unknown and no record
   can make it known, so retrying that key is refused forever, by design. What a
   caller *does*: reconcile against its own domain, then issue a **new** key.
   `client.key_state(key).await` asks without running anything, which is the only
   way to learn a state you are not willing to discover by executing.
7. **A `feed` has no delivery guarantee at all.** It is a local stream into an
   actor rather than a call: nobody sent it, nothing acks an item, nothing refuses
   one, and no key applies. It never reaches the wire. A feed that dies takes the
   actor with it, which is the only report a feed can make.

## Try it

```sh
cargo run -p demo -- id                            # the id peers dial

# machine A
KEYLINK_SECRET=<64-hex> KEYLINK_ALLOWED_IDS=<peer-id> cargo run -p demo -- serve

# machine B
cargo run -p demo -- connect <endpoint-id> [file-to-upload]
cargo run -p demo -- connect-sync <endpoint-id>    # the same, without a runtime
```

With one machine and no internet, set `KEYLINK_OFFLINE=1` on both sides: the
demo binds without discovery, prints a `<id>@<ip:port>` line, and accepts that
form as the dial target.

`cargo run -p demo --features demo/swap-variants -- connect <id>` builds a peer
whose `Currency` variants are swapped — identical shapes, so postcard would
decode `Brl` as `Usd` silently. The handshake refuses it instead.

The demo's `Warehouse` is served as an actor, so it also stands as the worked
example of that model against an unmodified client. Its upload is a second
service on a second ALPN, registered beside it on the same node, because a
`send` method cannot be served as an actor — the split every actor with a bulk
transfer now has to make.

## Working on it

```sh
./scripts/install-hooks.sh   # once per clone — the hooks are not automatic
mise run gate                # everything CI runs except deny and msrv
mise run deny                # by hand, whenever you change a dependency
```

You need [`mise`](https://mise.jdx.dev) and rustup, and nothing else.
`rust-toolchain.toml` asks for the `wasm32-unknown-unknown` target and the
`llvm-tools` component — the second because `ring` links its C through `ar`, and
Apple's writes an archive index `rust-lld` cannot read.

See [CONTRIBUTING.md](CONTRIBUTING.md) for the wire-contract pin, the public API
rules, and when `--no-verify` is legitimate. `docs/rfc-*.md` record the
decisions, including the reversed ones.

## Status

Early. The API will move. Not published to crates.io.

Greenfield: nothing here has ever run in production, so an ALPN move costs
nothing and no compatibility shim is owed. Changes are taken in their correct
form rather than their compatible one — every call site moves and the old path is
deleted. That stops being true the day somebody deploys this, which is the
argument for fixing the sharp things now.

Known gaps, in rough priority order:

- **Never run in a browser.** The crate compiles and links for
  `wasm32-unknown-unknown` and CI gates that, but no `.wasm` has been loaded in a
  tab. Threaded wasm is refused outright: the local mailbox holds a thread-affine
  task handle behind a `Send + Sync` type.
- **A running actor turn cannot be bounded from outside.** `Timeout` drops the
  wait for the reply, not the turn, so it sheds no load once a turn has started.
  It no longer settles an idempotency key as its own refusal — a turn that ran
  leaves `Interrupted`, which is the truth — but the retry does not receive the
  turn's real reply either. `QueueTimeout` is the half that does shed load: it
  refuses a message that has not started, and leaves its key claimable, because
  nothing ran. Past that point, bound the work inside the method. RFC-05 §7.
- Protocols cannot be added or removed after `spawn`. iroh-examples'
  `custom-router` shows a runtime-mutable alternative worth adopting.
- Idempotency records are in memory. They die with the process, so a retry
  across a restart runs the handler again. `keylink-persist` is durability for an
  actor's *state*, not for the ledger. The other six limits of the guarantee are
  in **Reliable delivery** above.
- A keyed instance has no `ended()`. `System::shutdown(&id, budget)` reports
  whether it drained or had to be killed, which is a different question from why
  its loop exited; the keyed loop computes that and throws it away.
- No supervision or restart. What a fault costs depends on which input caused it:
  a panicking `rpc` turn costs that message, the actor survives, and the caller is
  told rather than left to infer it. A panic in a `feed` handler ends the actor,
  and so does a feed that simply stops — nobody is waiting on a feed item, so
  dropping its fault would carry on over a half-applied state. `Ended` says which
  happened. RFC-04 §8 has the original argument against supervision; RFC-07 §5.1
  has the argument for an ending that names itself.
- `Error::SchemaMismatch` is classified from the rendered error string, because
  the TLS alert code is several private layers down. Narrow and tested, but it
  would break if iroh reworded its message.

## License

MIT or Apache-2.0, at your option.
