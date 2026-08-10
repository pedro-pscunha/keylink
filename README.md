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
warehouse.reserve("ADU-014".into(), 2).await?;          // Err = Unanswered

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
    Ended::Observer { name, .. } => error!("{name} ended it; the turn was not committed"),
    _ => {}
}
```

Returning `Err` ends the actor. The refused caller is told
`Unanswered::Turn(Withheld::Refused { name })`, so it learns which observer
without a second lookup, and `ended()` adds that observer's own error —
`Withheld::Gone` means "gone" and must not be stretched to mean "gave up on
durability". A refused turn reaches a peer as
`RemoteError::Withheld(Withheld::Refused { name })` rather than `NoSuchActor`,
which would say "there is nothing here, stop asking" about a turn that ran. The
peer decodes the same `Withheld` the local caller matches on, rather than reading
a prefix off the front of a string.

**An observer that panics is a third answer and not that one.** The value is
withheld either way — a veto says "do not commit", a crash says nobody read back
whether it committed — but both are told
`Withheld::ObserverPanicked { name }`, which is a variant of its own and not a
shade of the one above it. Reporting a crash as a refusal named a decision nobody
made.

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

**One function, either surface.** A function that runs against a local actor in a
test and a remote one in production could not be written: a local call answers
`Unanswered`, a served one answers `Error`, and no `Result` type is both.
`service!` also generates `inventory::Calls`, a trait carrying the service's `rpc`
methods with **one** error type — `Failure`, which puts a `Withheld` in the same
arm whichever side withheld the value. It is implemented for `Handle`, `ActorRef`,
`Client` and `IdempotentClient`, so one
`async fn stock<C: inventory::Calls>(surface: &C) -> Result<u64, Failure>` runs
against all four. The concrete surfaces are untouched — their own methods still
answer `Unanswered` and `Error`, and `Failure` is reachable only through the
bound, which is the act of saying *I have chosen not to know which surface I
hold*. `impl Calls + Governed` narrows it to the surfaces a `Dispatch` stands in
front of, which refuses a `Handle`: it means the layers and the allowlist ran, and
it does **not** mean at-most-once, for which the honest instrument is
`IdempotentClient` named in the signature.

It carries `rpc` and none of the other four kinds, because `rpc` is the only kind
all four surfaces declare: an `ActorRef` and an `IdempotentClient` have no `tell`,
a `Handle` has no `send` and no `recv`, and `blob` answers a `BlobId` locally
against a `BlobHandle` — a content address plus the node that holds it —
remotely. One behaviour diverges across the bound, and it is documented rather
than papered over: a handler panic **unwinds into the calling task** against a
`Handle`, and arrives as `Failure::Turn(Withheld::Panicked { .. })` against a
served surface. `Failure` cannot express an unwind, and `TurnLost` argues at
length why it should not try.

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
`compile_error!` — each needs an OS facility a browser does not have.
`mise run wasm-browser` loads the module into headless Chrome and runs the ALPN
derivation, a `postcard` round trip of the reply type, and both `web`
constructors there. A page cannot open a UDP socket, so no connection runs in a
tab and none is claimed; see Status.

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
   budget is capped by the served `Limits::attach`, which defaults to
   `MAX_ATTACH`, 30 s**, and the cap is the serving side's, not yours: the budget
   arrives from the network and a peer asking to be held for a year is asking
   for a QUIC stream to be held for a year. So a caller with a 90-second handler
   cannot wait it out — at the default it is refused at 30 s with the work still
   running, which is `InFlight` and still the truth. Ask again, or ask
   `key_state`. A service that will not **admit** an attacher at all passes
   `Attaching::Forbidden` to `Limits::attach`, which answers `InFlight` at once and
   spends no waiting place — distinct from `Attaching::UpTo(Duration::ZERO)`, which
   admits the attacher and holds it for no time.

`examples/reliable_call.rs` is that recipe, compiled by the gate and run over two
nodes. `tests/reliable.rs::a_retried_call_across_a_reconnect_runs_exactly_once` is
the proof: the work runs, the serving side closes the connection that was carrying
its answer, and the retry — over a second handshake, against the same ledger — is
answered from the record without entering the handler again.

That is effectively-once for one `rpc`. Here is what it does not cover.

**Limits 2, 3 and 5 are now partly the serving side's choice rather than only
keylink's**: `ServiceBuilder::keyable` decides which methods accept a key at all, so
a key on any other is refused as `NotKeyable` instead of filling the ledger — which
also **voids limit 2's remedy on every method outside the policy**, because
`cx.call()` is then `None` and an `Applied` there never fires. And
`ServiceBuilder::records` can decline to hold the reply at all — reaching limit 5's
`Unrecordable` refusal by configuration rather than by the method's shape, which
keeps at-most-once and gives up the answer on purpose so that a caller's payload is
not held for the 600 s of limit 3. It narrows limit 4's window from `MAX_TOMBSTONES`
plus `MAX_RECORDS` of churn to `MAX_TOMBSTONES` alone. And `ServiceBuilder::limits`
replaces the numbers themselves: `RECORD_TTL`, `MAX_RECORDS`, `MAX_RECORDED_BYTES`,
`MAX_RECORDED_FRAME` and `MAX_TOMBSTONES` are the **defaults** of `Limits` fields
rather than fixed values, which is how a service bounds its own memory instead of only
its exposure. Not every constant these limits print: `MAX_HEADER` is the wire's own cap
and `APPLIED_BOUND` bounds a set in the caller's state, and neither is a `Limits`
field. **The margin limit 4 names is the one to keep** — it is a property of the
default, so narrowing it takes on the degradation that limit says a caller cannot
detect. See `docs/rfc-08-serving-side-keys.md`.

1. **There is no reliable `tell`.** A `tell` has no reply stream, so nothing can
   ack it, nothing can refuse it, and it does not accept a key — there is no reply
   to record and answer a repeat with. `with_idempotency` returns an
   `IdempotentClient`, which has no `tell` method, so this limit is one the
   compiler enforces rather than one you have to remember. Work that has to
   arrive is an `rpc`.
2. **The ledger is memory, so across a restart the guarantee is yours to keep.**
   The ledger dies with the process. `keylink-persist` makes an actor's *state*
   durable and does nothing for the ledger; keylink has no journal and this is not
   one.

   **Durable state makes that sharper, not milder.** While nothing survived a
   restart, re-running a retried call was *correct* — a fresh process had a fresh
   state. It is not correct any more. A process that fsyncs turn *n*, records the
   reply in the in-memory ledger, and dies before the frame reaches the peer will,
   on restart, load a state that already contains *n*, find an empty ledger, and
   run *n* again. The state is durable and the record that would have suppressed
   the repeat is not.

   So **effectively-once is a property of one process's lifetime, not of a key** —
   for as long as the only thing that knows about the key is the ledger.
   `cx.call()` is what changes that. It hands the handler a `CallId` — the calling
   peer folded into a hash of its key, opaque so that no caller has to re-implement
   the ledger's peer-scoping rule — and `Applied<T>` is a bounded set to keep them
   in. Put one in your own state, and the dedupe is three lines:

   ```rust,ignore
   let call = cx.peer().and_then(Context::call);
   if let Some(&balance) = self.applied.get(call) { return balance; }
   self.balance += amount;
   self.applied.insert(call, self.balance);
   ```

   **The method has to be one your service keys.** `Keyable` decides that, and on a
   method outside the policy a keyed call is refused `NotKeyable` before the handler
   runs — so `cx.call()` is `None` there always, and an `Applied` in that handler
   never fires and never says so. Deduplicating in a handler means putting its
   method in the policy.

   Insert in the turn that **applies**, as above, and never in a turn that merely
   hears about the work: the two together are one snapshot and one `fsync`, and that
   is the whole reason the set lives in your state rather than in keylink's. A
   `#[defer]` method's parking turn is the trap — park the `CallId` beside the
   `Deferred` and let the releasing turn record it, or a restart in between leaves a
   record with no work behind it. `Applied<()>` is the valueless form for exactly
   that shape, where there is no answer to replay.

   `Applied` derives `Serialize`/`Deserialize` and no `Schema`, so it cannot reach an
   ALPN and `postcard::to_allocvec(self)` keeps compiling when you add the field.
   Take that encoder rather than writing your own: a hand-rolled snapshot that omits
   the field compiles, passes every test, and starts empty after every restart, which
   is the silent at-least-once this whole mechanism exists to prevent.

   **`APPLIED_BOUND` is 4096 calls — and the unit is calls, not seconds.** It means
   "4096 other pieces of keyed work may land between yours and its retry". Turning
   that into a duration needs *your* adapter's turn rate: over `keylink-persist`'s
   file store a turn is ~7.6 ms, so ~130/s and roughly 31 seconds — but the same turn
   with no `fsync` behind it is ~13 µs, where 4096 entries is well under a second.
   Measure and divide, and treat the count as the guarantee.

   **And the set is a term in that turn rate, so measure at the bound you mean to
   run.** `Persist` re-encodes the whole state every completed turn, so an `Applied`
   costs its entries on every turn whether one changed or not — 4096 entries is
   ~92 µs, which is 1.2% of an `fsync` turn and **6.5× a non-syncing one** — that
   ratio and its denominator are sourced in `APPLIED_BOUND`'s rustdoc. Raise the
   bound for slow callers; lower it for cheap turns. `APPLIED_BOUND`'s rustdoc has the
   measured table.

   The busiest window is the one right after the restart, which *concentrates*
   arrivals — every peer's backoff fires at once. What drains the set, though, is
   only **first-time** keyed work: a recognised retry returns before it touches the
   eviction order, so a herd of pure retries costs no places at all. A reconnecting
   client supplies both, and it is the new work that spends the bound.

   **What this does not buy, in full.** The duplicate still *enters the handler*
   and still costs a turn — about 7.6 ms of `fsync` on a state it did not change —
   because only a durable ledger could refuse it before dispatch, and that one is
   structural: the record lives in your state, so reaching it means dispatching to
   your state. And the dedupe is yours to write, because the correct version knows
   what your answer is and whether replaying it beats refusing.

   Separately, and *as the tree stands today*: `key_state(key)` answers `Fresh`
   after a restart, because the ledger it reads is memory. A peer therefore learns
   nothing about a key your domain is about to dedupe. That is a property of the
   ledger being volatile rather than a limit of the query — a durable record store
   would answer it again, with nothing here changing — so read it as the current
   state and not as a closed door.

   **It also rests on the calling peer keeping its identity.** The peer is in the
   hash, so a client that reconnects on a fresh keypair is a different caller, is
   not recognised, and runs the work again — silently, and indistinguishably from a
   first call, which is the degradation limit 4 below names as the one a caller
   cannot detect. A long-lived `SecretKey` on the calling side is the precondition,
   not a deployment preference.

   `tests/persist.rs::a_retried_call_across_a_restart_is_applied_once` is the
   proof, over two real nodes and the real file store: the process dies, the
   restarted ledger answers `Fresh`, the retry is dispatched, and the work does not
   happen twice.
3. **A record answers for 600 s.** That is `RECORD_TTL`, alongside volume bounds
   of `MAX_RECORDS` — 1024 entries — and `MAX_RECORDED_BYTES` — 1 MiB of held
   reply **in total, across every peer**, not per key. Past any of them the reply
   is dropped and the retry is *refused* as
   `Forgotten`:
   at-most-once still holds, and the answer does not. Read the result out of your
   own domain; do not re-run the work.

   **`cx.call()` is no help here, and it is worth saying so.** A `Forgotten` repeat
   is refused *before* dispatch, so there is no turn, no `Context`, and nothing for
   a handler to recognise — the advice above is the caller's to act on, in its own
   domain, and limit 2's remedy is the serving side's. The two limits look adjacent
   and are not.
4. **Past `MAX_TOMBSTONES` — 8192, eight times the record bound — a key is
   `Fresh` again and the retry runs.** This is the one edge where the *ledger's*
   guarantee degrades: effectively-once, to at-most-once, to at-least-once. It is
   also the one state a caller cannot detect, because `Fresh` is what "never used"
   looks like too. The wide margin over the record bound is what makes it unlikely
   rather than impossible.

   **The real bound, beside it.** The margin is over *this ledger's* traffic, so
   what it protects is the window between a call and its retry on one live process
   — and a restart resets it to nothing, which is limit 2. Two bounds, then, and
   they are different quantities: `MAX_TOMBSTONES` is how many other keys may pass
   before this one is vacant again, and `APPLIED_BOUND` is how many other keys
   may pass before your own state forgets it. The second is the one that survives a
   restart, and it is the one you can raise.

   Undetectable degradation has two doors, and this is the second: a caller on a
   fresh keypair also gets `Fresh` from `Applied`, for the same reason and with the
   same silence.
5. **A reply that carries a payload is never recorded.** A `recv` method answers
   with a body beside its reply, and a body is streamed rather than held, so the
   repeat is refused as `Unrecordable` instead of being answered with a header
   whose payload never arrives — a permanent property of the method, not an
   eviction. A `blob` reply is not
   affected: its bytes move on iroh-blobs and the reply itself is just a header
   carrying an address, so it records and replays like any other.

   **So is any reply frame over `MAX_RECORDED_FRAME` — 1 MiB, 1048576.** This line
   used to name `MAX_RECORDED_BYTES` and print no figure, which was wrong twice:
   the ledger's ceiling is a bound on **one frame**, and the pool is a separate
   number that happens to share its value today.

   The ceiling equals `MAX_HEADER`, the wire's own cap, and that is not a
   coincidence. `MAX_HEADER` bounds the postcard *payload*, while the ledger stores
   the whole frame including its four-byte length prefix — so a maximal payload
   arrives as 1048580 bytes, which **exceeds the 1 MiB pool on its own**. A reply
   that cannot fit the pool under any conditions is permanently unrecordable, so
   `Unrecordable` is the right answer for it and the ceiling stops at the payload
   cap to say so. Raise the pool and that changes: a bigger pool can hold a maximal
   frame, and then the ceiling gains the four bytes.

   The number is worth setting **lower** than the default, and
   `Limits::frame_bytes` is how: one large reply that fits the pool can evict many
   small ones. There is no floor to quote, because the smallest useful value is the
   encoding of one service's own response type.
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
mise run gate                # the whole gate. There is no other one
```

**CI is off by decision, and `mise run gate` is the gate.** The workflow was
deleted from `main` rather than left sitting there switched off, because a workflow
nobody runs still looks like coverage — and the repository's Actions setting is off
as well, so two independent things hold it. CONTRIBUTING.md measures both. The
`pre-push` hook runs `gate`, so the check happens
before a push instead of after one — and `./scripts/install-hooks.sh` is what
makes that true for a clone, which is why it is the first line above.

What that costs, stated rather than discovered: **the gate now runs on one
machine, on one OS.** CI ran Linux, and a developer on a Mac no longer has
anything checking that the tree builds on the other one. Nothing in the gate is
weaker than it was — `wasm-guards`, `msrv` and `deny` moved into `gate` in the
same change that removed the workflow, so the three checks only CI ran are still
run — but they are run wherever you are, not everywhere.

`wasm-guards` is also **stronger** than the loop it replaces. The workflow
asserted only that `blocking`, `fs` and `env` fail to build for wasm, which is
vacuous: they fail anyway, deep inside tokio, once the feature turns on
something a browser has no facility for. Disabling the `fs` guard left the CI
check green. The task now requires the failure to quote keylink's own
`compile_error!`, which is the claim the workflow's comment made and did not
check.

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

- **No connection has ever run in a browser.** `mise run wasm-browser` loads the
  module into headless Chrome and runs four tests there — the schema hash, a
  `postcard` round trip of `WireResponse`, and both `web` body constructors — so
  the module is no longer merely linked. What is untested in a tab is everything
  that needs a socket: no `Node`, no `Server`, no `Client`, no actor turn. A page
  cannot open a UDP socket, so QUIC is out of reach whatever keylink does.
  Threaded wasm is refused outright: the local mailbox holds a thread-affine
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
  its loop exited. The keyed loop does not throw that away — it returns its
  ending and `Registry::reap` hands it to every registered `Lifecycle`. What is
  missing is a per-actor future to await it on, so an ending reaches a listener
  and never a caller who asked for that one actor.
- **Supervision restarts one actor, and no strategy reaches its siblings.**
  `LocalActor::spawn_supervised`, `spawn_supervised_deep` and
  `run_keyed_supervised` take an `Intensity` and put a fresh incarnation on the
  same task and the same mailbox, so a caller whose reply died mid-restart is
  told `Restarting` rather than `Gone`. There is no `Supervisor` type, no
  `OneForAll`/`RestForOne` and no parent/child tree; RFC-07 §9.8 refuses each one
  and gives the reason. `compose::Group` is the part of that caveat which was
  built — shared fate across several actors, retired in reverse insertion order —
  and its two verdicts are `Forget` and `StopAll`, so nothing there restarts a
  sibling either. Restarting is opt-in, so the rest of this bullet is the
  unsupervised default, and under a supervisor it is what one incarnation costs.
  What a fault costs depends on which input caused it: a panicking `rpc` turn
  costs that message, the actor survives, and the caller is
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
