# RFC-04 — An actor model for keylink

**Status:** proposal, revision 2. Nothing here is implemented.
**Target:** `keylink` 0.1.0, iroh 1.0.3, edition 2021, rustc 1.97.
**Author's recommendation in one line:** answer **(c)** — an actor is one more `Dispatch`, not one more transport; ship it in four steps, and reject supervision.

**Revision 2** answers the four questions RFC-02 §10.7 puts to this RFC, in
[section 14](#14-the-actor-system-answering-rfc-02-10). It agrees with RFC-02 §10.3 on
every point and contradicts it on none. It also reports one **defect in keylink today**
that revision 1 walked past: `Outcome` is not in the hashed contract, although `reply.rs`
says it is ([section 14.6](#146-a-defect-found-while-answering-question-4)). That defect
blocks question 4 and is demonstrated with running code.

---

## 0. Summary

The request contains two asks: "call a function on another machine and get an answer",
and "be able to be an actor-like framework". keylink already does the first one. This
RFC is about the second.

The central finding is that the two asks do not conflict at the transport, because the
actor model and the concurrent model differ **only in how the handler is invoked**. That
invocation point is already a named, erased type in this codebase:

```rust
// rpc.rs:50
pub type Dispatch<Req, Resp> =
    Arc<dyn Fn(Req, BodyReader) -> BoxFut<(Outcome<Resp>, Body)> + Send + Sync>;
```

Everything the user values — the schema-hashed ALPN, `Allowlist`, `Middleware`, one
stream per call, `Body` streaming, the blocking client — sits **outside** `Dispatch`.
A serial actor is a `Dispatch` that sends into a mailbox instead of calling a method.

| | Capability | Cost | Recommendation |
|---|---|---|---|
| **A** | Serial dispatch: `&mut self` handlers, one message at a time | ~90 lines in the crate, ~60 in the macro | **Ship.** |
| **B** | `tell`: fire-and-forget over a unidirectional stream | ~50 lines, 1 accept loop change, 1 contract-hash change | **Ship, after A.** |
| **C** | Reaching a peer unprompted | **0 lines** — dial it back | **Document. Build nothing.** |
| **D** | Location-transparent handle (one type for local and remote) | ~40 lines | **Reject.** Section 5. |
| **E** | Supervision, restart, actor trees | large | **Reject.** Section 8. |
| **F** | The **actor system**: keyed instances, runtime mount and unmount | ~300 lines, new wire envelope | **Design now, build when named.** Section 14. |
| **G** | Fixing `Outcome`'s absence from the hashed contract | **1 line** | **Ship first.** Blocks F. Section 14.6. |

Capabilities A to E are the **singleton actor**: one actor per service, no id, no
registry, and the wire unchanged. That is what removes the `Mutex` from `Warehouse`, and
it is the only part with a justification today.

Capability F is the **actor system** RFC-02 §10 designs: many instances behind one ALPN,
addressed by an id inside the request. Section 14 answers RFC-02's four questions in full
and freezes its wire envelope, then leaves it unbuilt — the same treatment RFC-02 §9.7
gives its own capability A.

The honest cost is in [section 10](#10-what-this-breaks). One thing genuinely breaks:
**`send` (body up) serializes an entire upload inside one mailbox turn.** `recv` (body
down) does not, and section 10 explains why the asymmetry is not an accident.

---

## 1. The central conflict, resolved

### 1.1 The conflict is real

`rpc.rs` states the concurrent position plainly:

> So each call gets its own bidirectional stream. There is no client mutex, `call` takes
> `&self`, and the server answers requests in parallel.

`demo/src/main.rs:163-195` shows the price the user pays for it: a `Mutex<BTreeMap>`, a
guard accessor, and a comment about poison recovery.

The actor model inverts both. State is `&mut self`. Messages run one at a time. There is
no lock, because there is nothing to lock.

Both are legitimate. A read-mostly catalogue wants concurrency. A ledger wants
serialization.

### 1.2 The answer is (c)

**keylink offers actors as a layer on the concurrent core.**

Not (a): actors-only would delete `concurrent_calls_do_not_queue_behind_each_other`
(`tests/protocol.rs:167`), which is the crate's headline property and its only measured
one. It would also make an 8 MiB download block every later call on the same service.

Not (b): two coequal models means two `service!` grammars, two handler traits, two
dispatch paths, and two sets of middleware semantics. The surface roughly doubles for a
difference that lives in four lines of code.

(c) is what the code already permits. The `service!` block does not change. The wire does
not change. The ALPN does not change. **The caller cannot tell the two apart, and must
not be able to**, because "does the peer serialize its state" is a serving-side
implementation choice, exactly like the `Mutex` is today.

### 1.3 The choice is one word, at the serve site

```rust
keylink::service! {
    service Inventory;
    rpc  GetItem(sku: String) -> Option<Item>;
    rpc  Reserve(sku: String, qty: u32) -> ReserveOutcome;
}

// concurrent, today, unchanged:
//   impl inventory::Handler for Warehouse { async fn reserve(&self, ..) }
let service = inventory::service(Warehouse::seeded(), allow).build();

// serial:
//   impl inventory::Actor   for Warehouse { async fn reserve(&mut self, ..) }
let service = inventory::actor(Warehouse::seeded(), allow).build();
```

`service` and `actor` return the same `ServiceBuilder<Request, Response>`. Layers,
allowlists and registration are identical after that point.

### 1.4 What the mailbox buys, measured

This is the actor model's selling point, so it is counted rather than asserted.

`demo/src/main.rs` today:

```rust
use std::sync::Mutex;                                          // line 14

#[derive(Default)]
struct Warehouse {
    items: Mutex<BTreeMap<String, Item>>,                      // line 165
}

impl Warehouse {
    fn seeded() -> Self {
        // ...
        Self { items: Mutex::new(items) }                      // line 187
    }

    /// A panicking handler must not wedge every later request. Recover the guard
    /// instead of propagating poisoning — one bad request should cost one
    /// request, not the process.
    fn items(&self) -> std::sync::MutexGuard<'_, BTreeMap<String, Item>> {
        self.items.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
    }                                                          // lines 190-195
}

impl inventory::Handler for Warehouse {
    async fn reserve(&self, sku: String, qty: u32) -> ReserveOutcome {
        let mut items = self.items();
        let Some(item) = items.get_mut(&sku) else {
            return ReserveOutcome::NoSuchItem;
        };
        // ...
    }
}
```

The same service as an actor:

```rust
#[derive(Default)]
struct Warehouse {
    items: BTreeMap<String, Item>,
}

impl inventory::Actor for Warehouse {
    async fn reserve(&mut self, sku: String, qty: u32) -> ReserveOutcome {
        let Some(item) = self.items.get_mut(&sku) else {
            return ReserveOutcome::NoSuchItem;
        };
        // ...
    }
}
```

**Nine lines disappear**: one import, one field wrapper, one constructor wrapper, and a
six-line accessor with its poison comment. Three call sites lose a method call.

The line count is the small half. The large half is a **class of failure that becomes
unrepresentable**:

> `clippy::await_holding_lock` lives in `clippy::suspicious`, which `clippy::all`
> includes, which `Cargo.toml:23` sets to `deny`.

So today, a maintainer who adds one `.await` inside `reserve` — a price lookup, a write
to disk, an audit record — fails the build and must restructure the function around the
guard. Under the actor model that maintainer writes the `.await` and moves on, because
`&mut self` is held by the task rather than by a guard.

That is the ergonomic win, stated exactly: **not nine lines, but the fact that awaiting
while mutating stops being a lint failure and becomes the normal thing.**

The cost is equally exact: `reserve` no longer runs concurrently with `list`. Section 6
shows why that costs less than it appears to.

---

## 2. What an actor is here

### 2.1 The `service!` block

Unchanged, except for one new method kind. `rpc`, `send` and `recv` keep their present
meanings.

```rust
keylink::service! {
    service Inventory;

    rpc  Ping() -> String;
    rpc  GetItem(sku: String) -> Option<Item>;
    rpc  Reserve(sku: String, qty: u32) -> ReserveOutcome;

    tell Restock(sku: String, qty: u32) -> ();      // no reply. See section 3.

    recv Fetch(sku: String, kib: u32) -> Datasheet;
    send Upload(name: String) -> UploadOutcome;     // see section 10.4
}
```

`tell` is a **wire** kind. `actor` is a **dispatch** choice. They are orthogonal: a
`tell` on a concurrent `Handler` is legal and means only "no reply". Keeping the two axes
independent is what stops the surface from doubling.

### 2.2 The handler trait

The macro generates a second trait beside `Handler`. Same method names, same argument
lists, `&mut self` instead of `&self`.

```rust
pub trait Actor: Send + 'static {
    /// The proven identity of the peer whose message comes next.
    ///
    /// Called immediately before each message, and never concurrently with one,
    /// because the mailbox is serial. Stash what you need in your own state.
    /// The default does nothing.
    fn caller(&mut self, _cx: &Context) {}

    fn ping(&mut self) -> impl Future<Output = String> + Send;
    fn get_item(&mut self, sku: String) -> impl Future<Output = Option<Item>> + Send;
    fn reserve(&mut self, sku: String, qty: u32)
        -> impl Future<Output = ReserveOutcome> + Send;
    fn restock(&mut self, sku: String, qty: u32) -> impl Future<Output = ()> + Send;
    fn fetch(&mut self, sku: String, kib: u32)
        -> impl Future<Output = (Datasheet, Body)> + Send;
    fn upload(&mut self, name: String, body: BodyReader)
        -> impl Future<Output = UploadOutcome> + Send;
}
```

Two details earn their place.

**`Send + 'static`, not `Send + Sync + 'static`.** `Handler` needs `Sync` because it is
shared behind an `Arc` across concurrent tasks. An actor is owned by one task and never
shared, so `Sync` is not required. That is a real widening: a state holding an `Rc`,
a `RefCell` or a `!Sync` third-party client can be an actor and cannot be a `Handler`.

**`caller` is safe only because dispatch is serial.** A concurrent `Handler` could not
have this method: two calls would race on the stashed value. The mailbox removes the
race, so the actor gets an authenticated caller identity for free. Section 4 needs it.

### 2.3 The call site

Unchanged. This is the invariant the task named, and it holds:

```rust
let client = inventory::Client::connect(&node, peer_id).await?;
let outcome = client.reserve("ADU-014".into(), 2).await?;
```

No transport concept appears. No actor concept appears either. The caller does not know,
and does not need to know, that the peer serializes.

### 2.4 The crate-side machinery

One new private module, `actor.rs`. Three items, of which one is public.

```rust
/// One message on its way to an actor. Generated code constructs these.
#[doc(hidden)]
pub struct Mail<Req, Resp> {
    pub cx: Context,
    pub req: Req,
    pub body: BodyReader,
    /// `None` for a `tell`: nothing is listening for the answer.
    pub reply: Option<oneshot::Sender<(Outcome<Resp>, Body)>>,
}

/// Turn a mailbox sender into the `Dispatch` a `ServiceBuilder` wants.
#[doc(hidden)]
pub fn dispatch_to<Req, Resp>(tx: mpsc::Sender<Mail<Req, Resp>>) -> Dispatch<Req, Resp>;

/// Run one future, converting a panic into a value instead of a dead task.
#[doc(hidden)]
pub async fn guarded<F: Future>(fut: F) -> std::result::Result<F::Output, Panic>;
```

`guarded` is hand-rolled rather than taken from `futures`, because keylink declares nine
direct dependencies and `futures` is not one of them. It needs no `unsafe`, because
`Box::pin` supplies a safe `as_mut()`:

```rust
pub async fn guarded<F: Future>(fut: F) -> std::result::Result<F::Output, Panic> {
    let mut fut = Box::pin(fut);
    std::future::poll_fn(move |cx| {
        match std::panic::catch_unwind(AssertUnwindSafe(|| fut.as_mut().poll(cx))) {
            Ok(Poll::Pending) => Poll::Pending,
            Ok(Poll::Ready(value)) => Poll::Ready(Ok(value)),
            Err(panic) => Poll::Ready(Err(Panic::from(panic))),
        }
    })
    .await
}
```

The cost is one `Box::pin` per message. A panicked future is dropped immediately and
never polled again.

### 2.5 The generated loop

The macro emits the loop, so the match over `Request` stays inline and no boxed closure
has to borrow the state:

```rust
pub fn actor<A: Actor>(mut state: A, allow: Allowlist)
    -> $crate::ServiceBuilder<Request, Response>
{
    let (tx, mut rx) = tokio::sync::mpsc::channel::<Mail<Request, Response>>(MAILBOX);
    tokio::spawn(async move {
        while let Some(mail) = rx.recv().await {
            state.caller(&mail.cx);
            let answer = $crate::actor::guarded(async {
                match mail.req {
                    Request::Reserve { sku, qty } => (
                        Outcome::Ok(Response::Reserve(state.reserve(sku, qty).await)),
                        Body::empty(),
                    ),
                    // ... one arm per rpc, exactly as `service()` generates today
                }
            })
            .await
            .unwrap_or_else(|panic| (
                Outcome::Err(RemoteError::Failed(panic.to_string())),
                Body::empty(),
            ));

            if let Some(reply) = mail.reply {
                let _ = reply.send(answer);
            }
        }
    });
    $crate::ServiceBuilder::new(alpn(), allow, $crate::actor::dispatch_to(tx))
}
```

`unwrap_or_else` rather than `unwrap`, so `clippy::unwrap_used` stays satisfied.
`RemoteError::Failed(String)` already exists (`reply.rs:37`), so panic reporting needs
**no new public surface at all**.

### 2.6 One breaking change: `Dispatch` gains a `Context`

```rust
- pub type Dispatch<Req, Resp> =
-     Arc<dyn Fn(Req, BodyReader) -> BoxFut<(Outcome<Resp>, Body)> + Send + Sync>;
+ pub type Dispatch<Req, Resp> =
+     Arc<dyn Fn(Context, Req, BodyReader) -> BoxFut<(Outcome<Resp>, Body)> + Send + Sync>;
```

Today the `Context` is built in `serve_one` (`rpc.rs:117`), handed to the middleware
chain, and then **thrown away** by the two adapters that wrap a `Dispatch`:

```rust
// rpc.rs:92 and rpc.rs:237, identical
next: Arc::new(move |_cx, req, body| dispatch(req, body)),
```

So the proven peer identity already exists, already reaches the edge of the handler, and
is already discarded. Section 4 cannot work without it: an actor that wants to answer a
peer later needs `cx.peer()`, and the only alternative is for the caller to put its own
id in the request body — which `middleware.rs:40-41` explicitly warns against:

> `peer` is the key iroh proved during the handshake — not a claim carried in the request
> body, which is why it is safe to authorize against.

The change touches `Dispatch`, `RpcService::new`, `ServiceBuilder::new`, and the one
`Dispatch` the macro builds. `RpcService::new` and `Dispatch` are public. The crate is
0.1.0 and unpublished, and `README.md:91` says "the API will move".

**This is decision 1 for review.** The alternative is to leave `Dispatch` alone and pass
the peer as an extra generated argument on every actor method, which is noisier at every
call site and buys nothing.

---

## 3. `ask` and `tell`

### 3.1 `ask` already exists

`rpc`, `send` and `recv` are all ask-shaped: request out, reply back, one bidirectional
stream. Nothing changes. The actor's `ask` path adds one mailbox hop and one `oneshot`
between the middleware chain and the handler.

### 3.2 `tell` on the wire

**A `tell` takes its own unidirectional stream.**

```text
open_uni → [u32 LE header_len][postcard(Envelope<Request>)][body ...] FIN
```

The framing is byte-identical to a request today. Only the stream type changes.

Unidirectional is the honest encoding: the transport itself says there is no reply
direction, so no reply can be written by mistake and no reply stream is allocated.

The rejected alternative is a bidirectional stream that the caller never reads. It works,
but it is worse in two ways. It allocates a reply direction that the contract says will
never carry anything. And the caller's immediate drop of the `RecvStream` sends
`STOP_SENDING(0)` — verified at `noq-1.1.1/src/recv_stream.rs:630` — which the serving
side then has to tolerate on a stream it was never going to write. Unidirectional removes
both problems and costs one extra accept loop.

### 3.3 The accept loop

`RpcService::accept` runs one loop on `connection.accept_bi()` today. It gains a second
loop on `accept_uni()`. `Connection` is `Clone` (`iroh-1.0.3/src/endpoint/connection.rs:736`),
so the two loops are independent tasks in the existing `JoinSet`:

```rust
let mut calls = JoinSet::new();
calls.spawn(accept_calls(connection.clone(), self.next.clone(), peer));
calls.spawn(accept_tells(connection.clone(), self.next.clone(), peer));
```

**`tokio::select!` over both accepts on one connection is rejected.** `noq`'s
`poll_accept` (`noq-1.1.1/src/connection.rs:1088`) parks on a `Notified`, and reasoning
about whether a dropped `Notified` can swallow a wakeup is exactly the kind of argument
that is right in review and wrong in production. Two loops make the question disappear.

`accept_tells` is short:

```rust
async fn serve_tell<Req, Resp>(next: Next<Req, Resp>, peer: EndpointId, recv: RecvStream)
    -> Result<()>
{
    let mut frames = FrameReader::new(recv);
    let Some(request) = frames.read_header::<Req>().await? else {
        return Ok(());
    };
    let cx = Context::new(peer, Req::SERVICE, request.message.method());
    // The chain still runs. Its answer has nowhere to go, and is dropped.
    let _ = next(cx, request.message, BodyReader::new(frames, request.body_len)).await;
    Ok(())
}
```

### 3.4 What a `tell` guarantees, and what it does not

**Guaranteed:**

1. If the message arrives at all, it arrives **whole and correctly typed**. QUIC delivers
   a stream in order and the ALPN gate already refused a peer on another schema.
2. If it arrives, it **reaches the mailbox** and is handled in some turn.
3. Every `Middleware` layer still runs, and a layer that refuses still **prevents the
   handler from running**.
4. `tell` applies backpressure. The client's `open_uni()` blocks once 100 unidirectional
   streams are outstanding — `max_concurrent_uni_streams` defaults to 100, verified at
   `noq-proto-1.1.1/src/config/transport.rs:554`.

**Not guaranteed, and this list belongs in the rustdoc verbatim:**

1. **Delivery.** `tell` returns when the bytes are written locally. The peer may never
   see them. Closing the connection right after a `tell` can discard it before it reaches
   the wire. n0's own `irpc` documents the identical property for its `notify`
   (`irpc-0.17.0/src/lib.rs:1572-1577`): *"The returned future completes once the message
   is written locally. Therefore we have no guarantee that the remote has received the
   message."*
2. **Ordering.** Two tells are two streams, and two streams race. `tell(A)` followed by
   `tell(B)` can be handled B then A. This is a genuine departure from Erlang, which
   guarantees ordering between one sender and one receiver. It follows directly from one
   stream per call, which is the property `rpc.rs` was built around.
3. **Errors.** A layer that refuses a `tell` cannot report the refusal. Nothing is
   listening. Enforcement survives; reporting does not.
4. **Handler failure.** A panic inside a `tell` is caught and turned into an
   `Outcome::Err` that is then dropped. The caller learns nothing.

The remedy for ordering, stated in the same paragraph as the non-guarantee: **use `ask`
when order matters, or put both operations in one message.** A `rpc Restock(..) -> ()`
costs one round trip and guarantees everything on the second list.

The rejected alternative is one long-lived unidirectional stream per connection carrying
every `tell` in order. QUIC would then guarantee ordering per sender. It costs a mutex on
the client's write half, held across a `write_chunk` that can stall on flow control —
which is the head-of-line blocking that `rpc.rs` exists to avoid, reintroduced for the
one message kind that needs it least.

### 3.5 The contract hash must move when `tell` lands

This is a defect the design would otherwise introduce, so it is called out rather than
buried.

`derive_alpn` hashes `Contract { request: Envelope<Request>, response: Envelope<Response> }`
(`macros.rs:92-96`). Adding a `tell` variant to a *user's* service changes `Request` and
therefore moves the ALPN, so an old peer is refused. That part is correct.

But a keylink **capability** change is not covered. Consider two peers with byte-identical
user types, one built on keylink with `tell` and one without. `Envelope` did not change,
so the ALPN does not move, the handshake succeeds, and every `tell` the new peer sends
lands on a unidirectional stream that the old peer never accepts. The message is
**silently lost** — precisely the failure class the ALPN gate exists to prevent.

The fix is one line, in the same commit that adds `tell`:

```rust
#[derive(::postcard_schema::Schema)]
struct Contract {
    request: $crate::Envelope<Request>,
    response: $crate::Envelope<Response>,
    /// Present from the release that added `tell`. Moves the hash so a peer
    /// without unidirectional accept is refused instead of dropping messages.
    kinds: $crate::WireKinds,
}
```

`WireKinds` is a unit struct with a `Schema` derive. `node.rs:177` already asserts the
principle (`the_wire_envelope_is_part_of_the_hash`); this extends it from keylink's
framing to keylink's stream usage.

---

## 4. Server-initiated messages

### 4.1 The transport permits it. The peer does not.

QUIC is symmetric, and `RpcService::accept` holds a `Connection` it could call
`open_bi()` on. That is not the obstacle.

The obstacle is that **the dialing side runs no accept loop.** `RpcClient` holds a
`Connection` and never calls `accept_bi` or `accept_uni` on it. Streams opened toward a
keylink client are never read. They accumulate until the peer's limit of 100 outstanding
streams is reached, and then the accept side's `open_bi()` blocks forever.

So "the accept side sends unprompted" is not one change. It is a second, symmetric
service on the dialing side.

### 4.2 Design A — dial back. Recommended. Zero new lines.

The peer's identity is proven and already reaches the actor through `Actor::caller`
(section 2.2). An actor that wants to reach a peer dials it:

```rust
struct Warehouse {
    items: BTreeMap<String, Item>,
    node: Node,
    watchers: BTreeMap<EndpointId, notices::Client>,
}

impl inventory::Actor for Warehouse {
    async fn reserve(&mut self, sku: String, qty: u32) -> ReserveOutcome {
        // ... mutate ...
        for client in self.watchers.values() {
            let _ = client.stock_changed(sku.clone(), remaining).await;
        }
        ReserveOutcome::Reserved { remaining }
    }
}
```

`notices` is an ordinary `service!` block that the *other* machine serves. Nothing in
keylink changes. The "server" is a client of its peer, which is what peer-to-peer means.

**Cost, stated plainly.** A fresh dial is a fresh connection, so it needs discovery to
resolve the peer's key, and it needs the peer's own `Allowlist` to admit us. An inbound
connection proved a path existed; a fresh outbound dial has to find one again. For a peer
behind a NAT with no published discovery record, the reverse path may be the only path
that ever worked, and Design A cannot use it.

**Unreachable peer:** `Error::Transport(Connect(..))` at dial time, immediately, naming
the peer. This is the better error. It says "I cannot reach that machine", not "the
connection I happened to be holding has gone".

### 4.3 The one convenience worth adding

Dial-per-message is wasteful. `irpc` solves this with a lazy handle that connects on
first use, reuses the connection, and reconnects once on failure
(`irpc-0.17.0/src/lib.rs:2069-2108`). The same shape is about 50 lines here:

```rust
/// A lazily connected, self-healing client for one peer.
///
/// Connects on first use, reuses the connection, and reconnects exactly once
/// when a call fails on a dead connection. A second failure is returned.
pub struct Peer<C> { /* Node, EndpointAddr, Mutex<Option<C>> */ }
```

This is a convenience, not a capability. It can land after everything else, or never.

### 4.4 Design B — reuse the inbound connection. Rejected for now.

The accept side keeps the `Connection` and opens streams back on it. This is the only way
to reach a peer that can dial you but that you cannot dial — the callback shape.

It is viable. The single fact that makes it viable is verified: iroh sets
`keep_alive_interval` to 5 seconds by default
(`iroh-1.0.3/src/endpoint/quic.rs:156`, `HEARTBEAT_INTERVAL` at `src/socket.rs:109`),
against a 30 second `max_idle_timeout`. A held connection with no traffic does **not**
expire.

It costs:

- A handler registered on the **dialing** side, so `Client::connect` grows a variant.
- An `Allowlist` on the dialing side. It has none today, and adding one without a default
  is the fail-closed rule from `authz.rs`.
- Connection ownership. Something must hold every inbound `Connection` past the end of
  `accept`, and something must evict dead ones.
- A worse error. `open_bi` on a dead held connection reports the connection, not the peer.

**Recommendation: do not build B until somebody names the NAT case.** Design A costs
nothing and covers every peer that can be dialed.

---

## 5. Location transparency

### 5.1 The case for

`irpc` makes it work. Its `Client` is one type over two backends
(`irpc-0.17.0/src/lib.rs:1431-1448`):

```rust
Client::local(tx: impl Into<mpsc::Sender<S::Message>>)
Client::boxed(remote: impl rpc::RemoteConnection)
```

The payoff is real: an actor can be tested in-process and deployed across a network
without one call site changing. That is why n0 built it, on this exact QUIC stack.

### 5.2 The case against

Waldo, Wyant, Wollrath and Kendall, *A Note on Distributed Computing* (1994), is the
canonical statement: local and remote calls differ in latency, in memory access, in
concurrency, and above all in **partial failure**. A unified interface must either expose
remote failure modes on local calls, which is a lie in one direction, or hide them on
remote calls, which is a lie in the other.

keylink's entire error design is the opposite of hiding. `Error::SchemaMismatch` exists
solely because reporting a contract mismatch as a transport error sends the reader to the
wrong layer (`error.rs:59-69`). `Error::BodyTruncated` exists so a short file is never a
complete one. `Error::BlockingInAsync` refuses rather than deadlocks. Adding a handle
whose failure modes depend on where the actor happens to live would be the first place in
this crate where a caller cannot tell from the type what can go wrong.

### 5.3 The choice

**Do not unify the handle. Do unify the vocabulary.**

The macro generates two types with the same method names and different error types:

```rust
// local: same process, same runtime, no network
let handle: inventory::Handle = /* from `actor(..)` */;
let item: Option<Item> = handle.get_item(sku).await?;   // Err = MailboxClosed

// remote: another machine
let client: inventory::Client = inventory::Client::connect(&node, peer).await?;
let item: Option<Item> = client.get_item(sku).await?;   // Err = keylink::Error
```

The method names, argument order and success types match exactly, so moving an actor
across the boundary is a mechanical edit that the compiler drives. What does **not**
compile is the code that ignored the difference: the `?` changes error type, and
`keylink::Error::Transport` has no local counterpart to pattern-match on.

`MailboxClosed` is a unit error struct, six lines with `thiserror`. Revision 1 called it
`ActorGone`; it is renamed because section 14 introduces
`RemoteError::NoSuchActor`, and the two must not be read as the same thing.
`MailboxClosed` is **local** and means the task that owned this mailbox is gone.
`NoSuchActor` is a **remote refusal** and means the peer has no entry under that id.
`MailboxClosed` is nearly unreachable
once section 7.3's panic guard is in place, but it must be typed, because a runtime
shutdown can still take the task.

**This is decision 2 for review.** `Handle` is the item most likely to be cut. It is
justified here on three grounds: it makes step 1 of the sequencing testable with no QUIC
at all; it is the only honest way to *demonstrate* the local and remote difference rather
than assert it; and without it, talking to your own actor in your own process requires
binding an endpoint and opening a QUIC connection to yourself, which is absurd.

---

## 6. Mailbox and backpressure

### 6.1 The queue is already bounded, and not by us

This is the section's whole argument, and it comes from `rpc.rs:15-17`:

> The number of in-flight calls is bounded by QUIC's `max_concurrent_bidi_streams` (100
> by default), so an unbounded number of handler tasks is not reachable from the network.

Verified: `max_concurrent_bidi_streams: 100u32` and `max_concurrent_uni_streams: 100u32`,
`noq-proto-1.1.1/src/config/transport.rs:553-554`. iroh does not override either
(`iroh-1.0.3/src/endpoint/quic.rs:152-161` overrides only keepalive, multipath and
migration settings).

Every in-flight `ask` occupies one bidirectional stream for its whole life, including the
time it spends waiting in the mailbox. So a peer cannot have more than 100 messages
outstanding against one service, whatever the mailbox capacity is. The 101st `open_bi`
does not fail — it **blocks on the caller's machine**, which is exactly where the
pressure belongs.

### 6.2 Therefore: bounded, and small

**Capacity 1.**

`tokio::sync::mpsc::channel(0)` panics, so 1 is the floor. With capacity 1, at most one
message waits ahead of the running one; the other 99 serving tasks park on `send().await`
while still holding their streams. Nothing is dropped. Nothing errors. Nothing grows.

A larger buffer does not add safety, because the ceiling is 100 either way. It adds
latency, and it hides how far behind the actor is by moving messages out of the transport
— where the peer can feel them — into a queue where nobody can.

The memory cost is unchanged from today: 100 parked tasks each holding a `BodyReader`,
which is what 100 concurrent handler tasks already hold.

**When the mailbox is full, the serving task waits.** That is the entire policy. There is
no drop policy, no overflow error, no dead-letter queue, and nothing to configure.
`.mailbox(n)` can go on the builder later if somebody produces a latency measurement that
justifies a number.

### 6.3 What this composes with

`Timeout` wraps the `Dispatch`, and the mailbox send is inside the `Dispatch`. So a
`Timeout` layer on an actor service bounds **queue time plus handling time**, which is
the number a caller actually cares about. This falls out of the existing layering with no
new code, and `a_timeout_layer_refuses_a_slow_handler` already covers the machinery.

### 6.4 The gap this does not close

A client blocked in `open_bi()` because the peer's 100 streams are all outstanding has no
timeout. It waits until the connection dies. That is true today and this RFC does not
change it. It is listed here because the actor model makes it easier to reach: one slow
message now stalls every later one, where before only one handler was slow.

---

## 7. Lifecycle

### 7.1 Spawn

`inventory::actor(state, allow)` spawns the task and returns a `ServiceBuilder`. The task
owns the state and the receiver; the `Dispatch` closure owns a sender.

### 7.2 Stop

For a **singleton** actor there is no `stop` method, and that is deliberate.

The actor stops when every sender drops. `Server::shutdown()` shuts down the router,
which drops the handlers, which drops the `Dispatch` closures, which drops the senders,
so `rx.recv()` returns `None` and the loop returns. Adding a control channel to stop it
early would be a second way to do the same thing.

RFC-02 §10.5 specifies `stop` and `abort`. Those belong to the **actor system**, where
removing one instance must leave the service and the connection running. They are adopted
unchanged in [section 14.5](#145-stop-and-abort-adopted-from-rfc-02-105), which also
corrects one mechanism RFC-02 assumed. There is no contradiction: a singleton has nothing
to remove itself from.

One ordering detail, verified by reading: `Server::shutdown` waits for in-flight
connections, and a connection serving an `ask` is parked on the `oneshot`, so shutdown
transitively waits for that message to finish. A `tell` in flight is **not** covered,
because no connection is waiting for it. Say so in the rustdoc.

### 7.3 Panic

Today a panicking handler costs one call. `serve_one` is a task in a `JoinSet`, the panic
is captured in its `JoinHandle`, and `calls.try_join_next()` discards it. The client's
stream is finished with no reply, so it sees `Error::Closed`. The connection survives and
the next call works.

**The actor model breaks that, and it is the single largest hazard in this RFC.** One
panic kills the actor task, the receiver drops, every sender errors, and the service is
wedged for the life of the process.

The codebase has already ruled on this question, in `demo/src/main.rs:190-192`:

> A panicking handler must not wedge every later request. Recover the guard instead of
> propagating poisoning — one bad request should cost one request, not the process.

So the loop **must** catch. Section 2.5 shows it: `guarded` converts the panic into
`Outcome::Err(RemoteError::Failed(..))` and the loop continues with the state as it
stands.

Two consequences, both documented rather than solved:

1. **State may be logically torn.** A panic halfway through a mutation leaves a state
   that satisfies its types and violates its invariants. This is the same hazard as
   `Mutex` poisoning, and `demo/src/main.rs` already chooses to recover rather than
   propagate. Choosing differently here would contradict a decision already on record.
2. **`panic = "abort"` defeats it.** Under an abort profile the process dies and no guard
   helps. One line in the rustdoc.

### 7.4 Restart

Not in v1. Restarting requires reconstructing the state, which requires a
`fn init() -> Self` on the trait — `Actor::Args` in kameo, `pre_start` in ractor. That
is the door to supervision, and section 8 argues for keeping it shut.

### 7.5 What this needs from RFC-02

**Nothing.** That is the finding, and it is worth stating clearly because the framing
assumed otherwise.

"Spawning an actor at runtime is mounting a handler at runtime" is true only if each
actor gets its own ALPN. It must not. **An ALPN is a contract identifier — it is a hash
of the schema — not an instance identifier.** Giving instances distinct ALPNs would break
the invariant that the ALPN identifies the types, which is the crate's headline safety
property.

Actor identity therefore lives **inside the request**, and many instances share one ALPN
and one registered handler. Runtime spawn is then a map insertion behind an existing
mount, and RFC-02's capability A is not on the path.

RFC-02 §2.4 proposes `shutdown(&self)` instead of `shutdown(self)`. That is compatible
with everything here. RFC-02's capability C (`Service::foreign`) is orthogonal.

**Revision 2 note.** RFC-02 §10 reached the same conclusion independently, and proved it
far more thoroughly than the paragraph above: per-actor ALPN is O(N) per mount and
therefore O(N²) to start N actors, because `set_alpn_protocols` always takes the
`Arc::make_mut` deep-copy branch. This RFC accepts that section in full. What §10 then
asks in return — four questions, and one hard ordering requirement on the envelope — is
answered in [section 14](#14-the-actor-system-answering-rfc-02-10).

---

## 8. Supervision

**Recommendation: none. Not a smaller version. None.**

Erlang/OTP supervision is a coherent whole: links, monitors, exit signals, child specs,
restart strategies (`one_for_one`, `one_for_all`, `rest_for_one`), and a restart intensity
limiter that defaults to one restart in five seconds. Every piece assumes a process tree
and restartable state.

keylink has neither. There is one process, one node, and a flat set of services. There
are no links and no exit signals to supervise with.

The useful 80% of supervision, in this crate, is one sentence: **a panicking message must
not kill the service.** Section 7.3 delivers that in about ten lines with zero new public
types. Everything past it — a restart policy, an intensity window, a supervision tree —
requires `Actor::Args`, a restart strategy enum, and a limiter, and none of it helps with
the failures keylink actually has. A peer that vanished is not fixed by restarting a local
actor.

The honest alternative, and the one to recommend out loud: **if you need "restart from a
known state after a crash", you need durable state, and that is Restate.** Restate's
Virtual Object gives keyed serialized access *and* a K/V store that survives the process,
which is strictly more than an in-memory supervisor can offer. keylink should not grow a
worse copy of a system the owner already runs in production.

---

## 9. Prior art

### 9.1 Restate Virtual Objects — the closest match, and the reason to stay small

Verified against the current documentation: a Virtual Object is keyed; at most one
handler with write access runs per key at a time, "similar to a queue per key";
`ObjectSharedContext` handlers are read-only and run concurrently; state is a K/V store
scoped per key and retained indefinitely.

**Taken:** three things.

1. **Keying belongs in the request, not the address.** Restate routes on
   `(service, key, handler)`. keylink should route on `(ALPN, key, method)` if it ever
   grows instances. This is what section 7.5 concludes independently.
2. **The exclusive/shared split.** Restate does not force serialization on reads. keylink
   already has both models; this RFC's answer (c) is the same split, made at the service
   level rather than per handler.
3. **Serialized access is a *scheduling* property, not a *state* property.** Restate's
   key insight is that "one at a time per key" is worth having on its own, separate from
   durability.

**Rejected:** durable state, and everything downstream of it — journaling, replay,
suspension, and restart-from-a-known-point. That is the whole of Restate. A keylink actor
holds memory. When the process dies, the state is gone. Saying so in one sentence in the
rustdoc is better than shipping a fraction of Restate and letting somebody assume the
rest.

The keyed shape, if it is ever wanted, and **not proposed here**:

```rust
keylink::service! {
    service Ledger;
    key account: String;                       // hypothetical
    rpc Deposit(cents: u64) -> Balance;
}
```
It needs an instance registry, an eviction policy, a per-key mailbox, and an answer for
what happens on the first message to an unknown key. That is a second RFC.

### 9.2 `irpc` — the same transport, and the closest thing that exists

`irpc` 0.17.0, published 2026-06-15 by n0, is a streaming RPC system on `noq ^1` — the
same QUIC stack iroh 1.0.3 uses. It is the most relevant prior art by a wide margin,
because it is the only actor-shaped system running on this exact transport.

**Taken:** three things.

1. **`notify` exists and its documentation is honest.** Section 3.4 quotes it. keylink's
   `tell` documentation should read the same way.
2. **The lazy reconnecting remote handle** (`NoqLazyRemoteConnection`). Section 4.3 copies
   its shape: reuse, then reconnect exactly once, then fail.
3. **`notify` is typed by the absence of channels** — `Tx = NoSender, Rx = NoReceiver` —
   so misuse is a compile error. keylink gets the same property from the `tell` kind
   generating no client return type.

**Rejected:** two things.

1. **`Client::local` / `Client::boxed` as one type.** Section 5 argues this at length.
   irpc's own README says its purpose is "to reduce the boilerplate... not to provide an
   opinionated high level API". keylink's purpose is the opposite: `lib.rs:4-6` promises
   that the transport is "invisible from the call site", which is an opinion, and an
   opinionated crate must own its failure modes rather than erase them.
2. **`open_bi` for `notify`.** irpc uses a bidirectional stream even when there is no
   reply (`irpc-0.17.0/src/lib.rs:1600-1610`). Section 3.2 explains why unidirectional is
   the better encoding here.

Not adopting `irpc` wholesale is worth one line of justification: it has no schema-hashed
ALPN, no `Allowlist`, no middleware, no blocking API, and edition 2024 against keylink's
2021. keylink is not a worse `irpc`; it is a different set of promises.

### 9.3 `ractor` — Erlang, transplanted

`ractor` is explicitly modelled on `gen_server`: `cast` and `call`, `RpcReplyPort`,
`spawn_linked`, `SupervisionEvent`, and `handle_supervisor_evt`. Mailboxes can be bounded
or unbounded. `ractor_cluster` distributes over TCP with a shared magic cookie, and
requires a second derive (`RactorClusterMessage`) to make a message remotable.

**Taken:** one thing. The **two-derive split** is the right instinct — being remotable is
a property a message opts into, not a property everything has. keylink gets this for free
and more strictly: every message is already `Serialize + Schema`, and the ALPN hash makes
"remotable" checkable at the handshake rather than at the derive.

**Rejected:** supervision events and the magic cookie. `SupervisionEvent` is section 8.
The cookie is a shared secret; keylink authenticates with a proven Ed25519 key and an
allowlist that fails closed, which is strictly better and already built.

### 9.4 `kameo` — the best-tuned defaults

`kameo` uses a per-message-type `Message<T>` trait rather than one trait per service,
`ask` and `tell` with `.reply_timeout` and `.mailbox_timeout` modifiers, a **bounded
mailbox of 64 by default** for backpressure, and `on_panic` returning
`ControlFlow<ActorStopReason>` so an actor can survive N panics and then stop. Remoting
is libp2p with a Kademlia DHT for actor lookup.

**Taken:** two things.

1. **Bounded by default.** kameo picked 64; section 6 picks 1, for a reason kameo does not
   have — kameo has no transport-level stream limit doing the bounding already.
2. **A panic is a value, not a death.** `on_panic` is the same instinct as section 7.3.
   keylink takes the behaviour and rejects the knob: `ControlFlow` is a policy, and one
   policy that matches this codebase's existing `Mutex` decision is enough.

**Rejected:** two things.

1. **`Message<T>` per type.** It composes better in a large system and it costs a trait
   impl per message. `service!` already generates one trait with one method per rpc, and
   the actor trait should mirror it exactly so the two models stay swappable by one word.
2. **DHT-based actor lookup.** kameo resolves a *name* to a peer. keylink resolves a
   *public key* to a route, and the key is the identity, the address, and the
   authorization subject at once. A name registry would reintroduce a trust question that
   iroh's design removed.

### 9.5 `actix` — the cautionary one

`actix` has a default mailbox of 16 and two send methods: `send`, which respects the
bound, and `do_send`, which **ignores it and queues anyway**.

**Taken:** nothing.

**Rejected, and named as an anti-pattern:** `do_send`. A bound with a documented bypass is
not a bound, and the bypass is the method that is shorter to type and cannot fail, so it
is the one that gets used. Section 6's policy has no bypass. If the mailbox is full, the
caller waits, and there is no second method.

### 9.6 Erlang/OTP — what does not transfer

**Taken:** two ideas, neither of them code.

1. **"One bad message costs one message."** Section 7.3.
2. **Serialized state needs no lock.** Section 1.4.

**Rejected, and the reasons matter:**

1. **Unbounded mailboxes.** Erlang processes have them, and mailbox growth under overload
   is one of the best-known operational failure modes in the OTP world. Section 6 takes
   the opposite default.
2. **Per-sender message ordering.** Erlang guarantees it between a pair of processes,
   because distribution multiplexes onto one ordered channel — which is also why a slow
   receiver head-of-line blocks a whole distribution link. keylink chose one stream per
   call specifically to avoid that. Section 3.4 pays the price honestly instead of
   claiming a guarantee the transport does not provide.
3. **Supervision trees.** Section 8.

### 9.7 Has anyone built actors on iroh?

Searched crates.io, docs.rs and the web on 2026-08-05. **No.** `irpc` (section 9.2) is
n0's own and is actor-shaped but does not call itself an actor framework. `kameo` and
`ractor` distribute over libp2p and TCP respectively. `p2panda` uses iroh as a transport
but is an event-delivery system, not an actor system.

The gap is real. It is not, on its own, a reason to fill it.

---

## 10. What this breaks

### 10.1 The concurrent model — survives untouched

`Handler`, `service()`, `serve()`, `RpcClient`, and every existing test are unchanged. An
actor is an alternative `Dispatch`, and `Dispatch` is below everything the concurrent
model is made of. `concurrent_calls_do_not_queue_behind_each_other` still passes, because
nothing it exercises moved.

### 10.2 `Middleware` — applies identically, and gains a property

A layer wraps the `Dispatch`, and the mailbox lives inside the `Dispatch`. So:

- `WriteGuard`-style refusal works unchanged, and refuses **before** the message reaches
  the mailbox. A refused call does not consume an actor turn.
- `Timeout` covers queue time plus handling time. Section 6.3.
- A layer's `Outcome::Err` on a `tell` is enforced and not reported. Section 3.4.

### 10.3 The blocking API — survives, with a four-line addition

`inventory::actor(..)` calls `tokio::spawn`, which panics outside a runtime. A synchronous
program using `blocking::Node::serve` has a runtime, but it is not entered on the calling
thread, so `actor()` would panic.

The fix is tokio's own idiom on `blocking::Node`:

```rust
impl blocking::Node {
    /// Enter this node's runtime, so runtime-dependent constructors such as a
    /// generated `actor()` can be called from synchronous code.
    pub fn enter(&self) -> tokio::runtime::EnterGuard<'_>;
}
```

```rust
let node = keylink::blocking::Node::bind()?;
let service = { let _guard = node.enter(); inventory::actor(warehouse, allow).build() };
let server = node.serve([service])?;
```

The blocking **client** is unaffected. Actors are a serving-side concept.

### 10.4 Streaming — one kind composes, one does not

This asymmetry is the real cost of the actor model here.

**`recv` (body down) composes for free.** `body.rs:28-30` states why:

> This is a *value*, not a stream handle: a handler returns one and keylink drains it.

The handler returns `(Datasheet, Body)` and the turn ends. keylink drains the `Body`
**outside** the mailbox, on the connection's own task. So an 8 MiB download runs
concurrently with every later message. `a_download_in_flight_does_not_block_other_calls`
would pass against an actor unchanged.

**`send` (body up) does not compose.** The handler receives a `BodyReader` and reads it
**inside** the turn. A 4 GiB upload holds the mailbox for the whole transfer, and every
other message — including a one-line `ping` — waits behind it.

Three honest responses, in order of preference:

1. **Document it.** An actor that accepts uploads serializes them. For many services that
   is correct: an inventory that must not accept two conflicting uploads at once wants
   exactly this.
2. **Split the service.** Bulk transfer goes on a separate `service!` served with
   `service()` (concurrent); the actor gets an `rpc` carrying the resulting identifier.
   This is the same hybrid `README.md:95-100` already recommends for `iroh-blobs`.
3. **Hand the `BodyReader` off.** The handler spawns a task holding the reader and returns
   immediately. It works, and it gives up `&mut self` for the rest of the transfer, which
   is most of what the actor model was for.

**Do not** try to make `send` implicitly concurrent on an actor. That would mean the
handler's `&mut self` borrow ends before the upload does, which the borrow checker will
refuse, or a lock, which is the thing being removed.

### 10.5 `Dispatch` and `RpcService::new`

Breaking, by one parameter. Section 2.6.

---

## 11. Sequencing

Four steps. Each lands on its own, with native tests and `clippy --all-targets -- -D warnings`
green. No step depends on a later one.

### Step 1 — the mailbox, with no network

New `keylink/src/actor.rs`: `Mail`, `guarded`, `dispatch_to`, `ActorGone`. Macro gains
`trait Actor`, `fn actor()`, and `struct Handle`.

Tests are plain `#[tokio::test]` against a `Handle`. **No QUIC, no endpoints, no
`testing::link()`.** This is why `Handle` earns its place: the serialization property is
tested where it lives, not through a transport that is not under test.

Green when: an actor mutates a `BTreeMap` with no `Mutex`; two concurrent `Handle` calls
observe mutual exclusion; a panicking message returns `RemoteError::Failed` and the next
message still works.

### Step 2 — wire it to the transport

Change `Dispatch` to carry `Context` (§2.6). `actor()` returns a `ServiceBuilder`.
`Actor::caller` starts receiving the proven peer.

Tests move to `tests/protocol.rs` against real QUIC. Rewrite `demo`'s `Warehouse` as an
actor to prove §1.4, keeping the existing `Handler` impl in a test so both models stay
covered.

Green when: an actor service answers over QUIC; a `Timeout` layer refuses a slow actor;
`caller()` reports the dialing peer's key.

### Step 3 — `tell`

Macro gains the `tell` kind. `RpcService::accept` splits into two loops (§3.3). `Contract`
gains `WireKinds` (§3.5).

Green when: a `tell` reaches the handler; a `tell` client method returns `Result<()>` and
no value; a layer refusing a `tell` stops the handler; a peer built before `WireKinds` is
refused at the handshake.

### Step 4 — `Peer`, the lazy reconnecting handle

§4.3. Pure addition, no existing type changes.

**Deferred, not scheduled:** keyed instances (§9.1), Design B (§4.4), the `tell` grammar
without `-> ()` (§2.1), `.mailbox(n)`.

---

## 12. Test plan

Tests follow the naming style of `tests/protocol.rs`. Every wire test runs against real
QUIC on loopback via `presets::Minimal`, per `testing.rs`.

Following the house rule that a test which cannot be made red proves nothing, each row
names the exact edit that must break it.

### 12.1 Step 1 — the mailbox

| # | Test | Made red by |
|---|---|---|
| 1 | `an_actor_mutates_its_state_without_a_lock` | Nothing; this is the compile-level proof that `&mut self` reaches the state |
| 2 | `two_calls_to_one_actor_never_overlap` | Replacing the mailbox with a direct call |
| 3 | `a_panicking_message_costs_one_message_not_the_actor` | Removing `guarded` from the loop |
| 4 | `a_panic_is_reported_as_a_remote_failure_not_a_closed_stream` | Returning `Body::empty()` with `Outcome::Ok` on panic |
| 5 | `a_handle_to_a_stopped_actor_reports_actor_gone` | Returning `Ok` when `send` fails |

Test 2 is the load-bearing one. It sends two messages that each set a flag on entry, sleep,
and assert the other flag is unset on exit. It fails deterministically against a
non-serial dispatch.

### 12.2 Step 2 — the transport

| # | Test | Made red by |
|---|---|---|
| 6 | `an_actor_service_answers_over_quic` | Removing `dispatch_to` |
| 7 | `an_actor_serializes_calls_that_arrive_on_separate_streams` | Mailbox capacity raised with a concurrent drain |
| 8 | `a_timeout_layer_covers_time_spent_waiting_in_the_mailbox` | Moving `Timeout` inside the mailbox loop |
| 9 | `a_layer_refusing_a_call_does_not_consume_an_actor_turn` | Moving the layer chain inside the loop |
| 10 | `an_actor_sees_the_proven_key_of_its_caller` | Passing `EndpointId::default()` to `caller` |
| 11 | `the_concurrent_model_still_answers_in_parallel` | Routing `service()` through the mailbox |

Test 7 is test 2 over the wire: eight `slow(150)` calls, as in
`concurrent_calls_do_not_queue_behind_each_other`, asserting the **opposite** —
elapsed >= 8 x 150 ms. The two tests are mirror images, and having both on record is what
makes the (c) answer verifiable rather than asserted.

Test 11 is the existing concurrency test, unmodified. It is listed because it is the
regression guard for §10.1.

### 12.3 Step 3 — `tell`

| # | Test | Made red by |
|---|---|---|
| 12 | `a_tell_reaches_the_handler_and_returns_no_value` | Dropping the mail before `send` |
| 13 | `a_tell_travels_on_a_unidirectional_stream` | Switching `tell` to `open_bi` |
| 14 | `a_layer_refusing_a_tell_stops_the_handler` | Skipping the chain in `serve_tell` |
| 15 | `a_panicking_tell_does_not_wedge_the_actor` | Removing `guarded` |
| 16 | `two_tells_have_no_ordering_guarantee` | Nothing — this pins §3.4 |
| 17 | `a_peer_without_wire_kinds_in_its_contract_is_refused` | Removing `WireKinds` from `Contract` |

Test 13 asserts the encoding directly: a bare `iroh` endpoint speaking the derived ALPN
calls `accept_uni()` and reads the header. It fails if `tell` ever moves to a
bidirectional stream, which is the change most likely to be made as a "simplification".

Test 16 does not assert desirable behaviour. It records the behaviour this RFC accepts,
so a future reader finds the non-guarantee in a test rather than during an incident. It
asserts that **either** order is acceptable, and it is the place where the rustdoc
sentence and the code are kept in agreement.

Test 17 is the guard for §3.5, and it is the only evidence that a keylink capability
change moves the ALPN.

### 12.4 Step 4

| # | Test | Made red by |
|---|---|---|
| 18 | `an_actor_reaches_back_to_its_caller` | Dropping the peer from `caller` |
| 19 | `a_peer_handle_reconnects_once_after_a_dead_connection` | Removing the retry |
| 20 | `a_peer_handle_gives_up_after_the_second_failure` | Retrying in a loop |

Test 20 exists because "reconnect once" and "reconnect forever" look identical in review
and differ completely under a peer that is permanently gone.

---

## 13. Risks, and what could not be verified

### 13.1 Verified by reading source on this machine

- `noq-proto-1.1.1/src/config/transport.rs:553-554` — `max_concurrent_bidi_streams` and
  `max_concurrent_uni_streams` both default to 100.
- `iroh-1.0.3/src/endpoint/quic.rs:152-161` — iroh overrides keepalive, path idle timeout,
  multipath and migration, and **does not** override either stream limit.
- `iroh-1.0.3/src/socket.rs:109,117` — `HEARTBEAT_INTERVAL` is 5 s, `PATH_MAX_IDLE_TIMEOUT`
  is 15 s. A held connection does not idle out (§4.4).
- `iroh-1.0.3/src/endpoint/connection.rs:736` — `Connection` is `Clone`.
- `iroh-1.0.3/src/endpoint/connection.rs:874-901` — `open_bi`, `open_uni`, `accept_bi`,
  `accept_uni` are all available on both sides. QUIC is symmetric (§4.1).
- `iroh-1.0.3/src/endpoint.rs:1052-1071` — `Endpoint::connect` builds a **new** connection
  every time. There is no pooling (§4.3).
- `noq-1.1.1/src/recv_stream.rs:605-630` — dropping a `RecvStream` sends `STOP_SENDING(0)`
  (§3.2).
- `noq-1.1.1/src/send_stream.rs:350-373` — dropping a `SendStream` finishes it, and
  handles `FinishError::Stopped` by resetting.
- `noq-1.1.1/src/connection.rs:1088-1112` — `poll_accept` parks on a `Notified` (§3.3).
- `irpc-0.17.0/src/lib.rs:1567-1615` — `Client::notify`, its delivery documentation, and
  its use of `open_bi`. Crate published 2026-06-15, `noq ^1`, edition 2024, MSRV 1.91.
- `irpc-0.17.0/src/lib.rs:2069-2108` — the lazy reconnect-once remote connection (§4.3).
- Restate documentation, `ractor`, `kameo` and `actix` behaviour, retrieved 2026-08-05.
- `clippy::await_holding_lock` is in `clippy::suspicious`, inside `clippy::all`, which
  `Cargo.toml:23` sets to `deny` (§1.4).

### 13.2 Not verified

1. **Nothing was compiled or run.** This is a design document. No signature here has been
   type-checked, and every code block is illustrative.
2. **`AssertUnwindSafe` around a future holding `&mut State`.** The `guarded` helper in
   §2.4 is written to avoid `unsafe`, but the `poll_fn` closure capturing a `Pin<Box<F>>`
   and returning through `catch_unwind` was not compiled. It may need a different shape.
3. **Two accept loops on one cloned `Connection`.** §3.3 asserts they are independent.
   That follows from `Connection: Clone` and from `poll_accept` filtering by `Dir`, but it
   was traced by reading, not exercised.
4. **`select!` cancel-safety.** §3.3 rejects `select!` rather than resolving whether a
   dropped `Notified` can swallow a wakeup. The question is left open on purpose. If
   somebody later "simplifies" the two loops into a `select!`, that question becomes live
   again and nothing in the test plan catches it.
5. **The ordering non-guarantee** (§3.4) is derived from one stream per call. It was not
   measured. Test 16 records the decision; it cannot prove reordering happens.
6. **Mailbox capacity 1.** §6.2 argues it from the stream limit. No latency measurement
   was taken, so "a larger buffer only adds latency" is reasoning, not data.
7. **`tokio::sync::mpsc::Sender<Mail<Req, Resp>>` satisfying `Dispatch`'s `Send + Sync`.**
   It should hold for `Mail: Send`, but `Mail` contains a `BodyReader` and a
   `oneshot::Sender`, and this was not checked against the real bounds.
8. **The macro grammar.** §2.1 accepts `tell Name(..) -> ();` to leave the `service!`
   grammar untouched. Dropping the arrow needs a normalizing muncher, and macro recursion
   depth against a large service was not tested.
9. **`blocking::Node::enter`.** §10.3 assumes `tokio::runtime::Runtime::enter` returns an
   `EnterGuard<'_>` that makes `tokio::spawn` legal on the calling thread. Not exercised.
10. **Whether `Server::shutdown` waits for an in-flight `tell`.** §7.2 says it does not,
    traced by reading the ownership chain, not by test.

### 13.3 Risks if this ships

1. **`send` on an actor is a latent outage** (§10.4). One large upload stalls every later
   message. A reviewer reading the `service!` block cannot see it, because `send` looks
   like every other method. The mitigation is documentation, which is the weakest kind.
2. **A caught panic leaves state that satisfies its types and violates its invariants**
   (§7.3). This is `Mutex` poisoning by another name, and the codebase already chose to
   recover rather than propagate. The choice is consistent, and it is still a choice that
   can produce wrong answers rather than loud failures.
3. **`tell` loses middleware refusals silently** (§3.4). A future maintainer who adds an
   authorization layer will reasonably assume it reports. It enforces and stays quiet.
4. **The contract-hash gap is invisible** (§3.5). Without `WireKinds`, two peers with
   identical user types and different keylink versions silently drop messages. The fix is
   one line, and it is the kind of line that gets removed as "unused".
5. **Two dispatch models double the surface a reader must hold.** Every future feature has
   to answer "and what does this do on an actor?" — `send` in §10.4 is the first case where
   the answer is unsatisfying, and it will not be the last.
6. **`Handle` may not be worth it** (§5.3). If nobody runs a local actor, it is a generated
   type that exists to make step 1 testable, and there are cheaper ways to test a mailbox.

### 13.4 The argument against doing this at all

Stated so the decision is informed rather than assumed.

keylink today is a small crate that does one thing exactly: typed calls to another
machine, with a schema gate and an allowlist. The actor model adds a second dispatch
model, a second handler trait, a new wire kind, a new stream direction, a panic guard, and
a permanent obligation to answer "what does this do on an actor?" for every future
feature.

The stated use case is "call a function on another machine and get an answer". keylink does
that. "Be able to be an actor-like framework" is a shape, not a need.

**Two things make it worth doing anyway**, and they should be the test the owner applies:

1. §1.4 is a real, measured ergonomic win that costs the user one word, and it removes a
   deny-level lint interaction that will otherwise be hit the first time somebody awaits
   inside a handler that holds state.
2. The design does not fork the crate. The transport, the framing, the ALPN gate, the
   allowlist, the layers and the blocking API are untouched, because the actor lives
   entirely inside `Dispatch`. If the actor model turns out to be wrong, deleting
   `actor.rs` and two macro arms removes it completely.

**If either of those stops being true — if the actor model starts asking for changes in
`rpc.rs`, `wire.rs` or `authz.rs` — stop and reread this section.** That is the tripwire.
