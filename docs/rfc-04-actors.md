# RFC-04 — An actor model for keylink

**Status:** proposal, revision 3. Nothing here is implemented.
**Target:** `keylink` 0.1.0, iroh 1.0.3, edition 2021, rustc 1.97.
**Author's recommendation in one line:** answer **(c)** — an actor is one more `Dispatch`, not one more transport; ship it in four steps, and reject supervision.

**Revision 2** answers the four questions RFC-02 §10.7 puts to this RFC, in
[section 14](#14-the-actor-system-answering-rfc-02-10). It agrees with RFC-02 §10.3 on
every point and contradicts it on none. It also reports one **defect in keylink today**
that revision 1 walked past: `Outcome` is not in the hashed contract, although `reply.rs`
says it is ([section 14.6](#146-a-defect-found-while-answering-question-4)). That defect
blocks question 4 and is demonstrated with running code.

**Revision 3** records the review outcome. All three decisions are **approved**:
`Dispatch` gains a `Context` (§2.6), the local `Handle` ships (§5.3-5.5), and `WireKinds`
moves the contract hash (§3.5). Three arguments were corrected in review and are corrected
here rather than left as they were first written:

- §2.6 — the missing caller identity is a gap in the **concurrent** model too, not a cost
  the actor model introduces.
- §5.4 — `Handle` is justified because a local actor is the base case, **not** because it
  is convenient to test. Revision 1's argument was weak and is replaced. §5.5 records the
  rejection of location transparency as permanent, so a future proposal to unify the two
  handles has an answer already written.
- §3.5 and §12.3 — `WireKinds` ships with **test 17**, a pinned contract hash. Revision 1
  defended the field with a comment and named its deletion as a risk; revision 1's test 17
  could not have caught that deletion at all, and §12.3 explains why.

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

**The win is that you can write the obvious code.** Today you cannot:

> `clippy::await_holding_lock` lives in `clippy::suspicious`, which `clippy::all`
> includes, which `Cargo.toml:23` sets to `deny`.

So a maintainer who adds one `.await` inside `reserve` — a price lookup, a write to disk,
an audit record — **fails the build** and must restructure the function around the guard.
That is not a style warning. It is a deny-level stop, on the most ordinary edit anyone
will ever make to a handler that holds state.

Under the actor model that maintainer writes the `.await` and moves on, because `&mut self`
is held by the task rather than by a guard. Awaiting while mutating stops being a lint
failure and becomes the normal thing.

The line count is the smaller half, and it is listed second on purpose: **nine lines
disappear** — one import, one field wrapper, one constructor wrapper, and a six-line
accessor with its poison comment. Three call sites lose a method call.

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

**This is not an actor tax. It corrects a gap that the concurrent model has today.**

A `Handler` written against `service()` cannot find out who is calling it either. Any
handler that needs its caller's identity — to write an audit record, to scope a lookup to
a tenant, to answer the peer later — has exactly one option today, and it is the option
`middleware.rs` tells you not to take: read a peer id out of the request body, where the
caller wrote it and any caller could have written anything.

The actor model is what made the gap impossible to ignore, because section 4 cannot work
without it. It did not create it.

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

**Approved in review.** The alternative is to leave `Dispatch` alone and pass the peer as
an extra generated argument on every actor method, which is noisier at every call site and
buys nothing.

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

#### The field does not ship alone

Revision 1 defended this field with a doc comment and then listed "the kind of line a
future reader deletes as unused" as a risk. That is not a defence. **A field defended by a
comment is deleted. A field defended by a red test is not.**

So `WireKinds` ships in the same commit as this test:

```rust
keylink::service! {
    service Pinned;
    rpc Echo(text: String) -> String;
}

/// The ALPN of a frozen service, pinned as a literal.
///
/// Everything after the last slash is the schema hash of the whole contract, so
/// this literal goes red when ANY of these moves: `Pinned`'s own types,
/// `Envelope`, `Outcome`, `RemoteError`, or `WireKinds`. Every one of those
/// changes forces every peer in every fleet to be rebuilt, and this assertion is
/// the only thing in the crate that says so out loud.
///
/// When it goes red, in this order:
///   1. Decide whether the change was meant to be wire-visible. If not, revert
///      the change — do not touch this literal.
///   2. If it was, update the literal and record the rebuild in the release notes.
///
/// Never do 2 without doing 1. Producing the new value is trivial, which is
/// exactly why the ordering has to be written down.
#[test]
fn the_pinned_contract_hash_has_not_moved() {
    assert_eq!(
        std::str::from_utf8(pinned::alpn()).expect("alpn is ascii"),
        "keylink/Pinned/0000000000000000", // <- from one run, never by hand
    );
}
```

One test defends three separate things: `WireKinds` here, the `Outcome` fix in §14.6, and
any future accidental change to the wire. Deleting `WireKinds` moves the hash, and the
literal goes red with no further machinery.

**On precedent.** This is an extension of `alpn_shape_is_protocol_slash_16_hex`
(`node.rs:144`), which already asserts the ALPN's structure; this asserts its value. It is
**not** a golden wire-byte test — there is no such test in this repository, and a byte
literal would be the weaker instrument here, because it pins one encoded message while the
ALPN pins the entire transitive type graph on both sides at once.

**One honest caveat.** A `postcard-schema` upgrade can move the hash without any keylink
change. That is correct behaviour rather than a false positive — if the hash function
changed, peers built against the two versions genuinely cannot agree — but it will
surprise whoever bumps the dependency, so step 1 of the doc comment must be followed
rather than skipped.

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

### 5.4 Why `Handle` is not optional

Revision 1 justified `Handle` by testability and called it the item most likely to be cut.
That was the wrong argument, and it deserved to lose: testability is a weak reason to add
public API, and "we need it for a test" is how test-shaped types end up in a published
interface.

The real argument is definitional:

> **An actor framework whose actors must cross a network to be called is not an actor
> framework.** A local actor is the base case. Remote is the interesting case, not the
> only one.

Every system in section 9 agrees. `ractor`, `kameo` and `actix` are local actor systems
that grew distribution. `irpc` starts from `Client::local`. Erlang addresses a process on
the same node with the same `!` it uses across a cluster. Not one of them requires a
transport to deliver a message between two objects in one process, and a keylink that did
would be alone in that.

Testability follows from the definition rather than justifying it: because the base case
needs no network, step 1 of section 11 is tested with no QUIC at all.

### 5.5 This rejection of location transparency is permanent

Recorded here so that a future proposal to unify `Handle` and `Client` has an answer
already written, rather than relitigating section 5.2 from scratch.

**`MailboxClosed` and `keylink::Error` stay distinct, and the two types stay distinct.**
The reason is not taste. A unified handle must either claim failures the local case cannot
have, or hide failures the remote case does have. `keylink::Error::Transport`,
`Error::Closed`, `Error::SchemaMismatch` and `Error::Remote` have no local counterpart —
none of them, not one. A local mailbox has exactly one failure: the task is gone.

Merging them would make keylink the first place in this crate where a caller cannot tell
from the type what can go wrong. That is the opposite of the property `error.rs` is built
around, and it is the property section 5.2 says the crate should not trade away.

If a future RFC proposes the unification, it must first answer what
`Error::SchemaMismatch` means for two objects in one process. There is no honest answer,
which is the point.

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

#### The comment this needs in the code

Capacity 1 with no overflow policy reads as an oversight. Somebody will "fix" it by
raising the number or adding a `try_send` fallback, and both changes look like
improvements. The rustdoc on the constant must therefore carry the reason, not the value:

```rust
/// Messages that may wait ahead of the one being handled.
///
/// One, deliberately, and there is no overflow policy because there is no
/// overflow to have a policy about.
///
/// A message occupies a QUIC stream from the moment it arrives until its reply
/// is written, including the time it spends here. `max_concurrent_bidi_streams`
/// is 100, so a peer cannot have more than 100 messages outstanding against this
/// actor whatever this number is. The queue is bounded by the transport, and
/// the 101st `open_bi` blocks ON THE CALLING MACHINE — which is where you want
/// a slow actor to be felt.
///
/// Raising this does not raise the ceiling. It moves messages out of the
/// transport, where the peer feels them, into a buffer where nobody does, and
/// it adds that much latency to every message behind a slow one.
///
/// Before changing this, read RFC-04 section 6. If the transport ever stops
/// bounding in-flight calls, this whole argument dies and a real policy is
/// needed.
const MAILBOX: usize = 1;
```

That is the chapter 1 §1.6 rule applied where it earns its place: the comment carries the
*why*, which the code cannot, and it names the single condition under which the reasoning
stops holding.

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

New `keylink/src/actor.rs`: `Mail`, `guarded`, `dispatch_to`, `MailboxClosed`. Macro gains
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
| 5 | `a_handle_to_a_stopped_actor_reports_a_closed_mailbox` | Returning `Ok` when `send` fails |

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
| 17 | `the_pinned_contract_hash_has_not_moved` | **Removing `WireKinds` from `Contract`** — and any other unintended wire change |

Test 13 asserts the encoding directly: a bare `iroh` endpoint speaking the derived ALPN
calls `accept_uni()` and reads the header. It fails if `tell` ever moves to a
bidirectional stream, which is the change most likely to be made as a "simplification".

Test 16 does not assert desirable behaviour. It records the behaviour this RFC accepts,
so a future reader finds the non-guarantee in a test rather than during an incident. It
asserts that **either** order is acceptable, and it is the place where the rustdoc
sentence and the code are kept in agreement.

**Test 17 is the guard for §3.5, and revision 1 got it wrong.** Revision 1 named it
`a_peer_without_wire_kinds_in_its_contract_is_refused` and claimed removing the field
would make it red. It would not. Removing `WireKinds` changes *both* peers in one build,
so the handshake still agrees and an integration test still passes — the test would have
been decoration on the exact line it was supposed to defend.

The pinned-hash form in §3.5 does make it red, because it compares against a literal that
does not change when the code does. It is also the only test in the plan that fails
**without a peer**, which is why it belongs here rather than among the QUIC tests: a wire
change should be caught by `cargo test` in one crate, not by two endpoints agreeing to
disagree.

This test is shared with step 0 (§14.9). Whichever step lands first writes it.

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

---

## 14. The actor system: answering RFC-02 §10

Revision 2. RFC-02 §10.7 puts four questions and one hard constraint to this RFC. This
section answers all five, freezes the wire envelope RFC-02 asks to be frozen, and then
**leaves the actor system unbuilt** — the same treatment RFC-02 §9.7 gives its own
capability A.

### 14.0 Agreement, and the one place this RFC pushes back

**Agreement with RFC-02 §10.3 is total.** Actor lifecycle must never call
`Endpoint::set_alpns`. Nothing in sections 1 to 13 does, and nothing in this section does.
Revision 1 §7.5 reached the same conclusion from the invariant ("an ALPN is a contract
identifier, not an instance identifier"); §10.2 reached it from cost, with source-verified
complexity classes. Two independent arguments for one answer is the strongest position
available, and neither RFC needs to yield.

**One push-back, on scope rather than on design.** RFC-02 §10 designs the actor
*system* — registry, ids, mount and unmount. Sections 1 to 13 of this RFC design the
singleton actor, which has no id, no registry and no lifecycle. These are two features,
and only the second has a justification today (§1.4). Revision 2 therefore answers every
question §10 asks, freezes the envelope so that answering later is not a fleet rebuild,
and puts the justified half first in the build order.

**One correction to §10.5**, on mechanism rather than on conclusion: §14.5.

### 14.1 Question 1 — the `ActorId` type and its encoding

```rust
/// The name of one actor within one node's actor system.
///
/// Scoped to the node you are connected to. It is not globally unique and it is
/// not a route: the full address of an actor is `(EndpointId, ActorId)`, and the
/// caller resolves both. See section 14.3.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash,
         Serialize, Deserialize, postcard_schema::Schema)]
pub struct ActorId(String);

impl ActorId {
    /// The longest id keylink accepts, in bytes.
    ///
    /// 128 holds a UUID, a hierarchical key such as `tenant/user/42`, and a
    /// base64 32-byte key. The registry key stays small enough to be cheap to
    /// hash on every message.
    pub const MAX_LEN: usize = 128;

    /// # Errors
    /// [`ConfigError::MalformedActorId`] when empty or over [`Self::MAX_LEN`].
    pub fn new(name: impl Into<String>) -> Result<Self>;
}
```

**`String`, not `u64` or a fixed array.** Restate's Virtual Object key is a string, and
Restate is the model the owner already runs in production (§9.1). A `u64` would push a
name-to-number mapping into every user's code.

**Encoding:** postcard writes a varint length then UTF-8 bytes. An empty id is rejected so
that `ActorId::default()` cannot become an accidental catch-all.

**The cap is enforced twice, and the second time is the one that matters.** `new` cannot
protect the receiving side, because `#[derive(Deserialize)]` bypasses constructors. So the
router re-checks after decode and before the registry lookup. A manual `Deserialize` impl
would make an over-long id unrepresentable, and is **rejected**: it turns a protocol
violation into `Error::Codec`, which reads as "the bytes were garbage" rather than "your
id is too long". One explicit check, one honest error.

The allocation is already bounded before this point: the whole header frame is checked
against `MAX_HEADER` before a byte is buffered (`wire.rs:128`).

**On the oracle question §10.2 left open.** Under Option 2 there is no TLS oracle at all,
which is one of Option 2's wins. There is a weaker one at the message layer: a
`NoSuchActor` refusal tells the sender that no actor holds that id. It is available only
to a peer that already passed `Allowlist`, which fails closed and has no `Default`
(`authz.rs:17-23`). That is a much smaller surface than an unauthenticated dial, and it is
the same information any addressing scheme must return. **Guessable ids are still a bad
idea**, and the rustdoc should say so where `ActorId::new` is defined rather than in a
security section nobody reads.

### 14.2 Question 2 — per actor TYPE, not per instance

**Per type.** §10.4 assumed this and the assumption is correct, for a reason worth writing
down: the hash exists to detect **schema drift**, and a schema is a property of a Rust
type. Two instances of `Session` have identical message types by construction. A
per-instance hash would repeat one value and carry no information.

But §10.4 missed a second job the hash does, and it changes the check.

**One node can host several actor types behind one ALPN.** If `Session` and `Ledger` both
live in the system, the hash also **discriminates which type the payload is**. So the
comparison is not against one global constant. It is against the hash recorded for the
entry found under that id:

```rust
struct Entry {
    /// `hash_ty_path::<T::Request>("myapp/Session")`, computed once at mount.
    schema: u64,
    mail: mpsc::Sender<SealedMail>,
    task: JoinHandle<()>,
}
```

The dispatch order is therefore fixed, and each step must fail before the next:

1. Decode the envelope. Typed, shape-stable, no user types involved.
2. Check `id.len() <= ActorId::MAX_LEN`. → `RemoteError::Failed` on violation.
3. Look up the id. Miss → `RemoteError::NoSuchActor`.
4. Compare `envelope.schema` against `entry.schema`. Mismatch →
   `RemoteError::ActorSchemaMismatch`. **The payload is still bytes.**
5. Decode the payload as the entry's type.

### 14.3 The hard constraint — making decode-before-verify unrepresentable

RFC-02 §10.4 requires that step 5 be reachable only from step 4's success arm, and says
plainly that avoiding it in the example is not enough. **Agreed, and here is the
mechanism.** It is the type-state pattern, and it costs about twenty lines.

```rust
/// Payload bytes that have not been schema-checked.
///
/// There is no way to read these bytes. `Sealed` has no accessor, no `Deref`,
/// no `into_inner`, and no `Debug` that prints content. [`Sealed::open`] is the
/// only route from bytes to a typed message, and it compares the declared hash
/// against the hash of `T` before it decodes.
///
/// This is deliberate. postcard is not self-describing, so decoding before
/// checking is exactly the silent misparse the ALPN scheme exists to prevent —
/// see `same_shape_variant_swap_decodes_as_the_wrong_variant` (`node.rs:185`).
pub struct Sealed {
    declared: u64,
    bytes: Bytes,
}

impl Sealed {
    /// # Errors
    /// [`RemoteError::ActorSchemaMismatch`] when the hashes differ. The bytes
    /// are dropped without being interpreted.
    pub fn open<T>(self, ours: u64) -> std::result::Result<T, RemoteError>
    where
        T: DeserializeOwned + postcard_schema::Schema,
    {
        if self.declared != ours {
            return Err(RemoteError::ActorSchemaMismatch {
                ours,
                theirs: self.declared,
            });
        }
        postcard::from_bytes(&self.bytes)
            .map_err(|err| RemoteError::Failed(err.to_string()))
    }
}
```

Three properties make this unrepresentable rather than merely unwritten:

- `bytes` is private and no method returns it. There is no path to the bytes that does not
  go through `open`.
- `open` takes `self` by value, so a `Sealed` cannot be opened twice against two different
  hashes until one succeeds.
- `open` compares before it decodes, in that order, in one function that is the only place
  the ordering exists. There is no second copy of this logic to get wrong.

`ours` comes from the registry entry, never from the caller's frame. The generated code
passes `entry.schema`, which was computed at mount from the type the actor was registered
with.

**Do not** add `Sealed::bytes()`, `Sealed::peek()`, or a `Debug` impl that prints the
payload. Any of the three reopens the hole. A comment saying so belongs on the struct.

**One thing keylink must expose for this.** `node.rs:104` currently hides the `u64` inside
a formatted string:

```rust
pub fn derive_alpn<C: Schema>(protocol: &str) -> Vec<u8> {
    let hash = u64::from_le_bytes(hash_ty_path::<C>(protocol));
    format!("{protocol}/{hash:016x}").into_bytes()
}
```

Split it so both RFCs use one source of truth, and so nobody re-derives a hash by parsing
hex out of an ALPN:

```rust
/// The schema hash of `C` under `protocol`. The trailing 16 hex digits of
/// [`derive_alpn`] are this value.
pub fn schema_hash<C: Schema>(protocol: &str) -> u64;
```

Two lines moved. `derive_alpn` calls it.

### 14.4 Question 3 — addressing through a non-hosting node: **no, loudly**

RFC-02 §10.7 asked this to be said loudly if wanted. **It is not wanted, and the answer
should be recorded as a decision rather than an omission.**

Four reasons, in descending order of force.

1. **iroh already is the forwarding layer, and it is better at it.** A peer that cannot be
   reached directly is reached through a relay, transparently, at the transport layer,
   with end-to-end encryption preserved. An application-level forwarder would **terminate
   the encryption at each hop**, which is strictly worse than what the crate gets for free
   today.
2. **It destroys the authorization model.** `Allowlist` authorizes the directly connected
   peer, whose key iroh proved during the handshake. A forwarded message arrives carrying
   the *forwarder's* proven key, so the origin's identity degrades to a claim in the
   body — precisely what `middleware.rs:40-41` forbids. Restoring it needs per-message
   signatures, which is a second crypto layer and a second key-management story.
3. **It breaks the body invariant.** `body.rs:8-10` promises peak memory of one chunk. A
   forwarded body makes every intermediate node a proxy with its own flow control, and
   peak memory becomes one chunk per hop while the slowest hop sets the rate.
4. **It puts a routing table in the request path**, which needs a topology source, loop
   prevention, and an answer for a next hop that dies mid-transfer. §10.5's clean unmount
   semantics do not survive any of that.

So an `ActorId` is meaningful **only on the node you are connected to**. The full address
is `(EndpointId, ActorId)`, and the caller resolves both. This is stated in the `ActorId`
rustdoc in §14.1, not buried here.

### 14.5 Question 4 — the error variants, and a correction to §10.5

§10.7 asked for one variant name. **Two are needed**, and the second is what makes §10.4's
ordering observable:

```rust
/// No actor is mounted under this id.
///
/// Deliberately does not distinguish "never existed" from "was stopped". The
/// caller's next action is the same either way, and telling them apart would
/// leak that an id was once real.
#[error("no actor is mounted as {id}")]
NoSuchActor {
    /// The id that was addressed.
    id: String,
},

/// The actor exists, and the caller built its message against different types.
///
/// The payload was dropped without being decoded. This is the in-envelope
/// counterpart of [`Error::SchemaMismatch`], one layer down.
#[error("actor message schema mismatch: ours is {ours:016x}, theirs is {theirs:016x}")]
ActorSchemaMismatch {
    /// The hash of the type the actor is registered with.
    ours: u64,
    /// The hash the caller declared.
    theirs: u64,
},
```

`NoSuchActor` over `ActorGone` or `NotFound`: `ActorGone` implies the id was once real,
which both leaks and misdescribes a typo. `NotFound` is too bland beside `Unauthorized`
and `TimedOut`. `NoSuchActor` also matches the register already used in this repository —
`demo/src/main.rs:62` has `ReserveOutcome::NoSuchItem`.

**Both variants are blocked by §14.6.** Adding them is not free today, and that is a
defect rather than a design choice.

#### `stop` and `abort`, adopted from RFC-02 §10.5

The two-method shape is adopted verbatim. It is the right call, and it is the Flag
Argument rule from chapter 1 §1.8 applied correctly: `stop(id, force: bool)` would hide
"flush my document" and "kill this now" behind a boolean at the call site.

**The correction is to the mechanism, not the conclusion.** §10.5 says:

> Dispatch clones an `Arc` to the actor before it awaits, so removing the map entry drops
> a reference and never cancels a running future.

That is true for an `Arc<dyn Actor>` called directly. It is **not** the shape this RFC
designs. Here an actor is a task that owns its state as `&mut`, and the registry holds an
`mpsc::Sender`, not an `Arc` to the state. So:

- **`stop` works as §10.5 describes, by a different route.** Removing the entry drops one
  sender; the task lives while any sender lives and drains what is queued. Nothing is
  cancelled. The conclusion stands.
- **`abort` does not work as §10.5 describes.** Dropping a reference cannot cancel a
  future the task owns. `abort` requires `JoinHandle::abort()`, so the registry must hold
  the `JoinHandle` as well as the sender — which is why `Entry` in §14.2 has a `task`
  field.

That distinction has a consequence §10.5 could not have seen, and it is worth stating
because it is the reason the two methods are genuinely different rather than stylistically
different:

> **`stop` preserves the state until the drain finishes. `abort` destroys it.**
> `JoinHandle::abort` drops the task's future at an await point, and the state is owned by
> that task, so it is dropped with it. An actor holding unflushed work loses it.

One more consequence, from §10.4 of this RFC rather than of RFC-02: `abort` cannot
truncate a `recv` download, because the body is drained by the connection task after the
turn ends. It **can** truncate a `send` upload, because that body is read inside the turn.
That is the same `send`/`recv` asymmetry as §10.4, surfacing in a third place.

### 14.6 A defect found while answering question 4

Adding a `RemoteError` variant is supposed to move the ALPN. **It does not.** This was
found while costing question 4, and it is demonstrated with running code rather than
argued.

`reply.rs:10-12` states the guarantee:

> `Outcome<T>` sits inside the hashed contract, so adding a variant moves the ALPN and a
> drifted peer is refused at the handshake rather than misreading a reply.

The hashed contract is `macros.rs:92-96`:

```rust
struct Contract {
    request: $crate::Envelope<Request>,
    response: $crate::Envelope<Response>,   // <- bare Response
}
```

The wire carries something else. `rpc.rs:125` writes `&outcome`, which is
`Outcome<Resp>`, and `rpc.rs:485` reads `read_header::<Outcome<Resp>>()`. So the wire type
is `Envelope<Outcome<Resp>>` and the hashed type is `Envelope<Resp>`. **`Outcome` is not
in the hash, and `RemoteError` is not in the hash.**

Verified by building the two shapes with the real `serde` and `postcard`:

```
insert-in-middle  -> NoSuchActor { id: "disk full" }   <-- silent misparse
append            -> Err(SerdeDeCustom)                <-- loud, rejected
```

The first line is `RemoteError::Failed("disk full")`, encoded by a peer built before
`NoSuchActor` existed, decoded by a peer that inserted `NoSuchActor` ahead of `Failed`.
`Failed(String)` and `NoSuchActor { id: String }` have identical postcard encodings, so
the refusal changes meaning with no error — which is
`same_shape_variant_swap_decodes_as_the_wrong_variant` (`node.rs:185`) reproduced in
keylink's own error channel.

Severity, stated precisely rather than dramatically:

- **Appending** a variant is caught loudly: postcard rejects an out-of-range discriminant.
- **Inserting or reordering** is silent, and changes what a refusal means.
- `RemoteError` is `#[non_exhaustive]` (`reply.rs:23`), so nothing in the type system
  discourages inserting.
- No released version exists, so nothing is deployed against the wrong hash today.

**The fix is one line**, in `macros.rs`:

```rust
- response: $crate::Envelope<Response>,
+ response: $crate::Envelope<$crate::Outcome<Response>>,
```

After it, the hashed type equals the wire type in both directions, the `reply.rs`
paragraph becomes true, and every future `RemoteError` variant moves the ALPN — which is
what the crate advertises with "Nothing to bump by hand" (`README.md:52`).

**Ship this before anything else in this RFC.** It is capability G in section 0. It is one
line, it makes an existing doc comment honest, and it is a prerequisite for adding the two
variants in §14.5 without quietly weakening the guarantee that is keylink's headline
property.

**It ships with test 17**, the pinned contract hash from §3.5. Applying this fix moves the
hash, so the literal is written for the first time in the same commit — which is the right
moment, because from then on every wire change has to walk past it.

It is also the third instance of one pattern, and the pattern is now worth naming:
`WireKinds` (§3.5), this fix, and RFC-02's own reasoning all say the same thing. **The
hashed contract must equal the wire type. Every time the two drift apart, the gate stops
covering something it claims to cover.**

The ideal defence would be structural — a compile-time assertion that the type passed to
`write_header` is the type named in `Contract`. That is not expressible today, because
`Contract` is a struct built for its `Schema` derive and the wire type is chosen at two
call sites (`rpc.rs:125`, `rpc.rs:485`). Until somebody finds a way to state it in the type
system, **test 17 is the defence**, and it is a good one: it cannot be satisfied by
inspection, and it does not need a peer.

### 14.7 The envelope, frozen

RFC-02 §10.7 asks that the envelope be frozen early, because any change to it rebuilds
every peer at once. Here it is, complete, with the reasoning for each field.

```rust
/// The request type of an actor system. One per system, not one per actor.
///
/// FROZEN. Every field change moves the system ALPN and rebuilds every peer in
/// the fleet at once. Per-actor variation belongs in `payload`, which is opaque
/// and costs nothing here. See RFC-02 §10.7 and RFC-04 §14.7.
#[derive(Debug, Serialize, Deserialize, postcard_schema::Schema)]
pub struct ActorRequest {
    /// Which actor. Section 14.1.
    pub actor: ActorId,
    /// The schema hash of the actor's message type. Section 14.2.
    pub schema: u64,
    /// The actor's own `Request`, undecoded. Section 14.3.
    pub payload: Vec<u8>,
}
```

Three fields, and nothing else. Two fields that were considered and **rejected**:

- **A method name.** It would let outer middleware match on the method before the payload
  is decoded. Rejected because it duplicates the payload's own variant index, and a peer
  can make the two disagree. Consequence in §14.8.
- **A protocol version.** That is what the ALPN is. Adding one here would be the
  hand-maintained version number `README.md:52` says does not exist.

**`wire.rs` does not change.** `ActorRequest` is the `message` field inside the existing
`Envelope<T>`, so framing, `MAX_HEADER` and `FrameReader` are all untouched, and a `Body`
still rides beside the header exactly as today. The actor system is one ordinary
`Service`, which is what RFC-02 §10.6 concludes.

**The cost, named:** `payload: Vec<u8>` means postcard allocates and copies the payload on
decode, where a typed field decodes from the frame in place. One extra allocation and one
copy per actor message, bounded by `MAX_HEADER`. This is the price of the ordering
guarantee in §14.3, and it is worth paying. It is also why the singleton actor
(sections 1 to 13) does **not** use this envelope: it has one type, so there is nothing to
discriminate and nothing to seal.

### 14.8 What the actor system costs that the singleton does not

Four things, so the build-order decision is made with them visible.

1. **Per-method middleware stops working at the outer layer.** `Context::method()` is
   built at `rpc.rs:117` from `Req::method()`, before any handler runs. With an opaque
   payload, keylink cannot know the method until after §14.3's check. So a
   `WriteGuard`-style layer — `a_layer_can_refuse_one_method_and_pass_the_rest` — can no
   longer refuse one actor method from outside.
   **Per-actor rules still work**, because `actor` is a typed field. This needs
   `Context` to carry it:
   ```rust
   /// The actor this message addresses, when the service is an actor system.
   pub fn actor(&self) -> Option<&ActorId>;
   ```
   The layering this forces is arguably the correct one: the outer layer answers "may this
   peer reach this actor", the inner layer answers "may this peer call this method". It is
   still a capability that exists today and would not exist there.
2. **One extra allocation and copy per message.** §14.7.
3. **A registry, and everything a registry needs**: eviction, a policy for a message to an
   unmounted id (answered: `NoSuchActor`), and a decision about whether a first message
   creates an actor. Restate creates on first use. This RFC does **not** propose that:
   creating state from an unauthenticated-at-the-method-level message is how an actor
   system becomes a memory-exhaustion target.
4. **`stop`, `abort`, and the `JoinHandle` bookkeeping** of §14.5.

None of these is an argument against the actor system. They are the reason it should be
built when a use is named rather than alongside the singleton.

### 14.9 Sequencing, revised

The lead is right that the router change disappears, and section 11 never had one. The
order changes in one place: **capability G moves to the front**, because it is one line
and it blocks §14.5.

| Step | Content | Depends on |
|---|---|---|
| **0** | Capability G: `Envelope<Outcome<Response>>` in `Contract`, **plus test 17**, the pinned contract hash (§3.5) | nothing |
| 1 | The mailbox, with no network (§11) | 0 |
| 2 | Wire it to the transport; `Dispatch` gains `Context` (§11) | 1 |
| 3 | `tell`, `accept_uni`, `WireKinds` (§11) | 2 |
| 4 | `Peer`, the lazy reconnecting handle (§11) | 2 |
| **5** | **The actor system** (§14): `ActorId`, `Sealed`, `ActorRequest`, registry, `stop`/`abort`, `Context::actor` | 0, 2 |

Step 0 is worth doing even if every other step is rejected — it is one line plus the test
that stops the next four steps from weakening the gate by accident.

Step 5 is **designed and unbuilt**. RFC-02 §10.8's tests 23 to 30 belong to it, and test 29
in particular is the one that pins §14.3 — RFC-02 is right that it must use two types with
identical postcard shapes, or it passes against the broken ordering.

Note that steps 3 and 5 both move the pinned hash, deliberately. That is the whole
mechanism: a wire change cannot land without somebody updating a literal and reading the
comment above it.

### 14.10 Added to "not verified"

Beyond §13.2, and specific to this section:

1. **The `Sealed` type-state was not compiled.** In particular, whether
   `postcard_schema::Schema` and `DeserializeOwned` compose on the generated actor request
   types without a lifetime obstruction.
2. **The O(N²) claim in RFC-02 §10.2 was not independently re-derived here.** This RFC
   accepts it. The `Arc::make_mut` reasoning was spot-checked and the shape is right, but
   no allocation count was measured, and RFC-02 says the same about itself.
3. **`ActorId::MAX_LEN = 128` is a judgement, not a measurement.** No workload was
   profiled. It is chosen to fit the four key shapes named in §14.1.
4. **Restate's own key-length limit was not confirmed**, so the 128 is not copied from a
   precedent.
5. **`JoinHandle::abort` dropping the state** (§14.5) follows from tokio's documented
   cancellation behaviour. It was not exercised against an actor holding unflushed work.
6. **The §14.6 defect was reproduced, but only in isolation.** The two enums were built
   with real `serde` and `postcard` outside this workspace. It was **not** reproduced by
   editing `RemoteError` in the crate and observing an unchanged ALPN, which is the test
   that belongs in step 0.

---

## 15. Postscript — what shipped, and in what order (2026-08-06)

Appended, not edited. Everything above this line is the RFC as it was reviewed and
approved; this section records what building it actually did, including where the RFC was
wrong.

### 15.1 The order was inverted, and nobody decided that

§14.9 sequences six steps. Read against the git history, the work went:

| Step | §14.9 says | What happened |
|---|---|---|
| 0 | capability G, the pinned hash | first, as specified (`4903601`, `7bc9d6d`) |
| **5** | **the actor system — "designed and unbuilt"** | **second** (`b0e8ae5`, twelfth commit of fifty) |
| 2 | wire it to the transport | with it |
| 3 | `tell`, `accept_uni`, `WireKinds` | fourth (`d4cb94f`, `0936db9`) |
| 4 | `Peer` | fifth (`a1d0e0d`) |
| **1** | **the mailbox, with no network** | **last** (`354f5af`, 2026-08-06) |

So the capability §0 marks **"Ship."** was the last thing built, and the capability §0
marks **"Design now, build when named"** was among the first. No commit message argues for
that order and no review approved it. It is what happens when a sequencing table sits in
one document and the work is picked up from another: §14 is the longest and most specific
section in this RFC, so it read as the instruction, and "designed and unbuilt" three lines
into a 400-line design is easy to walk past.

The lesson is about where a decision is written down, not about actors. A step marked
**do not build this yet** has to be marked at the point where somebody would start
building, which for §14 is §14.1, not §14.9.

### 15.2 What the inversion cost

Four things, none catastrophic, all avoidable.

1. **Every user paid §14.8's four costs for a capability most of them did not want.** An
   extra allocation and copy per message, a registry with eviction and a capacity cap,
   `stop`/`abort` bookkeeping, and — the expensive one — per-method `Middleware` stops
   working from outside, because the payload is opaque until after §14.3's check.
2. **§12.1 tests 1, 2 and 5 were unwritable**, exactly as §11 step 1 predicted, so the
   serialization property was tested only through a transport that was not under test.
3. **§12.2 test 10 was never written.** `Actor::caller` shipped with a default empty body
   and **nothing in the tree ever overrode it** — not one test, not the demo. A hook with
   no implementor and no test is a hook nobody can say works. It did not, in one case
   (§15.6).
4. **`blob` methods could not be reached through an actor at all**, and
   `__keylink_actor_client_method!` carries the paragraph explaining why: the system
   cannot read the address off an opaque reply, so it cannot record a grant. That is a
   true statement about capability F which was silently a statement about *all* actors,
   because F was the only actor there was.

### 15.3 Capability A is not what §14.8 assumed it would be

§14.8 lists its four costs so "the build-order decision is made with them visible", and
the natural reading is that the singleton would be the actor system with one entry — a
system with one actor whose id you never write. Built that way it pays all four.

It is not built that way. **A singleton actor is served on the service's own ALPN, with
the service's own `Request` and `Response`, and the wire does not move.** Peers use the
ordinary generated `Client`. `Handle::service(allow)` is `service(handler, allow)` with a
mailbox where the `Arc` was, and that is the entire difference on the serving side.

It therefore pays none of the four:

| §14.8 cost | The keyed system | The singleton |
|---|---|---|
| per-method `Middleware` stops working | yes | **no** — the layer sees the typed enum |
| an extra allocation and copy per message | yes | **no** — there is no second envelope |
| a registry, eviction, capacity | yes | **no** — one actor, one task |
| `stop`, `abort`, `JoinHandle` bookkeeping | yes | on the handle, where the owner is |

And two capabilities the keyed system cannot have come back with it: the idempotency
ledger applies unchanged, and `blob` methods work, because the server can still read the
address off a typed reply to record a grant.

This is not a criticism of §14. Every one of those four costs is the price of §14.3's
property — that a payload is opaque to keylink until its schema hash is checked — and that
property is what makes keyed actors safe. The point is narrower: **the costs belong to
keyed addressing, not to actors**, and building A first would have shown that a year of
commits earlier.

`System` and the keyed envelope are unchanged and stay. A node serves either an actor
system or one singleton per service, because one router owns one ALPN — refused at
startup, with a test.

### 15.4 §1.4's arithmetic is wrong. The win is bigger than claimed

§1.4 is this RFC's justification, so it was checked rather than repeated. The demo's
`Warehouse` is now an actor, and the count is:

    35 removed = 13 that disappeared + 22 rewritten
    39 added   = 17 that appeared   + 22 rewritten

| item | §1.4 | actually gone |
|---|---|---|
| `use std::sync::Mutex;` | 1 | 1 |
| the field wrapper | 1 | **0** — that line survives, edited |
| the constructor wrapper | 1 | **2** — rustfmt writes it on three lines |
| the accessor and its poison comment | 6 | **8** — 3 doc, 5 code |
| the blank line the accessor owned | — | 1 |
| `let mut items = self.items();` in `reserve` | — | **1** |
| the three `self.items()` call sites | "lose a method call" | 0, all three rewritten |
| | **9** | **13** |

Two of the four errors are one mistake: **§1.4 counted its own snippet rather than the
file.** It writes the accessor body on one line and the constructor on one line; at 100
columns rustfmt writes three of each. A line count taken from prose is a line count of the
prose.

The other two are omissions. An edited line is not a saving, so the field wrapper should
not have been counted. And `reserve` loses a whole line nobody counted, because
`self.items.get_mut(&sku)` borrows directly where a guard had to be bound first.

The call-site count — three — is right.

§1.4 also counted **only deletions**. Seventeen lines appeared; sixteen are comments the
change earns and one is code. So the honest figure is **13 out, 1 in, net −12 of code**,
and the file grows by 4.

### 15.5 §1.3's "one word at the serve site" did not survive contact

§1.3 shows `actor(state, allow)` beside `service(handler, allow)` — symmetric, one word
apart. What shipped is `actor(state) -> Handle`, with the allowlist on
`Handle::service(allow)`, because **a local handle has no peer to allow**. The allowlist
belongs where the network is, and that is a better shape than the sketch.

The cost of it showed up the first time somebody outside the crate used the API: the
concurrent model had a one-line front door (`inventory::serve`) and the actor model made
you drop to `Server::builder(..).service(..).spawn()`, four lines, which made the actor the
*more verbose* choice at exactly the site §1.3 advertises as one word. `Handle::serve`
exists because of that report, and the serve site is two lines now.

§2.2's `fn caller(&mut self, _cx: &Context)` is likewise stale — see §15.6.

### 15.6 Corrections to sections above

**§2.4 and §5.3 — `Actor::caller` takes `Option<&Context>`, not `&Context`.** A local
message has no peer, no handshake and no proven key. Skipping the hook for local messages
was the first design, and it is wrong in a way worth recording: an actor that stashes its
caller and is reachable both ways would then authorize an in-process message against **the
last peer that happened to dial it**. Verified by removing the `None` — the local call
reports the remote peer's key. `Option` is the fix, and assigning rather than merging is
the caller's part, said on the trait.

**§5.3 — the success types do not all match.** §5.3 says the local and remote methods have
matching "method names, argument order and success types". True for `rpc` and `tell`.
`send` and `recv` have **no local method at all**: a `send` handler is defined by the
`BodyReader` it reads and a `recv` handler by the `Body` it returns, and both of those are
one half of a QUIC stream. In this process there is no stream. The two ways to pretend
otherwise — buffer the body in memory, or generalize `FrameReader` over `AsyncRead` — are
worse than the absence, and the absence is held by a compile-time probe rather than by a
comment. `blob` has a local method returning the `BlobId` rather than a `BlobHandle`, for
the same reason in the other direction: a handle names the node that holds the content,
and here that is you.

**§10.3 — `enter()` returns a `Result`.** The sketch is
`pub fn enter(&self) -> tokio::runtime::EnterGuard<'_>`. It cannot be, for the reason the
rest of the module exists: called from inside a runtime it must refuse, the way every
other entry point here refuses, so it returns `Result<EnterGuard<'_>>` carrying
`Error::BlockingInAsync`. Otherwise the one method that exists to fix a panic would itself
be the only one in the module that can be misused silently.

### 15.7 What two adversarial reviews found, and what §12 could not

Two reviewers read the delivered code with different lenses. Recorded because §13's "not
verified" list is the part of an RFC that ages worst, and because one finding is the shape
this RFC keeps warning about.

**The doc said a self `tell` was safe. It deadlocks.** Both reviewers found it
independently. `notify` awaits a permit on a mailbox of capacity one; the actor released
one permit when it took the message it is running, so a self `tell` succeeds *only until
another sender takes it* — and a served actor always has other senders. Then the send
blocks, and only the actor can free the permit. §6.2 argues the capacity from the transport
side and never considers a sender that is the actor. Fixed in the docs, listed with the
other two self-call cases, and worth noting as the failure mode this whole design invites:
load-dependent, so it passes every test.

**A test in §12's own shape was green and proved nothing.**
`a_served_actor_serializes_calls_that_arrive_on_separate_streams` — §12.2's test 7 — built
eight futures with `.map(..)` and awaited them in a loop. A Rust future does nothing until
polled, so it ran eight sequential round trips and its assertion held under *any* serving
model, including the concurrent one it was written to contradict. Its mirror,
`concurrent_calls_do_not_queue_behind_each_other`, uses `tokio::join!`; that is the whole
difference. Fixed, then verified by swapping in the concurrent handler: 62 ms against a
480 ms floor. The old version passed that swap.

§12's rule — "each row names the exact edit that must break it" — would not have caught
either. Both were red-tested against the wrong edit.

Also corrected, each because a reviewer showed the code rather than the claim: a layer can
refuse a call and cannot cancel a turn, so `Timeout` on an actor sheds no load and settles
an idempotency key for a message that then happens; a dropped call still runs, which is the
one `Handler` → `Actor` change the compiler cannot show; `Panic` had a public constructor
and so proved nothing about panics; the absence probe lacked the `Deref` companion
`representability.rs` documents; and threaded wasm was unguarded, because the
`compile_error!` for that assumption was gated on the `web` feature while the local
mailbox is gated on nothing.

### 15.8 The estimate

§0 costs capability A at "~90 lines in the crate, ~60 in the macro".

| | §0 estimate | shipped |
|---|---|---|
| crate | ~90 | 115 code lines (`local.rs`, excluding its tests and every comment) |
| macro | ~60 | 199 added code lines in `macros.rs` |

The crate estimate is good. The macro estimate is off by more than three times, and the
reason is that §11 step 1 scopes the macro's share to `trait Actor`, `fn actor()` and
`struct Handle` — the local half. The served half, `Handle::service`, is not in that
estimate because §11 puts it in step 2, and step 2 was costed as a `Dispatch` change.
Thirty-seven of those 199 lines are the compile-time probe and the `Actor` impl it needs,
which no estimate would have included because §12 does not name it.

### 15.9 What is still unbuilt, and why that is right

- **Supervision and restart (§8).** Unbuilt, and this RFC's own recommendation is
  "none. Not a smaller version. None." A panic costs one message; that is the whole policy.
- **Design B, reusing the inbound connection (§4.4).** Unbuilt. §4.2's answer — dial it
  back — still costs zero lines.
- **The `tell` grammar without `-> ()` (§2.1)**, and `.mailbox(n)`. Both deferred, both
  still deferred.
- **Location transparency (§5.5).** Rejected permanently, and building both handles has
  not weakened the argument: the local one has exactly one failure mode and the remote one
  has five, and `MailboxClosed` deliberately does not convert into `Error`.

### 15.10 Added to "not verified"

1. **No `.wasm` has been loaded in a tab.** The local mailbox uses `n0_future::task::spawn`
   and nothing else, so it should hold on the browser's single-threaded executor, but
   "should" is the word.
2. **`Panic::resume` was not exercised under `panic = "abort"`**, where `catch_unwind`
   catches nothing and the actor dies with the process. `guarded` has always said so; the
   local handle inherits it and no test can assert it without a second profile.
3. **The mailbox capacity of 1 is not measured on the local path, and §6.1's argument does
   not carry to it.** Over the network QUIC bounds in-flight calls, which is what §6.1
   rests on. In-process nothing does, and the mailbox is not a substitute: a parked sender
   still holds its message, so memory grows with waiting callers whatever the capacity is,
   and the queue moves into a wait list with no length anybody can read. What capacity 1
   buys in-process is narrower — a caller cannot get more than one message ahead of the
   actor, so a caller that measures its own latency sees the actor's. The number was
   reasoned about, not profiled.

---

# 16. Append: what the correctness programme changed here

**Appended, not edited.** Nothing above this line has been altered. §15 is the
earlier postscript recording where §1.4, §2.2, §5.3 and §10.3 were wrong; this
section is the same discipline applied to four more sections, plus one item that
was designed and deliberately not built. RFC-05 records the idempotency half of
the same programme.

## 16.1 Answering §2.6 — who is calling is an argument, not a hook

`Actor::caller` is **deleted**. Every generated `Actor` method now takes
`cx: Caller<'_>` first, and every generated `Handler` method takes
`cx: &Context` first.

```rust
pub enum Caller<'a> { Local, Peer(&'a Context) }
```

The hook it replaces ran before each message and asked the state to stash the
value. Three things were wrong with that, and all three are gone.

- **It occupied a name.** A service with an `rpc Caller()` could not be an
  actor, because the generated method and the hook were two trait items called
  `caller`. That restriction did not exist for a `Handler`, so it appeared only
  when a service gained an actor — as a duplicate-definition error pointing at
  library code.
- **It was reachable from one dispatcher of two.** `__actor_turn` called it;
  the local handle's methods each repeated it by hand. A sixth method kind wired
  into the local macro without that line would have silently inherited the last
  remote peer. That is RFC-01 §11.2b's "build the path twice" hazard, and the
  code carried a comment admitting it. An argument cannot be forgotten: a
  missing `Caller` is a compile error.
- **The idiomatic way to write the hook was wrong.**
  `if let Some(cx) = cx { self.peer = Some(cx.peer()) }` reads perfectly and
  leaves the previous remote peer in place for the next *local* message, so an
  actor authorizing on the stashed value authorizes an in-process caller as
  whoever last dialled in. `Caller::Local` carries no peer, so there is nothing
  to inherit.

The asymmetry between the two traits is the point. An actor is reachable two
ways and a handler one, so only one of them has a "nobody" state, and that state
is a named variant rather than an `Option` — `authz.rs` records what an `Option`
on an identity costs.

## 16.2 Answering §6 — `QueueTimeout`, and why `Timeout` stays

§6 argues the mailbox capacity. What it does not say is that **no layer can
bound a turn**: a layer runs outside the mailbox, so a budget expiring drops the
*wait for the reply*, not the turn. `Timeout::after(100ms)` in front of a
ten-second method sheds no load at all — ten callers are refused at 100 ms and
the actor still spends a hundred seconds producing replies nobody wants.

`QueueTimeout` refuses only a message that has **not started**, and it can,
because the check happens where a layer cannot reach:

1. the wait for a mailbox permit is bounded, and giving up there means the
   message was never queued;
2. having been queued, the deadline is re-read **on the actor's own task**,
   immediately before the turn begins.

There is no third place, and there does not need to be: once the turn starts,
stopping it means dropping `&mut A` mid-mutation, which is the one thing an
actor must never do.

`Timeout` is **kept**, and this wave decided that explicitly rather than by
default. Nothing prevents layering it on an actor-served builder, because after
RFC-05 §2 it is no longer *unsafe* there — merely ineffective at shedding load,
and honest about the key it settles. On a concurrent `Handler`, dropping the
future genuinely cancels the work, and `QueueTimeout` cannot do that. Two
behaviours, not two doors to one thing, so "one way to build a thing" does not
apply. A marker bound on the actor path's `layer` was considered and rejected:
it would split `ServiceBuilder` in two to prevent a combination that is now
merely suboptimal.

On a `Handler`, `QueueTimeout` refuses nothing. That is not a lie — a handler is
dispatched immediately and never queues, so no message it serves is ever "still
queued when the budget expired". Item 10 below is the case where the same shape
*would* have been a lie, and is why it was not built.

## 16.3 Answering §10.4 — a `send` method cannot be served as an actor

§10.4 records the asymmetry: a `send` handler reads its upload **inside** the
turn, so an upload holds the actor for the whole transfer. Combined with §16.2 —
no layer can bound a turn — that is not a cost, it is a denial of service with a
one-line trigger. A peer declares a large body, stops writing, and holds the
actor until iroh's connection idle timeout with nothing else answered, including
the local `Handle`.

So it does not compile. `DECLARES_SEND` folds at compile time exactly as
`DECLARES_BLOBS` does, and `Handle::service` opens with

```rust
let () = ServableAsActor::<A>::CHECK;
```

where `ServableAsActor<A>` is generic in `A` **on purpose**: an associated const
of a generic type is evaluated when it is instantiated, so the assertion fires at
the site that serves an actor and nowhere else. A plain `const` would be
evaluated where it is written and would refuse every service that merely
*declares* a `send` method, including every one served the concurrent way, where
a `send` is perfectly safe.

Chosen over a startup `ConfigError`, which the programme's own success criteria
asked for. A compile error beats a runtime one, keeps `Result` off the generated
serve site, costs no new error variant, does not ripple into `spawn_with`'s
`Fn(&ActorId) -> Option<Mounted>`, and matches what the rest of this crate does
with claims of the form "there is no way to do X" — `representability.rs` exists
for exactly that.

Scoped to `send` alone, deliberately. A `recv` body is drained by the connection
*after* the turn ends and a `blob`'s bytes move later on a different connection,
so neither can hold an actor. Widening it would refuse two shapes that are safe.

`keylink/tests/singleton.rs` used to serve a `send` method on an actor and
*measure* the cost. Measuring a hazard is weaker than removing it, so that test
is now the split it recommended: two services, two ALPNs, one node.

## 16.4 Answering §4.4 — Design B, and the case it does not solve

Design B is built, and it added **nothing to the wire**. The reverse contract is
an ordinary `service!` block with its own derived ALPN, served by the side that
dialled; the side that accepted dials back and becomes an ordinary client of it.
No new method kind, no `WireKinds` field, no second meaning for an existing one.

What keylink adds is `Subscribers<C>`, and its only non-obvious content — the
reason it is a type rather than a `HashMap` at a call site — is that it evicts on
`Departure` rather than on `Connection::closed`. `closed()` borrows a strong
handle, so a registry that waited on one would keep every connection it had ever
seen alive: a diagnostic that pins its subject open, which `watch.rs:35-42`
calls a leak wearing a diagnostic's clothes.

The property that makes the second connection worth its cost: **the reverse
direction has its own allowlist**. A reverse channel bolted onto the forward
connection would inherit the forward direction's admission, so admitting a peer
to *ask* would admit it to *tell*, and those are not the same decision.

**The NAT case, named because §4.4 did not name it.** The accept side dials
back, so it needs a route to the peer that dialled in. With a discovery service
the proven `EndpointId` is the whole address and this is free. Without one — a
LAN, an air-gapped network, the hermetic test suite — the peer has to say where
it is, and that is a **claim** where the identity is a **proof**. The two are
reconciled at the one place that holds both: the handler compares the declared
address's id against `cx.peer()` and refuses a mismatch, so a peer cannot make
the accept side dial a third party by asking nicely. Behind a symmetric NAT with
no relay, no address the peer can name will work, and Design B does not solve
that; a relay does.

## 16.5 Item 10, designed and NOT built: an abandoned turn

The programme asked for a long-running turn to be able to observe that its
caller has gone and bail at a safe point. The signal exists and costs nothing:
`Mail.reply` is a `oneshot::Sender`, closed exactly when the caller's future was
dropped — no task, no timer, no cancellation.

It was not built, and the goal permitted that outcome in terms: *"Either put it
somewhere only an actor turn can reach, or do not build it."* Three placements
exist and each costs more than the item is worth.

- **On `Context`.** `Context` is shared with `Handler`, where nothing wires the
  signal, so the method would answer "no" for every handler forever. A method
  that lies to half its callers is worse than an absent one.
- **Inside `Caller`.** It stops being `Local | Peer(&Context)` — the enum §16.1
  exists to introduce — and becomes a struct wrapping an `Origin` enum plus a
  type-erased closed-signal. Distorting the wave's centrepiece for its smallest
  item is the wrong trade.
- **A second parameter on every actor method.** Every user pays, in every
  signature, for a signal few need.

Recorded here rather than forgotten. The shape that would justify it is a turn
long enough that abandoning it matters — a batch, a scan — and that is also the
shape §16.2 says to bound from inside the method, where the state is.
