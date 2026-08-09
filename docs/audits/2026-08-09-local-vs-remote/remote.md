# keylink — the remote / served surface

Repository `/Users/pedrocunha/repos/keylink`, `main` at `01195040bfcfff83be7a0af4f69cdba22b644cfb`.
Read-only map. No recommendations.

**How to read the signatures.** Most client and server types are written inside
`macro_rules!` bodies in `keylink/src/macros.rs`. The signatures below are copied
from those bodies, so they still carry `$crate` and `$svc`. In an expansion:

- `$crate` is `keylink`.
- `$svc` is the service name. `service Ledger;` produces the module `ledger`
  (`[<$svc:snake>]`, `keylink/src/macros.rs:490`).
- `$rpc` is one declared method. `[<$rpc:snake>]` is its snake_case name.

---

## 1. The generated client types

`service! { service Svc; .. }` writes one module. That module holds **four**
client types on the async side, plus two more behind the `blocking` feature.

| Type | Path after expansion | Addresses | Constructed by |
|---|---|---|---|
| `Client` | `svc::Client` | one singleton actor or handler | `Client::connect` |
| `IdempotentClient` | `svc::IdempotentClient` | the same, with a key attached | `Client::with_idempotency` |
| `KeyedClient` | `svc::KeyedClient` | a whole registry of keyed actors | `KeyedClient::connect` |
| `ActorRef<'a>` | `svc::ActorRef<'_>` | **one** keyed actor | the free function `svc::at` |
| `blocking::Client` | `svc::blocking::Client` | one singleton | `blocking::Client::connect` |
| `blocking::IdempotentClient` | `svc::blocking::IdempotentClient` | the same, with a key | `blocking::Client::with_idempotency` |

### 1.1 `Client` — the singleton client

`keylink/src/macros.rs:2609`

```rust
pub struct Client {
    inner: $crate::RpcClient<Request, Response>,
}
```

`keylink/src/macros.rs:2626-2705`

```rust
pub async fn connect(
    node: &$crate::Node,
    peer: impl Into<$crate::EndpointAddr>,
) -> $crate::Result<Self> {
    Ok(Self {
        inner: $crate::RpcClient::connect(node, peer, alpn()).await?,
    })
}

#[must_use]
pub fn with_idempotency(
    &self,
    idempotency: impl ::core::convert::Into<$crate::Idempotency>,
) -> IdempotentClient { .. }

pub async fn key_state(
    &self,
    key: $crate::IdempotencyKey,
) -> $crate::Result<$crate::KeyState> { .. }

pub fn watch(&self) -> $crate::Watch { .. }

pub async fn close(self) -> $crate::Result<()> { .. }
```

`Client` also implements `Clone` (`keylink/src/macros.rs:2620`) and
`keylink::Dial` (`keylink/src/macros.rs:2598`).

### 1.2 The per-method client signatures

The macro `__keylink_client_method!` emits one method per declaration.
`keylink/src/macros.rs:3963-4033`. `$name` is the snake_case method name, `$ret`
is the declared return type.

| Kind | Emitted signature | file:line |
|---|---|---|
| `rpc` | `pub async fn $name(&self $(, $arg: $ty )*) -> $crate::Result<$ret>` | `macros.rs:3968` |
| `tell` | `pub async fn $name(&self $(, $arg: $ty )*) -> $crate::Result<()>` | `macros.rs:3986` |
| `send` | `pub async fn $name(&self $(, $arg: $ty )*, body: $crate::Body) -> $crate::Result<$ret>` | `macros.rs:3994` |
| `recv` | `pub async fn $name(&self $(, $arg: $ty )*) -> $crate::Result<($ret, $crate::BodyReader)>` | `macros.rs:4006` |
| `blob` | `pub async fn $name(&self $(, $arg: $ty )*) -> $crate::Result<($ret, $crate::BlobHandle)>` | `macros.rs:4023` |

### 1.3 `IdempotentClient`

`keylink/src/macros.rs:2719`

```rust
pub struct IdempotentClient {
    inner: $crate::IdempotentClient<Request, Response>,
}
```

It carries `key_state`, `watch` and `close` with the same signatures as `Client`
(`keylink/src/macros.rs:2735`, `2746`, `2764`). Its methods come from
`__keylink_idempotent_client_method!`, `keylink/src/macros.rs:2901-2914`, which
**drops the `tell` arm and forwards every other kind** to
`__keylink_client_method!`:

```rust
([$( #[$meta:meta] )*], tell, $name:ident, $req:ident, $variant:ident, $extract:ident,
    ($( $arg:ident : $ty:ty ),*), $ret:ty) => {};
```

So an `IdempotentClient` has no `tell` method at all.

### 1.4 `KeyedClient` — the registry client

`keylink/src/macros.rs:2412`

```rust
pub struct KeyedClient {
    inner: $crate::RpcClient<$crate::Addressed<Request>, Response>,
    idempotency: ::core::option::Option<$crate::Idempotency>,
}
```

`keylink/src/macros.rs:2445-2533`

```rust
pub async fn connect(
    node: &$crate::Node,
    peer: impl Into<$crate::EndpointAddr>,
) -> $crate::Result<Self> {
    Ok(Self {
        inner: $crate::RpcClient::connect(node, peer, keyed_alpn()).await?,
        idempotency: ::core::option::Option::None,
    })
}

#[must_use]
pub fn with_idempotency(
    &self,
    idempotency: impl ::core::convert::Into<$crate::Idempotency>,
) -> Self { .. }

pub async fn key_state(
    &self,
    key: $crate::IdempotencyKey,
) -> $crate::Result<$crate::KeyState> { .. }

pub fn watch(&self) -> $crate::Watch { .. }

pub async fn close(self) -> $crate::Result<()> { .. }
```

Two differences from `Client` are structural.

1. `KeyedClient::with_idempotency` returns `Self`, not a second type
   (`macros.rs:2473`). `Client::with_idempotency` returns `IdempotentClient`
   (`macros.rs:2653`).
2. `KeyedClient` has no per-method calls of its own. The private
   `send_to` (`macros.rs:2510`) wraps the request in an `Addressed` and applies
   the stored key:

```rust
async fn send_to(
    &self,
    actor: &$crate::ActorId,
    request: Request,
    body: $crate::Body,
) -> $crate::Result<(Response, $crate::BodyReader)> { .. }
```

`KeyedClient` implements `Clone` (`macros.rs:2428`), `Debug` (`macros.rs:2437`)
and `keylink::Dial` (`macros.rs:2535`).

### 1.5 `ActorRef<'a>` — one keyed actor

`keylink/src/macros.rs:2550-2596`

```rust
pub fn at(
    client: &KeyedClient,
    actor: $crate::ActorId,
) -> ActorRef<'_> {
    ActorRef { client, actor }
}

#[derive(Debug)]
pub struct ActorRef<'a> {
    client: &'a KeyedClient,
    actor: $crate::ActorId,
}

impl ActorRef<'_> {
    pub fn id(&self) -> &$crate::ActorId { .. }
    // one method per declaration, from __keylink_actor_client_method!
}
```

`__keylink_actor_client_method!`, `keylink/src/macros.rs:3693-3793`:

| Kind | Emitted signature | file:line |
|---|---|---|
| `rpc` | `pub async fn $name(&self $(, $arg: $ty )*) -> $crate::Result<$ret>` | `macros.rs:3723` |
| `tell` | **nothing is emitted** (empty arm) | `macros.rs:3717-3718` |
| `send` | `pub async fn $name(&self $(, $arg: $ty )*, body: $crate::Body) -> $crate::Result<$ret>` | `macros.rs:3735` |
| `recv` | `pub async fn $name(&self $(, $arg: $ty )*) -> $crate::Result<($ret, $crate::BodyReader)>` | `macros.rs:3747` |
| `blob` | `pub async fn $name(&self $(, $arg: $ty )*) -> $crate::Result<($ret, $crate::BlobHandle)>` | `macros.rs:3783` |

The comment at `macros.rs:3705-3716` gives the reason for the empty `tell` arm.
Four refusals are decided **after** the bytes reach the wire — a malformed id, an
id nothing is mounted under, a registry at capacity, and an expired queue budget
— and all four are reported on the reply.

### 1.6 The blocking clients

Behind `__keylink_if_blocking!` (`keylink/src/macros.rs:2944-2951`), which is
`#[cfg(feature = "blocking")]`.

`keylink/src/macros.rs:2779-2884`

```rust
pub mod blocking {
    pub struct Client {
        inner: super::Client,
        runtime: ::std::sync::Arc<$crate::blocking::Runtime>,
    }

    impl Client {
        pub fn connect(
            node: &$crate::blocking::Node,
            peer: impl Into<$crate::EndpointAddr>,
        ) -> $crate::Result<Self> { .. }

        #[must_use]
        pub fn with_idempotency(
            &self,
            idempotency: impl ::core::convert::Into<$crate::Idempotency>,
        ) -> IdempotentClient { .. }

        pub fn key_state(
            &self,
            key: $crate::IdempotencyKey,
        ) -> $crate::Result<$crate::KeyState> { .. }

        pub fn close(self) -> $crate::Result<()> { .. }
    }

    pub struct IdempotentClient {
        inner: super::IdempotentClient,
        runtime: ::std::sync::Arc<$crate::blocking::Runtime>,
    }
}
```

`__keylink_blocking_method!`, `keylink/src/macros.rs:3076-3101`, emits only two
of the five kinds:

| Kind | Emitted signature | file:line |
|---|---|---|
| `tell` | `pub fn $name(&self $(, $arg: $ty )*) -> $crate::Result<()>` | `macros.rs:3084` |
| `rpc` | `pub fn $name(&self $(, $arg: $ty )*) -> $crate::Result<$ret>` | `macros.rs:3091` |
| `send` | nothing | `macros.rs:3095` |
| `recv` | nothing | `macros.rs:3096` |
| `blob` | nothing | `macros.rs:3100` |

`blocking::IdempotentClient` uses `__keylink_idempotent_blocking_method!`
(`macros.rs:2920-2927`), which drops `tell` and forwards the rest. The blocking
idempotent client therefore carries `rpc` methods only.

### 1.7 The error type on every client method

Every generated client method returns `keylink::Result<T>`, which is
`std::result::Result<T, keylink::Error>` (`keylink/src/error.rs:318`). Section 5
lists the variants.

---

## 2. How a user reaches a remote actor

### 2.1 The node

`keylink/src/node.rs:18-135`

```rust
pub struct Node { .. }

impl Node {
    pub async fn bind() -> Result<Self>                                  // :29
    pub async fn bind_local() -> Result<Self>                            // :54
    pub async fn bind_local_with_secret(secret: SecretKey) -> Result<Self> // :67
    pub async fn bind_with_secret(secret: SecretKey) -> Result<Self>     // :76
    pub async fn bind_from_env(var: &str) -> Result<Self>                // :92
    pub fn from_endpoint(endpoint: Endpoint) -> Self                     // :110
    pub async fn online(&self) -> &Self                                  // :122
    pub fn id(&self) -> EndpointId                                       // :128
    pub fn endpoint(&self) -> &Endpoint                                  // :133
}
```

### 2.2 The four addressing paths

| Path | Signature | Needs | file:line |
|---|---|---|---|
| Singleton | `svc::Client::connect(node: &Node, peer: impl Into<EndpointAddr>)` | a `Node`, a `NodeAddr`. The ALPN comes from `svc::alpn()` | `macros.rs:2628` |
| Registry | `svc::KeyedClient::connect(node: &Node, peer: impl Into<EndpointAddr>)` | the same. The ALPN comes from `svc::keyed_alpn()` | `macros.rs:2447` |
| One keyed actor | `svc::at(client: &KeyedClient, actor: ActorId) -> ActorRef<'_>` | a live `KeyedClient` **and** an `ActorId` | `macros.rs:2550` |
| Raw | `RpcClient::connect(node: &Node, peer: impl Into<EndpointAddr>, alpn: &'static [u8])` | a `Node`, a `NodeAddr` and an explicit ALPN | `rpc.rs:2051` |

`svc::at` is a free function and takes the id by value. `ActorRef` borrows the
client, so it cannot outlive it.

### 2.3 `RpcClient<Req, Resp>` — the transport client

`keylink/src/rpc.rs:1834-2196`

```rust
pub struct RpcClient<Req, Resp> { .. }

impl<Req, Resp> RpcClient<Req, Resp> {
    pub fn blob_handle(&self, id: BlobId) -> BlobHandle                  // :1880
    pub fn with_idempotency(
        &self,
        idempotency: impl Into<Idempotency>,
    ) -> IdempotentClient<Req, Resp>                                     // :1936
}

impl<Req, Resp> RpcClient<Req, Resp>
where
    Req: Serialize + Send + Sync,
    Resp: DeserializeOwned + Send + Sync,
{
    pub async fn connect(
        node: &Node,
        peer: impl Into<EndpointAddr>,
        alpn: &'static [u8],
    ) -> Result<Self>                                                    // :2051
    pub async fn call(&self, request: Req, body: Body) -> Result<(Resp, BodyReader)> // :2075
    pub async fn key_state(&self, key: IdempotencyKey) -> Result<KeyState>           // :2127
    pub async fn tell(&self, request: Req) -> Result<()>                 // :2178
    pub async fn close(self) -> Result<()>                               // :2191
}
```

`call` opens a bidirectional stream (`rpc.rs:2076`). `tell` opens a
unidirectional stream and writes `None` for the key and `Some(0)` for the body
length (`rpc.rs:2179-2186`).

`IdempotentClient<Req, Resp>`, `keylink/src/rpc.rs:1968-2034`:

```rust
pub struct IdempotentClient<Req, Resp> { .. }

impl<Req, Resp> IdempotentClient<Req, Resp> {
    pub fn blob_handle(&self, id: BlobId) -> BlobHandle                  // :1995
}

impl<Req, Resp> IdempotentClient<Req, Resp>
where
    Req: Serialize + Send + Sync,
    Resp: DeserializeOwned + Send + Sync,
{
    pub async fn call(&self, request: Req, body: Body) -> Result<(Resp, BodyReader)> // :2012
    pub async fn key_state(&self, key: IdempotencyKey) -> Result<KeyState>           // :2022
    pub async fn close(self) -> Result<()>                               // :2031
}
```

`IdempotentClient` has no `tell`.

### 2.4 `Peer<C>` and `Dial` — the reconnecting holder

`keylink/src/peer.rs:39-45`

```rust
pub trait Dial: Sized {
    fn dial(
        node: &Node,
        addr: EndpointAddr,
    ) -> impl std::future::Future<Output = Result<Self>> + Send;
}
```

`keylink/src/peer.rs:56-134`

```rust
pub struct Peer<C> { .. }

impl<C: Dial + Send + Sync> Peer<C> {
    pub fn new(node: &Node, addr: impl Into<EndpointAddr>) -> Self       // :67
    pub fn addr(&self) -> &EndpointAddr                                  // :76
    pub async fn connect(&self) -> Result<Arc<C>>                        // :85
    pub fn disconnect(&self)                                             // :101
    pub async fn call<T, F, Fut>(&self, mut f: F) -> Result<T>
    where
        F: FnMut(Arc<C>) -> Fut,
        Fut: std::future::Future<Output = Result<T>>,                    // :115
}
```

`Peer::call` runs `f` once. On any `Err` it disconnects, re-dials and runs `f` a
second time (`peer.rs:120-133`). Both `Client` and `KeyedClient` implement
`Dial`, so both work as the `C` of a `Peer` (`macros.rs:2598`, `macros.rs:2535`).

### 2.5 The blocking node

`keylink/src/blocking.rs:124-247`

```rust
pub struct Node { .. }

impl Node {
    pub fn bind() -> Result<Self>                                        // :131
    pub fn bind_with_secret(secret: SecretKey) -> Result<Self>           // :138
    pub fn bind_from_env(var: &str) -> Result<Self>                      // :149
    pub fn online(&self) -> Result<()>                                   // :156
    pub fn id(&self) -> EndpointId                                       // :163
    pub fn inner(&self) -> &crate::Node                                  // :168
    pub fn runtime(&self) -> &Arc<Runtime>                               // :173
    pub fn enter(&self) -> Result<tokio::runtime::EnterGuard<'_>>        // :229
    pub fn serve<I: IntoIterator<Item = Service>>(&self, services: I) -> Result<Server> // :234
}
```

```rust
pub fn connect_with<F, Fut, T>(node: &Node, peer: impl Into<EndpointAddr>, dial: F) -> Result<T> // :281
```

---

## 3. The serving side

### 3.1 The three doors, and what each publishes

| Door | Publishes | Signature | file:line |
|---|---|---|---|
| `svc::service(handler, allow)` | one **`Handler` impl**, no actor | `pub fn service<H: Handler>(handler: H, allow: $crate::Allowlist) -> $crate::ServiceBuilder<Request, Response>` | `macros.rs:700` |
| `svc::serve(node, handler, allow)` | the same, already spawned | `pub fn serve<H: Handler>(node: &$crate::Node, handler: H, allow: $crate::Allowlist) -> $crate::Result<$crate::Server>` | `macros.rs:764` |
| `Handle::service(allow)` | one **actor** as a singleton | `pub fn service(&self, allow: $crate::Allowlist) -> $crate::ServiceBuilder<Request, Response>` | `macros.rs:2138` |
| `Handle::serve(node, allow)` | the same, already spawned | `pub fn serve(&self, node: &$crate::Node, allow: $crate::Allowlist) -> $crate::Result<$crate::Server>` | `macros.rs:2233` |
| `svc::keyed_service(system, allow)` | a **registry** of keyed actors | `pub fn keyed_service(system: &System, allow: $crate::Allowlist) -> $crate::ServiceBuilder<$crate::Addressed<Request>, Response>` | `macros.rs:1202` |

`serve` on both paths is a two-line convenience.
`keylink/src/macros.rs:764-772`:

```rust
pub fn serve<H: Handler>(
    node: &$crate::Node,
    handler: H,
    allow: $crate::Allowlist,
) -> $crate::Result<$crate::Server> {
    $crate::Server::builder(node)
        .service(service(handler, allow).build())
        .spawn()
}
```

`keylink/src/macros.rs:2233-2241`:

```rust
pub fn serve(
    &self,
    node: &$crate::Node,
    allow: $crate::Allowlist,
) -> $crate::Result<$crate::Server> {
    $crate::Server::builder(node)
        .service(self.service(allow).build())
        .spawn()
}
```

There is **no** `keyed_serve`. A registry reaches a `Server` through
`Server::builder(node).service(keyed_service(&system, allow).build()).spawn()`.

### 3.2 The `send` refusal, and where it fires

`ServableAsActor::<A>::CHECK` is a compile-time assertion.
`keylink/src/macros.rs:642-657`:

```rust
struct ServableAsActor<A>(::core::marker::PhantomData<A>);

impl<A> ServableAsActor<A> {
    const CHECK: () = ::core::assert!(
        !DECLARES_SEND,
        "this service declares a `send` method, so it cannot be \
         served as an actor. .. Serve this with `service(handler, allow)` instead, \
         or move the upload to a second service. RFC-04 s10.4."
    );
}
```

Three sites instantiate it, so three doors refuse a `send` service at compile
time:

| Site | file:line |
|---|---|
| `Handle::service` | `macros.rs:2145` |
| `spawn` | `macros.rs:1383` |
| `spawn_wired` | `macros.rs:1468` |

`svc::service(handler, allow)` — the handler door — does **not** instantiate it,
so a `send` method is servable there.

`SpawnableUnfed::<A>::CHECK` (`macros.rs:671-685`) is the second assertion. It
refuses `spawn` for a service that declares a `feed`. `spawn` instantiates both
(`macros.rs:1383-1384`); `spawn_wired` instantiates only the first
(`macros.rs:1468`).

### 3.3 Mounting keyed actors

The keyed doors return `Mounted`, which is `keylink::Mounted<Mail>`
(`macros.rs:1158`).

`keylink/src/macros.rs:1368-1600`

```rust
pub fn spawn<A: Actor>(state: A) -> Mounted                              // :1368

pub fn spawn_wired<A: Actor>(
    state: A,
    observers: ::std::vec::Vec<::std::boxed::Box<dyn $crate::Observer<A>>>,
    feeds: Feeds,
) -> Mounted                                                             // :1463

pub fn spawn_supervised<A: Actor, N, Fut, F>(
    factory: N,
    limit: $crate::Intensity,
) -> Mounted
where
    N: ::core::ops::FnMut() -> Fut + ::core::marker::Send + 'static,
    Fut: ::core::future::Future<
        Output = ::core::result::Result<$crate::Incarnation<A, F>, $crate::Stop>,
    > + ::core::marker::Send,
    F: $crate::Feeds<A>,                                                 // :1560
```

The local twins of the same three are `actor`, `actor_with`, `actor_wired`,
`actor_deep`, `supervised` and `supervised_deep` (`macros.rs:1602`, `1630`,
`1667`, `1714`, `1765`, `1799`). They return `Handle<A>`, not `Mounted`.

`Mounted<M>` itself, `keylink/src/actor/registry.rs:353-361`:

```rust
#[must_use = "an unmounted actor is dropped, and the state it was built around \
              goes with it"]
pub struct Mounted<M> { .. }
```

`keylink/src/actor/registry.rs:441-448`:

```rust
#[doc(hidden)]
pub fn mount<M, F>(run: impl FnOnce(Mailbox<M>) -> F + Send + 'static) -> Mounted<M>
where
    F: Future<Output = Ended> + Send + 'static,
```

### 3.4 The server

`keylink/src/rpc.rs:1655-1812`

```rust
pub struct Service { .. }

impl Service {
    pub fn alpn(&self) -> &'static [u8]                                  // :1666
    pub fn foreign(alpn: &'static [u8], handler: impl ProtocolHandler) -> Self // :1697
}

#[must_use = "the server stops when dropped; hold it for as long as you serve"]
pub struct Server { .. }

impl Server {
    pub fn builder(node: &Node) -> ServerBuilder<'_>                     // :1726
    pub async fn shutdown(&self) -> Result<()>                           // :1738
    pub fn router(&self) -> &Router                                      // :1751
}

pub struct ServerBuilder<'a> { .. }

impl ServerBuilder<'_> {
    #[must_use]
    pub fn service(mut self, service: Service) -> Self                   // :1771
    pub fn spawn(self) -> Result<Server>                                 // :1780
}
```

`ServerBuilder::spawn` raises four startup errors, in this order
(`rpc.rs:1784-1805`): `ReservedAlpn`, `DuplicateAlpn`, `MissingBlobStore`,
`ConflictingBlobStores`. `RESERVED_ALPNS` holds exactly one entry
(`rpc.rs:1648`):

```rust
const RESERVED_ALPNS: &[&[u8]] = &[iroh_blobs::ALPN];
```

### 3.5 `ServiceBuilder<Req, Resp>`

`keylink/src/rpc.rs:1281-1622`

```rust
pub struct ServiceBuilder<Req, Resp> { .. }

impl<Req, Resp> ServiceBuilder<Req, Resp>
where
    Req: Rpc + DeserializeOwned + Send + Sync + 'static,
    Resp: Serialize + Granting + Send + Sync + 'static,
{
    #[doc(hidden)]
    pub fn new(alpn: &'static [u8], allow: Allowlist, dispatch: Dispatch<Req, Resp>) -> Self // :1310
    #[doc(hidden)]
    #[must_use]
    pub fn __keylink_declares_blobs(mut self, declares: bool) -> Self    // :1342
    pub fn with_blobs(mut self, blobs: &Blobs) -> Self                   // :1360
    pub fn admission(mut self, admission: Arc<dyn Admission>) -> Self    // :1382
    pub fn layer(mut self, layer: impl Middleware<Req, Resp>) -> Self    // :1390
    pub fn keyable(mut self, keyable: Keyable) -> Self                   // :1522
    pub fn records(mut self, records: Records) -> Self                   // :1565
    pub fn limits(mut self, limits: Limits) -> Self                      // :1599
    pub fn build(self) -> Service                                        // :1605
}
```

The defaults are written out at `rpc.rs:1328-1330`:

```rust
keyable: Keyable::Any,
records: Records::Memory,
limits: Limits::default(),
```

`build` wraps the layers around the base dispatch in reverse order
(`rpc.rs:1606-1609`), so the first `layer` call is the outermost layer.

`Dispatch`, `keylink/src/rpc.rs:89`:

```rust
pub type Dispatch<Req, Resp> =
    Arc<dyn Fn(Context, Req, BodyReader) -> BoxFut<(Outcome<Resp>, Body)> + Send + Sync>;
```

### 3.6 What a served call runs through

Singleton actor path — `keylink/src/turns.rs:4679`:

```rust
#[doc(hidden)]
pub async fn deliver_to_local<A, M, Resp>(
    actor: &LocalActor<A, M>,
    cx: Context,
    service: &'static str,
    mail: impl FnOnce(Origin, Reply<(Resp, Body)>) -> M,
) -> (Outcome<Resp>, Body)
where
    M: Send + 'static,
    Resp: Send + 'static,
```

Keyed path — `keylink/src/actor/remote.rs:425`:

```rust
#[doc(hidden)]
pub async fn deliver_to_actor<S: KeyedService>(
    system: &System<S::Mail>,
    cx: Context,
    request: Addressed<S::Request>,
    body: BodyReader,
) -> std::result::Result<(Outcome<S::Response>, Body), RemoteError>
```

The generated glue types:

`keylink/src/actor/remote.rs:102-133`

```rust
pub struct Addressed<T> { .. }

impl<T> Addressed<T> {
    pub fn new(actor: ActorId, message: T) -> Self                       // :115
    pub fn split(self) -> (ActorId, T)                                   // :123
}
```

`keylink/src/actor/remote.rs:313-335`

```rust
#[doc(hidden)]
pub trait KeyedService: Send + 'static {
    type Request: Rpc + Send + 'static;
    type Response: Send + 'static;
    type Mail: Send + 'static;

    fn mail(
        origin: Origin,
        request: Self::Request,
        body: BodyReader,
        reply: Reply<(Self::Response, Body)>,
    ) -> Self::Mail;
}
```

`keylink/src/actor/remote.rs:237-291`

```rust
#[doc(hidden)]
pub struct Origin(Provenance);

impl Origin {
    pub(crate) fn local() -> Self                                        // :267
    pub(crate) fn peer(cx: Context) -> Self                              // :273
    #[doc(hidden)]
    #[must_use]
    pub fn caller(&self) -> Caller<'_>                                   // :285
}
```

Both constructors are `pub(crate)`. A downstream crate cannot build an `Origin`.

### 3.7 Authorization at the door

`keylink/src/authz.rs:133-217`

```rust
pub enum Allowlist {
    Only(BTreeSet<EndpointId>),
    Anyone,
}

impl Allowlist {
    pub fn only<I: IntoIterator<Item = EndpointId>>(ids: I) -> Self      // :142
    pub fn anyone() -> Self                                              // :147
    pub fn parse(raw: &str) -> Result<Self>                              // :160
    pub fn from_env(var: &str) -> Result<Self>                           // :187
    pub fn admit(&self, peer: ProvenPeer) -> Option<AdmittedPeer>        // :202
}
```

`Allowlist` has no `Default` (`authz.rs:128-131`).

`keylink/src/authz.rs:49-123`

```rust
pub struct ProvenPeer(EndpointId);

impl ProvenPeer {
    pub fn on(connection: &Connection<HandshakeCompleted>) -> Self       // :58
    pub fn id(&self) -> EndpointId                                       // :64
}

pub struct AdmittedPeer(ProvenPeer);

impl AdmittedPeer {
    pub fn proven(&self) -> ProvenPeer                                   // :117
    pub fn id(&self) -> EndpointId                                       // :123
}
```

`keylink/src/admission.rs:92-199`

```rust
pub enum Admittance {
    Admitted,                                                            // :100
    Refused,                                                             // :102
    Unavailable(String),                                                 // :104
}

pub trait Admission: Send + Sync + 'static {
    #[must_use = "an admission decision that is computed and dropped is the \
                  failure this type exists to prevent"]
    fn admits(&self, peer: &ProvenPeer) -> Admittance;                   // :164

    fn admitted(&self, peer: &AdmittedPeer, live: Revocation) { .. }     // :171
}

pub trait Revocable: Admission {
    #[must_use = "a revocation that closed nothing is usually a mistake"]
    fn replace(&self, next: Allowlist) -> Vec<EndpointId>;               // :185
}

pub struct Revocation { .. }

impl Revocation {
    pub fn peer(&self) -> ProvenPeer                                     // :129
    pub fn close(&self)                                                  // :138
    pub fn watch(&self) -> Watch                                         // :145
}

pub struct DynamicAdmission { .. }

impl DynamicAdmission {
    pub fn new(initial: Allowlist) -> Self                               // :205
    pub fn live(&self) -> usize                                          // :217
}
```

`Allowlist` itself implements `Admission` (`admission.rs:286`).
`DynamicAdmission` implements both `Admission` (`:228`) and `Revocable` (`:260`).

---

## 4. `System<M>` and `SystemBuilder<M>`

`keylink/src/actor/registry.rs`. `M` is the mail type. A generated module aliases
it (`macros.rs:1151-1158`):

```rust
pub type System = $crate::System<Mail>;
pub type SystemBuilder = $crate::SystemBuilder<Mail>;
pub type Mounted = $crate::Mounted<Mail>;
#[doc(hidden)]
pub type Mailbox = $crate::Mailbox<Mail>;
```

### 4.1 `System<M>` — every public method

`keylink/src/actor/registry.rs:1431-1828`

| Signature | file:line |
|---|---|
| `#[must_use] pub fn builder(eviction: Eviction) -> SystemBuilder<M>` | `:1454` |
| `pub fn mount(&self, id: ActorId, actor: Mounted<M>) -> std::result::Result<(), MountError> where M: Send + 'static` | `:1486` |
| `pub async fn shutdown(&self, id: &ActorId, budget: Duration) -> Option<Ended>` | `:1662` |
| `pub async fn abort(&self, id: &ActorId) -> bool` | `:1686` |
| `pub fn contains(&self, id: &ActorId) -> bool` | `:1711` |
| `pub fn len(&self) -> usize` | `:1717` |
| `pub fn is_empty(&self) -> bool` | `:1723` |
| `pub fn status(&self) -> Vec<ActorStatus>` | `:1768` |
| `pub async fn evict_idle(&self)` | `:1825` |

`System<M>` implements `Clone` (`:1442`) and `Debug` (`:1830`). The clone shares
one `Arc<Registry<M>>` (`:1443-1447`).

There is **no** `System::get`, no `System::ids` and no per-actor handle getter.
`status()` is the only enumeration.

### 4.2 `SystemBuilder<M>` — every public method

`keylink/src/actor/registry.rs:1846-2170`

| Signature | file:line |
|---|---|
| `pub const DEFAULT_CAPACITY: usize = self::DEFAULT_CAPACITY;` (= `10_000`, `:1859`) | `:1875` |
| `pub fn spawn_with<F>(mut self, factory: F) -> Self where F: Fn(&ActorId) -> Option<Mounted<M>> + Send + Sync + 'static` | `:1888` |
| `pub fn capacity(mut self, actors: usize) -> Self` | `:1904` |
| `pub fn lifecycle(mut self, lifecycle: impl Lifecycle) -> Self` | `:1955` |
| `pub fn mailbox_depth(mut self, depth: NonZeroUsize) -> Self` | `:2138` |
| `pub fn build(self) -> System<M> where M: Send + 'static` | `:2150` |

Defaults set at `System::builder`, `registry.rs:1455-1461`:

```rust
SystemBuilder {
    capacity: DEFAULT_CAPACITY,       // 10_000  (:1859)
    mailbox_depth: DEFAULT_MAILBOX_DEPTH, // 1   (:82)
    eviction,
    factory: None,
    lifecycle: Vec::new(),
}
```

`lifecycle` appends to a `Vec<Arc<dyn Lifecycle>>` (`:1854`), so several
lifecycle observers can be installed.

### 4.3 The types `System` names

`keylink/src/actor/registry.rs:458-527`

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Eviction {
    Idle {
        after: Duration,               // :479
        shutdown_budget: Duration,     // :520
    },
    Never,                             // :527
}
```

`keylink/src/actor/registry.rs:536-552`

```rust
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
#[non_exhaustive]
pub enum MountError {
    #[error("an actor is already mounted as {id}")]
    Occupied { id: ActorId },
    #[error("the actor system is at its capacity of {capacity} live actors")]
    AtCapacity { capacity: usize },
}
```

`keylink/src/actor/registry.rs:1185-1214` — four fields:

```rust
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, postcard_schema::Schema, PartialEq, Eq)]
#[non_exhaustive]
pub struct ActorStatus {
    pub id: ActorId,
    pub idle_for: Duration,
    pub busy: bool,
    pub buffered: u64,
}
```

`keylink/src/actor/registry.rs:1314-1329`

```rust
pub trait Lifecycle: Send + Sync + 'static {
    fn mounted(&self, id: &ActorId);
    fn ended(&self, id: &ActorId, why: &Ended);
}
```

`keylink/src/actor/registry.rs:216-262`

```rust
pub struct Mailbox<M> { .. }

impl<M> Mailbox<M> {
    pub async fn recv(&mut self) -> Option<Box<M>>                       // :261
}
```

### 4.4 The private route error, and how it renders

`RouteError` is `pub(super)` (`registry.rs:567-580`). A peer never sees it
directly. `keylink/src/actor/remote.rs:349-356` converts it:

```rust
impl From<RouteError> for RemoteError {
    fn from(err: RouteError) -> Self {
        match &err {
            RouteError::NoSuchActor { id } => no_such_actor(id),
            RouteError::AtCapacity { .. } => Self::Failed(err.to_string()),
        }
    }
}
```

So a registry at capacity reaches a peer as `RemoteError::Failed` carrying the
whole sentence `"the actor system is at its capacity of {capacity} live actors"`,
under **no** prefix from the five in section 5.2.

---

## 5. The remote error types

### 5.1 `RemoteError` — five variants

`keylink/src/reply.rs:54-109`

```rust
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, postcard_schema::Schema, thiserror::Error)]
#[non_exhaustive]
pub enum RemoteError {
    #[error("unauthorized: {0}")]
    Unauthorized(String),                                                // :61

    #[error("timed out after {millis} ms")]
    TimedOut { millis: u64 },                                            // :65

    #[error("idempotency: {0}")]
    Idempotency(crate::IdempotencyRefusal),                              // :75

    #[error("no actor is mounted as {id}")]
    NoSuchActor { id: String },                                          // :98

    #[error("{0}")]
    Failed(String),                                                      // :108
}
```

A sixth variant, `ActorSchemaMismatch { ours, theirs }`, was removed. That
renumbered `Failed` from index 5 to index 4, which is wire-visible for **every**
service (`reply.rs:34-53`).

### 5.2 The five served failure prefixes

Associated constants on `RemoteError`, `keylink/src/reply.rs:148-207`:

| Constant | Value | Meaning | file:line |
|---|---|---|---|
| `RemoteError::PANICKED` | `"panicked: "` | the turn panicked; the actor kept its state | `reply.rs:154` |
| `RemoteError::REFUSED` | `"observer refused: "` | an observer vetoed the turn; the turn ran and was not committed | `reply.rs:161` |
| `RemoteError::OBSERVER_PANICKED` | `"observer panicked: "` | an observer panicked watching the turn | `reply.rs:181` |
| `RemoteError::RESTARTING` | `"restarting: "` | the incarnation that owed the answer is gone; another is coming. **Retry** | `reply.rs:189` |
| `RemoteError::DROPPED` | `"dropped: "` | the actor is serving and this one reply was never answered | `reply.rs:206` |

The property the constants carry is narrow, and `reply.rs:138-147` states it: no
one of the five is a prefix of another, and the claim covers **only** the prefixes
that `rendered` and `lost` build. `turns::tests::no_failure_prefix_is_a_prefix_of_another`
holds it. `reply.rs:332` pins the five literal values.

They are **not** part of the hashed contract. `postcard_schema` hashes variants
and fields, and associated constants are neither (`reply.rs:119-124`).

### 5.3 Where each prefix is written

`rendered` — `keylink/src/turns.rs:4538-4572`. It converts an `Answered` into an
`Outcome`:

| `Answered` arm | Result | file:line |
|---|---|---|
| `Ran((response, body))` | `Outcome::Ok(response)` | `turns.rs:4543` |
| `Faulted(panic)` | `Failed(format!("{PANICKED}{panic}"))` | `turns.rs:4547` |
| `Refused { name }` | `Failed(format!("{REFUSED}{name}"))` | `turns.rs:4554` |
| `ObserverPanicked { name }` | `Failed(format!("{OBSERVER_PANICKED}{name}"))` | `turns.rs:4562` |
| `Expired` | `RemoteError::TimedOut { millis }` | `turns.rs:4568` |

`lost` — `keylink/src/turns.rs:4616-4646`:

| `Standing` | Result | file:line |
|---|---|---|
| `Restarting(reason)` | `Failed(format!("{RESTARTING}{reason}"))` | `turns.rs:4618` |
| `Running` | `Failed(format!("{DROPPED}the actor is serving and this reply was never answered"))` | `turns.rs:4639` |
| `Gone` | `RemoteError::NoSuchActor { id }` | `turns.rs:4644` |

Three producers reach the `DROPPED` arm and the sentence names none of them
(`turns.rs:4619-4638`): a handler that took `reply.later()` and dropped the
`Deferred`; a handler that panicked after `reply.later()`; a handler that
panicked before consuming the channel.

### 5.4 `Error` — what a client method returns

`keylink/src/error.rs:17-178`

| Variant | `#[error(..)]` text | file:line |
|---|---|---|
| `Transport(#[from] Transport)` | `"transport: {0}"` | `:23` |
| `Codec(#[from] postcard::Error)` | `"codec: {0}"` | `:27` |
| `FrameTooLarge { len: u64, cap: u32 }` | `"header frame of {len} bytes exceeds the {cap} byte cap"` | `:33` |
| `FrameTruncated { got: usize, want: usize }` | `"stream ended {got} bytes into a {want} byte header frame"` | `:43` |
| `Closed` | `"peer closed the connection"` | `:52` |
| `Remote(#[from] crate::RemoteError)` | `"remote error: {0}"` | `:70` |
| `SchemaMismatch { ours: String }` | `"schema mismatch: the peer was built against a different contract (ours is {ours})"` | `:79` |
| `BlockingInAsync` | `"blocking keylink call from inside an async runtime - use the async API here"` | `:90` |
| `HandlerPanic(String)` | `"a protocol handler panicked: {0}"` | `:99` |
| `NotAByteStream` | `"the ReadableStream is locked to a reader, or it is not a byte stream"` | `:114` |
| `BodyTruncated { declared: u64, received: u64 }` | `"body ended after {received} of {declared} declared bytes"` | `:120` |
| `BodyOverrun { declared: u64 }` | `"body exceeded its declared length of {declared} bytes"` | `:129` |
| `BodyTooLarge { cap: usize }` | `"body exceeds the {cap} byte cap the caller allowed"` | `:136` |
| `UnexpectedResponse { expected: &'static str, actual: &'static str }` | `"expected a {expected} response, got {actual}"` | `:144` |
| `BlobRefused { id: crate::BlobId }` | `"the holder refused to serve blob {id}"` | `:166` |
| `Config(#[from] ConfigError)` | `"config: {0}"` | `:173` |
| `Io(#[from] std::io::Error)` | `"body io: {0}"` | `:177` |

`Error::Remote` does not narrow its inner value (`error.rs:56-68`). Every
`RemoteError` arrives through it, including a `Failed` under the `PANICKED`
prefix and a `TimedOut`.

`Transport`, `keylink/src/error.rs:184-205`, has six variants: `Bind`, `Connect`,
`Connection`, `Write`, `Read`, `StreamClosed`. `transport_source!`
(`error.rs:296-315`) generates the `From` impls so `?` reaches `Error` in one
step.

### 5.5 `ConfigError` — startup only

`keylink/src/error.rs:211-291`. Ten variants, every one raised at startup
(`error.rs:207-210`).

| Variant | Text | file:line |
|---|---|---|
| `MissingEnvVar { var }` | `"{var} is not set - refusing to start"` | `:216` |
| `MalformedSecretKey { var, source }` | `"{var} is not a valid secret key: {source}"` | `:224` |
| `MalformedEndpointId { entry, source }` | `"malformed endpoint id: {entry:?}"` | `:233` |
| `EmptyAllowlist` | `"allowlist is empty - refusing to serve a protocol nobody can reach"` | `:241` |
| `DuplicateAlpn(String)` | `"two services registered the same ALPN: {0}"` | `:248` |
| `ReservedAlpn(String)` | `"{0} is reserved - keylink mounts this protocol itself, with a policy"` | `:256` |
| `MissingBlobStore { service }` | `"service {service} declares blob methods but was given no store - add .with_blobs(..)"` | `:266` |
| `ConflictingBlobStores` | `"two services were given different blob stores - one node has one content store"` | `:279` |
| `MalformedKey { kind, reason }` | `"invalid {kind}: {reason}"` | `:285` |

### 5.6 The same condition, remote and local

| Condition | A remote caller reads | A local caller reads |
|---|---|---|
| the handler panicked, the actor survived | `Error::Remote(Failed("panicked: .."))`, `turns.rs:4547` | `TurnLost::Faulted(Panic)`, `observe.rs:710` |
| an observer refused the turn | `Failed("observer refused: {name}")`, `turns.rs:4554` | `TurnLost::Refused { name }`, `observe.rs:687` |
| an observer panicked | `Failed("observer panicked: {name}")`, `turns.rs:4562` | `TurnLost::ObserverPanicked { name }`, `observe.rs:705` |
| the reply was never answered | `Failed("dropped: the actor is serving and this reply was never answered")`, `turns.rs:4639` | `TurnLost::Dropped`, `observe.rs:671` |
| the incarnation is gone, a restart is granted | `Failed("restarting: {reason}")`, `turns.rs:4618` | `TurnLost::Restarting(Arc<Ended>)`, `observe.rs:684` |
| the actor is gone | `RemoteError::NoSuchActor { id }`, `turns.rs:4644` | `TurnLost::Gone`, `observe.rs:653` |
| a queue budget expired | `RemoteError::TimedOut { millis }`, `turns.rs:4568` | `TurnLost::Expired`, `observe.rs:719` |
| the call would deadlock | not reachable — a peer's call runs on a connection task | `TurnLost::Reentrant`, `observe.rs:741` |
| the registry is at capacity | `Failed("the actor system is at its capacity of {capacity} live actors")`, `remote.rs:353` | `MountError::AtCapacity { capacity }`, `registry.rs:547` |
| the actor ended, and why | nothing on the reply channel | `LocalActor::ended() -> impl Future<Output = Ended>`, `turns.rs:4194` |

The remote side collapses six distinct local variants into `Failed(String)` plus
a prefix. `TurnLost` is a typed enum; the prefixes are text a peer matches with
`starts_with`.

---

## 6. What crosses the wire

### 6.1 The two ALPNs a `service!` block pins

Every service pins **two** ALPNs, one per served shape.

| ALPN | Protocol string | Hashed type | file:line |
|---|---|---|---|
| `svc::alpn()` | `concat!(env!("CARGO_PKG_NAME"), "/", stringify!($svc))` | `Contract` | `macros.rs:496`, `:567` |
| `svc::keyed_alpn()` | `concat!(env!("CARGO_PKG_NAME"), "/", stringify!($svc), "/keyed")` | `KeyedContract` | `macros.rs:786`, `:814` |

`keylink/src/macros.rs:553-561`

```rust
#[derive(::postcard_schema::Schema)]
struct Contract {
    request: $crate::Envelope<$crate::WireRequest<Request>>,
    response: $crate::Envelope<$crate::WireResponse<Response>>,
    kinds: $crate::WireKinds,
}
```

`keylink/src/macros.rs:800-807`

```rust
#[derive(::postcard_schema::Schema)]
struct KeyedContract {
    request: $crate::Envelope<$crate::WireRequest<$crate::Addressed<Request>>>,
    response: $crate::Envelope<$crate::WireResponse<Response>>,
    kinds: $crate::WireKinds,
}
```

Both are computed once, in a `OnceLock` (`macros.rs:568-570`, `:815-818`).

### 6.2 What moves an ALPN

`keylink/src/node.rs:163-166`

```rust
pub fn derive_alpn<C: Schema>(protocol: &str) -> Vec<u8> {
    let hash = schema_hash::<C>(protocol);
    format!("{protocol}/{hash:016x}").into_bytes()
}
```

The hash walks the whole transitive type graph, so a field added three structs
deep still moves it (`node.rs:161-162`). `schema_hash` is `pub(crate)`
(`node.rs:149`).

| Change | Moves which ALPN | Why |
|---|---|---|
| a field or variant of the caller's `Request` or `Response` | that one service's two ALPNs | both `Contract`s name them |
| a `RemoteError` variant | **every** service in the fleet | `Envelope<WireResponse<Response>>` reaches `Outcome::Err(RemoteError)` (`reply.rs:48-53`) |
| a field of `Envelope<T>` | every service | `wire.rs:54-59` |
| a field of `WireKinds` | every service | `wire.rs:77-82` |
| a `RemoteError` associated **constant** | nothing | constants are not hashed (`reply.rs:119-124`) |

`WireKinds` carries values that do not matter to the hash. `postcard_schema`
hashes types, not values (`wire.rs:72-75`).

### 6.3 The envelope and the framing

`keylink/src/wire.rs:83-108`

```rust
#[derive(Debug, Serialize, Deserialize, Schema)]
pub struct WireKinds {
    pub(crate) uni: bool,
}

#[derive(Debug, Serialize, Deserialize, Schema)]
pub struct Envelope<T> {
    pub(crate) body_len: Option<u64>,
    pub(crate) idempotency: Option<crate::Idempotency>,
    pub(crate) message: T,
}
```

Every field of `Envelope` is `pub(crate)`. The type stays public because each
generated `Contract` names it (`wire.rs:91-94`).

`keylink/src/reply.rs:222-250`

```rust
#[derive(Debug, Clone, Serialize, Deserialize, postcard_schema::Schema)]
pub enum WireRequest<Req> {
    Call(Req),
    KeyState(IdempotencyKey),
}

#[derive(Debug, Clone, Serialize, Deserialize, postcard_schema::Schema)]
pub enum WireResponse<Resp> {
    Call(Outcome<Resp>),
    KeyState(KeyState),
}
```

`keylink/src/reply.rs:210-216`

```rust
#[derive(Debug, Clone, Serialize, Deserialize, postcard_schema::Schema)]
pub enum Outcome<T> {
    Ok(T),
    Err(RemoteError),
}
```

`Outcome::Err` carries no `T`, so its encoding is identical for every
instantiation (`reply.rs:282-292`). That is what lets a layer refuse a call
without knowing the method's return type.

### 6.4 Size limits

| Constant | Value | Bounds | file:line |
|---|---|---|---|
| `MAX_HEADER` | `1 << 20` (1 MiB) | one header frame, checked before a byte is buffered | `wire.rs:30` |
| `PREFIX` (`pub(crate)`) | `size_of::<u32>()` | the length prefix ahead of every frame | `wire.rs:42` |
| `READ_CHUNK` (private) | `64 * 1024` | one transport read | `wire.rs:47` |
| `MAX_KEY_LEN` | `128` | an `ActorId` and an `IdempotencyKey` | `keys.rs:26` |
| `MAX_GRANTS` | `4096` | the blob grant ledger | `blobs.rs:86` |
| `APPLIED_BOUND` | `4096` | an `Applied<T>` set | `call.rs:215` |
| `MAX_FOLLOWERS` | `128` | consoles parked on one node | `console.rs:479` |

Bodies are **not** subject to `MAX_HEADER`. They are streamed, never held, and
their size is a declared value the receiver can refuse before a byte moves
(`wire.rs:26-30`).

### 6.5 `Body` and `BodyReader`

`keylink/src/body.rs:32-113`

```rust
pub struct Body { .. }

impl Body {
    pub fn empty() -> Self                                               // :48
    pub fn from_bytes(bytes: impl Into<Bytes>) -> Self                   // :58
    pub async fn from_file(path: impl AsRef<Path>) -> Result<Self>       // :73
    pub fn from_reader<R: AsyncRead + Send + 'static>(reader: R, len: Option<u64>) -> Self // :88
    pub fn declared_len(&self) -> Option<u64>                            // :97
}
```

`keylink/src/body.rs:135-260`

```rust
pub struct BodyReader { .. }

impl BodyReader {
    pub fn declared_len(&self) -> Option<u64>                            // :156
    pub fn received(&self) -> u64                                        // :162
    pub async fn next_chunk(&mut self) -> Result<Option<Bytes>>          // :175
    pub async fn copy_to<W: AsyncWrite + Unpin>(mut self, sink: &mut W) -> Result<u64> // :204
    pub async fn read_to_bytes(mut self, cap: usize) -> Result<Bytes>    // :221
    pub async fn drain(mut self) -> Result<u64>                          // :240
}
```

Two wasm-only constructors exist: `Body::from_web_file` (`:304`) and
`Body::from_web_stream` (`:334`).

### 6.6 Blobs

`keylink/src/blobs.rs`

```rust
pub const MAX_GRANTS: usize = 4096;                                      // :86

pub struct BlobId([u8; 32]);                                             // :111
impl BlobId { pub fn as_bytes(&self) -> &[u8; 32] }                      // :116

pub enum Retention { .. }                                                // :344

pub struct Blobs { .. }                                                  // :406
impl Blobs {
    pub fn memory() -> Self                                              // :430
    pub async fn persistent(root: impl AsRef<std::path::Path>, keep: Retention) -> Result<Self> // :440
    pub async fn add_bytes(&self, data: impl Into<Bytes>) -> Result<BlobId> // :462
    pub fn grant(&self, peer: &AdmittedPeer, id: BlobId)                 // :483
    pub fn protocol(&self) -> BlobsAdmission                             // :494
}

pub trait Granting {                                                     // :539
    fn granted(&self) -> Option<BlobId>;
}

pub struct BlobsAdmission { .. }                                         // :552

pub struct BlobHandle { .. }                                             // :738
impl BlobHandle {
    pub fn new(node: &Node, holder: impl Into<EndpointAddr>, id: BlobId) -> Self // :751
    pub fn id(&self) -> BlobId                                           // :768
    pub async fn copy_to<W: AsyncWrite + Unpin>(self, sink: &mut W) -> Result<u64> // :783
    pub async fn fetch_into(self, store: &Blobs) -> Result<u64>          // :841
}
```

The generated code implements `Granting for Response`
(`macros.rs:601-608`), so `serve_one` reads the address off the typed reply and
records a grant before the reply is written. This holds on **both** the singleton
and the keyed path (`macros.rs:3769-3774`).

`DECLARES_BLOBS` is a generated `const` (`macros.rs:616-617`). All three service
builders pass it through `__keylink_declares_blobs` (`macros.rs:733`, `:1225`,
`:2183`), and `ServerBuilder::spawn` turns a missing store into
`ConfigError::MissingBlobStore` (`rpc.rs:1798-1801`).

### 6.7 `Middleware`

`keylink/src/middleware.rs:26-27`

```rust
pub type Next<Req, Resp> =
    Arc<dyn Fn(Context, Req, BodyReader) -> BoxFut<(Outcome<Resp>, Body)> + Send + Sync>;
```

`keylink/src/middleware.rs:581-591`

```rust
pub trait Middleware<Req, Resp>: Send + Sync + 'static {
    fn handle(
        &self,
        cx: Context,
        req: Req,
        body: BodyReader,
        next: Next<Req, Resp>,
    ) -> BoxFut<(Outcome<Resp>, Body)>;
}
```

`keylink/src/middleware.rs:31-37`

```rust
pub trait Rpc {
    const SERVICE: &'static str;
    fn method(&self) -> &'static str;
}
```

The generated `impl Rpc for Request` sets `SERVICE` to `stringify!($svc)` and
matches every variant for `method` (`macros.rs:687-695`).

Two layers ship with the crate:

```rust
pub struct QueueTimeout { budget: Duration }                             // middleware.rs:538
impl QueueTimeout { #[must_use] pub fn after(budget: Duration) -> Self }  // :545

pub struct Timeout { budget: Duration }                                  // middleware.rs:650
impl Timeout { pub fn after(budget: Duration) -> Self }                  // :656
```

`QueueTimeout` stamps its deadline when the call reaches that layer, so an outer
layer that took its time spends this budget rather than adding to it
(`middleware.rs:558-560`).

`Handle::service`'s rustdoc states what `Timeout` does on a serial actor
(`macros.rs:2101-2111`): `Timeout::after(100ms)` in front of a method that takes
ten seconds does not shed load. Ten callers read `TimedOut` at 100 ms, and the
actor still spends a hundred seconds producing replies nobody waits for.
`QueueTimeout` is the layer that sheds load there.

### 6.8 `Context` — what a layer and a handler can read

`keylink/src/middleware.rs:211-359`

```rust
pub struct Context { .. }

impl Context {
    pub fn queueing(&self) -> Option<Queueing>                           // :241
    pub fn progress(&self) -> Progress                                   // :254
    pub fn peer(&self) -> EndpointId                                     // :273
    pub fn service(&self) -> &'static str                                // :278
    pub fn method(&self) -> &'static str                                 // :290
    pub fn actor(&self) -> Option<&ActorId>                              // :299
    pub fn call(&self) -> Option<CallId>                                 // :357
}
```

```rust
pub struct Queueing { .. }                                               // :422
impl Queueing {
    pub fn deadline(self) -> Instant                                     // :431
    pub fn millis(self) -> u64                                           // :438
    pub fn expired(self) -> bool                                         // :482
}

pub struct Progress(Arc<AtomicU8>);                                      // :122
impl Progress {
    pub fn enqueued(&self)                                               // :157
    pub fn unrun(&self)                                                  // :167
    pub fn dispatched(&self)                                             // :173
    pub fn completed(&self)                                              // :179
}

#[derive(Debug, Clone, Copy)]
pub enum Caller<'a> {                                                    // :389
    Local,                                                               // :394
    Peer(&'a Context),                                                   // :396
}
impl<'a> Caller<'a> {
    #[must_use]
    pub fn peer(self) -> Option<&'a Context>                             // :406
}
```

`Context::actor()` returns `Some` only on the keyed path. `Caller` is how a
handler tells a local call from a served one.

### 6.9 Idempotency keys

`keylink/src/idempotency.rs:781-800`

```rust
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, postcard_schema::Schema)]
pub struct Idempotency {
    key: IdempotencyKey,
    attach_for_millis: Option<u64>,
}

impl Idempotency {
    #[must_use]
    pub fn new(key: IdempotencyKey) -> Self                              // :795
    pub fn attaching_for(key: IdempotencyKey, budget: Duration) -> Self   // :859
    pub fn key(&self) -> &IdempotencyKey                                 // :868
}
```

`From<IdempotencyKey> for Idempotency` exists (`idempotency.rs:950`), which is
why `with_idempotency` takes `impl Into<Idempotency>`.

`keylink/src/idempotency.rs:735-762`

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, postcard_schema::Schema)]
pub enum KeyState {
    Fresh,           // :746
    Running,         // :748
    Done,            // :750
    Interrupted,     // :752
    Unrecordable,    // :759
    Forgotten,       // :761
}
```

`keylink/src/idempotency.rs:319-362`

```rust
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, postcard_schema::Schema, thiserror::Error)]
#[non_exhaustive]
pub enum IdempotencyRefusal {
    #[error("the key is malformed: {0}")]
    Malformed(String),                                                   // :325
    #[error("another call with this key is in flight - ask again when it finishes")]
    InFlight,                                                            // :335
    #[error("a call with this key was interrupted - whether it took effect is unknown")]
    Interrupted,                                                         // :360
    #[error("a call with this key already ran, and this service does not hold its reply")]
    Unrecordable,                                                        // :387
    #[error("a call with this key already ran, and its reply is no longer held")]
    Forgotten,                                                           // :406
    #[error("too many calls with idempotency keys are in flight - the limit is {limit}")]
    TooManyRunning { limit: u32 },                                       // :429
    #[error("this service does not accept an idempotency key on {method}")]
    NotKeyable { method: String },                                       // :463
}
```

Seven variants. `NotKeyable.method` is a `String` rather than a `&'static str`,
because it crosses the wire inside `RemoteError` and has to `Deserialize`
(`idempotency.rs:459-462`).

`IdempotencyRefusal` converts into `RemoteError::Idempotency`
(`idempotency.rs:469`).

Serving-side policy:

```rust
#[derive(Debug, Clone)]
pub enum Keyable {                                                       // :566
    Any,                          // :569
    Only(&'static [&'static str]),// :571
    Except(&'static [&'static str]), // :581
    None,                         // :583
}

#[derive(Debug, Clone)]
pub enum Records {                                                       // :711
    Memory,   // :716
    None,     // :719
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Attaching {                                                     // :1662
    UpTo(Duration),  // :1669
    Forbidden,       // :1700
}
```

`Limits`, `keylink/src/idempotency.rs:1202-1600`, is the one type on the builder
list that has a `Default` (`:1237`). Ten setters:

| Method | file:line |
|---|---|
| `pub fn pool_records(mut self, records: NonZeroUsize) -> Self` | `:1291` |
| `pub fn frame_bytes(mut self, bytes: NonZeroUsize) -> Self` | `:1333` |
| `pub fn pool_bytes(mut self, bytes: NonZeroUsize) -> Self` | `:1367` |
| `pub fn tombstones(mut self, tombstones: NonZeroUsize) -> Self` | `:1398` |
| `pub fn running(mut self, running: NonZeroUsize) -> Self` | `:1425` |
| `pub fn running_per_peer(mut self, running: NonZeroUsize) -> Self` | `:1440` |
| `pub fn waiting(mut self, waiting: NonZeroUsize) -> Self` | `:1466` |
| `pub fn waiting_per_peer(mut self, waiting: NonZeroUsize) -> Self` | `:1489` |
| `pub fn ttl(mut self, ttl: Duration) -> Self` | `:1510` |
| `pub fn attach(mut self, attach: Attaching) -> Self` | `:1579` |

The matching public constants (`idempotency.rs:143-308`): `MAX_RECORDS = 1024`,
`MAX_RECORDED_BYTES = 1 << 20`, `MAX_RECORDED_FRAME = MAX_HEADER as usize`,
`MAX_TOMBSTONES = 8192`, `RECORD_TTL = 600 s`, `MAX_ATTACH = 30 s`,
`MAX_RUNNING_PER_PEER = 128`, `MAX_RUNNING = 4096`, `MAX_WAITING_PER_PEER = 128`,
`MAX_WAITING = 1024`.

### 6.10 `CallId` and `Applied<T>`

`keylink/src/call.rs:263-325`

```rust
#[derive(Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct CallId([u8; DIGEST_BYTES]);

impl CallId {
    pub(crate) fn of(peer: EndpointId, key: &IdempotencyKey) -> Self     // :293
    #[must_use] pub fn as_bytes(&self) -> &[u8; DIGEST_BYTES]            // :312
    #[must_use] pub fn from_bytes(bytes: [u8; DIGEST_BYTES]) -> Self     // :323
}
```

The digest is `blake3(peer.as_bytes() || key.as_str().as_bytes())`
(`call.rs:303-307`). Neither the service nor the method is folded in, and
`call.rs:273-292` gives the argument. `CallId` deliberately does **not** derive
`Schema`, so it cannot freeze an ALPN (`call.rs:255-259`).

`keylink/src/call.rs:435-557`

```rust
pub struct Applied<T> { .. }

impl<T> Applied<T> {
    pub fn new() -> Self                                                 // :446
    pub fn bounded(bound: NonZeroUsize) -> Self                          // :463
    pub fn get(&self, call: Option<CallId>) -> Option<&T>                // :476
    pub fn seen(&self, call: Option<CallId>) -> bool                     // :494
    pub fn insert(&mut self, call: Option<CallId>, answer: T)            // :507
    pub fn len(&self) -> usize                                           // :525
    pub fn is_empty(&self) -> bool                                       // :531
    pub fn bound(&self) -> NonZeroUsize                                  // :546
    pub fn iter(&self) -> impl Iterator<Item = (CallId, &T)>             // :557
}
```

`Context::call()` returns `None` for a method that `Keyable` excludes, so an
`Applied` in such a handler never fires and nothing reports that
(`middleware.rs:311-322`).

---

## 7. Remote-side lifecycle and observability

### 7.1 What a client can learn from the connection

`keylink/src/watch.rs:152-193`

```rust
#[must_use = "a watch does nothing unless you await it"]
pub struct Watch { .. }

impl Watch {
    pub fn on(connection: &Connection) -> Self                           // :170
}

impl Future for Watch {
    type Output = Departure;                                             // :178
}
```

`keylink/src/watch.rs:76-113`

```rust
#[derive(Debug)]
pub enum Departure {
    Closed,             // :87   the peer closed cleanly
    Local,              // :94   we closed it
    Lost(Transport),    // :112  the connection failed
}
```

`Departure::classify` (`watch.rs:123-141`) orders the arms deliberately.
`ConnectionError::LocallyClosed` maps to `Local` **before** the clean-close
guard runs. Moving that arm below the guard would report every local shutdown as
the peer walking away.

`watch()` is available on four types:

| Type | file:line |
|---|---|
| `RpcClient<Req, Resp>` | `watch.rs:201` |
| `IdempotentClient<Req, Resp>` | `watch.rs:212` |
| `svc::Client` | `macros.rs:2684` |
| `svc::IdempotentClient` | `macros.rs:2746` |
| `svc::KeyedClient` | `macros.rs:2498` |
| `Revocation` (serving side) | `admission.rs:145` |

`Watch` reports the **connection**, never an actor. A `Departure` cannot tell a
caller that one actor ended while the node stayed up.

### 7.2 What a client cannot learn

The generated singleton `Handle` carries `ended()`
(`keylink/src/macros.rs:2378-2381`):

```rust
pub fn ended(&self) -> impl ::core::future::Future<Output = $crate::Ended>
    + ::core::marker::Send + 'static {
    self.inner.ended()
}
```

Backed by `LocalActor::ended` (`keylink/src/turns.rs:4194`):

```rust
pub fn ended(&self) -> impl Future<Output = Ended> + Send + 'static {
```

No client type has `ended`. The macro states it twice: `macros.rs:1441` ("loop
runs turns with nothing dialled and no `KeyedClient` built") and
`macros.rs:1508` ("A keyed actor still has no `ended()`"). `reply.rs:177` puts
the consequence plainly for the remote reader: "A peer has no `ended` to fall
back on, so this prefix is the whole of what it learns."

`Ended`, `keylink/src/observe.rs:452-540`, has six variants:

```rust
#[derive(Debug, Clone)]
pub enum Ended {
    Stopped,                                                             // :455
    Vanished,                                                            // :458
    Panicked { message: String },                                        // :465
    Observer { name: &'static str, error: Arc<dyn Error + Send + Sync>, fatal: bool }, // :470
    Feed { name: &'static str },                                         // :502
    GaveUp { after: u32, error: Arc<dyn Error + Send + Sync> },           // :522
}
```

`TurnLost`, `keylink/src/observe.rs:647-742`, has eight variants: `Gone`,
`Dropped`, `Restarting(Arc<Ended>)`, `Refused { name }`,
`ObserverPanicked { name }`, `Faulted(Panic)`, `Expired`, `Reentrant`.

### 7.3 The local watcher on a keyed actor

`keylink/src/compose/keyed.rs:225-273`

```rust
pub struct Endings { .. }

impl Endings {
    pub fn new() -> Self                                                 // :233
    pub fn watch(&self, name: &ActorId) -> impl Future<Output = Ended> + Send + 'static // :271
}

impl Lifecycle for Endings { .. }                                        // :310
```

`Endings` is installed with `SystemBuilder::lifecycle` and runs **in the serving
process**. A remote caller cannot reach it.

### 7.4 The `Inspect` service — the one remote lifecycle channel

`keylink/src/console.rs:657-693`

```rust
crate::service! {
    service Inspect;

    rpc Actors() -> Vec<ActorStatus>;                                    // :666
    rpc Node() -> NodeStatus;                                            // :669

    #[defer]
    rpc Follow(after: u64) -> ::core::result::Result<EventPage, FollowRefused>; // :688

    feed Events(event: LifecycleEvent);                                  // :692
}
```

`Follow` is `#[defer]`, so a call with nothing to report is parked and its turn
ends. A console attached to an actor costs that actor nothing between events
(`console.rs:673-676`).

#### The client

`keylink/src/console.rs:944-1035`

```rust
pub struct Console {
    client: inspect::Client,
    at: u64,
}

impl Console {
    pub async fn connect(node: &Node, peer: impl Into<EndpointAddr>) -> Result<Self> // :968
    pub async fn actors(&self) -> Result<Vec<ActorStatus>>               // :980
    pub async fn node(&self) -> Result<NodeStatus>                       // :989
    pub async fn events(&mut self) -> Result<EventPage>                  // :1026
}
```

`Console::events` advances its own cursor (`console.rs:1032`) and converts a
`FollowRefused` into `crate::RemoteError::Failed(refused.to_string())`
(`console.rs:1031`).

#### The serving side

`keylink/src/console.rs:720-780`

```rust
pub struct Inspector { .. }

impl Inspector {
    #[must_use]
    pub fn new<M>(system: &System<M>, node: &Node, events: &Events, history: NonZeroUsize) -> Self
    where
        M: Send + 'static,                                               // :765
}

impl inspect::Actor for Inspector { .. }                                 // :845
```

`keylink/src/console.rs:505-583`

```rust
pub struct Events { .. }

impl Events {
    pub fn new(depth: NonZeroUsize) -> (Self, EventStream)                // :527
    pub fn record(&self, event: LifecycleEvent)                          // :542
    pub fn lost(&self) -> u64                                            // :558
    pub fn observer<A>(&self, id: ActorId) -> Box<dyn Observer<A>>       // :574
}

impl Lifecycle for Events { .. }                                         // :583

pub struct EventStream { .. }                                            // :641
impl Stream for EventStream { .. }                                       // :647
```

`Events` is both a `Lifecycle` (for the `System`) and an `Observer` factory (for
one actor). `Events::record` never blocks, so a burst deeper than the channel
loses its tail (`console.rs:310-315`).

#### What crosses

`keylink/src/console.rs:133-166`

```rust
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Schema)]
pub enum LifecycleEvent {
    Mounted { id: ActorId },                                             // :136
    Turn { id: ActorId, method: String },                                // :141
    Faulted { id: ActorId, method: String, message: String },            // :150
    Ended { id: ActorId, why: EndReason },                               // :160
}
```

`keylink/src/console.rs:183-228`

```rust
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Schema)]
pub enum EndReason {
    Stopped,                                                             // :186
    Vanished,                                                            // :188
    Panicked { message: String },                                        // :190
    Observer { name: String, error: String, fatal: bool },               // :196
    Feed { name: String },                                               // :210
    GaveUp { after: u32, error: String },                                // :220
}
```

`From<&Ended> for EndReason` (`console.rs:230`) is a variant-for-variant mapping.
`Ended::Observer` carries `Arc<dyn Error>` and `&'static str`; `EndReason::Observer`
carries two `String`s.

`keylink/src/console.rs:296-318`

```rust
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Schema)]
#[non_exhaustive]
pub struct NodeStatus {
    pub id: NodeKey,
    pub actors: u64,
    pub lost: u64,
}
```

`keylink/src/console.rs:340-376`

```rust
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Schema)]
#[non_exhaustive]
pub struct EventPage {
    pub next: u64,
    pub lost: u64,
    pub dropped: u64,
    pub events: Vec<LifecycleEvent>,
}
```

`EventPage::lost` and `NodeStatus::lost` count two different things.
`NodeStatus::lost` is the node's undelivered events for the life of the process.
`EventPage::lost` is what **this** console missed between its cursor and the
oldest event still held (`console.rs:307-317`, `:345-352`). `EventPage::dropped`
carries the same reading `NodeStatus::lost` returns, as a running total
(`console.rs:361-366`).

`keylink/src/console.rs:405-479`

```rust
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Schema, thiserror::Error)]
#[non_exhaustive]
pub enum FollowRefused {
    #[error("cursor {after} is ahead of this node's log, which is at {seq}: \
             either the cursor is not this node's, or this node has restarted")]
    Ahead { after: u64, seq: u64 },                                      // :430
    #[error("{parked} callers are already following this node, which is its limit")]
    Crowded { parked: u64 },                                             // :442
}

pub const MAX_FOLLOWERS: usize = 128;                                    // :479
```

`keylink/src/console.rs:274-282`

```rust
pub struct NodeKey(pub [u8; 32]);
impl From<&crate::EndpointId> for NodeKey { .. }
impl TryFrom<NodeKey> for crate::EndpointId { .. }
```

### 7.5 Remote against local, on lifecycle

| Question | A remote caller | A local caller |
|---|---|---|
| did the actor end, and why | only through a **separate** `Inspect` connection, as `LifecycleEvent::Ended { id, why: EndReason }` (`console.rs:160`) | `Handle::ended() -> impl Future<Output = Ended>` (`macros.rs:2378`) |
| did a keyed actor end | `LifecycleEvent::Ended` over `Inspect` | `Endings::watch(&ActorId) -> impl Future<Output = Ended>` (`compose/keyed.rs:271`) |
| is the actor running | not asked directly. `Console::actors()` lists what is mounted (`console.rs:980`) | `Handle::is_running() -> bool` (`macros.rs:2333`) |
| is the actor busy | `ActorStatus::busy` over `Inspect` (`registry.rs:1213`) | `Handle::is_busy() -> bool` (`macros.rs:2344`) |
| how deep is the queue | `ActorStatus::buffered` over `Inspect` (`registry.rs:1241`) | `Handle::buffered() -> u64` (`macros.rs:2354`) |
| did the connection end | `Watch` → `Departure` (`watch.rs:178`) | not applicable |
| a restart happened | `Failed("restarting: {reason}")` on the one call that lost its reply (`turns.rs:4618`) | `TurnLost::Restarting(Arc<Ended>)` (`observe.rs:684`) |
| read the state | not reachable | `Handle::with_state<T, F>(&self, read: F) -> Result<T, Undelivered>` (`macros.rs:2275`) |
| stop the actor | not reachable from a client | `Handle::stop(&self) -> Result<(), NotQueued>` (`macros.rs:2311`), `Handle::abort(&self)` (`macros.rs:2326`), `System::shutdown` (`registry.rs:1662`), `System::abort` (`registry.rs:1686`) |

`Console::actors()` is stale on arrival, and `console.rs:662-664` says so: an id
read there may have been evicted before the reply arrived.

---

## 8. Flags

Facts only. The evaluation is not mine.

**8.1 A remote capability with no local counterpart.** Idempotency is served-side
only. `Client::with_idempotency`, `KeyedClient::with_idempotency`,
`key_state`, `Applied<T>` and the whole `Limits` surface exist on the wire path.
The local `Handle` has no key, no ledger and no `key_state`. `CallId::of` takes an
`EndpointId` (`call.rs:293`), so a local call cannot produce one, and
`Context::call()` answers `None` there.

**8.2 A second one.** `Watch`/`Departure` has no local twin at all. A local caller
never learns that "the channel to the actor closed" as a separate fact from a
turn's own outcome.

**8.3 The asymmetry in what a caller can learn.** A local caller reads a typed
eight-variant `TurnLost`. A remote caller reads `Failed(String)` plus one of five
text prefixes for five of those conditions, and `NoSuchActor` for a sixth. The
prefixes are not in the hashed contract (`reply.rs:119-124`), so a peer built
against an older keylink meets a moved prefix as a `Failed` it silently stops
recognising. `reply.rs:320-325` names that as the more dangerous kind of break
and the reason the pin at `reply.rs:332` exists.

**8.4 A remote caller cannot learn that the actor ended.** The only channel is a
second service on a second ALPN — `Inspect` — with its own connection, its own
allowlist and its own `Events` wiring. Nothing on `svc::Client` or
`svc::KeyedClient` reaches it.

**8.5 Three names for the class of thing that reports a lost turn.** `TurnLost`
(`observe.rs:647`), `Undelivered` (`turns.rs:3167`) and `NotQueued`
(`turns.rs:3353`) are three enums over one fact, chosen by which local door was
used. The wire has one: `RemoteError`.

**8.6 `keyed_service` has no `keyed_serve`.** `service`/`serve` are a pair
(`macros.rs:700`, `:764`), `Handle::service`/`Handle::serve` are a pair
(`macros.rs:2138`, `:2233`), and `keyed_service` (`macros.rs:1202`) stands alone.
A registry always spells out `Server::builder(..).service(..).spawn()`.

**8.7 `with_idempotency` returns two different shapes.** On `Client` it returns a
distinct `IdempotentClient` type (`macros.rs:2653`). On `KeyedClient` it returns
`Self` (`macros.rs:2473`). The `tell` method is therefore absent from
`IdempotentClient` by construction, while a keyed client never had one to lose.

**8.8 `at` is a free function, not a method.** Reaching one keyed actor reads
`svc::at(&client, id)` (`macros.rs:2550`), not `client.at(id)`. The singleton
path has no equivalent free function.

**8.9 The `send` refusal fires at three of four doors.** `Handle::service`
(`macros.rs:2145`), `spawn` (`macros.rs:1383`) and `spawn_wired`
(`macros.rs:1468`) instantiate `ServableAsActor::<A>::CHECK`. The handler door
`svc::service(handler, allow)` (`macros.rs:700`) does not, and `svc::serve` goes
through it. The same service is therefore refused as an actor and accepted as a
handler.

**8.10 The registry capacity refusal carries no prefix.** It reaches a peer as
`Failed("the actor system is at its capacity of {capacity} live actors")`
(`remote.rs:353`), a whole sentence with none of the five prefixes.
`reply.rs:138-147` names this and two other producers of an unprefixed `Failed`:
a malformed `ActorId` from a peer, and a console subscription refusal.

**8.11 Two `lost` counters with one name.** `NodeStatus::lost` and
`EventPage::lost` count different things (`console.rs:307-317`, `:345-352`), and
`EventPage::dropped` is the field that carries `NodeStatus::lost`'s reading. The
rustdoc at `console.rs:314-317` states the confusion it is guarding against.

**8.12 `Mail` and `Feed` are `#[doc(hidden)] pub`.** `macros.rs:862` and
`macros.rs:992`. For keylink's own `Inspect` service, `lib.rs:294-312` narrows
the re-export to five documented names plus a `#[doc(hidden)]` group. For a
caller's own service, the whole ~30-item module stays public in their crate.

**8.13 `MAX_FOLLOWERS` is a hard refusal, not a queue.** A `Follow` arriving with
128 callers already parked reads `FollowRefused::Crowded { parked }`
(`console.rs:442`) rather than waiting. `Console::events` flattens that to
`RemoteError::Failed(refused.to_string())` (`console.rs:1031`), so a `Console`
user loses the typed distinction between `Ahead` and `Crowded`.
