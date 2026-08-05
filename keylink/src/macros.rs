/// Declare a service. Generates the wire types, a handler trait, and a client
/// whose methods look like ordinary async functions.
///
/// ```ignore
/// keylink::service! {
///     service Inventory;
///
///     rpc  Ping() -> String;                       // request in, response out
///     rpc  GetItem(id: u64) -> Option<Item>;
///     send Upload(name: String) -> Receipt;        // client also streams bytes up
///     recv Fetch(id: u64) -> FetchInfo;            // server also streams bytes down
/// }
///
/// // server:  impl inventory::Handler for MyState { ... }
/// // client:  let item = client.get_item(7).await?;
/// //          let receipt = client.upload(name, Body::from_file(p).await?).await?;
/// //          let (info, body) = client.fetch(7).await?;
/// ```
///
/// # The three kinds
///
/// `send` and `recv` are written from the *client's* point of view, and they
/// change only the signatures — the wire types are identical, because a body
/// travels beside the message rather than inside it.
///
/// | kind   | handler                                  | client                                |
/// |--------|------------------------------------------|---------------------------------------|
/// | `rpc`  | `async fn f(&self, a: A) -> R`           | `f(a) -> R`                           |
/// | `send` | `async fn f(&self, a: A, b: BodyReader) -> R` | `f(a, body: Body) -> R`          |
/// | `recv` | `async fn f(&self, a: A) -> (R, Body)`   | `f(a) -> (R, BodyReader)`             |
///
/// RPC names are written in `CamelCase` (they become enum variants); the
/// generated methods are `snake_case`.
///
/// The calling crate needs `serde`, `postcard-schema` and `tokio` as
/// dependencies — the generated code derives against them by path.
#[macro_export]
macro_rules! service {
    (
        service $svc:ident;
        $( $( #[$meta:meta] )* $kind:ident $rpc:ident ( $( $arg:ident : $ty:ty ),* $(,)? ) -> $ret:ty; )+
    ) => {
        $crate::pastey::paste! {
        pub mod [<$svc:snake>] {
            #![allow(unused_imports, dead_code, missing_docs)]
            use super::*;

            /// Namespaced by crate, so two crates can define `Inventory` without
            /// their endpoints colliding.
            const PROTOCOL: &str = concat!(env!("CARGO_PKG_NAME"), "/", stringify!($svc));

            #[derive(Debug, Clone, ::serde::Serialize, ::serde::Deserialize, ::postcard_schema::Schema)]
            pub enum Request {
                $( $( #[$meta] )* $rpc { $( $arg: $ty ),* } ),+
            }

            #[derive(Debug, Clone, ::serde::Serialize, ::serde::Deserialize, ::postcard_schema::Schema)]
            pub enum Response {
                $( $rpc($ret) ),+
            }

            impl Response {
                /// Which rpc this reply answers. Reported when a hand-rolled
                /// peer answers the wrong question, so the error names both
                /// sides instead of printing a debug dump.
                fn variant_name(&self) -> &'static str {
                    match self {
                        $( Self::$rpc(_) => stringify!($rpc) ),+
                    }
                }

                $(
                    /// Extract the reply to one rpc, or say what came instead.
                    ///
                    /// Unreachable against a peer running this same schema —
                    /// which the ALPN gate guarantees. Kept so a hand-rolled
                    /// peer gets an error, not a panic.
                    fn [<into_ $rpc:snake>](self) -> $crate::Result<$ret> {
                        match self {
                            Self::$rpc(value) => Ok(value),
                            other => Err($crate::Error::UnexpectedResponse {
                                expected: stringify!($rpc),
                                actual: other.variant_name(),
                            }),
                        }
                    }
                )+
            }

            /// Hashed to produce the ALPN, so the gate covers BOTH directions
            /// *and* keylink's own framing.
            ///
            /// The response field MUST name `WireResponse`, the same alias the
            /// wire names. Spelling the bare `Response` here is what let the
            /// contract and the wire drift apart once already.
            #[derive(::postcard_schema::Schema)]
            struct Contract {
                request: $crate::Envelope<Request>,
                response: $crate::Envelope<$crate::WireResponse<Response>>,
            }

            /// `<crate>/<Service>/<16-hex schema hash>`.
            ///
            /// Derived, never hand-maintained: change any type reachable from
            /// `Request` or `Response` and this moves on its own.
            pub fn alpn() -> &'static [u8] {
                static CELL: ::std::sync::OnceLock<::std::vec::Vec<u8>> =
                    ::std::sync::OnceLock::new();
                CELL.get_or_init(|| $crate::derive_alpn::<Contract>(PROTOCOL)).as_slice()
            }

            /// Implement this to serve the protocol. One method per rpc.
            pub trait Handler: Send + Sync + 'static {
                $(
                    $crate::__keylink_handler_method!(
                        [$( #[$meta] )*],
                        [<$kind>], [<$rpc:snake>], ($( $arg : $ty ),*), $ret
                    );
                )+
            }

            $(
                $crate::__keylink_invoke_fn!(
                    [<$kind>],
                    [<__invoke_ $rpc:snake>],
                    Handler,
                    Response,
                    $rpc,
                    [<$rpc:snake>],
                    ($( $arg : $ty ),*)
                );
            )+

            impl $crate::Rpc for Request {
                const SERVICE: &'static str = stringify!($svc);

                fn method(&self) -> &'static str {
                    match self {
                        $( Request::$rpc { .. } => stringify!($rpc), )+
                    }
                }
            }

            /// Build this service so it can be registered on a
            /// [`Server`](keylink::Server), alongside other services or foreign
            /// iroh protocols, on one node.
            pub fn service<H: Handler>(
                handler: H,
                allow: $crate::Allowlist,
            ) -> $crate::ServiceBuilder<Request, Response> {
                let handler = ::std::sync::Arc::new(handler);
                let dispatch: $crate::Dispatch<Request, Response> =
                    ::std::sync::Arc::new(move |_cx: $crate::Context,
                                                req: Request,
                                                body: $crate::BodyReader| {
                        let handler = handler.clone();
                        ::std::boxed::Box::pin(async move {
                            let (__response, __body) = match req {
                                $(
                                    Request::$rpc { $( $arg ),* } =>
                                        [<__invoke_ $rpc:snake>](
                                            handler.as_ref(), body $(, $arg )*
                                        ).await,
                                )+
                            };
                            ($crate::Outcome::Ok(__response), __body)
                        })
                    });
                $crate::ServiceBuilder::new(alpn(), allow, dispatch)
            }

            /// Serve this service alone on `node`.
            ///
            /// For several services on one endpoint, build each with
            /// [`service`] and register them on a single
            /// [`Server`](keylink::Server): one router must own every ALPN.
            pub fn serve<H: Handler>(
                node: &$crate::Node,
                handler: H,
                allow: $crate::Allowlist,
            ) -> $crate::Result<$crate::Server> {
                $crate::Server::builder(node)
                    .service(service(handler, allow).build())
                    .spawn()
            }

            // ───────────────────────────────────────────── the actor twin

            /// What the sealed payload carries in each direction: the bare
            /// `Request` up, the bare `Response` down.
            ///
            /// A refusal is deliberately not in here. It travels in the actor
            /// system's own `Outcome`, one layer out, where the system's ALPN
            /// already covers it.
            #[derive(::postcard_schema::Schema)]
            #[allow(dead_code)]
            struct ActorContract {
                request: Request,
                response: Response,
            }

            /// The schema hash a caller declares and a registry compares
            /// against, for actors of this service.
            ///
            /// Derived from the same type graph as [`alpn`], under the same
            /// protocol name. Change any type reachable from `Request` or
            /// `Response` and this moves on its own, so a peer built against
            /// the old shapes is refused with
            /// [`RemoteError::ActorSchemaMismatch`](keylink::RemoteError)
            /// before its payload is decoded.
            pub fn schema() -> u64 {
                static CELL: ::std::sync::OnceLock<u64> = ::std::sync::OnceLock::new();
                *CELL.get_or_init(|| $crate::actor_schema::<ActorContract>(PROTOCOL))
            }

            /// Serve this service as an actor: `&mut self`, one message at a
            /// time, state owned by a task rather than shared behind a lock.
            ///
            /// # Messages are handled in the order they ARRIVE
            ///
            /// Read this before you rely on the order of anything.
            ///
            /// Each call takes its own QUIC stream, and QUIC orders bytes
            /// within a stream, not across streams. So two messages sent to
            /// one actor in sequence can arrive in either order, and the actor
            /// then handles them one at a time, serially, in the order they
            /// turned up — which may not be the order they were sent.
            ///
            /// **A serial mailbox gives you serialization, not sequencing.**
            /// The two are easy to conflate and the difference is expensive:
            /// an order book with price-time priority, fed by a client that
            /// sends two `Submit`s in a row, gets the right answer to the
            /// wrong question and reports nothing.
            ///
            /// Arrival order is the truth, and the actor stamps sequence as it
            /// processes. If your domain needs SEND order, you own that:
            ///
            /// - await each reply before sending the next, which is exact and
            ///   costs one round trip per message, or
            /// - carry your own sequence number in the message and buffer on
            ///   it, where you choose what happens to a gap that never fills.
            ///
            /// # Send + 'static, and NOT Sync
            ///
            /// A [`Handler`] is shared across concurrent tasks behind an `Arc`
            /// and must be `Sync`; an actor is owned by one task and never
            /// shared, so state holding an `Rc`, a `RefCell` or a `!Sync`
            /// client can be an actor and cannot be a `Handler`.
            pub trait Actor: Send + 'static {
                /// The proven identity of the peer whose message comes next.
                ///
                /// Called immediately before each message and never
                /// concurrently with one, because the mailbox is serial. Stash
                /// what you need in your own state. The default does nothing.
                ///
                /// A concurrent [`Handler`] could not have this: two calls
                /// would race on the stashed value.
                fn caller(&mut self, _cx: &$crate::Context) {}

                $(
                    $crate::__keylink_actor_method!(
                        [$( #[$meta] )*],
                        [<$kind>], [<$rpc:snake>], ($( $arg : $ty ),*), $ret
                    );
                )+
            }

            $(
                $crate::__keylink_actor_invoke_fn!(
                    [<$kind>],
                    [<__actor_ $rpc:snake>],
                    Actor,
                    Response,
                    $rpc,
                    [<$rpc:snake>],
                    ($( $arg : $ty ),*)
                );
            )+

            /// Spawn one actor of this service, ready to be mounted.
            ///
            /// Hand it to [`System::mount`](keylink::System::mount) under an
            /// id, or return it from
            /// [`spawn_with`](keylink::SystemBuilder::spawn_with) to have it
            /// built on the first message to an unmounted id.
            ///
            /// A panicking message costs that message, not the actor: the turn
            /// is caught and reported as a refusal, and the next message runs
            /// against the state as it stands.
            ///
            /// # Panics
            ///
            /// Panics when called outside a tokio runtime.
            pub fn spawn<A: Actor>(mut state: A) -> $crate::Mounted {
                $crate::mount(schema(), move |mut inbox: $crate::Mailbox| async move {
                    while let ::std::option::Option::Some(mail) = inbox.recv().await {
                        let $crate::Mail { cx, payload, body, reply } = *mail;
                        state.caller(&cx);
                        // The ONLY route from the payload's bytes to a typed
                        // message, and it compares the hashes first.
                        let __answer = match payload.open::<Request>(schema()) {
                            ::std::result::Result::Ok(__request) => {
                                let __turn = $crate::guarded(async {
                                    match __request {
                                        $(
                                            Request::$rpc { $( $arg ),* } =>
                                                [<__actor_ $rpc:snake>](
                                                    &mut state, body $(, $arg )*
                                                ).await,
                                        )+
                                    }
                                })
                                .await;
                                match __turn {
                                    ::std::result::Result::Ok((__response, __body)) => {
                                        match $crate::ActorReply::encode(&__response) {
                                            ::std::result::Result::Ok(__encoded) =>
                                                ($crate::Outcome::Ok(__encoded), __body),
                                            ::std::result::Result::Err(__refusal) => (
                                                $crate::Outcome::Err(__refusal),
                                                $crate::Body::empty(),
                                            ),
                                        }
                                    }
                                    ::std::result::Result::Err(__panic) => (
                                        $crate::Outcome::Err($crate::RemoteError::Failed(
                                            __panic.to_string(),
                                        )),
                                        $crate::Body::empty(),
                                    ),
                                }
                            }
                            ::std::result::Result::Err(__refusal) => (
                                $crate::Outcome::Err(__refusal),
                                $crate::Body::empty(),
                            ),
                        };
                        // The caller may have gone away mid-turn. Its stream is
                        // dead either way, so there is nothing to report.
                        let _ = reply.send(__answer);
                    }
                })
            }

            /// Address one actor of this service on a connected node.
            ///
            /// The handle borrows the connection, so every actor on that node
            /// is reached over one connection with one congestion-control
            /// state — which is what a per-actor ALPN would have thrown away.
            pub fn at(
                client: &$crate::ActorClient,
                actor: $crate::ActorId,
            ) -> ActorRef<'_> {
                ActorRef { client, actor }
            }

            /// A typed handle to one actor. Its methods are the ones written in
            /// `service!`, and they name no transport concept and no actor
            /// concept beyond the id.
            ///
            /// # Two calls in a row can arrive in either order
            ///
            /// Every call takes its own QUIC stream, and QUIC orders bytes
            /// within a stream, not across them. The actor handles messages one
            /// at a time in the order they ARRIVE, which is not necessarily the
            /// order you sent them.
            ///
            /// To send two messages in a defined order, await the first reply
            /// before sending the second. Anything else is your own sequence
            /// number inside your own message. See [`Actor`].
            #[derive(Debug)]
            pub struct ActorRef<'a> {
                client: &'a $crate::ActorClient,
                actor: $crate::ActorId,
            }

            impl ActorRef<'_> {
                /// Which actor this handle addresses.
                pub fn id(&self) -> &$crate::ActorId {
                    &self.actor
                }

                $(
                    $crate::__keylink_actor_client_method!(
                        [$( #[$meta] )*],
                        [<$kind>],
                        [<$rpc:snake>],
                        Request,
                        Response,
                        $rpc,
                        [<into_ $rpc:snake>],
                        ($( $arg : $ty ),*),
                        $ret
                    );
                )+
            }

            /// A connected client. Every method is an ordinary async call, and
            /// they run concurrently: each takes its own QUIC stream.
            pub struct Client {
                inner: $crate::RpcClient<Request, Response>,
            }

            impl Client {
                /// Dial a peer by public key.
                pub async fn connect(
                    node: &$crate::Node,
                    peer: impl Into<$crate::EndpointAddr>,
                ) -> $crate::Result<Self> {
                    Ok(Self {
                        inner: $crate::RpcClient::connect(node, peer, alpn()).await?,
                    })
                }

                /// The same connection, with an idempotency key on every call
                /// made through the handle it returns.
                ///
                /// The key is a transport concern rather than a domain one, so
                /// it goes here instead of into every method signature. See
                /// [`RpcClient::with_idempotency`](keylink::RpcClient::with_idempotency)
                /// for why the handle is meant for one call.
                #[must_use]
                pub fn with_idempotency(&self, key: $crate::IdempotencyKey) -> Self {
                    Self {
                        inner: self.inner.with_idempotency(key),
                    }
                }

                /// Await this peer's departure instead of meeting it on the
                /// next call.
                ///
                /// See [`Watch`](keylink::Watch).
                #[must_use]
                pub fn watch(&self) -> $crate::Watch {
                    self.inner.watch()
                }

                $(
                    $crate::__keylink_client_method!(
                        [$( #[$meta] )*],
                        [<$kind>],
                        [<$rpc:snake>],
                        Request,
                        $rpc,
                        [<into_ $rpc:snake>],
                        ($( $arg : $ty ),*),
                        $ret
                    );
                )+

                /// Close the connection gracefully.
                pub async fn close(self) -> $crate::Result<()> {
                    self.inner.close().await
                }
            }

            /// The same service, for programs that are not async.
            ///
            /// Streaming methods (`send`/`recv`) are deliberately absent: their
            /// bodies are async by nature, and a synchronous wrapper would
            /// either buffer a whole file or hand back a handle that only works
            /// inside a runtime.
            pub mod blocking {
                use super::*;

                /// A connected client whose methods are ordinary blocking calls.
                pub struct Client {
                    inner: super::Client,
                    runtime: ::std::sync::Arc<$crate::blocking::Runtime>,
                }

                impl Client {
                    /// Dial a peer by public key.
                    pub fn connect(
                        node: &$crate::blocking::Node,
                        peer: impl Into<$crate::EndpointAddr>,
                    ) -> $crate::Result<Self> {
                        let runtime = ::std::sync::Arc::clone(node.runtime());
                        let inner = $crate::blocking::connect_with(
                            node,
                            peer,
                            |node, addr| async move { super::Client::connect(&node, addr).await },
                        )?;
                        Ok(Self { inner, runtime })
                    }

                    /// The same connection, with an idempotency key on every
                    /// call made through the handle it returns.
                    ///
                    /// See the async twin,
                    /// [`Client::with_idempotency`](super::Client::with_idempotency).
                    #[must_use]
                    pub fn with_idempotency(&self, key: $crate::IdempotencyKey) -> Self {
                        Self {
                            inner: self.inner.with_idempotency(key),
                            runtime: ::std::sync::Arc::clone(&self.runtime),
                        }
                    }

                    $(
                        $crate::__keylink_blocking_method!(
                            [$( #[$meta] )*],
                            [<$kind>],
                            [<$rpc:snake>],
                            ($( $arg : $ty ),*),
                            $ret
                        );
                    )+

                    /// Close the connection gracefully.
                    pub fn close(self) -> $crate::Result<()> {
                        self.runtime.run(self.inner.close())?
                    }
                }
            }
        }
        }
    };
}

/// One blocking client method. Streaming kinds produce nothing.
#[doc(hidden)]
#[macro_export]
macro_rules! __keylink_blocking_method {
    ([$( #[$meta:meta] )*], rpc, $name:ident, ($( $arg:ident : $ty:ty ),*), $ret:ty) => {
        $( #[$meta] )*
        #[doc = concat!("Call `", stringify!($name), "`, blocking until it answers.")]
        pub fn $name(&self $(, $arg: $ty )*) -> $crate::Result<$ret> {
            self.runtime.run(self.inner.$name($( $arg ),*))?
        }
    };
    ([$( #[$meta:meta] )*], send, $name:ident, ($( $arg:ident : $ty:ty ),*), $ret:ty) => {};
    ([$( #[$meta:meta] )*], recv, $name:ident, ($( $arg:ident : $ty:ty ),*), $ret:ty) => {};
}

/// One handler-trait method, shaped by the rpc kind.
#[doc(hidden)]
#[macro_export]
macro_rules! __keylink_handler_method {
    ([$( #[$meta:meta] )*], rpc, $name:ident, ($( $arg:ident : $ty:ty ),*), $ret:ty) => {
        $( #[$meta] )*
        #[doc = concat!("Answer the `", stringify!($name), "` rpc.")]
        fn $name(&self $(, $arg: $ty )*)
            -> impl ::core::future::Future<Output = $ret> + Send;
    };
    ([$( #[$meta:meta] )*], send, $name:ident, ($( $arg:ident : $ty:ty ),*), $ret:ty) => {
        $( #[$meta] )*
        #[doc = concat!("Answer the `", stringify!($name), "` rpc, reading the payload the caller streams up.")]
        fn $name(&self $(, $arg: $ty )*, body: $crate::BodyReader)
            -> impl ::core::future::Future<Output = $ret> + Send;
    };
    ([$( #[$meta:meta] )*], recv, $name:ident, ($( $arg:ident : $ty:ty ),*), $ret:ty) => {
        $( #[$meta] )*
        #[doc = concat!("Answer the `", stringify!($name), "` rpc, returning a payload to stream back.")]
        fn $name(&self $(, $arg: $ty )*)
            -> impl ::core::future::Future<Output = ($ret, $crate::Body)> + Send;
    };
}

/// The bridge from one `Request` variant to one handler method.
///
/// A free function per rpc, so the dispatch `match` stays uniform across kinds:
/// a macro cannot expand to a match arm, but it can expand to an item.
#[doc(hidden)]
#[macro_export]
macro_rules! __keylink_invoke_fn {
    (rpc, $fname:ident, $handler:ident, $resp:ident, $variant:ident, $method:ident,
        ($( $arg:ident : $ty:ty ),*)) => {
        async fn $fname<H: $handler>(
            handler: &H,
            _body: $crate::BodyReader
            $(, $arg: $ty )*
        ) -> ($resp, $crate::Body) {
            (
                $resp::$variant(handler.$method($( $arg ),*).await),
                $crate::Body::empty(),
            )
        }
    };
    (send, $fname:ident, $handler:ident, $resp:ident, $variant:ident, $method:ident,
        ($( $arg:ident : $ty:ty ),*)) => {
        async fn $fname<H: $handler>(
            handler: &H,
            body: $crate::BodyReader
            $(, $arg: $ty )*
        ) -> ($resp, $crate::Body) {
            (
                $resp::$variant(handler.$method($( $arg, )* body).await),
                $crate::Body::empty(),
            )
        }
    };
    (recv, $fname:ident, $handler:ident, $resp:ident, $variant:ident, $method:ident,
        ($( $arg:ident : $ty:ty ),*)) => {
        async fn $fname<H: $handler>(
            handler: &H,
            _body: $crate::BodyReader
            $(, $arg: $ty )*
        ) -> ($resp, $crate::Body) {
            let (value, body) = handler.$method($( $arg ),*).await;
            ($resp::$variant(value), body)
        }
    };
}

/// One `Actor`-trait method, shaped by the rpc kind. `&mut self`, because the
/// task owns the state instead of sharing it.
#[doc(hidden)]
#[macro_export]
macro_rules! __keylink_actor_method {
    ([$( #[$meta:meta] )*], rpc, $name:ident, ($( $arg:ident : $ty:ty ),*), $ret:ty) => {
        $( #[$meta] )*
        #[doc = concat!("Answer the `", stringify!($name), "` rpc.")]
        fn $name(&mut self $(, $arg: $ty )*)
            -> impl ::core::future::Future<Output = $ret> + Send;
    };
    ([$( #[$meta:meta] )*], send, $name:ident, ($( $arg:ident : $ty:ty ),*), $ret:ty) => {
        $( #[$meta] )*
        #[doc = concat!("Answer the `", stringify!($name), "` rpc, reading the payload the caller streams up.")]
        ///
        /// The body is read inside the turn, so
        /// [`abort`](keylink::System::abort) can truncate it.
        fn $name(&mut self $(, $arg: $ty )*, body: $crate::BodyReader)
            -> impl ::core::future::Future<Output = $ret> + Send;
    };
    ([$( #[$meta:meta] )*], recv, $name:ident, ($( $arg:ident : $ty:ty ),*), $ret:ty) => {
        $( #[$meta] )*
        #[doc = concat!("Answer the `", stringify!($name), "` rpc, returning a payload to stream back.")]
        ///
        /// The body is drained by the connection after the turn ends, so
        /// [`abort`](keylink::System::abort) cannot truncate it.
        fn $name(&mut self $(, $arg: $ty )*)
            -> impl ::core::future::Future<Output = ($ret, $crate::Body)> + Send;
    };
}

/// The bridge from one `Request` variant to one `Actor` method.
///
/// A free function per rpc for the same reason as its `Handler` twin: a macro
/// cannot expand to a match arm, but it can expand to an item.
#[doc(hidden)]
#[macro_export]
macro_rules! __keylink_actor_invoke_fn {
    (rpc, $fname:ident, $actor:ident, $resp:ident, $variant:ident, $method:ident,
        ($( $arg:ident : $ty:ty ),*)) => {
        async fn $fname<A: $actor>(
            state: &mut A,
            _body: $crate::BodyReader
            $(, $arg: $ty )*
        ) -> ($resp, $crate::Body) {
            (
                $resp::$variant(state.$method($( $arg ),*).await),
                $crate::Body::empty(),
            )
        }
    };
    (send, $fname:ident, $actor:ident, $resp:ident, $variant:ident, $method:ident,
        ($( $arg:ident : $ty:ty ),*)) => {
        async fn $fname<A: $actor>(
            state: &mut A,
            body: $crate::BodyReader
            $(, $arg: $ty )*
        ) -> ($resp, $crate::Body) {
            (
                $resp::$variant(state.$method($( $arg, )* body).await),
                $crate::Body::empty(),
            )
        }
    };
    (recv, $fname:ident, $actor:ident, $resp:ident, $variant:ident, $method:ident,
        ($( $arg:ident : $ty:ty ),*)) => {
        async fn $fname<A: $actor>(
            state: &mut A,
            _body: $crate::BodyReader
            $(, $arg: $ty )*
        ) -> ($resp, $crate::Body) {
            let (value, body) = state.$method($( $arg ),*).await;
            ($resp::$variant(value), body)
        }
    };
}

/// One method on the typed actor handle, shaped by the rpc kind.
#[doc(hidden)]
#[macro_export]
macro_rules! __keylink_actor_client_method {
    ([$( #[$meta:meta] )*], rpc, $name:ident, $req:ident, $resp:ident, $variant:ident, $extract:ident,
        ($( $arg:ident : $ty:ty ),*), $ret:ty) => {
        $( #[$meta] )*
        #[doc = concat!("Call the `", stringify!($variant), "` rpc on this actor.")]
        pub async fn $name(&self $(, $arg: $ty )*) -> $crate::Result<$ret> {
            let (response, _body) = self
                .client
                .call::<$req, $resp>(
                    &self.actor,
                    schema(),
                    &$req::$variant { $( $arg ),* },
                    $crate::Body::empty(),
                )
                .await?;
            response.$extract()
        }
    };
    ([$( #[$meta:meta] )*], send, $name:ident, $req:ident, $resp:ident, $variant:ident, $extract:ident,
        ($( $arg:ident : $ty:ty ),*), $ret:ty) => {
        $( #[$meta] )*
        #[doc = concat!("Call the `", stringify!($variant), "` rpc on this actor, streaming `body` up alongside it.")]
        pub async fn $name(&self $(, $arg: $ty )*, body: $crate::Body) -> $crate::Result<$ret> {
            let (response, _body) = self
                .client
                .call::<$req, $resp>(
                    &self.actor,
                    schema(),
                    &$req::$variant { $( $arg ),* },
                    body,
                )
                .await?;
            response.$extract()
        }
    };
    ([$( #[$meta:meta] )*], recv, $name:ident, $req:ident, $resp:ident, $variant:ident, $extract:ident,
        ($( $arg:ident : $ty:ty ),*), $ret:ty) => {
        $( #[$meta] )*
        #[doc = concat!("Call the `", stringify!($variant), "` rpc on this actor, returning its reply and a reader for the payload streaming back.")]
        pub async fn $name(&self $(, $arg: $ty )*)
            -> $crate::Result<($ret, $crate::BodyReader)> {
            let (response, body) = self
                .client
                .call::<$req, $resp>(
                    &self.actor,
                    schema(),
                    &$req::$variant { $( $arg ),* },
                    $crate::Body::empty(),
                )
                .await?;
            Ok((response.$extract()?, body))
        }
    };
}

/// One client method, shaped by the rpc kind.
#[doc(hidden)]
#[macro_export]
macro_rules! __keylink_client_method {
    ([$( #[$meta:meta] )*], rpc, $name:ident, $req:ident, $variant:ident, $extract:ident,
        ($( $arg:ident : $ty:ty ),*), $ret:ty) => {
        $( #[$meta] )*
        #[doc = concat!("Call the `", stringify!($variant), "` rpc.")]
        pub async fn $name(&self $(, $arg: $ty )*) -> $crate::Result<$ret> {
            let (response, _body) = self
                .inner
                .call($req::$variant { $( $arg ),* }, $crate::Body::empty())
                .await?;
            response.$extract()
        }
    };
    ([$( #[$meta:meta] )*], send, $name:ident, $req:ident, $variant:ident, $extract:ident,
        ($( $arg:ident : $ty:ty ),*), $ret:ty) => {
        $( #[$meta] )*
        #[doc = concat!("Call the `", stringify!($variant), "` rpc, streaming `body` up alongside it.")]
        pub async fn $name(&self $(, $arg: $ty )*, body: $crate::Body) -> $crate::Result<$ret> {
            let (response, _body) = self
                .inner
                .call($req::$variant { $( $arg ),* }, body)
                .await?;
            response.$extract()
        }
    };
    ([$( #[$meta:meta] )*], recv, $name:ident, $req:ident, $variant:ident, $extract:ident,
        ($( $arg:ident : $ty:ty ),*), $ret:ty) => {
        $( #[$meta] )*
        #[doc = concat!("Call the `", stringify!($variant), "` rpc, returning its reply and a reader for the payload streaming back.")]
        pub async fn $name(&self $(, $arg: $ty )*)
            -> $crate::Result<($ret, $crate::BodyReader)> {
            let (response, body) = self
                .inner
                .call($req::$variant { $( $arg ),* }, $crate::Body::empty())
                .await?;
            Ok((response.$extract()?, body))
        }
    };
}
