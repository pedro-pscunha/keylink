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
/// # The four kinds
///
/// `send` and `recv` are written from the *client's* point of view, and they
/// change only the signatures — the wire types are identical, because a body
/// travels beside the message rather than inside it. `blob` is the one that
/// changes the wire: a content address IS the message, so it rides in the
/// response.
///
/// | kind   | handler                                  | client                                |
/// |--------|------------------------------------------|---------------------------------------|
/// | `rpc`  | `async fn f(&self, a: A) -> R`           | `f(a) -> R`                           |
/// | `send` | `async fn f(&self, a: A, b: BodyReader) -> R` | `f(a, body: Body) -> R`          |
/// | `recv` | `async fn f(&self, a: A) -> (R, Body)`   | `f(a) -> (R, BodyReader)`             |
/// | `blob` | `async fn f(&self, a: A) -> (R, BlobId)` | `f(a) -> (R, BlobHandle)`             |
///
/// ## Picking between `recv` and `blob`
///
/// **Does the payload exist as bytes before the call?** If no, use `recv` —
/// blobs must hash content before serving it, so it cannot stream something
/// being generated. **Will the same payload go to more than one peer, or more
/// than once?** If yes, use `blob` and get deduplication, resume and content
/// addressing. If no, use `recv` and pay for none of it.
///
/// A service with a `blob` method must be given a store —
/// `service(..).with_blobs(&blobs)` — or [`ServerBuilder::spawn`](crate::ServerBuilder::spawn)
/// refuses it at startup. Returning the [`BlobId`](crate::BlobId) is what
/// grants it to the caller: no other peer can fetch it, and nothing else in the
/// store becomes reachable.
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
                $( $rpc($crate::__keylink_response_ty!([<$kind>], $ret)) ),+
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
                    fn [<into_ $rpc:snake>](self)
                        -> $crate::Result<$crate::__keylink_response_ty!([<$kind>], $ret)> {
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
                /// keylink's own wire surface, so a peer built against a
                /// keylink that cannot accept a `tell` is refused instead of
                /// losing every one silently. See `WireKinds`.
                kinds: $crate::WireKinds,
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

            /// Whether this reply hands out a content address, and which.
            ///
            /// Read by `serve_one`, which is the only place that holds BOTH the
            /// store and the [`AdmittedPeer`](keylink::AdmittedPeer) the grant
            /// must be recorded against. The handler has neither and needs
            /// neither: returning the id is the whole of its part.
            impl $crate::Granting for Response {
                fn granted(&self) -> ::core::option::Option<$crate::BlobId> {
                    match self {
                        $( Self::$rpc(__value) =>
                            $crate::__keylink_granted_id!([<$kind>], __value), )+
                    }
                }
            }

            /// Whether any method here serves content, folded at compile time.
            ///
            /// `ServerBuilder::spawn` reads it to refuse a service that
            /// declares blob methods and carries no store, which is why
            /// `ConfigError::MissingBlobStore` is a check that can fire rather
            /// than a flag that is always false.
            const DECLARES_BLOBS: bool =
                false $( | $crate::__keylink_is_blob!([<$kind>]) )+;

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
                    .__keylink_declares_blobs(DECLARES_BLOBS)
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

            impl $crate::Dial for Client {
                async fn dial(
                    node: &$crate::Node,
                    addr: $crate::EndpointAddr,
                ) -> $crate::Result<Self> {
                    Self::connect(node, addr).await
                }
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
                // No `#[must_use]`: `Watch` already carries one, and repeating it
                // here is `clippy::double_must_use`. Only visible since the
                // library expands this macro itself — clippy suppresses lints
                // from an external macro, so no downstream expansion reports it.
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

            // The doc comment lives INSIDE the wrap: attached to the macro
            // invocation instead, it documents nothing and is
            // `unused_doc_comments`.
            $crate::__keylink_if_blocking! {
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
        }
    };
}

/// Emit the enclosed items only when keylink itself was built with `blocking`.
///
/// A `macro_rules!` body is inert tokens until it is invoked, so `service!`
/// could name `$crate::blocking::Runtime` while that module did not exist and
/// `cargo build -p keylink` stayed green. The failure appeared only in a
/// DOWNSTREAM crate that expanded the macro without the feature — which for a
/// browser build is every consumer, since `mise run wasm` builds
/// `--no-default-features`.
///
/// Two definitions rather than a `#[cfg]` inside one, because the decision
/// belongs to keylink's own feature set at the point keylink is compiled, not
/// to the caller's.
#[cfg(feature = "blocking")]
#[macro_export]
#[doc(hidden)]
macro_rules! __keylink_if_blocking {
    ($($item:tt)*) => { $($item)* };
}

#[cfg(not(feature = "blocking"))]
#[macro_export]
#[doc(hidden)]
macro_rules! __keylink_if_blocking {
    ($($item:tt)*) => {};
}

/// What one `Response` variant carries, which is the only place the four kinds
/// disagree about the wire.
///
/// `rpc`, `send` and `recv` put the declared return type in and nothing else: a
/// body travels beside the message rather than inside it. `blob` puts the
/// content address in beside the reply, because a content address IS the
/// message — there is no second channel for it to ride on.
///
/// RFC-01 §3.4 rejected the alternative, which was a `BlobId` field on
/// `Envelope`: every unary rpc would pay a byte for a field it never sets, and
/// the generated `Response` would stop showing which methods hand out content.
///
/// A `macro_rules!` call is legal in type position. Checked rather than
/// assumed, because a `derive` receives the item as TOKENS and therefore sees
/// this invocation rather than a type: `Serialize`, `Deserialize` and
/// `postcard_schema::Schema` all expand correctly over it.
#[doc(hidden)]
#[macro_export]
macro_rules! __keylink_response_ty {
    (rpc, $ret:ty) => {
        $ret
    };
    (tell, $ret:ty) => {
        $ret
    };
    (send, $ret:ty) => {
        $ret
    };
    (recv, $ret:ty) => {
        $ret
    };
    (blob, $ret:ty) => {
        ($ret, $crate::BlobId)
    };
}

/// The content address one `Response` variant hands out, if any.
///
/// The three non-blob arms discard the value they were given and answer
/// `None` — they have no address to report, and this is where that fact is
/// stated once instead of at every reading site.
#[doc(hidden)]
#[macro_export]
macro_rules! __keylink_granted_id {
    (rpc, $value:expr) => {{
        let _ = $value;
        ::core::option::Option::None
    }};
    (tell, $value:expr) => {{
        let _ = $value;
        ::core::option::Option::None
    }};
    (send, $value:expr) => {{
        let _ = $value;
        ::core::option::Option::None
    }};
    (recv, $value:expr) => {{
        let _ = $value;
        ::core::option::Option::None
    }};
    (blob, $value:expr) => {
        ::core::option::Option::Some($value.1)
    };
}

/// Whether one kind needs a content store, folded into `DECLARES_BLOBS`.
///
/// A `const`, so "this service serves content" is answered at compile time and
/// `ServerBuilder::spawn` can refuse a service that declares blob methods and
/// carries no store. Without it, `ConfigError::MissingBlobStore` would be a
/// check whose flag is always false.
#[doc(hidden)]
#[macro_export]
macro_rules! __keylink_is_blob {
    (rpc) => {
        false
    };
    (tell) => {
        false
    };
    (send) => {
        false
    };
    (recv) => {
        false
    };
    (blob) => {
        true
    };
}

/// One blocking client method. Streaming kinds produce nothing.
#[doc(hidden)]
#[macro_export]
macro_rules! __keylink_blocking_method {
    ([$( #[$meta:meta] )*], tell, $name:ident, ($( $arg:ident : $ty:ty ),*), $ret:ty) => {
        $( #[$meta] )*
        #[doc = concat!("Send the `", stringify!($name), "` tell and return.")]
        ///
        /// Unlike `send` and `recv`, a blocking `tell` is buildable: there is no
        /// stream to hand back and no body to buffer, so the synchronous wrapper
        /// is the whole operation rather than half of one.
        pub fn $name(&self $(, $arg: $ty )*) -> $crate::Result<()> {
            self.runtime.run(self.inner.$name($( $arg ),*))?
        }
    };
    ([$( #[$meta:meta] )*], rpc, $name:ident, ($( $arg:ident : $ty:ty ),*), $ret:ty) => {
        $( #[$meta] )*
        #[doc = concat!("Call `", stringify!($name), "`, blocking until it answers.")]
        pub fn $name(&self $(, $arg: $ty )*) -> $crate::Result<$ret> {
            self.runtime.run(self.inner.$name($( $arg ),*))?
        }
    };
    ([$( #[$meta:meta] )*], send, $name:ident, ($( $arg:ident : $ty:ty ),*), $ret:ty) => {};
    ([$( #[$meta:meta] )*], recv, $name:ident, ($( $arg:ident : $ty:ty ),*), $ret:ty) => {};
    // Absent for the same reason `send` and `recv` are: a `BlobHandle` fetches
    // asynchronously, so a synchronous wrapper would either buffer the whole
    // blob or hand back a handle that only works inside a runtime.
    ([$( #[$meta:meta] )*], blob, $name:ident, ($( $arg:ident : $ty:ty ),*), $ret:ty) => {};
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
    ([$( #[$meta:meta] )*], tell, $name:ident, ($( $arg:ident : $ty:ty ),*), $ret:ty) => {
        $( #[$meta] )*
        #[doc = concat!("Handle the `", stringify!($name), "` tell. Nothing is sent back.")]
        ///
        /// The caller has already returned. Whatever this produces is dropped,
        /// including a panic, which is caught and costs one message.
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
    ([$( #[$meta:meta] )*], blob, $name:ident, ($( $arg:ident : $ty:ty ),*), $ret:ty) => {
        $( #[$meta] )*
        #[doc = concat!("Answer the `", stringify!($name), "` rpc, returning the address of content this node already holds.")]
        ///
        /// Returning the [`BlobId`](keylink::BlobId) IS the grant: the
        /// generated dispatch records it against the peer that asked, so the
        /// handler names no ledger and there is no call to forget. Nothing else
        /// in the store becomes reachable.
        fn $name(&self $(, $arg: $ty )*)
            -> impl ::core::future::Future<Output = ($ret, $crate::BlobId)> + Send;
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
    // Identical to `rpc`: the dispatch chain is typed `($resp, Body)` for every
    // kind, so a tell still builds a response. `serve_one_tell` drops it —
    // there is no stream to write it to.
    (tell, $fname:ident, $handler:ident, $resp:ident, $variant:ident, $method:ident,
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
    // The pair goes into the response WHOLE, and the grant is recorded from
    // there rather than here. `serve_one` is the only place that holds both the
    // `AdmittedPeer` and the store, and it reads the id back off this response
    // through `Granting`.
    (blob, $fname:ident, $handler:ident, $resp:ident, $variant:ident, $method:ident,
        ($( $arg:ident : $ty:ty ),*)) => {
        async fn $fname<H: $handler>(
            handler: &H,
            _body: $crate::BodyReader
            $(, $arg: $ty )*
        ) -> ($resp, $crate::Body) {
            let (value, id) = handler.$method($( $arg ),*).await;
            ($resp::$variant((value, id)), $crate::Body::empty())
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
    ([$( #[$meta:meta] )*], tell, $name:ident, ($( $arg:ident : $ty:ty ),*), $ret:ty) => {
        $( #[$meta] )*
        #[doc = concat!("Handle the `", stringify!($name), "` tell. Nothing is sent back.")]
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
    ([$( #[$meta:meta] )*], blob, $name:ident, ($( $arg:ident : $ty:ty ),*), $ret:ty) => {
        $( #[$meta] )*
        #[doc = concat!("Answer the `", stringify!($name), "` rpc, returning the address of content this node already holds.")]
        ///
        /// Nothing is streamed during the turn: the address is the whole reply,
        /// and the bytes move later on the blobs connection. So
        /// [`abort`](keylink::System::abort) has nothing here to truncate.
        fn $name(&mut self $(, $arg: $ty )*)
            -> impl ::core::future::Future<Output = ($ret, $crate::BlobId)> + Send;
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
    // The twin of the `Handler` arm above, on purpose. If these two diverge,
    // that is the bug RFC-01 §11.2b calls "build the path twice".
    (tell, $fname:ident, $actor:ident, $resp:ident, $variant:ident, $method:ident,
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
    // RFC-01 §11.2b calls this the "build the blob path twice" risk: there are
    // TWO generators, and a kind added to one and not the other compiles
    // everywhere except the service that uses it. Identical to the `Handler`
    // twin above on purpose — if these two ever diverge, that is the bug.
    (blob, $fname:ident, $actor:ident, $resp:ident, $variant:ident, $method:ident,
        ($( $arg:ident : $ty:ty ),*)) => {
        async fn $fname<A: $actor>(
            state: &mut A,
            _body: $crate::BodyReader
            $(, $arg: $ty )*
        ) -> ($resp, $crate::Body) {
            let (value, id) = state.$method($( $arg ),*).await;
            ($resp::$variant((value, id)), $crate::Body::empty())
        }
    };
}

/// One method on the typed actor handle, shaped by the rpc kind.
#[doc(hidden)]
#[macro_export]
macro_rules! __keylink_actor_client_method {
    // No actor handle method for `tell`, the same way `blob` has none.
    //
    // A `tell` is delivered on a unidirectional stream, and the actor system
    // reaches its registry over `ActorClient::call`, which is bidirectional by
    // construction — the reply is what carries the schema-mismatch refusal.
    // Emitting a method that silently used the bidirectional path would give a
    // `tell` a reply channel it is documented not to have.
    //
    // Absent and documented, rather than present and lying. A caller who wants
    // fire-and-forget to an actor writes an `rpc` returning `()` and drops the
    // result, which is honest about the round trip it pays for.
    ([$( #[$meta:meta] )*], tell, $name:ident, $req:ident, $resp:ident, $variant:ident, $extract:ident,
        ($( $arg:ident : $ty:ty ),*), $ret:ty) => {};
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
    // Deliberately absent, the same way `__keylink_blocking_method!` omits
    // `send` and `recv` — and for a reason of the same shape.
    //
    // `serve_one` records a grant by reading the address off the reply through
    // `Granting`. On the actor path that reply is an `ActorReply`: an opaque
    // payload the system never decodes, which is precisely the property RFC-01
    // §11.2a wanted, since it means a `BlobId` cannot arrive under a mismatched
    // schema. The cost is that the system cannot see the address either, so no
    // grant is recorded and any handle built here would fetch content nobody
    // was granted.
    //
    // Emitting a method that fails at runtime, or a `compile_error!` that would
    // fire for every service declaring a `blob` method whether or not it is
    // mounted as an actor, are both worse than emitting nothing. Wiring it
    // needs the store inside the actor's own dispatch, which is RFC-04's to
    // reconcile (§11.2b, §11.2c).
    ([$( #[$meta:meta] )*], blob, $name:ident, $req:ident, $resp:ident, $variant:ident, $extract:ident,
        ($( $arg:ident : $ty:ty ),*), $ret:ty) => {};
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
    ([$( #[$meta:meta] )*], tell, $name:ident, $req:ident, $variant:ident, $extract:ident,
        ($( $arg:ident : $ty:ty ),*), $ret:ty) => {
        $( #[$meta] )*
        #[doc = concat!("Send the `", stringify!($variant), "` tell and return.")]
        ///
        /// Returns `Result<()>` rather than the method's declared type: there
        /// is no reply stream, so there is nothing to decode. The `Ok` means
        /// the bytes reached the transport and nothing more — see
        /// [`RpcClient::tell`](keylink::RpcClient::tell) for what that does and
        /// does not promise.
        pub async fn $name(&self $(, $arg: $ty )*) -> $crate::Result<()> {
            self.inner.tell($req::$variant { $( $arg ),* }).await
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
    ([$( #[$meta:meta] )*], blob, $name:ident, $req:ident, $variant:ident, $extract:ident,
        ($( $arg:ident : $ty:ty ),*), $ret:ty) => {
        $( #[$meta] )*
        #[doc = concat!("Call the `", stringify!($variant), "` rpc, returning its reply and a handle to the content it named.")]
        ///
        /// The call site names no ALPN, no second dial and no hash: a
        /// [`BlobHandle`](keylink::BlobHandle) is a content address plus the
        /// node that holds it, and it opens the blobs connection itself.
        pub async fn $name(&self $(, $arg: $ty )*)
            -> $crate::Result<($ret, $crate::BlobHandle)> {
            let (response, _body) = self
                .inner
                .call($req::$variant { $( $arg ),* }, $crate::Body::empty())
                .await?;
            let (value, id) = response.$extract()?;
            Ok((value, self.inner.blob_handle(id)))
        }
    };
}

/// One expansion of [`service!`], inside the library's own test build.
///
/// keylink never expands its own macro anywhere else, and that is exactly why
/// the `__keylink_if_blocking!` guard above was missing for so long. A
/// `macro_rules!` body is inert tokens until it is invoked, so the crate
/// compiled green while the code it *generates* named `$crate::blocking`, a
/// module that does not exist without the `blocking` feature. The failure was
/// reachable only from a downstream crate — which, for a browser build, is
/// every consumer, because `mise run wasm` builds `--no-default-features`.
///
/// Expanding it here puts the generated code into the library's test build, so
/// `cargo test -p keylink --lib --no-run` compiles it. `mise run features` runs
/// that for all eight feature subsets, which is what turns the guard from a
/// thing we believe into a thing the gate checks.
///
/// Nothing here is called. Compiling is the whole assertion.
#[cfg(test)]
mod expansion {
    crate::service! {
        service Expansion;

        rpc Ping(n: u64) -> u64;
        recv Fetch(name: String) -> u64;
        send Store(name: String) -> u64;
        // The fourth kind belongs here for the same reason the other three do.
        // It also expands the ACTOR twin, which is where RFC-01 §11.2b's
        // "build the blob path twice" risk lives: a kind added to one generator
        // and not the other compiles until somebody declares it.
        blob Manual(sku: String) -> u64;
    }
}
