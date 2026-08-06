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
/// ## A `send` method cannot be served as an actor
///
/// A `send` handler reads its upload **inside** the turn — RFC-04 §10.4 — and no
/// layer can bound that, because a layer refuses the caller without ending the
/// turn. So a peer that declares a large body and then stops writing would hold
/// the actor until iroh's connection idle timeout, with nothing else answered,
/// including the local `Handle`.
///
/// That is refused at compile time, at the site that serves the actor:
///
/// ```compile_fail
/// use keylink::{Allowlist, BodyReader, Caller};
///
/// keylink::service! {
///     service Depot;
///     send Ingest(name: String) -> u64;
/// }
///
/// struct Store;
/// impl depot::Actor for Store {
///     async fn ingest(&mut self, _cx: Caller<'_>, _n: String, _b: BodyReader) -> u64 { 0 }
/// }
///
/// let actor = depot::actor(Store);
/// let _ = actor.service(Allowlist::anyone());   // <- this line does not compile
/// ```
///
/// Serving the same service the concurrent way is fine, which is what makes the
/// refusal above a statement about the actor path rather than about `send`:
///
/// ```no_run
/// use keylink::{Allowlist, BodyReader, Context};
///
/// keylink::service! {
///     service Depot;
///     send Ingest(name: String) -> u64;
/// }
///
/// struct Store;
/// impl depot::Handler for Store {
///     async fn ingest(&self, _cx: &Context, _n: String, _b: BodyReader) -> u64 { 0 }
/// }
///
/// let _ = depot::service(Store, Allowlist::anyone()).build();
/// ```
///
/// Put bulk transfer on a second service, which is what `README.md` recommends
/// for it anyway.
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
            /// Both fields MUST name the `Wire*` types, the same ones the wire
            /// names. Spelling the bare `Request` or `Response` here is what let
            /// the contract and the wire drift apart once already.
            #[derive(::postcard_schema::Schema)]
            struct Contract {
                request: $crate::Envelope<$crate::WireRequest<Request>>,
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

            /// Whether any method here reads an upload inside its turn, folded
            /// the same way. Read by [`Handle::service`], which refuses to
            /// compile for such a service.
            const DECLARES_SEND: bool =
                false $( | $crate::__keylink_is_send!([<$kind>]) )+;

            /// The compile-time refusal for [`DECLARES_SEND`].
            ///
            /// Generic in `A` **on purpose**, and that is the whole trick: an
            /// associated const of a generic type is evaluated when it is
            /// instantiated, so this fires at the site that serves an actor and
            /// nowhere else. A plain `const` would be evaluated where it is
            /// written, and would refuse every service that merely *declares* a
            /// `send` method — including every one served the concurrent way,
            /// where a `send` is perfectly safe.
            struct ServableAsActor<A>(::core::marker::PhantomData<A>);

            impl<A> ServableAsActor<A> {
                const CHECK: () = ::core::assert!(
                    !DECLARES_SEND,
                    "this service declares a `send` method, so it cannot be \
                     served as an actor. A `send` handler reads its upload \
                     INSIDE the serial turn, so a peer that declares a large \
                     body and then stops writing holds the actor until iroh's \
                     connection idle timeout, during which nothing else is \
                     answered - including the local Handle. No layer can bound \
                     that: a layer refuses the caller without ending the turn. \
                     Serve this with `service(handler, allow)` instead, or move \
                     the upload to a second service. RFC-04 s10.4."
                );
            }

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
                    ::std::sync::Arc::new(move |cx: $crate::Context,
                                                req: Request,
                                                body: $crate::BodyReader| {
                        let handler = handler.clone();
                        // Marked HERE rather than around `base` in
                        // `ServiceBuilder::build`, because only this code knows
                        // when the work itself begins. What the idempotency
                        // ledger does with a claimed key depends entirely on
                        // these two calls: neither means a layer refused
                        // without dispatching, one means the handler was
                        // entered and never answered, both mean it answered.
                        let __progress = cx.progress();
                        ::std::boxed::Box::pin(async move {
                            __progress.dispatched();
                            let (__response, __body) = match req {
                                $(
                                    Request::$rpc { $( $arg ),* } =>
                                        [<__invoke_ $rpc:snake>](
                                            handler.as_ref(), &cx, body $(, $arg )*
                                        ).await,
                                )+
                            };
                            __progress.completed();
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
            ///
            /// # Who is calling
            ///
            /// Every method takes a [`Caller`](keylink::Caller) first, because an
            /// actor is reachable two ways and the local way has no peer:
            ///
            /// ```ignore
            /// async fn submit(&mut self, cx: Caller<'_>, order: Order) -> Fill {
            ///     let who = match cx {
            ///         Caller::Local     => Who::House,
            ///         Caller::Peer(cx)  => Who::Trader(cx.peer()),
            ///     };
            /// }
            /// ```
            ///
            /// This replaces a `caller(&mut self, Option<&Context>)` hook that
            /// ran before each message and asked the state to stash the value.
            /// Three things were wrong with it and all three are gone. It
            /// occupied the name `caller`, so a service with an `rpc Caller()`
            /// could not be an actor. It was only reachable from
            /// `__actor_turn`, so the local-handle dispatcher had to repeat it
            /// by hand in every arm and a sixth method kind wired in without
            /// that line would silently inherit the last remote peer — RFC-01
            /// §11.2b's hazard, in the code rather than in a comment. And
            /// `if let Some(cx) = cx { self.peer = Some(cx.peer()) }` is the
            /// idiomatic way to write it and leaves a stale peer behind, so a
            /// local message could be authorized against whoever dialled in
            /// last. An argument cannot be forgotten, cannot go stale, and
            /// costs no name.
            pub trait Actor: Send + 'static {
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

            /// One message against one actor.
            ///
            /// Generated **once** and used by both served paths: the keyed
            /// system's mailbox loop and the singleton service's dispatch. One
            /// match, so a kind cannot be routed two ways *over the wire*.
            ///
            /// [`Handle`]'s own methods do not come through here — they call the
            /// trait method directly, since a local call has no `Request` to
            /// build and no `BodyReader` to pass. So there **are** two
            /// dispatchers, and the second one is `__keylink_local_method!`.
            /// That used to matter: each arm repeated a `caller(None)` hook by
            /// hand, and a sixth method kind wired in without it would silently
            /// inherit the last remote peer. The caller is an argument now, so
            /// the two dispatchers can disagree about many things and no longer
            /// about this one — a missing `Caller` is a compile error.
            async fn __actor_turn<A: Actor>(
                state: &mut A,
                cx: $crate::Caller<'_>,
                request: Request,
                body: $crate::BodyReader,
            ) -> (Response, $crate::Body) {
                match request {
                    $(
                        Request::$rpc { $( $arg ),* } =>
                            [<__actor_ $rpc:snake>](state, cx, body $(, $arg )*).await,
                    )+
                }
            }

            /// Spawn one actor of this service, ready to be mounted.
            ///
            /// Hand it to [`System::mount`](keylink::System::mount) under an
            /// id, or return it from
            /// [`spawn_with`](keylink::SystemBuilder::spawn_with) to have it
            /// built on the first message to an unmounted id.
            ///
            /// For **one** actor rather than many, use [`actor`], which needs
            /// no id, no registry and no second ALPN.
            ///
            /// A panicking message costs that message, not the actor: the turn
            /// is caught and reported as a refusal, and the next message runs
            /// against the state as it stands.
            ///
            /// # No observers on this path
            ///
            /// A keyed actor gets **no** [`Observer`](keylink::Observer)s. This
            /// runs its own loop over `__actor_turn` and never touches
            /// `LocalActor`, which is where the observer chain lives. So an
            /// actor mounted here and expecting durability would get no
            /// durability, no compile error and no explanation — hence this
            /// paragraph. Use [`actor_with`] for one actor that must be
            /// observed. RFC-06 §5.3.
            ///
            /// # Panics
            ///
            /// Panics when called outside a tokio runtime.
            #[must_use = "an unmounted actor is dropped, which stops the task \
                          this just started"]
            pub fn spawn<A: Actor>(mut state: A) -> $crate::Mounted {
                $crate::mount(schema(), move |mut inbox: $crate::Mailbox| async move {
                    while let ::std::option::Option::Some(mail) = inbox.recv().await {
                        let $crate::Mail { cx, payload, body, reply } = *mail;
                        // The ONLY route from the payload's bytes to a typed
                        // message, and it compares the hashes first.
                        let __answer = match payload.open::<Request>(schema()) {
                            ::std::result::Result::Ok(__request) => {
                                let __turn = $crate::guarded(__actor_turn(
                                    &mut state,
                                    $crate::Caller::Peer(&cx),
                                    __request,
                                    body,
                                ))
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

            /// Start one actor of this service, here, in this process.
            ///
            /// No id, no registry, no schema hash, no serialization and no
            /// network. The state moves onto its own task and is reached
            /// through the returned [`Handle`], whose methods are the ones
            /// written in `service!`.
            ///
            /// ```ignore
            /// let warehouse = inventory::actor(Warehouse::seeded());
            /// let item = warehouse.get_item("ADU-014".into()).await?;   // Err = MailboxClosed
            /// ```
            ///
            /// To serve the **same** actor to other machines as well, call
            /// [`Handle::service`]. To serve many keyed instances instead, use
            /// [`spawn`] with a [`System`](keylink::System).
            ///
            /// # Panics
            ///
            /// Panics when called outside a tokio runtime, like every other
            /// spawn. From synchronous code, enter one first with
            /// [`blocking::Node::enter`](keylink::blocking::Node::enter).
            #[must_use]
            pub fn actor<A: Actor>(state: A) -> Handle<A> {
                actor_with(state, ::std::vec::Vec::new())
            }

            /// Start one actor of this service with observers watching its
            /// turns.
            ///
            /// An [`Observer`](keylink::Observer) is called inside each turn,
            /// after the method and **before the reply**, and it sees the state
            /// and the outcome — never a request or a reply payload, which is
            /// [`Middleware`](keylink::Middleware)'s half. Returning `Err` ends
            /// the actor, and [`Handle::ended`] says which observer and why.
            ///
            /// ```ignore
            /// let (state, saver) = keylink_durable::open(&store, id, Ledger::default).await?;
            /// let ledger = ledger::actor_with(state, vec![Box::new(saver)]);
            /// ledger.post(1).await?;   // stored before this returns
            /// ```
            ///
            /// # Panics
            ///
            /// Panics when called outside a tokio runtime, like every other
            /// spawn.
            #[must_use]
            pub fn actor_with<A: Actor>(
                state: A,
                observers: ::std::vec::Vec<::std::boxed::Box<dyn $crate::Observer<A>>>,
            ) -> Handle<A> {
                Handle { inner: $crate::LocalActor::spawn_observed(state, observers) }
            }

            /// A handle to an actor in this process.
            ///
            /// Its methods are the ones written in `service!`, with the same
            /// names, the same argument order and the same success types as
            /// [`Client`]'s. The error type is the difference, and it is the
            /// whole difference: a local call fails one way, so it fails with
            /// [`MailboxClosed`](keylink::MailboxClosed) and never with
            /// [`Error`](keylink::Error).
            ///
            /// That is deliberate and permanent. RFC-04 §5.5: a handle that is
            /// one type in both places must either claim failures the local
            /// case cannot have, or hide failures the remote case does.
            /// `Error::SchemaMismatch` means nothing between two objects in one
            /// process.
            ///
            /// Cheap to clone: every clone names the same actor. The actor runs
            /// until the last handle is dropped, or until [`stop`](Self::stop)
            /// — **unless it is served**, in which case
            /// [`service`](Self::service) put a clone inside the server and that
            /// one outlives every handle you hold.
            ///
            /// # A panic reaches the caller of an `rpc`
            ///
            /// The actor survives a panicking message, and the caller of that
            /// message gets the panic — resumed with its own payload, close to
            /// the way an ordinary function call would deliver it. Both halves
            /// matter: the first keeps one bad message from costing every later
            /// one, the second keeps `MailboxClosed` meaning "the actor is gone"
            /// rather than "something went wrong somewhere".
            ///
            /// Two places it is **not** like an ordinary call, both structural:
            /// a `tell` has no channel to carry the panic, so it is caught and
            /// dropped; and a caller that dropped its future before the turn
            /// finished is not there to receive one. In both cases the panic
            /// hook has already run and printed at the fault, which is where the
            /// backtrace is worth having.
            ///
            /// # A dropped call still runs
            ///
            /// This is the one behaviour change a `Handler` → `Actor` migration
            /// makes that the compiler cannot show you. Dropping a `Handler`
            /// call's future drops the handler's future with it. Dropping an
            /// actor call's future does not: the message is in a mailbox on
            /// another task, and the turn runs to completion. A `reserve` that
            /// the caller timed out has still decremented the stock.
            ///
            /// # An actor must not call itself. Any method, including `tell`.
            ///
            /// A handler that awaits **any** method on a handle to its own actor
            /// can deadlock, and the mechanism is worth knowing because two of
            /// the three cases are load-dependent — they pass every test and
            /// appear in production.
            ///
            /// - **`rpc`, always.** The reply cannot be produced until the actor
            ///   takes the next message, and the actor is busy being this
            ///   handler.
            /// - **`tell`, under contention.** The mailbox holds one message.
            ///   The actor released that permit when it took the message it is
            ///   running now, so a self `tell` succeeds *only if nobody else
            ///   took the permit first*. A served actor always has other
            ///   senders. When the permit is gone the `send` itself blocks, and
            ///   only the actor can free it.
            /// - **[`stop`](Self::stop), always.** It takes a permit like any
            ///   message, and then waits for a loop that cannot exit.
            ///
            /// An earlier version of this comment said a self `tell` was fine
            /// because it waits for nothing. It waits for a permit.
            ///
            /// This is true of every actor system and it is not detected here:
            /// no type can tell one actor's handle from another's. Call the
            /// method on `&mut self` instead — it is the same code without the
            /// round trip. If the work must happen *after* the current turn,
            /// spawn a task that holds a handle; that task is not the actor and
            /// can wait for a permit safely.
            ///
            /// # Which methods are here
            ///
            /// `rpc`, `tell` and `blob`. `send` and `recv` are **absent**, for
            /// the reason given on their arms in
            /// `__keylink_local_method!`: both are defined by a
            /// [`BodyReader`](keylink::BodyReader) or a
            /// [`Body`](keylink::Body), which are a QUIC stream's receiving and
            /// sending halves. In this process there is no stream. An absent
            /// method is better than one that buffers a 4 GiB upload in memory
            /// to pretend otherwise.
            pub struct Handle<A> {
                inner: $crate::LocalActor<A>,
            }

            impl<A> ::core::clone::Clone for Handle<A> {
                fn clone(&self) -> Self {
                    Self { inner: self.inner.clone() }
                }
            }

            impl<A> ::core::fmt::Debug for Handle<A> {
                fn fmt(&self, f: &mut ::core::fmt::Formatter<'_>) -> ::core::fmt::Result {
                    f.debug_struct("Handle")
                        .field("service", &stringify!($svc))
                        .field("running", &self.inner.is_running())
                        .finish()
                }
            }

            impl<A: Actor> Handle<A> {
                /// Serve **this** actor to other machines, on this service's
                /// own ALPN.
                ///
                /// The wire does not change: peers call it with the ordinary
                /// [`Client`], with the same [`alpn`], the same `Request` and
                /// the same `Response`. Nothing here is an actor concept from
                /// the outside. What changes is the serving side — messages
                /// queue in the mailbox and run one at a time against
                /// `&mut self` instead of concurrently behind an `Arc`.
                ///
                /// This is what [`service`] is, with the mailbox in place of the
                /// `Arc`, and the two are alternatives: one router owns one
                /// ALPN, so registering both on a node is
                /// [`ConfigError::DuplicateAlpn`](keylink::ConfigError).
                ///
                /// Because the reply is the service's own typed `Response`,
                /// everything that reads one keeps working: per-method
                /// [`Middleware`](keylink::Middleware), the idempotency ledger,
                /// and `blob` methods, whose address the server must see to
                /// record a grant. The keyed actor system can offer none of
                /// those three — RFC-04 §14.8 — because there the payload is
                /// opaque to keylink by design.
                ///
                /// The local [`Handle`] keeps working while this is served, and
                /// messages from both sides share the one mailbox.
                ///
                /// # What it costs
                ///
                /// Calls no longer overlap. Four concurrent `list`s take four
                /// turns, and a slow method delays every later message rather
                /// than only its own caller.
                ///
                /// # A layer can refuse a call. It cannot cancel a turn.
                ///
                /// Read this before adding [`Timeout`](keylink::Timeout).
                ///
                /// A layer runs **outside** the mailbox, which is what keeps a
                /// refusal from costing a turn. The other half of that is that a
                /// layer's budget expiring drops the *wait for the reply*, not
                /// the turn: the turn is on the actor's task and runs to
                /// completion whatever the caller does.
                ///
                /// So `Timeout::after(100ms)` in front of a method that takes
                /// ten seconds does not shed load. Ten callers get `TimedOut` at
                /// 100 ms and the actor still spends a hundred seconds producing
                /// replies nobody is waiting for.
                ///
                /// [`QueueTimeout`](keylink::QueueTimeout) is the one that does
                /// shed load here, and it is why `Timeout` is still worth
                /// keeping elsewhere rather than being replaced: `QueueTimeout`
                /// refuses only a message that has not started, and `Timeout` on
                /// a concurrent [`Handler`] genuinely cancels the work. Two
                /// behaviours, not two doors to one.
                ///
                /// What this paragraph used to say next was that a key and a
                /// layer do not compose on this path — that a refused call was
                /// recorded as the settled reply and the turn then ran and
                /// mutated the state anyway. **That is fixed.** A turn that ran
                /// and produced no answer anybody received now settles the key
                /// as `Interrupted`, which is the truth rather than a lie the
                /// caller would keep being told on every retry. It still does
                /// not hand the retry the turn's real reply; RFC-05 §7 says why
                /// and what that would cost.
                ///
                /// # A `send` method cannot be served this way at all
                ///
                /// A `send` handler reads its upload inside the turn — RFC-04
                /// §10.4 — and no layer can bound that (see above). A peer that
                /// declares a large body and then stops writing would hold the
                /// actor until iroh's connection idle timeout, during which
                /// nothing else is answered, including the local [`Handle`].
                ///
                /// So this no longer compiles for a service declaring one. Not a
                /// startup error and not an opt-out: a compile error beats a
                /// runtime one, keeps `Result` off this call site, and matches
                /// what the rest of the crate does with claims of the form
                /// "there is no way to do X". Put bulk transfer on a second
                /// service served with [`service`], which is what the README
                /// already recommends for it.
                pub fn service(
                    &self,
                    allow: $crate::Allowlist,
                ) -> $crate::ServiceBuilder<Request, Response> {
                    // Reached by `serve` too, which goes through here. A service
                    // declaring a `send` method stops compiling AT THIS CALL
                    // SITE, and nowhere else.
                    let () = ServableAsActor::<A>::CHECK;
                    let actor = self.inner.clone();
                    let dispatch: $crate::Dispatch<Request, Response> =
                        ::std::sync::Arc::new(move |cx: $crate::Context,
                                                    req: Request,
                                                    body: $crate::BodyReader| {
                            let actor = actor.clone();
                            // Moved INTO the turn, which runs on the actor's own
                            // task. That is what makes "dispatched" mean the
                            // turn started rather than the message was queued:
                            // a layer that refuses while the message is still
                            // waiting leaves the key claimable, and a `Timeout`
                            // that drops the wait for a turn already running
                            // leaves it `Interrupted` — which is the truth,
                            // because the turn runs on regardless.
                            let __progress = cx.progress();
                            // A SECOND handle, marked outside the turn. The
                            // turn closure can only say the work started; only
                            // out here is it known whether the answer survived
                            // an observer. See the `completed()` call below.
                            let __settled = __progress.clone();
                            // A `QueueTimeout` above this put a deadline on the
                            // context. Honoured HERE rather than by the layer,
                            // because a layer cannot tell a queued message from
                            // a running one and the actor's own task can.
                            let __queueing = cx.queueing();
                            ::std::boxed::Box::pin(async move {
                                let __answer = actor
                                    .ask_catching_before(
                                        __queueing.map($crate::Queueing::deadline),
                                        move |__state: &mut A| {
                                        ::std::boxed::Box::pin(async move {
                                            __progress.dispatched();
                                            __actor_turn(
                                                __state,
                                                $crate::Caller::Peer(&cx),
                                                req,
                                                body,
                                            )
                                            .await
                                        })
                                    })
                                    .await;
                                match __answer {
                                    // Marked HERE, and not inside the turn.
                                    // An observer runs after the method and can
                                    // refuse the turn it just watched, and a
                                    // refused turn was NOT committed — so
                                    // marking `completed` where the method
                                    // returns would settle an idempotency key
                                    // as the observer's veto and replay that
                                    // veto to every retry, for work whose
                                    // effect was thrown away. This arm is the
                                    // only one holding an answer the turn
                                    // itself produced; every other one leaves
                                    // the marker at `dispatched`, which settles
                                    // `Interrupted` — "it ran, and whether it
                                    // took effect is unknown". That is the
                                    // truth for a refused turn, whose mutation
                                    // lived in memory and never reached a
                                    // store. RFC-06 §13.3.
                                    ::std::result::Result::Ok($crate::Queued::Ran(
                                        ::std::result::Result::Ok((__response, __body)),
                                    )) => {
                                        __settled.completed();
                                        ($crate::Outcome::Ok(__response), __body)
                                    }
                                    // A panic must not unwind out of a
                                    // connection's task, so the remote caller is
                                    // told what the keyed system tells it. The
                                    // actor is still alive.
                                    //
                                    // BOTH sides of `Failed` carry a prefix, and
                                    // prefixing only the refusal below would be a
                                    // guard that cannot fail: `Display for Panic`
                                    // writes the bare message, panic text is
                                    // often caller-derived, and a handler that
                                    // panicked with a refusal's exact text would
                                    // produce a byte-identical `Failed`.
                                    ::std::result::Result::Ok($crate::Queued::Ran(
                                        ::std::result::Result::Err(__panic),
                                    )) => (
                                        $crate::Outcome::Err($crate::RemoteError::Failed(
                                            ::std::format!("panicked: {}", __panic),
                                        )),
                                        $crate::Body::empty(),
                                    ),
                                    // Refused while still queued, so nothing
                                    // ran. The progress marker is untouched,
                                    // which is what leaves an idempotency key
                                    // claimable rather than settled - the only
                                    // bound that composes with one.
                                    //
                                    // The `0` is unreachable: without a
                                    // `QueueTimeout` there is no deadline, and
                                    // without a deadline nothing expires.
                                    ::std::result::Result::Ok($crate::Queued::Expired) => (
                                        $crate::Outcome::Err($crate::RemoteError::TimedOut {
                                            millis: __queueing
                                                .map_or(0, $crate::Queueing::millis),
                                        }),
                                        $crate::Body::empty(),
                                    ),
                                    // An observer refused THIS TURN. Not
                                    // `NoSuchActor`, which says "there is
                                    // nothing here, stop asking" — the turn ran
                                    // and was not committed, and the actor is
                                    // ending for a reason the peer should be
                                    // able to read.
                                    //
                                    // A `String` payload and NOT a new
                                    // `RemoteError` variant: a variant is inside
                                    // the hashed contract, so it would move the
                                    // ALPN and the pinned literal in
                                    // `tests/protocol.rs`. The cost is that a
                                    // peer must match on the string to tell a
                                    // refusal from a panic, which is why both
                                    // carry a prefix.
                                    ::std::result::Result::Err(
                                        $crate::TurnLost::Refused { name: __name },
                                    ) => (
                                        $crate::Outcome::Err($crate::RemoteError::Failed(
                                            ::std::format!("observer refused: {}", __name),
                                        )),
                                        $crate::Body::empty(),
                                    ),
                                    // NOT `Failed`, which is what a panic is.
                                    // A peer must be able to tell "your message
                                    // exploded, try another" from "there is
                                    // nothing here any more, stop asking" —
                                    // and after `stop()` the ALPN stays
                                    // registered, so this is answered for as
                                    // long as the server runs. The id is the
                                    // service name because a singleton has no
                                    // other one.
                                    ::std::result::Result::Err($crate::TurnLost::Gone) => (
                                        $crate::Outcome::Err(
                                            $crate::RemoteError::NoSuchActor {
                                                id: stringify!($svc).to_owned(),
                                            },
                                        ),
                                        $crate::Body::empty(),
                                    ),
                                }
                            })
                        });
                    $crate::ServiceBuilder::new(alpn(), allow, dispatch)
                        .__keylink_declares_blobs(DECLARES_BLOBS)
                }

                /// Serve this actor alone on `node`.
                ///
                /// The twin of [`serve`], and it exists because the first use
                /// of this API outside the crate found the asymmetry: the
                /// concurrent model had a one-line front door and the actor
                /// model made you drop to the builder, which made the actor the
                /// more verbose choice at exactly the site RFC-04 §1.3
                /// advertises as "one word".
                ///
                /// For several services on one endpoint, build each with
                /// [`Self::service`] and register them on a single
                /// [`Server`](keylink::Server): one router must own every ALPN.
                ///
                /// # The server holds the actor, and holds it past `shutdown`
                ///
                /// The returned [`Server`](keylink::Server) owns a clone of this
                /// handle, so the actor outlives every handle a caller holds.
                /// [`Server::shutdown`](keylink::Server::shutdown) stops
                /// accepting; it does not drop the router, so the actor and its
                /// state live until the `Server` value itself is dropped. To end
                /// the actor deterministically, call [`Self::stop`].
                ///
                /// # Errors
                ///
                /// [`ConfigError`](keylink::ConfigError) from
                /// [`spawn`](keylink::ServerBuilder::spawn), the same as
                /// [`serve`].
                pub fn serve(
                    &self,
                    node: &$crate::Node,
                    allow: $crate::Allowlist,
                ) -> $crate::Result<$crate::Server> {
                    $crate::Server::builder(node)
                        .service(self.service(allow).build())
                        .spawn()
                }

                /// Read the state between turns, for something the service's
                /// own methods do not express.
                ///
                /// The seam for a cross-cutting concern: a snapshot for
                /// persistence, a metric, a debug dump. It takes a turn like any
                /// other message, so it **cannot observe a half-applied state**
                /// — which is the entire reason it exists rather than the state
                /// being shared behind a lock.
                ///
                /// ```ignore
                /// // `snapshot` is the caller's own trait, not keylink's.
                /// let bytes = warehouse.with_state(Warehouse::snapshot).await?;
                /// ```
                ///
                /// The closure is **synchronous and takes `&A`**, on purpose.
                /// It cannot await, so it cannot hold the actor open, and it
                /// cannot mutate, so a concern bolted on from outside cannot
                /// quietly become a second way to change the state. Use a
                /// declared method for that.
                ///
                /// # Errors
                ///
                /// [`MailboxClosed`](keylink::MailboxClosed) when the actor is
                /// gone.
                pub async fn with_state<T, F>(
                    &self,
                    read: F,
                ) -> ::core::result::Result<T, $crate::MailboxClosed>
                where
                    T: ::core::marker::Send + 'static,
                    F: ::core::ops::FnOnce(&A) -> T + ::core::marker::Send + 'static,
                {
                    // A READ, not a turn: it takes `&A`, cannot mutate, and
                    // therefore runs no observers. Storing after it would be
                    // waste, and it also closes a loop a future background
                    // writer would create — a writer that read through a turn
                    // would mark the actor dirty and schedule another read,
                    // forever.
                    self.inner.read(read).await
                }

                /// Stop after the messages already queued, then wait for the
                /// actor to end. Every other handle reports
                /// [`MailboxClosed`](keylink::MailboxClosed) from then on.
                pub async fn stop(&self) {
                    self.inner.stop().await;
                }

                /// Cancel the actor where it stands, destroying its state.
                ///
                /// Cooperative, like every tokio abort: the turn is dropped at
                /// its next await point. A turn that never reaches one — a
                /// compute loop, a blocking syscall — is not interrupted, and
                /// this returns having done nothing. That is the one case
                /// [`stop`](Self::stop) also cannot fix.
                ///
                /// Asynchronous, too: [`is_running`](Self::is_running) can still
                /// say `true` between this call and the runtime dropping the
                /// task. `stop` does not have that gap, because it waits.
                pub fn abort(&self) {
                    self.inner.abort();
                }

                /// Whether the actor is still handling messages. Advisory: it
                /// can stop between this answer and the next call.
                #[must_use]
                pub fn is_running(&self) -> bool {
                    self.inner.is_running()
                }

                /// Resolve when this actor ends, and say why.
                ///
                /// [`MailboxClosed`](keylink::MailboxClosed) says "gone", and it
                /// must not be stretched to say "gave up on durability". This is
                /// the second question, asked separately — and it is the only
                /// way to learn that an [`Observer`](keylink::Observer) refused
                /// a turn, because a local call is told `MailboxClosed` either
                /// way.
                ///
                /// Holds no mailbox sender, so awaiting it does not keep the
                /// actor alive: dropping every handle both ends the actor and
                /// resolves this.
                pub fn ended(&self) -> impl ::core::future::Future<Output = $crate::Ended>
                    + ::core::marker::Send + 'static {
                    self.inner.ended()
                }

                $(
                    $crate::__keylink_local_method!(
                        [$( #[$meta] )*],
                        [<$kind>],
                        [<$rpc:snake>],
                        $rpc,
                        ($( $arg : $ty ),*),
                        $ret
                    );
                )+
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

            /// Every clone names the same connection, and calls through them all
            /// share its congestion-control state. Cheap, because a
            /// `Connection` is a handle.
            ///
            /// Written out rather than derived: `#[derive(Clone)]` would demand
            /// `Request: Clone, Response: Clone`, and neither appears in a
            /// field.
            impl ::core::clone::Clone for Client {
                fn clone(&self) -> Self {
                    Self { inner: self.inner.clone() }
                }
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
                /// it goes here instead of into every method signature. Takes a
                /// bare [`IdempotencyKey`](keylink::IdempotencyKey), which
                /// refuses on collision, or an
                /// [`Idempotency`](keylink::Idempotency), which can wait for the
                /// call already running under the key. See
                /// [`RpcClient::with_idempotency`](keylink::RpcClient::with_idempotency)
                /// for why the handle is meant for one call.
                #[must_use]
                pub fn with_idempotency(
                    &self,
                    idempotency: impl ::core::convert::Into<$crate::Idempotency>,
                ) -> Self {
                    Self {
                        inner: self.inner.with_idempotency(idempotency),
                    }
                }

                /// Ask what this peer's ledger knows about one of your keys.
                ///
                /// See
                /// [`RpcClient::key_state`](keylink::RpcClient::key_state). Runs
                /// no handler, so it never appears in the [`Handler`] or
                /// [`Actor`] traits — it is a question about keylink's own
                /// bookkeeping rather than about your service.
                pub async fn key_state(
                    &self,
                    key: $crate::IdempotencyKey,
                ) -> $crate::Result<$crate::KeyState> {
                    self.inner.key_state(key).await
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
                    pub fn with_idempotency(
                        &self,
                        idempotency: impl ::core::convert::Into<$crate::Idempotency>,
                    ) -> Self {
                        Self {
                            inner: self.inner.with_idempotency(idempotency),
                            runtime: ::std::sync::Arc::clone(&self.runtime),
                        }
                    }

                    /// Ask what this peer's ledger knows about one of your keys.
                    ///
                    /// See the async twin,
                    /// [`Client::key_state`](super::Client::key_state).
                    pub fn key_state(
                        &self,
                        key: $crate::IdempotencyKey,
                    ) -> $crate::Result<$crate::KeyState> {
                        self.runtime.run(self.inner.key_state(key))?
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

/// Whether one kind reads its upload inside the turn, folded into
/// `DECLARES_SEND`.
///
/// Only `send`, and the narrowness is the point. A `recv` body is drained by the
/// connection *after* the turn ends and a `blob`'s bytes move later on a
/// different connection, so neither can hold an actor. Widening this to every
/// streaming kind would refuse two services that are perfectly safe.
#[doc(hidden)]
#[macro_export]
macro_rules! __keylink_is_send {
    (rpc) => {
        false
    };
    (tell) => {
        false
    };
    (send) => {
        true
    };
    (recv) => {
        false
    };
    (blob) => {
        false
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
        fn $name(&self, cx: &$crate::Context $(, $arg: $ty )*)
            -> impl ::core::future::Future<Output = $ret> + Send;
    };
    ([$( #[$meta:meta] )*], tell, $name:ident, ($( $arg:ident : $ty:ty ),*), $ret:ty) => {
        $( #[$meta] )*
        #[doc = concat!("Handle the `", stringify!($name), "` tell. Nothing is sent back.")]
        ///
        /// The caller has already returned. Whatever this produces is dropped,
        /// including a panic, which is caught and costs one message.
        fn $name(&self, cx: &$crate::Context $(, $arg: $ty )*)
            -> impl ::core::future::Future<Output = $ret> + Send;
    };
    ([$( #[$meta:meta] )*], send, $name:ident, ($( $arg:ident : $ty:ty ),*), $ret:ty) => {
        $( #[$meta] )*
        #[doc = concat!("Answer the `", stringify!($name), "` rpc, reading the payload the caller streams up.")]
        fn $name(&self, cx: &$crate::Context $(, $arg: $ty )*, body: $crate::BodyReader)
            -> impl ::core::future::Future<Output = $ret> + Send;
    };
    ([$( #[$meta:meta] )*], recv, $name:ident, ($( $arg:ident : $ty:ty ),*), $ret:ty) => {
        $( #[$meta] )*
        #[doc = concat!("Answer the `", stringify!($name), "` rpc, returning a payload to stream back.")]
        fn $name(&self, cx: &$crate::Context $(, $arg: $ty )*)
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
        fn $name(&self, cx: &$crate::Context $(, $arg: $ty )*)
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
            cx: &$crate::Context,
            _body: $crate::BodyReader
            $(, $arg: $ty )*
        ) -> ($resp, $crate::Body) {
            (
                $resp::$variant(handler.$method(cx $(, $arg )*).await),
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
            cx: &$crate::Context,
            _body: $crate::BodyReader
            $(, $arg: $ty )*
        ) -> ($resp, $crate::Body) {
            (
                $resp::$variant(handler.$method(cx $(, $arg )*).await),
                $crate::Body::empty(),
            )
        }
    };
    (send, $fname:ident, $handler:ident, $resp:ident, $variant:ident, $method:ident,
        ($( $arg:ident : $ty:ty ),*)) => {
        async fn $fname<H: $handler>(
            handler: &H,
            cx: &$crate::Context,
            body: $crate::BodyReader
            $(, $arg: $ty )*
        ) -> ($resp, $crate::Body) {
            (
                $resp::$variant(handler.$method(cx, $( $arg, )* body).await),
                $crate::Body::empty(),
            )
        }
    };
    (recv, $fname:ident, $handler:ident, $resp:ident, $variant:ident, $method:ident,
        ($( $arg:ident : $ty:ty ),*)) => {
        async fn $fname<H: $handler>(
            handler: &H,
            cx: &$crate::Context,
            _body: $crate::BodyReader
            $(, $arg: $ty )*
        ) -> ($resp, $crate::Body) {
            let (value, body) = handler.$method(cx $(, $arg )*).await;
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
            cx: &$crate::Context,
            _body: $crate::BodyReader
            $(, $arg: $ty )*
        ) -> ($resp, $crate::Body) {
            let (value, id) = handler.$method(cx $(, $arg )*).await;
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
        fn $name(&mut self, cx: $crate::Caller<'_> $(, $arg: $ty )*)
            -> impl ::core::future::Future<Output = $ret> + Send;
    };
    ([$( #[$meta:meta] )*], tell, $name:ident, ($( $arg:ident : $ty:ty ),*), $ret:ty) => {
        $( #[$meta] )*
        #[doc = concat!("Handle the `", stringify!($name), "` tell. Nothing is sent back.")]
        fn $name(&mut self, cx: $crate::Caller<'_> $(, $arg: $ty )*)
            -> impl ::core::future::Future<Output = $ret> + Send;
    };
    ([$( #[$meta:meta] )*], send, $name:ident, ($( $arg:ident : $ty:ty ),*), $ret:ty) => {
        $( #[$meta] )*
        #[doc = concat!("Answer the `", stringify!($name), "` rpc, reading the payload the caller streams up.")]
        ///
        /// The body is read inside the turn, so
        /// [`abort`](keylink::System::abort) can truncate it.
        fn $name(&mut self, cx: $crate::Caller<'_> $(, $arg: $ty )*, body: $crate::BodyReader)
            -> impl ::core::future::Future<Output = $ret> + Send;
    };
    ([$( #[$meta:meta] )*], recv, $name:ident, ($( $arg:ident : $ty:ty ),*), $ret:ty) => {
        $( #[$meta] )*
        #[doc = concat!("Answer the `", stringify!($name), "` rpc, returning a payload to stream back.")]
        ///
        /// The body is drained by the connection after the turn ends, so
        /// [`abort`](keylink::System::abort) cannot truncate it.
        fn $name(&mut self, cx: $crate::Caller<'_> $(, $arg: $ty )*)
            -> impl ::core::future::Future<Output = ($ret, $crate::Body)> + Send;
    };
    ([$( #[$meta:meta] )*], blob, $name:ident, ($( $arg:ident : $ty:ty ),*), $ret:ty) => {
        $( #[$meta] )*
        #[doc = concat!("Answer the `", stringify!($name), "` rpc, returning the address of content this node already holds.")]
        ///
        /// Nothing is streamed during the turn: the address is the whole reply,
        /// and the bytes move later on the blobs connection. So
        /// [`abort`](keylink::System::abort) has nothing here to truncate.
        fn $name(&mut self, cx: $crate::Caller<'_> $(, $arg: $ty )*)
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
            cx: $crate::Caller<'_>,
            _body: $crate::BodyReader
            $(, $arg: $ty )*
        ) -> ($resp, $crate::Body) {
            (
                $resp::$variant(state.$method(cx $(, $arg )*).await),
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
            cx: $crate::Caller<'_>,
            _body: $crate::BodyReader
            $(, $arg: $ty )*
        ) -> ($resp, $crate::Body) {
            (
                $resp::$variant(state.$method(cx $(, $arg )*).await),
                $crate::Body::empty(),
            )
        }
    };
    (send, $fname:ident, $actor:ident, $resp:ident, $variant:ident, $method:ident,
        ($( $arg:ident : $ty:ty ),*)) => {
        async fn $fname<A: $actor>(
            state: &mut A,
            cx: $crate::Caller<'_>,
            body: $crate::BodyReader
            $(, $arg: $ty )*
        ) -> ($resp, $crate::Body) {
            (
                $resp::$variant(state.$method(cx, $( $arg, )* body).await),
                $crate::Body::empty(),
            )
        }
    };
    (recv, $fname:ident, $actor:ident, $resp:ident, $variant:ident, $method:ident,
        ($( $arg:ident : $ty:ty ),*)) => {
        async fn $fname<A: $actor>(
            state: &mut A,
            cx: $crate::Caller<'_>,
            _body: $crate::BodyReader
            $(, $arg: $ty )*
        ) -> ($resp, $crate::Body) {
            let (value, body) = state.$method(cx $(, $arg )*).await;
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
            cx: $crate::Caller<'_>,
            _body: $crate::BodyReader
            $(, $arg: $ty )*
        ) -> ($resp, $crate::Body) {
            let (value, id) = state.$method(cx $(, $arg )*).await;
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

/// One method on a local `Handle`, shaped by the rpc kind.
///
/// Every arm goes through the trait method **directly**: no `Request` variant is
/// built, nothing is serialized, and no `Response` is taken apart. That is why
/// none of these can fail in a second way — there is no encode step to fail, and
/// no "wrong variant" to extract.
#[doc(hidden)]
#[macro_export]
macro_rules! __keylink_local_method {
    ([$( #[$meta:meta] )*], rpc, $name:ident, $variant:ident,
        ($( $arg:ident : $ty:ty ),*), $ret:ty) => {
        $( #[$meta] )*
        #[doc = concat!("Call the `", stringify!($variant), "` rpc on this actor.")]
        ///
        /// # Errors
        ///
        /// [`MailboxClosed`](keylink::MailboxClosed) when the actor is gone.
        ///
        /// # Panics
        ///
        /// With whatever the handler panicked with, after the actor has
        /// survived it.
        pub async fn $name(&self $(, $arg: $ty )*)
            -> ::core::result::Result<$ret, $crate::MailboxClosed> {
            self.inner
                .ask(move |__state: &mut A| ::std::boxed::Box::pin(async move {
                    __state.$name($crate::Caller::Local $(, $arg )*).await
                }))
                .await
        }
    };
    ([$( #[$meta:meta] )*], tell, $name:ident, $variant:ident,
        ($( $arg:ident : $ty:ty ),*), $ret:ty) => {
        $( #[$meta] )*
        #[doc = concat!("Send the `", stringify!($variant), "` tell to this actor and return.")]
        ///
        /// Returns `()` rather than the method's declared type, exactly as the
        /// remote client does, and for the same reason: a `tell` has no reply
        /// channel. The `Ok` means the message is in the mailbox — it has not
        /// run, it may panic, and there is nothing here on which to learn
        /// either.
        ///
        /// It **is** ordered where the remote one is not, and the guarantee is
        /// narrower than "one handle": two tells awaited **one after the other
        /// on one task** are queued in that order and handled in that order,
        /// because the mailbox is FIFO. Two tells started concurrently — a
        /// `join!`, two spawned tasks, one handle shared across both — race for
        /// the permit like any other pair of senders.
        ///
        /// # Errors
        ///
        /// [`MailboxClosed`](keylink::MailboxClosed) when the actor is gone.
        pub async fn $name(&self $(, $arg: $ty )*)
            -> ::core::result::Result<(), $crate::MailboxClosed> {
            self.inner
                .notify(move |__state: &mut A| ::std::boxed::Box::pin(async move {
                    __state.$name($crate::Caller::Local $(, $arg )*).await
                }))
                .await
        }
    };
    ([$( #[$meta:meta] )*], blob, $name:ident, $variant:ident,
        ($( $arg:ident : $ty:ty ),*), $ret:ty) => {
        $( #[$meta] )*
        #[doc = concat!("Call the `", stringify!($variant), "` rpc on this actor, returning the address of the content it named.")]
        ///
        /// A [`BlobId`](keylink::BlobId), not a
        /// [`BlobHandle`](keylink::BlobHandle): a handle is an address plus the
        /// node that holds it, and in this process the holder is you. No grant
        /// is recorded either, because a grant is permission for a *peer* and
        /// there is no peer here.
        ///
        /// # Errors
        ///
        /// [`MailboxClosed`](keylink::MailboxClosed) when the actor is gone.
        pub async fn $name(&self $(, $arg: $ty )*)
            -> ::core::result::Result<($ret, $crate::BlobId), $crate::MailboxClosed> {
            self.inner
                .ask(move |__state: &mut A| ::std::boxed::Box::pin(async move {
                    __state.$name($crate::Caller::Local $(, $arg )*).await
                }))
                .await
        }
    };
    // `send` and `recv` are deliberately absent, the same way
    // `__keylink_blocking_method!` omits them — and the reason is the same one,
    // one layer further in.
    //
    // A `send` handler is DEFINED by the `BodyReader` it reads, and a
    // `BodyReader` reads a QUIC `RecvStream`: `body.rs` builds one from a
    // `FrameReader`, which holds that stream by value. A `recv` handler returns
    // a `Body`, which `body.rs` calls "a *value*, not a stream handle: a handler
    // returns one and keylink drains it" — into a `SendStream`. Neither half
    // exists in this process.
    //
    // The two ways to emit something anyway are both worse. Buffering the body
    // in memory would make a 4 GiB upload resident, which is the one thing the
    // streaming API exists to prevent. Generalizing `FrameReader` over
    // `AsyncRead` would rewrite the transport to give a local caller a
    // convenience.
    //
    // So: absent here, and present the moment the same actor is served with
    // `Handle::service`, where there IS a stream. Streaming is a transport
    // capability, and this handle names no transport.
    ([$( #[$meta:meta] )*], send, $name:ident, $variant:ident,
        ($( $arg:ident : $ty:ty ),*), $ret:ty) => {};
    ([$( #[$meta:meta] )*], recv, $name:ident, $variant:ident,
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
        // The fifth kind, here for the same reason as the fourth, plus one of
        // its own: `tell` is the kind with the most arms that emit NOTHING —
        // no actor-handle method, and no local method until this file gained
        // one. An omission compiles; a wrong signature does not. Only an
        // expansion can tell those apart.
        tell Note(line: String) -> ();
        // The fourth kind belongs here for the same reason the other three do.
        // It also expands the ACTOR twin, which is where RFC-01 §11.2b's
        // "build the blob path twice" risk lives: a kind added to one generator
        // and not the other compiles until somebody declares it.
        blob Manual(sku: String) -> u64;
    }

    /// The one claim in `__keylink_local_method!` that a reader cannot check by
    /// reading: `Handle` has **no** `send` and no `recv` method.
    ///
    /// Absence is invisible. This is the shadowing probe from
    /// `representability.rs`, applied to generated code: an inherent method
    /// outranks a trait method, so while `Handle` has no `store` and no `fetch`,
    /// these calls resolve to the trait below and the marker type checks out.
    /// Add either one — by buffering a body in memory, which is the tempting
    /// way — and this stops compiling.
    ///
    /// It has to live here rather than in `representability.rs` because the
    /// generated module is private to this one.
    mod no_streaming_on_a_local_handle {
        #![allow(dead_code, reason = "the proof is that this module compiles")]

        use super::expansion;

        struct NoSuchMethod;

        trait HandleHasNoStreamingMethod {
            fn store(&self) -> NoSuchMethod {
                NoSuchMethod
            }
            fn fetch(&self) -> NoSuchMethod {
                NoSuchMethod
            }
        }

        impl<A> HandleHasNoStreamingMethod for expansion::Handle<A> {}

        /// The hole the shadowing probe cannot see, closed the way
        /// `representability.rs` closes it for `Sealed`.
        ///
        /// A trait method on `Self` outranks a deref step, so the probe above
        /// would stay green while `handle.store(..)` quietly started working
        /// through a `Deref` target. The blanket impl overlaps the concrete one
        /// exactly when `Handle<A>: Deref`, and an overlap is `E0119`.
        ///
        /// This works only because `Handle` is a LOCAL type — in `tests/` rustc
        /// answers "upstream crates may add a new impl in future versions",
        /// which is the correct answer to the wrong question.
        trait HandleDoesNotDeref {}
        impl<T: ::core::ops::Deref> HandleDoesNotDeref for T {}
        impl<A> HandleDoesNotDeref for expansion::Handle<A> {}

        /// One state, only so the handle has something to be generic over.
        struct Probe;

        impl expansion::Actor for Probe {
            async fn ping(&mut self, _cx: crate::Caller<'_>, n: u64) -> u64 {
                n
            }
            async fn fetch(&mut self, _cx: crate::Caller<'_>, _name: String) -> (u64, crate::Body) {
                (0, crate::Body::empty())
            }
            async fn store(
                &mut self,
                _cx: crate::Caller<'_>,
                _name: String,
                _body: crate::BodyReader,
            ) -> u64 {
                0
            }
            async fn note(&mut self, _cx: crate::Caller<'_>, _line: String) {}
            async fn manual(
                &mut self,
                _cx: crate::Caller<'_>,
                _sku: String,
            ) -> (u64, crate::BlobId) {
                unreachable!("never called; this module only has to compile")
            }
        }

        fn a_local_handle_streams_nothing(handle: expansion::Handle<Probe>) {
            let _: NoSuchMethod = handle.store();
            let _: NoSuchMethod = handle.fetch();

            // And the three that ARE there, so this probe cannot pass by the
            // handle having no methods at all. `drop` rather than `let _`,
            // which clippy reads as forgetting to await a future — here the
            // future is the assertion and running it is not the point.
            drop(handle.ping(1));
            drop(handle.note(String::new()));
            drop(handle.manual(String::new()));
        }
    }
}
