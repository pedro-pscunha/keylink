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
/// And at the other site that serves one, which for a long time it was not.
/// `CHECK` is an associated const on a generic type, so it is evaluated where it
/// is *instantiated*; while `Handle::service` held the only instantiation, the
/// identical service was refused there and mounted into a keyed
/// [`System`](crate::System) without a word — over an ALPN, reachable by any
/// admitted peer, with the body read inside the turn exactly as above. §16.9.
///
/// (`crate::` and not `keylink::` there, unlike every link in the *expansion*
/// below: this comment is rendered inside keylink, and those are rendered in the
/// caller's crate.)
///
/// ```compile_fail
/// use keylink::{BodyReader, Caller};
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
/// let _ = depot::spawn(Store);                   // <- nor does this one
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
/// ## `#[defer]`: one attribute keylink reads, and the rest it forwards
///
/// An `rpc` marked `#[defer]` is handed its own reply channel and may answer from
/// a later turn. Nothing about the wire changes — a deferred method is an ordinary
/// `rpc` to the caller, which blocks on its stream exactly as today, and the ALPN
/// does not move. See [`Reply`](crate::Reply) for the handler shape and
/// [`Replied`](crate::Replied) for why forgetting to answer is a compile error.
///
/// It is the **only** attribute this macro interprets. Every other one is
/// forwarded verbatim to the sites that take one, which is why reading this one at
/// all needs a token-muncher: `service!`'s matcher captures attributes as
/// `#[$meta:meta]`, and a `meta` fragment is opaque to further matching. So the
/// declarations are munched a token at a time *before* the fragment forms, with
/// interpreted attributes accumulated separately from pass-through ones. See
/// [`__keylink_service_munch`](crate::__keylink_service_munch).
///
/// `#[defer]` on a `tell`, `send`, `recv` or `blob` is a compile error naming the
/// reason — `__keylink_defer_check!` has the four messages, and this is one of
/// them:
///
/// ```compile_fail
/// keylink::service! {
///     service Orders;
///     rpc Fill(price: u64) -> u64;
///     #[defer]
///     tell Note(line: String) -> ();      // <- a `tell` has no reply to defer
/// }
/// ```
///
/// Nothing on the fence, because rustdoc does not check what follows
/// `compile_fail` and the failure here is a `compile_error!` rather than an error
/// code anyway. The refusal is watched to fail: `__keylink_defer_check!`'s arm for
/// `tell` renamed away leaves this fence green, which was red-tested.
///
/// The `rpc` above is not padding. A `service!` of one method generates an
/// `into_<rpc>` whose catch-all arm is `unreachable_patterns`, and a probe should
/// not be able to fail for that instead.
///
/// ## `feed`: an actor's second input, and the one declaration with no wire form
///
/// ```ignore
/// keylink::service! {
///     service Orders;
///     rpc  Cancel(id: OrderId) -> Result<(), Rejected>;
///     feed Fills(fill: Fill);            // local only, never on the wire
/// }
///
/// // generated on the Actor trait — no Caller (nobody called), no return (no
/// // reply channel)
/// async fn on_fills(&mut self, fill: Fill);
/// ```
///
/// A feed is a locally wired [`Stream`](crate::Stream), selected against the
/// mailbox inside the actor's own loop, so a feed item is a **turn**: the panic
/// guard, the observer chain and a durability adapter's commit all apply to one.
/// Wire it at spawn with `actor_wired(state, observers, Feeds::new(stream))`, or
/// `spawn_wired(..)` for a keyed actor.
///
/// Four things follow, and each is a decision rather than an accident:
///
/// - **No `Caller` and no return.** Nobody called, so there is nobody to answer. A
///   feed that needs a reply is an `rpc`.
/// - **Not on the wire.** A `feed` produces no `Request` variant, no `Response`
///   variant, no `WireKinds` entry and no client method, so **the ALPN does not
///   move** when you add one. It is not a sixth `kind` at all: the muncher puts it
///   in a second list, and the wire generators never see that list.
/// - **Exactly one argument**, which is its item — a stream yields one value at a
///   time. Several fields is a struct, which is a domain type rather than an
///   argument list.
/// - **A feed that ends, ends the actor**, as
///   [`Ended::Feed`](crate::Ended::Feed). A silently dead subscription serving
///   stale state is the failure that prevents.
///
/// `#[defer]` on a `feed` is a compile error, for the reason a `tell` is one: there
/// is no reply to defer. `__keylink_defer_check!` has the message and this is it:
///
/// ```compile_fail
/// keylink::service! {
///     service Orders;
///     rpc Fill(price: u64) -> u64;
///     #[defer]
///     feed Ticks(tick: u64);       // <- a `feed` has no reply, and no caller
/// }
/// ```
///
/// Nothing on the fence, for the reason the `tell` fence above gives. And the same
/// hazard applies: renaming `__keylink_defer_check!`'s `feed` arm away leaves this
/// fence red for the *wrong* reason — "no rule expected `feed`" — so it is
/// red-tested the other way, by making that arm a no-op, which turns the fence
/// green with *"Test compiled successfully, but it's marked `compile_fail`"*.
///
/// A service that declares a `feed` cannot be spawned through a door that wires
/// none. An unwired feed would be a stream that never yields, which is silence a
/// caller cannot tell apart from a quiet exchange, so the doors that take no
/// `Feeds` refuse it at the call site instead:
///
/// ```compile_fail
/// keylink::service! {
///     service Orders;
///     rpc Fill(price: u64) -> u64;
///     feed Ticks(tick: u64);
/// }
///
/// struct Book;
/// impl orders::Actor for Book {
///     async fn fill(&mut self, _cx: keylink::Caller<'_>, price: u64) -> u64 { price }
///     async fn on_ticks(&mut self, _tick: u64) {}
/// }
///
/// // Fails in `SpawnableUnfed::<Book>::CHECK`, a `const` assertion on a generic
/// // type, so it is evaluated HERE — at the instantiation — and nowhere else.
/// // `actor_wired(Book, vec![], orders::Feeds::new(stream))` is the door.
/// let _ = orders::actor(Book);
/// ```
///
/// The `rpc` in both fences is not padding: a `service!` of one method generates an
/// `into_<rpc>` whose catch-all arm is `unreachable_patterns`, and a probe should not
/// be able to fail for that instead. In the second, the `impl Actor` block is not
/// padding either — without it the fence would pass on the missing impl rather than
/// on the refusal, which is the shape of a guard that proves nothing.
///
/// The calling crate needs `serde`, `postcard-schema` and `tokio` as
/// dependencies — the generated code derives against them by path.
#[macro_export]
macro_rules! service {
    ( service $svc:ident; $( $rest:tt )+ ) => {
        $crate::__keylink_service_munch! {
            @svc [ $svc ]
            @done [ ]
            @feeds [ ]
            @attrs [ ]
            @defer [ ]
            @rest [ $( $rest )+ ]
        }
    };
}

/// The token-muncher that reads `#[defer]` before a `meta` fragment can hide it.
///
/// **This exists because `macro_rules!` cannot pattern-match on `#[defer]` once it
/// has been captured.** `service!` had one matcher capturing
/// `$( #[$meta:meta] )*`, and a `meta` fragment is an opaque AST node: nothing
/// downstream can ask whether it is `defer`. Those metas are forwarded verbatim to
/// nine sites — the `Request` variant, the `Actor`, `Handler`, `Client`,
/// `ActorRef` and blocking methods, and the local `Handle` method — so `#[defer]`
/// parsed happily and was re-emitted where it is not an attribute at all,
/// producing `cannot find attribute` from inside a `paste!` and pointing at
/// `macros.rs` rather than at the declaration the author wrote.
///
/// So the raw tokens are walked one declaration at a time, and each declaration's
/// attribute run is split in two: `@defer` for the one keylink reads and `@attrs`
/// for everything it passes on. Both are closed by the declaration that follows
/// them and handed to [`__keylink_service`](crate::__keylink_service) as one
/// record per method.
///
/// Wave 1b established that a per-*kind* answer needs a helper macro
/// (`__keylink_mail_body!` and its four siblings). A per-*attribute* answer needs a
/// muncher, and this is the whole of it.
///
/// # It also partitions, and that is how `feed` cost two arms instead of twelve
///
/// A sixth declaration kind reached through `$kind:ident` would need an arm in each
/// of the thirteen helper macros — `__keylink_response_ty!`, `__keylink_is_blob!`,
/// `__keylink_client_method!` and the rest — every one of them saying "not on the
/// wire" in a different shape, and one of them forgetting would put a `feed` in a
/// `Request` enum.
///
/// So a `feed` is not a kind. It goes into a **second list** here, and
/// [`__keylink_service`](crate::__keylink_service) receives
/// `@wire [ .. ] @feeds [ .. ]`. The wire list expands into `Request`, `Response`,
/// `Contract`, `Handler`, `Client`, `Mail` and the rest exactly as before, and the
/// feed list reaches none of them. **"Feeds are not on the wire" is therefore a fact
/// about which list a declaration is in**, checked by the compiler at every one of
/// those sites, rather than a claim thirteen macros have to keep agreeing about.
///
/// The two arms it did cost are the well-formed `feed Name(item: Type);` and the
/// refusal for any other shape of one.
///
/// `#[defer]` is idempotent rather than an error when doubled, because a matcher
/// that refused the second one would report it as "no rule expected this token",
/// which is a worse error than none.
///
/// Recursion is one step per attribute plus one per declaration, so the deepest
/// service in this repository munches about a dozen frames against a default
/// `recursion_limit` of 128.
#[doc(hidden)]
#[macro_export]
macro_rules! __keylink_service_munch {
    // The one attribute keylink reads. Never forwarded.
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @defer [ $( $_seen:tt )* ]
        @rest [ #[defer] $( $rest:tt )* ]
    ) => {
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ $( $attrs )* ] @defer [ defer ]
            @rest [ $( $rest )* ]
        }
    };
    // Everything else, kept in order and forwarded verbatim.
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ #[$meta:meta] $( $rest:tt )* ]
    ) => {
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ $( $attrs )* #[$meta] ] @defer [ $( $seen )* ]
            @rest [ $( $rest )* ]
        }
    };
    // One `feed`, into the OTHER list. No return type, and exactly one argument.
    //
    // BEFORE the wire arm, and the order is load-bearing: `feed Fills(f: F) -> ();`
    // matches the wire arm with `$kind = feed`, which would put a feed in the
    // `Request` enum and then fail inside `__keylink_response_ty!` with "no rule
    // expected `feed`", pointing at a macro the author did not write. The arm below
    // catches that shape instead and says what is wrong with it.
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ feed $feed:ident ( $arg:ident : $ty:ty ) ; $( $rest:tt )* ]
    ) => {
        $crate::__keylink_service_munch! {
            @svc $svc
            @done [ $( $done )* ]
            @feeds [
                $( $feeds )*
                { [ $( $attrs )* ] [ $( $seen )* ] feed $feed ( $arg : $ty ) }
            ]
            @attrs [ ] @defer [ ]
            @rest [ $( $rest )* ]
        }
    };
    // A `feed` written with a return type.
    (
        @svc $svc:tt @done $done:tt @feeds $feeds:tt @attrs $attrs:tt @defer $defer:tt
        @rest [ feed $feed:ident ( $( $anything:tt )* ) -> $ret:ty ; $( $rest:tt )* ]
    ) => {
        ::core::compile_error!(::core::concat!(
            "`feed ",
            ::core::stringify!($feed),
            "` has no return type, because nothing is waiting for one. An item \
             arrived off a stream; there is no caller and no reply channel, which \
             is why the generated handler is `async fn on_",
            ::core::stringify!($feed),
            "(&mut self, item: Item)` and returns nothing. Write \
             `feed ",
            ::core::stringify!($feed),
            "(item: Item);`. If something IS waiting for an answer, that something \
             called: make it an `rpc`."
        ));
    };
    // A `feed` that is not one argument. Said here rather than left to the
    // catch-all, because the catch-all cannot name the declaration.
    (
        @svc $svc:tt @done $done:tt @feeds $feeds:tt @attrs $attrs:tt @defer $defer:tt
        @rest [ feed $feed:ident ( $( $anything:tt )* ) ; $( $rest:tt )* ]
    ) => {
        ::core::compile_error!(::core::concat!(
            "`feed ",
            ::core::stringify!($feed),
            "` must declare exactly one argument, which is its item. A feed is \
             wired from a `Stream`, and a stream yields one value at a time - so \
             `feed ",
            ::core::stringify!($feed),
            "(fill: Fill);` and then `Feeds::new(stream_of_fills)`. For several \
             fields, name the struct: that is a domain type, not an argument list, \
             and a feed with two arguments would only make the item an anonymous \
             tuple."
        ));
    };
    // One complete wire declaration, which closes the attribute run above it.
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ $kind:ident $rpc:ident ( $( $args:tt )* ) -> $ret:ty ; $( $rest:tt )* ]
    ) => {
        $crate::__keylink_service_munch! {
            @svc $svc
            @done [
                $( $done )*
                { [ $( $attrs )* ] [ $( $seen )* ] $kind $rpc ( $( $args )* ) -> $ret }
            ]
            @feeds [ $( $feeds )* ]
            @attrs [ ] @defer [ ]
            @rest [ $( $rest )* ]
        }
    };
    // Done, with at least one wire declaration and no attribute left dangling.
    (
        @svc [ $svc:ident ] @done [ $( $done:tt )+ ] @feeds [ $( $feeds:tt )* ]
        @attrs [ ] @defer [ ] @rest [ ]
    ) => {
        $crate::__keylink_service! {
            $svc;
            @wire [ $( $done )+ ]
            @feeds [ $( $feeds )* ]
        }
    };
    // Feeds and nothing else. Refused, and the message says what is missing rather
    // than "at least one declaration is required" — there ARE declarations.
    (
        @svc [ $svc:ident ] @done [ ] @feeds [ $( $feeds:tt )+ ]
        @attrs [ ] @defer [ ] @rest [ ]
    ) => {
        ::core::compile_error!(::core::concat!(
            "`service ",
            ::core::stringify!($svc),
            "` declares only feeds. A `service!` generates a wire contract - a \
             `Request`, a `Response`, an ALPN, a `Client` and a `Handler` - and a \
             feed is local: it has no variant, no schema and no client method, \
             which is what 'feeds are not on the wire' means. So a feeds-only \
             service would be an empty protocol with a state attached. Declare at \
             least one `rpc`, `tell`, `send`, `recv` or `blob` beside the feeds, \
             or write a plain struct on a task."
        ));
    };
    // Anything else is a malformed declaration, and saying so beats "no rule
    // expected this token" pointing into a macro the author did not write.
    ( $( $anything:tt )* ) => {
        ::core::compile_error!(
            "a `service!` body is `service Name;` followed by one or more \
             declarations of the form `kind Method(arg: Type) -> Ret;` or \
             `feed Name(item: Type);`, each optionally preceded by attributes. At \
             least one non-feed declaration is required, and an attribute must be \
             followed by a declaration."
        );
    };
}

/// Everything `service!` generates, from declarations the muncher has already
/// split into pass-through attributes and the interpreted `#[defer]`.
///
/// Two lists, and the split is load-bearing.
///
/// `@wire` is one record per method — `{ [attrs] [defer?] kind Name(args) -> Ret }`.
/// The `[defer?]` group is `[]` or `[defer]`, and it reaches exactly three of the
/// thirteen helper macros — the `Actor` method, the mailbox arm and the served arm.
/// The other ten do not know the attribute exists, because a deferred method's
/// wire types, client methods, blocking method and local `Handle` method are
/// byte-for-byte an ordinary `rpc`'s. That is the same statement as "the ALPN does
/// not move", made in the shape of which macros take a parameter.
///
/// `@feeds` is one record per feed — `{ [attrs] [defer?] feed Name(item: Type) }` —
/// and it expands at exactly four sites: the `Actor` trait's `on_*` method, the
/// `Feed` item enum with its `Turn` impl, the `Feeds` bundle, and the `DECLARES_FEEDS`
/// fold. It reaches **no** wire site at all, which is what makes "a feed is not on the
/// wire" a compile-time property rather than thirteen agreeing macros. The `[defer?]`
/// group is carried only so `__keylink_defer_check!` can refuse it.
#[doc(hidden)]
#[macro_export]
macro_rules! __keylink_service {
    (
        $svc:ident;
        @wire [ $( { [ $( #[$meta:meta] )* ] [ $( $defer:tt )? ]
             $kind:ident $rpc:ident ( $( $arg:ident : $ty:ty ),* $(,)? ) -> $ret:ty } )+ ]
        @feeds [ $( { [ $( #[$fmeta:meta] )* ] [ $( $fdefer:tt )? ]
             feed $feed:ident ( $farg:ident : $fty:ty ) } )* ]
    ) => {
        $crate::pastey::paste! {
        pub mod [<$svc:snake>] {
            #![allow(unused_imports, dead_code, missing_docs)]
            use super::*;

            /// Namespaced by crate, so two crates can define `Inventory` without
            /// their endpoints colliding.
            const PROTOCOL: &str = concat!(env!("CARGO_PKG_NAME"), "/", stringify!($svc));

            // One check per declaration, and the only place `#[defer]` is refused.
            // The three sites that shape a deferred method fall back to the
            // ordinary signatures for a kind that cannot defer, so a mistake here
            // produces exactly one error — the one that says why — instead of that
            // error plus a service whose trait no longer matches its mailbox.
            $( $crate::__keylink_defer_check!([$( $defer )?], [<$kind>], $rpc); )+
            // And once per feed, which is why the feed record carries a `[defer?]`
            // group it otherwise has no use for.
            $( $crate::__keylink_defer_check!([$( $fdefer )?], feed, $feed); )*

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

            /// Whether this service declares a `feed`.
            ///
            /// Folded from the feed list itself rather than from a per-kind helper,
            /// which is the shape of the whole decision: a feed is a second list,
            /// so "are there any" is a repetition over that list and needs no arm
            /// anywhere else.
            const DECLARES_FEEDS: bool = false $( | { let _ = ::core::stringify!($feed); true } )*;

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

            /// The compile-time refusal for the doors that wire no feeds.
            ///
            /// Generic in `A` for the reason [`ServableAsActor`] is: an associated
            /// const of a generic type is evaluated where it is *instantiated*, so
            /// this fires at the call site that spawned an actor without its feeds
            /// and nowhere else. A plain `const` would refuse the whole service at
            /// the `service!` invocation.
            ///
            /// The alternative was letting an unwired feed be a stream that never
            /// yields, which is the silent-fallback shape this crate refuses: an
            /// actor that quietly never sees a fill is indistinguishable from an
            /// exchange that is quiet.
            struct SpawnableUnfed<A>(::core::marker::PhantomData<A>);

            impl<A> SpawnableUnfed<A> {
                const CHECK: () = ::core::assert!(
                    !DECLARES_FEEDS,
                    "this service declares a `feed`, so it cannot be spawned \
                     through a door that wires none - an unwired feed would be a \
                     stream that never yields, which is silence a caller cannot \
                     tell from a quiet feed. Use `actor_wired(state, observers, \
                     Feeds::new(..))` for one actor in this process, or \
                     `spawn_wired(state, observers, Feeds::new(..))` for one \
                     mounted in a System. `Feeds::new` takes one argument per \
                     declared feed, so forgetting one is an argument count."
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

            /// Namespaced like [`PROTOCOL`] and then some: a node can serve this
            /// service both ways at once, so the two need different ALPNs.
            ///
            /// The hashes would differ anyway — one contract names
            /// `WireRequest<Request>` and the other
            /// `WireRequest<Addressed<Request>>` — so this suffix is not what
            /// makes them distinct. It is what makes them *readable*: an ALPN is
            /// the one keylink string that turns up in a packet capture and in a
            /// `DuplicateAlpn` error, and `<crate>/<Service>/keyed/<hash>` says
            /// which of the two you are looking at without hashing anything.
            const KEYED_PROTOCOL: &str =
                concat!(env!("CARGO_PKG_NAME"), "/", stringify!($svc), "/keyed");

            /// Hashed to produce the keyed ALPN, exactly as [`Contract`] is for
            /// the singleton one.
            ///
            /// Both fields MUST name the `Wire*` types, the same ones the wire
            /// names — and the request arm MUST name `Addressed`, which is what
            /// the keyed wire actually carries. The hand-written actor contract
            /// this replaces declared a bare `ActorRequest` while `serve_one`
            /// read a `WireRequest` off the frame, so its ALPN was derived from a
            /// type that was not what it put on the wire. That is the exact
            /// drift `tests/protocol.rs` refuses for the generated path, and the
            /// hand-written one evaded it by not being generated.
            #[derive(::postcard_schema::Schema)]
            struct KeyedContract {
                request: $crate::Envelope<$crate::WireRequest<$crate::Addressed<Request>>>,
                response: $crate::Envelope<$crate::WireResponse<Response>>,
                /// keylink's own wire surface, for the reason [`Contract`] has
                /// this field.
                kinds: $crate::WireKinds,
            }

            /// `<crate>/<Service>/keyed/<16-hex schema hash>`.
            ///
            /// Derived, never hand-maintained, and distinct from [`alpn`]: a
            /// node can serve this service as a singleton and as an actor system
            /// at the same time, and one router owns one ALPN.
            pub fn keyed_alpn() -> &'static [u8] {
                static CELL: ::std::sync::OnceLock<::std::vec::Vec<u8>> =
                    ::std::sync::OnceLock::new();
                CELL.get_or_init(|| $crate::derive_alpn::<KeyedContract>(KEYED_PROTOCOL))
                    .as_slice()
            }

            /// What one of this service's mailboxes carries: one variant per
            /// declared method, plus the one a request from a peer lands in.
            ///
            /// **Local only, and never serialized.** That is what lets a variant
            /// hold a [`BodyReader`](keylink::BodyReader) and a reply channel at
            /// all — the wire types are [`Request`] and [`Response`], and this is
            /// neither. It derives nothing: no `Debug`, no `Serialize`, no
            /// `Schema`. Nothing hashes it, so nothing about it can move an ALPN.
            ///
            /// Each per-method variant carries that method's own declared
            /// arguments and a [`Reply`](keylink::Reply) of that method's own
            /// return type. There is no shared `Response` to pull a value out of,
            /// so the "wrong variant" failure that cannot happen has no type to
            /// happen in — and the reply is a value the loop **holds** rather
            /// than a sender sealed inside a closure it can only call.
            ///
            /// The attributes written on a method in `service!` are deliberately
            /// **not** forwarded here, exactly as they are not forwarded to
            /// [`Response`]'s variants: they are written for the wire enum, and
            /// a `#[serde(..)]` re-emitted onto an enum with no `Serialize`
            /// derive is `cannot find attribute` pointing at this file.
            ///
            /// # `Served` is one variant and not one per method
            ///
            /// A request that arrived over the wire is already a [`Request`], and
            /// `__actor_turn` is the match that takes one apart. One variant
            /// keeps that decode in one place and keeps the reply typed as the
            /// [`Response`] a peer is owed. A method **named** `Served` would
            /// collide with it; rename the method.
            ///
            /// # `send` and `recv` get variants nothing builds
            ///
            /// [`Handle`] has no `send` and no `recv` method — a `BodyReader`
            /// and a [`Body`](keylink::Body) are a QUIC stream's two halves and
            /// there is no stream in this process — so for a service declaring
            /// one, that variant is dead. It exists because `macro_rules!`
            /// cannot emit a variant for some kinds and not others: a macro call
            /// is not legal where a variant goes, so the alternative is a
            /// token-muncher over the whole declaration list. Both are written to
            /// be *correct* if something ever did build one, which is the honest
            /// alternative to an arm that panics.
            #[doc(hidden)]
            pub enum Mail {
                $(
                    $rpc {
                        /// Where this message came from. Built by keylink and
                        /// handed in; generated code never constructs one.
                        origin: $crate::Origin,
                        $( $arg: $ty, )*
                        /// A `BodyReader` for `send` and `()` for every other
                        /// kind, because only a `send` handler reads an upload
                        /// inside its turn.
                        body: $crate::__keylink_mail_body!([<$kind>]),
                        /// Where this method's own answer goes.
                        reply: $crate::Reply<$crate::__keylink_mail_answer!([<$kind>], $ret)>,
                    },
                )+
                /// One request that arrived from a peer, answered with this
                /// service's own typed [`Response`].
                Served {
                    /// Which peer, for [`Caller`](keylink::Caller) and for the
                    /// idempotency marks a turn makes.
                    origin: $crate::Origin,
                    /// The service's own request, already decoded.
                    request: Request,
                    /// The payload streaming up beside it.
                    body: $crate::BodyReader,
                    /// Where the answer goes, before it is rendered for the wire.
                    reply: $crate::Reply<(Response, $crate::Body)>,
                },
            }

            /// **The** dispatch path: every message an actor of this service can
            /// receive, in one match, running one turn each.
            ///
            /// There used to be two dispatchers and the second one was
            /// `__keylink_local_method!`, whose arms called the trait method
            /// directly and bypassed everything a turn owes a caller. The comment
            /// that admitted it — on `__actor_turn`, where it is now recorded as
            /// history — argued the split was harmless "because the caller is an
            /// argument now". It was not harmless: a `Handle` call ran no
            /// observer chain through the loop's own reply channel, so a method
            /// kind that later wanted to defer its answer, or a feed that later
            /// wanted to share this loop, would have had to be built twice and
            /// would have compiled fine when it was only built once.
            ///
            /// Every arm is [`turn`](keylink::turn) or its deferring twin, which is
            /// where the queue deadline, the panic guard, the observer chain, the
            /// idempotency marks and the reply all are. An arm can get its own
            /// arguments wrong; it cannot get a turn's policy wrong.
            ///
            /// # `Served` decodes the request HERE, and Wave 2c is why
            ///
            /// It used to hand the whole undecoded `Request` to one `turn` and let
            /// a generated `__actor_turn` match on it inside the turn's closure.
            /// That cannot express a deferred method: which of the two turns to run
            /// is decided by the method, the closure runs *inside* the turn already
            /// chosen, and a `macro_rules!` invocation is not legal where a match
            /// arm goes — so the arms are written out here and the per-method
            /// expression is a macro call, exactly as the local arms above are.
            ///
            /// This is forced rather than chosen, and the criterion that forces it
            /// is "the contract hash does not move for adding `#[defer]`": the
            /// `Request` variant of a deferred method must therefore exist, so this
            /// match must have an arm for it, so that arm must be expressible.
            ///
            /// What it also buys: `__actor_turn` is gone, and with it the last
            /// dispatcher that was not this one.
            impl<A: Actor> $crate::Turn<A> for Mail {
                async fn run(
                    self,
                    __state: &mut A,
                    __hooks: &mut $crate::Hooks<A>,
                ) -> ::core::option::Option<$crate::Ended> {
                    match self {
                        $(
                            Mail::$rpc { origin, $( $arg, )* body, reply } =>
                                $crate::__keylink_mail_arm!(
                                    [$( $defer )?],
                                    [<$kind>],
                                    A,
                                    __state,
                                    __hooks,
                                    origin,
                                    ::core::stringify!($rpc),
                                    reply,
                                    body,
                                    [<$rpc:snake>],
                                    ($( $arg ),*)
                                ),
                        )+
                        Mail::Served { origin, request, body, reply } => match request {
                            $(
                                Request::$rpc { $( $arg, )* } =>
                                    $crate::__keylink_served_arm!(
                                        [$( $defer )?],
                                        [<$kind>],
                                        A,
                                        __state,
                                        __hooks,
                                        origin,
                                        ::core::stringify!($rpc),
                                        reply,
                                        body,
                                        [<$rpc:snake>],
                                        Response,
                                        $rpc,
                                        ($( $arg ),*)
                                    ),
                            )+
                        },
                    }
                }
            }

            /// One item from one of this service's `feed`s.
            ///
            /// **Local only, and never serialized**, like [`Mail`] and for a
            /// stronger reason: a feed has no wire form at all. It derives nothing
            /// and no `Contract` field can reach it, which is what makes "adding a
            /// feed does not move the ALPN" a property of the code rather than a
            /// promise — `tests/protocol.rs` pins it by comparing two declarations
            /// of the same service that differ only by a feed.
            ///
            /// One enum across every feed rather than one type per feed, so that
            /// [`Feeds::poll_next`] can rotate among the feeds through a table of
            /// pollers that all answer the same type.
            ///
            /// Uninhabited for a service that declares no feed, which is what lets
            /// the loop's feed arm exist unconditionally: a branch on a value that
            /// cannot be built costs nothing.
            #[doc(hidden)]
            pub enum Feed {
                $(
                    /// One item from this feed, carrying its declared argument.
                    $feed($fty),
                )*
            }

            /// A feed item is a turn, and this is where that stops being a slogan.
            ///
            /// The same trait [`Mail`] implements, so the loop dispatches both
            /// through one call. What differs is what a turn owes anybody:
            /// [`feed_turn`](keylink::feed_turn) runs the handler inside the panic
            /// guard and then the observer chain, and there is no queue deadline to
            /// read, no idempotency mark to make and nobody to answer — a feed has
            /// no [`Origin`](keylink::Origin) and no reply channel, because nobody
            /// called it.
            impl<A: Actor> $crate::Turn<A> for Feed {
                async fn run(
                    self,
                    __state: &mut A,
                    __hooks: &mut $crate::Hooks<A>,
                ) -> ::core::option::Option<$crate::Ended> {
                    match self {
                        $(
                            Feed::$feed($farg) => $crate::feed_turn(
                                __state,
                                __hooks,
                                // The DECLARED name, so `TurnInfo::method` spells a
                                // feed the way it spells an rpc and an operator
                                // reads one metrics label.
                                ::core::stringify!($feed),
                                move |__on: &mut A| ::std::boxed::Box::pin(async move {
                                    __on.[<on_ $feed:snake>]($farg).await
                                }),
                            )
                            .await,
                        )*
                    }
                }
            }

            /// Every feed this service declares, wired to a stream each.
            ///
            /// Built once, at spawn, and handed to
            /// [`actor_wired`] or [`spawn_wired`]. There is no way to add a feed to
            /// a running actor and that is deliberate: the loop selects over a fixed
            /// set, so a feed appearing later would need the loop to be
            /// reconfigurable — a second input to the second input.
            ///
            /// # `new` takes one argument per feed, which is the guard
            ///
            /// Forgetting to wire a declared feed is `error[E0061]` at the call site
            /// rather than a stream that never yields. That is why this is a
            /// constructor and not a builder with one method per feed: a builder can
            /// always be stopped early, and the sketch this replaces —
            /// `actor(state).feed_fills(rx)` — cannot even be written, because
            /// `actor` has already spawned the loop by then.
            ///
            /// Each stream is boxed. One allocation per feed per actor, paid once at
            /// spawn, against making `Handle`, `Mounted` and `System` generic in one
            /// type parameter per declared feed.
            #[doc(hidden)]
            pub struct Feeds {
                $( [<$feed:snake>]: ::core::pin::Pin<
                    ::std::boxed::Box<dyn $crate::Stream<Item = $fty> + ::core::marker::Send>
                >, )*
                /// Which feed gets first refusal on the next poll, so a saturated
                /// feed cannot starve its siblings. See [`Feeds::poll_next`].
                at: ::core::primitive::usize,
            }

            impl Feeds {
                /// Wire every declared feed. One argument per `feed`, in
                /// declaration order.
                #[must_use]
                pub fn new(
                    $( [<$feed:snake>]: impl $crate::Stream<Item = $fty>
                        + ::core::marker::Send + 'static, )*
                ) -> Self {
                    Self {
                        $( [<$feed:snake>]: ::std::boxed::Box::pin([<$feed:snake>]), )*
                        at: 0,
                    }
                }

                /// One closure per feed, all answering the same type, so the
                /// rotation below can be an index rather than a match written per
                /// pair of feeds.
                ///
                /// Function pointers and not a `match` on the cursor, because
                /// `macro_rules!` has no counter: numbering the arms of a match
                /// would need the declarations munched into unary indices, and an
                /// array literal needs none.
                const POLLERS: &'static [fn(
                    &mut Self,
                    &mut ::core::task::Context<'_>,
                ) -> ::core::task::Poll<$crate::Fed<Feed>>] = &[
                    $(
                        |__feeds: &mut Self, __cx: &mut ::core::task::Context<'_>| {
                            match $crate::Stream::poll_next(
                                ::core::pin::Pin::as_mut(&mut __feeds.[<$feed:snake>]),
                                __cx,
                            ) {
                                // Boxed HERE, because an item spelled out in the
                                // loop's future is a cost every idle actor of this
                                // service pays — see `Fed::Item`.
                                ::core::task::Poll::Ready(::core::option::Option::Some(
                                    __item,
                                )) => ::core::task::Poll::Ready($crate::Fed::Item(
                                    ::std::boxed::Box::new(Feed::$feed(__item)),
                                )),
                                // The stream is done, which ends the actor. Never
                                // polled again, so no `poll_next` after `None`.
                                ::core::task::Poll::Ready(::core::option::Option::None) =>
                                    ::core::task::Poll::Ready($crate::Fed::Ended {
                                        name: ::core::stringify!($feed),
                                    }),
                                ::core::task::Poll::Pending =>
                                    ::core::task::Poll::Pending,
                            }
                        },
                    )*
                ];
            }

            impl<A: Actor> $crate::Feeds<A> for Feeds {
                type Item = Feed;

                fn poll_next(
                    &mut self,
                    __cx: &mut ::core::task::Context<'_>,
                ) -> ::core::task::Poll<$crate::Fed<Feed>> {
                    let __n = Self::POLLERS.len();
                    // Starts where the last item came from plus one, so `n` feeds
                    // that are all ready are served round-robin instead of the
                    // first one starving the rest. Zero feeds runs zero steps,
                    // which is why the modulo below cannot divide by zero.
                    for __step in 0..__n {
                        let __i = (self.at + __step) % __n;
                        if let ::core::task::Poll::Ready(__fed) = Self::POLLERS[__i](self, __cx) {
                            self.at = (__i + 1) % __n;
                            return ::core::task::Poll::Ready(__fed);
                        }
                    }
                    ::core::task::Poll::Pending
                }
            }

            /// The receiving half of one actor's mailbox.
            #[doc(hidden)]
            pub type Mailbox = $crate::Mailbox<Mail>;

            /// A registry of **this service's** live actors: which ones exist,
            /// and for how long.
            ///
            /// One system per service, not one per node. That is what makes an
            /// actor of another service a type error at the mount rather than a
            /// schema comparison on the serving node.
            pub type System = $crate::System<Mail>;

            /// Assembles a [`System`]: its eviction policy, its cap, and how it
            /// spawns.
            pub type SystemBuilder = $crate::SystemBuilder<Mail>;

            /// One live actor of this service, ready to be mounted.
            pub type Mounted = $crate::Mounted<Mail>;

            /// What keylink dispatches this service's keyed system through.
            ///
            /// A marker with no values. The generated [`Actor`] trait has no
            /// associated types and is expanded here, in your crate, so keylink
            /// cannot reach this service's `Request`, `Response` or mailbox type
            /// through it; [`KeyedService`](keylink::KeyedService) is the trait
            /// that carries them across, and this is what implements it.
            #[doc(hidden)]
            pub struct Actors;

            impl $crate::KeyedService for Actors {
                type Request = Request;
                type Response = Response;
                type Mail = Mail;

                fn mail(
                    origin: $crate::Origin,
                    request: Request,
                    body: $crate::BodyReader,
                    reply: $crate::Reply<(Response, $crate::Body)>,
                ) -> Mail {
                    Mail::Served { origin, request, body, reply }
                }
            }

            /// Serve `system`'s actors to other machines, on this service's own
            /// keyed ALPN. Register it on a [`Server`](keylink::Server) like any
            /// other service.
            ///
            /// A free function taking `&System`, deliberately: it compiles
            /// identically whether the registry lives in keylink or in a crate
            /// of its own, which an inherent method on `System` cannot do.
            ///
            /// Because the reply is this service's own typed `Response`,
            /// everything that reads one works here: per-method
            /// [`Middleware`](keylink::Middleware), per-method authorization,
            /// and `blob` methods, whose address the server must see to record a
            /// grant. A service with a `blob` method must be given a store —
            /// `keyed_service(..).with_blobs(&blobs)` — or
            /// [`ServerBuilder::spawn`](keylink::ServerBuilder::spawn) refuses
            /// it at startup, exactly as it does for [`service`].
            #[must_use]
            pub fn keyed_service(
                system: &System,
                allow: $crate::Allowlist,
            ) -> $crate::ServiceBuilder<$crate::Addressed<Request>, Response> {
                let system = system.clone();
                let dispatch: $crate::Dispatch<$crate::Addressed<Request>, Response> =
                    ::std::sync::Arc::new(move |cx: $crate::Context,
                                                request: $crate::Addressed<Request>,
                                                body: $crate::BodyReader| {
                        let system = system.clone();
                        ::std::boxed::Box::pin(async move {
                            match $crate::deliver_to_actor::<Actors>(&system, cx, request, body)
                                .await
                            {
                                ::std::result::Result::Ok(__answer) => __answer,
                                ::std::result::Result::Err(__refusal) => (
                                    $crate::Outcome::Err(__refusal),
                                    $crate::Body::empty(),
                                ),
                            }
                        })
                    });
                $crate::ServiceBuilder::new(keyed_alpn(), allow, dispatch)
                    .__keylink_declares_blobs(DECLARES_BLOBS)
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
            /// # A `feed` method takes no `Caller` and returns nothing
            ///
            /// Written out here rather than in a helper macro, because there is one
            /// shape and no kind to dispatch on — which is the point of a feed being
            /// a second list.
            ///
            /// **No `Caller`, because nobody called.** A feed item came off a
            /// stream this process wired; there is no local caller to name and no
            /// peer to authorize against. Handing one in would mean inventing a
            /// caller, and `Caller::Local` on a market-data item is exactly the
            /// stale-peer hazard the `caller(&mut self, ..)` hook was deleted for.
            ///
            /// **No return, because there is no reply channel.** Nothing is waiting.
            /// A feed that needs to answer somebody is an `rpc` — the distinction is
            /// type-level rather than conventional, which is why it is a different
            /// declaration and not a convention about a `tell`.
            ///
            /// It is a turn in every other respect: the panic guard, the observer
            /// chain and a durability adapter's commit all apply, because the loop is
            /// still keylink's. A panic here ends the actor rather than costing one
            /// message — see [`feed_turn`](keylink::feed_turn) for why the `rpc`
            /// policy cannot hold where nobody is told.
            pub trait Actor: Send + 'static {
                $(
                    $crate::__keylink_actor_method!(
                        [$( $defer )?],
                        [$( #[$meta] )*],
                        [<$kind>], [<$rpc:snake>], ($( $arg : $ty ),*), $ret
                    );
                )+
                $(
                    $( #[$fmeta] )*
                    #[doc = concat!("Handle one item from the `", stringify!($feed), "` feed.")]
                    ///
                    /// No [`Caller`](keylink::Caller): nobody called. No return:
                    /// there is no reply channel. See this trait's own
                    /// documentation.
                    fn [<on_ $feed:snake>](&mut self, $farg: $fty)
                        -> impl ::core::future::Future<Output = ()> + ::core::marker::Send;
                )*
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
            /// # No observers and no feeds on this door
            ///
            /// A shorthand rather than a limit, which it was until Wave 3. Use
            /// [`spawn_wired`] for a keyed actor that must be observed, or that has
            /// a feed — and a service that declares a feed cannot reach *this* door
            /// at all, because `SpawnableUnfed` refuses it at the call site.
            ///
            /// What it no longer is, is a loop. The keyed loop used to read the
            /// queue deadline, mark idempotency progress, guard the panic and render
            /// the refusal itself, in ~150 lines that had to agree with `local.rs`'s
            /// and twice did not: one spelled the deadline `>` where the other
            /// spelled `>=`, and one rendered a panic bare where the other prefixed
            /// it. Wave 1b made the *dispatch* one path and left two loops around
            /// it; there is one loop now, and this is one call into it.
            ///
            /// Nothing runs until this is mounted:
            /// [`System::mount`](keylink::System::mount) is what starts the
            /// task, so a `Mounted` that is refused or dropped costs no task at
            /// all. That is where the tokio-runtime panic lives now, and it is
            /// why this function has none.
            #[must_use = "an unmounted actor is dropped, and the state it was \
                          built around goes with it"]
            pub fn spawn<A: Actor>(state: A) -> Mounted {
                // The same refusal `Handle::service` makes, and for longer than
                // it was made here: `CHECK` is an associated const on a generic
                // type, so it is evaluated only where it is INSTANTIATED, and
                // for a long time the one instantiation was in `Handle::service`.
                // A service declaring `send` was therefore refused by the local
                // handle at compile time and accepted here without a word —
                // where the body is read inside the serial turn exactly as it
                // would have been, so a peer declaring a large body and then
                // not writing it holds the actor until iroh's idle timeout.
                //
                // Not bounded by a layer, and not by a `QueueTimeout` either:
                // that refuses a message still QUEUED, and this one has already
                // started. RFC-04 §10.4 argues the whole thing and named only
                // the path that was guarded. §16.9.
                let () = ServableAsActor::<A>::CHECK;
                let () = SpawnableUnfed::<A>::CHECK;
                __mount(state, ::std::vec::Vec::new(), ())
            }

            /// Spawn one keyed actor with its observers and its feeds.
            ///
            /// **The general keyed door, and the one that closed the observer gap.**
            /// A keyed actor mounted through a [`System`](keylink::System) is watched
            /// by whatever this was given: the same [`Observer`](keylink::Observer)
            /// chain, the same panic guard and the same
            /// [`finish`](keylink::Observer::finish) a local actor gets, because it
            /// is the same loop. RFC-06 §5.3's "no observers on the keyed path" is
            /// no longer true and the paragraph that said so is in the past tense.
            ///
            /// State, observers and feeds together, which is the shape a factory
            /// needs — [`spawn_with`](keylink::SystemBuilder::spawn_with) builds an
            /// actor on the first message to an unmounted id, and everything that
            /// actor needs has to be produced by that one closure:
            ///
            /// ```ignore
            /// let system = orders::System::builder(Eviction::Never)
            ///     .spawn_with(move |id| {
            ///         Some(orders::spawn_wired(
            ///             Book::new(),
            ///             vec![Box::new(metrics.watching(id))],
            ///             orders::Feeds::new(exchange.subscribe(id)),
            ///         ))
            ///     })
            ///     .build();
            /// ```
            ///
            /// # A factory is synchronous, and a durable actor is the case that hits it
            ///
            /// That closure is a plain `Fn`, so it cannot `await`. A
            /// [`Persist`](keylink::Persist) has to claim its state before it can
            /// observe anything, and claiming is I/O — so a durable keyed actor is
            /// mounted **eagerly**, with
            /// [`System::mount`](keylink::System::mount) after
            /// `Persist::open(..).await`, rather than built on first message.
            /// `keylink/tests/persist.rs::a_keyed_actors_state_survives_its_own_process`
            /// is that shape. It is a real limit, discovered by the wave that wrote
            /// the first adapter, and it is stated here rather than left for the next
            /// author to find at the borrow checker.
            ///
            /// # What a keyed actor still does not get
            ///
            /// An `ended()` channel. The reason an actor stopped — an observer's
            /// refusal, a fault, a feed that ended — reaches its own observers'
            /// `finish` and nothing else, so a durability adapter still commits but
            /// nobody outside can await *why*. `System::stop` and `System::abort`
            /// await the task, not a reason.
            ///
            /// # Panics
            ///
            /// Panics when called outside a tokio runtime.
            #[must_use = "an unmounted actor is dropped, which stops the task \
                          this just started"]
            pub fn spawn_wired<A: Actor>(
                state: A,
                observers: ::std::vec::Vec<::std::boxed::Box<dyn $crate::Observer<A>>>,
                feeds: Feeds,
            ) -> Mounted {
                let () = ServableAsActor::<A>::CHECK;
                __mount(state, observers, feeds)
            }

            /// What both keyed doors are, and the only place this service names
            /// [`mount`](keylink::mount).
            ///
            /// Generic in the feeds so that [`spawn`]'s `()` and [`spawn_wired`]'s
            /// [`Feeds`] go through one body: a second body is a second loop by
            /// another name, and this wave exists to have one.
            fn __mount<A: Actor, F: $crate::Feeds<A>>(
                state: A,
                observers: ::std::vec::Vec<::std::boxed::Box<dyn $crate::Observer<A>>>,
                feeds: F,
            ) -> Mounted {
                $crate::mount(move |inbox: Mailbox| async move {
                    $crate::run_keyed(state, observers, feeds, inbox).await;
                })
            }

            /// Spawn one **supervised** keyed actor, ready to be mounted.
            ///
            /// [`spawn_wired`] with the triple produced by an `async` factory
            /// rather than written at the call site — which is also what makes
            /// this the keyed door a *durable* actor can use lazily. The
            /// factory [`spawn_with`](keylink::SystemBuilder::spawn_with) takes
            /// is a plain `Fn` and cannot `await`, so a keyed durable actor had
            /// to be mounted eagerly after `Persist::open(..).await`; this one
            /// awaits, once per incarnation, which is the same call the first
            /// mount makes.
            ///
            /// Nothing changes in the registry: [`mount`](keylink::mount) takes
            /// a closure over a mailbox, and the supervisor is that closure's
            /// whole body. `System::mount`, eviction, `shutdown` and
            /// `System::status` see one task, exactly as for an unsupervised
            /// keyed actor.
            ///
            /// # A keyed actor still has no `ended()`
            ///
            /// So [`Ended::GaveUp`](keylink::Ended::GaveUp) reaches its own
            /// observers' `finish` and nothing a caller can await. An actor that
            /// has given up is an entry whose task has exited, which the
            /// registry sees as an actor to re-route on the next message.
            ///
            /// # Panics
            ///
            /// Panics when called outside a tokio runtime.
            #[must_use = "an unmounted actor is dropped, and the supervisor \
                          that would have kept it alive goes with it"]
            pub fn spawn_supervised<A: Actor, N, Fut, F>(
                factory: N,
                limit: $crate::Intensity,
            ) -> Mounted
            where
                N: ::core::ops::FnMut() -> Fut + ::core::marker::Send + 'static,
                Fut: ::core::future::Future<
                        Output = ::core::result::Result<
                            $crate::Incarnation<A, F>,
                            $crate::Stop,
                        >,
                    > + ::core::marker::Send + 'static,
                F: $crate::Feeds<A>,
            {
                let () = ServableAsActor::<A>::CHECK;
                $crate::mount(move |inbox: Mailbox| async move {
                    $crate::run_keyed_supervised(factory, limit, inbox).await;
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
                let () = SpawnableUnfed::<A>::CHECK;
                Handle { inner: $crate::LocalActor::spawn_fed(state, ::std::vec::Vec::new(), ()) }
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
            /// // `Persist` is keylink's own adapter, behind the `persist-file`
            /// // feature. It named a `keylink_durable` crate here until Wave 5
            /// // decided one adapter is a module and not a crate.
            /// let (restored, saver) = keylink::Persist::<Ledger, _>::open(&store, &id).await?;
            /// let ledger = ledger::actor_with(restored.unwrap_or_default(), vec![Box::new(saver)]);
            /// ledger.post(1).await?;   // stored, and fsynced, before this returns
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
                let () = SpawnableUnfed::<A>::CHECK;
                Handle { inner: $crate::LocalActor::spawn_fed(state, observers, ()) }
            }

            /// Start one actor of this service with observers **and** feeds.
            ///
            /// The general local door: the triple a keyed factory returns, on the
            /// path that has a [`Handle`]. [`actor`] and [`actor_with`] are
            /// shorthands for it, and a service declaring a `feed` can use neither.
            ///
            /// ```ignore
            /// let orders = orders::actor_wired(
            ///     Book::new(),
            ///     vec![Box::new(saver)],
            ///     orders::Feeds::new(exchange.subscribe()),
            /// );
            /// orders.cancel(id).await?;      // the mailbox, as always
            /// ```
            ///
            /// A feed item and a mailbox message are ordered at **one** `select`, so
            /// the class of bug where an event is applied after the command that
            /// should have preempted it does not arise: there is one arrival order,
            /// not two.
            ///
            /// [`Feeds::new`] takes one argument per declared `feed`, in declaration
            /// order, so a feed left unwired is an argument count rather than a
            /// stream that never yields.
            ///
            /// # Panics
            ///
            /// Panics when called outside a tokio runtime, like every other
            /// spawn.
            #[must_use]
            pub fn actor_wired<A: Actor>(
                state: A,
                observers: ::std::vec::Vec<::std::boxed::Box<dyn $crate::Observer<A>>>,
                feeds: Feeds,
            ) -> Handle<A> {
                Handle { inner: $crate::LocalActor::spawn_fed(state, observers, feeds) }
            }

            /// Start one actor of this service that is **rebuilt** when an
            /// incarnation fails.
            ///
            /// [`actor_wired`]'s three arguments, produced by one `async`
            /// closure instead of written at the call site, so that every
            /// incarnation gets its own state, its own observers and its own
            /// feed subscriptions — and the previous ones are already dropped
            /// when it is called.
            ///
            /// ```ignore
            /// let ledger = ledger::supervised(
            ///     move || {
            ///         let store = store.clone();
            ///         let id = id.clone();
            ///         async move {
            ///             // The claim the last incarnation held is already free.
            ///             let (restored, saver) =
            ///                 keylink::Persist::<Ledger, _>::open(&store, &id).await?;
            ///             Ok((restored.unwrap_or_default(), vec![Box::new(saver) as _], ()))
            ///         }
            ///     },
            ///     Intensity::new(NonZeroU32::new(3).unwrap(), Duration::from_secs(10)),
            /// );
            /// ```
            ///
            /// The feeds are whatever the factory returns: `()` for a service
            /// with none, and [`Feeds::new`] for one that declares them. That is
            /// [`actor`], [`actor_with`] and [`actor_wired`] collapsed into one
            /// door rather than three, because a factory names its own triple
            /// and there is nothing left for a shorthand to leave out.
            ///
            /// See [`LocalActor::spawn_supervised`](keylink::LocalActor::spawn_supervised)
            /// for what a restart is worth, what it costs, and the four
            /// properties that fall out of supervising in the actor's own task.
            ///
            /// # Panics
            ///
            /// Panics when called outside a tokio runtime, like every other
            /// spawn.
            #[must_use]
            pub fn supervised<A: Actor, N, Fut, F>(
                factory: N,
                limit: $crate::Intensity,
            ) -> Handle<A>
            where
                N: ::core::ops::FnMut() -> Fut + ::core::marker::Send + 'static,
                Fut: ::core::future::Future<
                        Output = ::core::result::Result<
                            $crate::Incarnation<A, F>,
                            $crate::Stop,
                        >,
                    > + ::core::marker::Send,
                F: $crate::Feeds<A>,
            {
                Handle { inner: $crate::LocalActor::spawn_supervised(factory, limit) }
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
                inner: $crate::LocalActor<A, Mail>,
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
                /// [`Middleware`](keylink::Middleware) and `blob` methods,
                /// whose address the server must see to record a grant.
                ///
                /// **That list used to be a difference from the keyed system,
                /// and it is not any more.** It said the keyed path could offer
                /// neither, RFC-04 §14.8, because there the payload was opaque
                /// to keylink by design. A keyed request is
                /// [`Addressed<Request>`](keylink::Addressed) now and its reply
                /// is this same typed `Response`, so per-method layers, per-method
                /// authorization and `blob` methods all work through
                /// [`keyed_service`] too. What is left of §14.8 is one QUIC
                /// connection per actor *service* rather than one for a node's
                /// whole fleet — and observers came off that list as well, since
                /// [`spawn_wired`] runs the same loop this handle does. What a
                /// keyed actor still lacks is an `ended()` channel, so nobody
                /// outside can await *why* one stopped.
                ///
                /// The idempotency ledger was never on that list, which is what
                /// this paragraph used to say and is worth keeping: a key is
                /// claimed and settled off the *envelope*, and the envelope was
                /// typed on both paths all along. What the keyed path lacked was
                /// a way to *send* one, and
                /// [`KeyedClient::with_idempotency`] is that way.
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
                            ::std::boxed::Box::pin(async move {
                                // Everything this used to do by hand — three
                                // clones of `Progress` for three facts, the
                                // queue deadline, the five-arm match that turned
                                // a `Queued`/`TurnLost` back into an `Outcome` —
                                // lives in `deliver_to_local` now, beside the
                                // keyed twin it kept disagreeing with. What is
                                // left here is the one thing only generated code
                                // can do: name the variant.
                                //
                                // The `Origin` is keylink's to build. Both of
                                // its constructors are `pub(crate)`, which is
                                // what stops a downstream crate making a second
                                // origin around a `Context` it already
                                // delivered — so it is handed IN to this
                                // closure rather than written here.
                                $crate::deliver_to_local(
                                    &actor,
                                    cx,
                                    stringify!($svc),
                                    move |__from, __reply| Mail::Served {
                                        origin: __from,
                                        request: req,
                                        body,
                                        reply: __reply,
                                    },
                                )
                                .await
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

            /// A connection to one node's actors **of this service**.
            ///
            /// One connection carries every actor of this service on that node,
            /// and no actor of any other. That is the bill the per-service ALPN
            /// comes with, and it is worth stating plainly: two actor services on
            /// one node are two QUIC connections, each with its own handshake,
            /// admission decision, idempotency ledger, keep-alive and congestion
            /// window. A key claimed against one service means nothing to
            /// another.
            ///
            /// RFC-04 §10.2's O(N²) argument is about `Endpoint::set_alpns` and
            /// the number of ALPNs a node announces — one per hand-written
            /// service, a bounded number — and **not** about connection count.
            /// Do not conflate them.
            ///
            /// Address one actor with [`at`], which gives back a typed handle
            /// whose methods are the ones written in `service!`.
            pub struct KeyedClient {
                inner: $crate::RpcClient<$crate::Addressed<Request>, Response>,
                /// Attached to every call [`ActorRef`] makes through this
                /// handle. Held here rather than inside the client, because
                /// `RpcClient::with_idempotency` gives back an
                /// [`IdempotentClient`](keylink::IdempotentClient) — a type
                /// with no `tell`, which is the point of it — and there is no
                /// `tell` on this path to protect. So the key rides alongside
                /// and `send_to` attaches it, instead of a second keyed twin of
                /// this type and of [`ActorRef`] existing to hold it.
                idempotency: ::core::option::Option<$crate::Idempotency>,
            }

            /// Every clone names the same connection. Written out rather than
            /// derived for the reason [`Client`]'s is: a derive would demand
            /// `Request: Clone, Response: Clone`, and neither appears in a field.
            impl ::core::clone::Clone for KeyedClient {
                fn clone(&self) -> Self {
                    Self {
                        inner: self.inner.clone(),
                        idempotency: self.idempotency.clone(),
                    }
                }
            }

            impl ::core::fmt::Debug for KeyedClient {
                fn fmt(&self, f: &mut ::core::fmt::Formatter<'_>) -> ::core::fmt::Result {
                    f.debug_struct("KeyedClient")
                        .field("service", &stringify!($svc))
                        .finish_non_exhaustive()
                }
            }

            impl KeyedClient {
                /// Dial a peer's actor system for this service, by public key.
                pub async fn connect(
                    node: &$crate::Node,
                    peer: impl Into<$crate::EndpointAddr>,
                ) -> $crate::Result<Self> {
                    Ok(Self {
                        inner: $crate::RpcClient::connect(node, peer, keyed_alpn()).await?,
                        idempotency: ::core::option::Option::None,
                    })
                }

                /// The same connection, with an idempotency key on every call
                /// made through the handle it returns.
                ///
                /// **This is the capability a keyed actor was missing**, and it
                /// was never the ledger: a key is claimed and settled off the
                /// envelope, and the envelope was typed on both paths, so the
                /// ledger has always worked here. What did not exist was a
                /// generated client that could attach one — a key reached a
                /// keyed actor only from a peer building the envelope through
                /// [`RpcClient`](keylink::RpcClient) by hand.
                ///
                /// See the singleton twin,
                /// [`Client::with_idempotency`], for what a bare key does on
                /// collision and how to wait for the call already in flight
                /// instead.
                #[must_use]
                pub fn with_idempotency(
                    &self,
                    idempotency: impl ::core::convert::Into<$crate::Idempotency>,
                ) -> Self {
                    Self {
                        inner: self.inner.clone(),
                        idempotency: ::core::option::Option::Some(idempotency.into()),
                    }
                }

                /// Ask what this peer's ledger knows about one of your keys.
                ///
                /// A ledger is per service, so this answers about **this
                /// service's** keyed connection and nothing else. See
                /// [`RpcClient::key_state`](keylink::RpcClient::key_state).
                pub async fn key_state(
                    &self,
                    key: $crate::IdempotencyKey,
                ) -> $crate::Result<$crate::KeyState> {
                    self.inner.key_state(key).await
                }

                /// Await this peer's departure instead of meeting it on the
                /// next call. See [`Watch`](keylink::Watch).
                // No `#[must_use]`: `Watch` already carries one.
                pub fn watch(&self) -> $crate::Watch {
                    self.inner.watch()
                }

                /// Close the connection gracefully.
                pub async fn close(self) -> $crate::Result<()> {
                    self.inner.close().await
                }

                /// Send one message to one actor. Called by [`ActorRef`], which
                /// is the only thing that should build an [`Addressed`].
                async fn send_to(
                    &self,
                    actor: &$crate::ActorId,
                    request: Request,
                    body: $crate::Body,
                ) -> $crate::Result<(Response, $crate::BodyReader)> {
                    let envelope = $crate::Addressed::new(actor.clone(), request);
                    match &self.idempotency {
                        ::core::option::Option::Some(key) => {
                            self.inner
                                .with_idempotency(key.clone())
                                .call(envelope, body)
                                .await
                        }
                        ::core::option::Option::None => self.inner.call(envelope, body).await,
                    }
                }

                /// Point a handle at content this peer just named. Called by a
                /// generated `blob` method on [`ActorRef`].
                fn blob_handle(&self, id: $crate::BlobId) -> $crate::BlobHandle {
                    self.inner.blob_handle(id)
                }
            }

            impl $crate::Dial for KeyedClient {
                async fn dial(
                    node: &$crate::Node,
                    addr: $crate::EndpointAddr,
                ) -> $crate::Result<Self> {
                    Self::connect(node, addr).await
                }
            }

            /// Address one actor of this service on a connected node.
            ///
            /// The handle borrows the connection, so every actor of this service
            /// on that node is reached over one connection with one
            /// congestion-control state — which is what a per-actor ALPN would
            /// have thrown away.
            pub fn at(
                client: &KeyedClient,
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
                client: &'a KeyedClient,
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
                ///
                /// Returns an [`IdempotentClient`], which has **no `tell`
                /// method** — see that type for why the difference is a type
                /// rather than a paragraph.
                #[must_use]
                pub fn with_idempotency(
                    &self,
                    idempotency: impl ::core::convert::Into<$crate::Idempotency>,
                ) -> IdempotentClient {
                    IdempotentClient {
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

            /// The same connection, scoped to one idempotency key.
            ///
            /// Built by [`Client::with_idempotency`]. Its methods are
            /// [`Client`]'s, minus every `tell`: a key promises the work runs
            /// at most once, and the ledger keeps that promise by recording the
            /// reply and answering a repeat with it, so a method with no reply
            /// has nothing to record and could only discard the key.
            ///
            /// It used to be `Client` itself, and
            /// `client.with_idempotency(key).some_tell(..)` therefore compiled
            /// and sent no key at all. See
            /// [`IdempotentClient`](keylink::IdempotentClient).
            pub struct IdempotentClient {
                inner: $crate::IdempotentClient<Request, Response>,
            }

            /// Every clone names the same connection and carries the same key.
            /// Written out rather than derived for the reason [`Client`]'s is.
            impl ::core::clone::Clone for IdempotentClient {
                fn clone(&self) -> Self {
                    Self { inner: self.inner.clone() }
                }
            }

            impl IdempotentClient {
                /// Ask what this peer's ledger knows about one of your keys.
                ///
                /// See [`Client::key_state`].
                pub async fn key_state(
                    &self,
                    key: $crate::IdempotencyKey,
                ) -> $crate::Result<$crate::KeyState> {
                    self.inner.key_state(key).await
                }

                /// Await this peer's departure instead of meeting it on the
                /// next call. See [`Watch`](keylink::Watch).
                // No `#[must_use]`: `Watch` already carries one, and repeating
                // it here is `clippy::double_must_use`.
                pub fn watch(&self) -> $crate::Watch {
                    self.inner.watch()
                }

                $(
                    $crate::__keylink_idempotent_client_method!(
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
                    /// The returned type has no `tell`, for the reason given on
                    /// [`IdempotentClient`](super::IdempotentClient).
                    #[must_use]
                    pub fn with_idempotency(
                        &self,
                        idempotency: impl ::core::convert::Into<$crate::Idempotency>,
                    ) -> IdempotentClient {
                        IdempotentClient {
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

                /// The same connection, scoped to one idempotency key.
                ///
                /// See the async twin,
                /// [`IdempotentClient`](super::IdempotentClient). No `tell`
                /// here either, for the same reason.
                pub struct IdempotentClient {
                    inner: super::IdempotentClient,
                    runtime: ::std::sync::Arc<$crate::blocking::Runtime>,
                }

                impl IdempotentClient {
                    /// Ask what this peer's ledger knows about one of your keys.
                    ///
                    /// See the async twin,
                    /// [`IdempotentClient::key_state`](super::IdempotentClient::key_state).
                    pub fn key_state(
                        &self,
                        key: $crate::IdempotencyKey,
                    ) -> $crate::Result<$crate::KeyState> {
                        self.runtime.run(self.inner.key_state(key))?
                    }

                    $(
                        $crate::__keylink_idempotent_blocking_method!(
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

/// One method on a key-scoped client, or nothing at all when it is a `tell`.
///
/// The filter, and the only place the absence is decided. `service!` walks the
/// declared methods once per client type; this arm set is what makes the
/// key-scoped walk skip the kind that cannot carry a key. Everything else
/// delegates to [`__keylink_client_method`], so a key-scoped `rpc` and a plain
/// one are the same code rather than two copies that can drift.
#[doc(hidden)]
#[macro_export]
macro_rules! __keylink_idempotent_client_method {
    // The whole finding, in one arm. `RpcClient::tell` writes `None` for the
    // key and always did; what changed is that a client holding a key no longer
    // has a `tell` to call, so the discard is unreachable rather than silent.
    ([$( #[$meta:meta] )*], tell, $name:ident, $req:ident, $variant:ident, $extract:ident,
        ($( $arg:ident : $ty:ty ),*), $ret:ty) => {};
    ([$( #[$meta:meta] )*], $kind:ident, $name:ident, $req:ident, $variant:ident, $extract:ident,
        ($( $arg:ident : $ty:ty ),*), $ret:ty) => {
        $crate::__keylink_client_method!(
            [$( #[$meta] )*], $kind, $name, $req, $variant, $extract,
            ($( $arg : $ty ),*), $ret
        );
    };
}

/// The blocking twin of [`__keylink_idempotent_client_method`], and it drops the
/// same kind for the same reason.
#[doc(hidden)]
#[macro_export]
macro_rules! __keylink_idempotent_blocking_method {
    ([$( #[$meta:meta] )*], tell, $name:ident, ($( $arg:ident : $ty:ty ),*), $ret:ty) => {};
    ([$( #[$meta:meta] )*], $kind:ident, $name:ident, ($( $arg:ident : $ty:ty ),*), $ret:ty) => {
        $crate::__keylink_blocking_method!(
            [$( #[$meta] )*], $kind, $name, ($( $arg : $ty ),*), $ret
        );
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

/// What one `Mail` variant carries beside its declared arguments.
///
/// `send` reads its upload **inside** the turn, so its variant has to carry the
/// reader. The other four never touch one.
///
/// A unit rather than `Option<BodyReader>`, and the argument is **type honesty**,
/// not size: there is no case where a non-`send` kind *might* have a body — the
/// kind decides it at compile time, and a field that can only ever be one thing
/// should be the type of that thing. An `Option` would invite an arm for a `None`
/// that cannot occur, which is the shape `__keylink_mail_arm!` destructures the
/// unit to refuse.
///
/// # The size argument this used to make is measurably false
///
/// It read: paying `size_of::<BodyReader>()` on every variant "would be a tax on
/// the mailbox — which is sized to its largest variant and eagerly allocated 32
/// deep". Every clause is true and the conclusion does not follow, because the
/// mailbox is **already** sized to `Mail::Served`, which carries a `BodyReader`
/// unconditionally. Measured on `tests/actor_memory.rs`'s `ledger`, which declares
/// one `u64` method: `size_of::<ledger::Mail>()` is 248 bytes, and 248 is exactly
/// `Served` — `Origin` 120 + `Request` 8 + `BodyReader` 96 + `Reply` 24 — against
/// 152 for the per-method variant. `Option<BodyReader>` is also 96 bytes, the
/// niche being free, so widening all four unit fields would leave the enum at 248.
/// The tax is zero.
///
/// Recorded rather than deleted, because a wrong reason on a hot-path type is
/// worse than no reason: the next wave cites it instead of measuring.
#[doc(hidden)]
#[macro_export]
macro_rules! __keylink_mail_body {
    (rpc) => {
        ()
    };
    (tell) => {
        ()
    };
    (send) => {
        $crate::BodyReader
    };
    (recv) => {
        ()
    };
    (blob) => {
        ()
    };
}

/// What one `Mail` variant's reply channel carries.
///
/// The **handler's** own return type, which is not the same as the wire
/// `Response`'s payload: a `recv` handler returns `($ret, Body)` and the wire
/// carries only `$ret`, because the body travels beside the message rather than
/// inside it. `__keylink_response_ty!` answers the other question, and the two
/// deliberately disagree for exactly that kind.
#[doc(hidden)]
#[macro_export]
macro_rules! __keylink_mail_answer {
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
        ($ret, $crate::Body)
    };
    (blob, $ret:ty) => {
        ($ret, $crate::BlobId)
    };
}

/// One arm of the `Mail` match, shaped by `#[defer]` and by the rpc kind.
///
/// Three of the four arms differ only in what reaches the handler, and that is the
/// point: a kind differs in what its variant *carries*, which
/// `__keylink_mail_body!` and `__keylink_mail_answer!` already decided, so only
/// `send` — the one kind that hands its reader to the handler — and `#[defer]` —
/// the one attribute that hands over the reply — need arms of their own.
///
/// Every arm is one call to [`turn`](crate::turn) or [`turn_deferred`](crate::turn_deferred)
/// and nothing else. A kind added here without the queue deadline, the panic guard,
/// the observer chain or the idempotency marks is not a thing that can be written:
/// they are inside that call, not beside it.
///
/// An expression and not a statement, because a macro cannot expand to a match arm.
/// That constraint is also what makes the `Served` arms per-method in the caller
/// rather than a match inside one turn — see `Turn for Mail`.
///
/// The origin is passed to the closure **by value** and borrowed inside. A
/// `Caller` borrowed in the loop and captured here would have to outlive every
/// lifetime `turn`'s higher ranked bound quantifies over, and RFC-07 §1.6 proves
/// a peer's `Caller` never reaches `'static`.
///
/// `$declared` is the method's **wire** name and `$method` its snake-case trait
/// method: one is what a `TurnInfo` reports and the other is what gets called.
/// Two parameters rather than one derivation, because `pastey` can go from
/// `Reserve` to `reserve` and nothing can reliably go back — `GetItem` is the
/// counter-example.
#[doc(hidden)]
#[macro_export]
macro_rules! __keylink_mail_arm {
    // A deferred `rpc`: the reply goes to the handler and comes back inside the
    // `Replied` token, which is what lets `turn_deferred` still put the observer
    // chain between an answer and its caller.
    ([defer], rpc, $actor:ty, $state:expr, $hooks:expr, $origin:expr, $declared:expr,
        $reply:expr, $body:expr, $method:ident, ($( $arg:ident ),*)) => {{
        let () = $body;
        $crate::turn_deferred(
            $state,
            $hooks,
            $origin,
            $declared,
            $reply,
            move |__from: $crate::Origin, __on: &mut $actor, __reply| {
                ::std::boxed::Box::pin(async move {
                    __on.$method(__from.caller(), __reply $(, $arg )*).await
                })
            },
        )
        .await
    }};
    // `#[defer]` on a kind that cannot defer: `__keylink_defer_check!` is emitting
    // the error, and this arm keeps the mailbox agreeing with the trait so that
    // error is the only one.
    ([defer], $kind:ident, $actor:ty, $state:expr, $hooks:expr, $origin:expr, $declared:expr,
        $reply:expr, $body:expr, $method:ident, ($( $arg:ident ),*)) => {
        $crate::__keylink_mail_arm!(
            [], $kind, $actor, $state, $hooks, $origin, $declared, $reply, $body, $method,
            ($( $arg ),*)
        )
    };
    ([], send, $actor:ty, $state:expr, $hooks:expr, $origin:expr, $declared:expr, $reply:expr,
        $body:expr, $method:ident, ($( $arg:ident ),*)) => {
        $crate::turn(
            $state,
            $hooks,
            $origin,
            $declared,
            $reply,
            move |__from: $crate::Origin, __on: &mut $actor| {
                ::std::boxed::Box::pin(async move {
                    __on.$method(__from.caller() $(, $arg )*, $body).await
                })
            },
        )
        .await
    };
    ([], $kind:ident, $actor:ty, $state:expr, $hooks:expr, $origin:expr, $declared:expr,
        $reply:expr, $body:expr, $method:ident, ($( $arg:ident ),*)) => {{
        // The unit `__keylink_mail_body!` chose for this kind, destructured
        // rather than dropped: a `let _ = ..` would go on compiling if the field
        // ever became something else, and this says which kinds have no body.
        let () = $body;
        $crate::turn(
            $state,
            $hooks,
            $origin,
            $declared,
            $reply,
            move |__from: $crate::Origin, __on: &mut $actor| {
                ::std::boxed::Box::pin(async move {
                    __on.$method(__from.caller() $(, $arg )*).await
                })
            },
        )
        .await
    }};
}

/// One arm of the `Mail::Served` match: a request from a peer, against one actor.
///
/// The twin of `__keylink_mail_arm!` for the one variant that arrives over the
/// wire, and the replacement for the `__actor_turn` free function plus the
/// `__keylink_actor_invoke_fn!` item per method that Wave 2c deleted. Those two
/// existed because a macro cannot expand to a match arm — but they put the match
/// on `Request` *inside* the turn's closure, and by then which turn to run has
/// already been decided. A deferred method needs that decision made first, so the
/// arms are written out by `service!` and this is the expression each one is.
///
/// Where the reply comes from differs from `__keylink_mail_arm!` and it is the
/// interesting part. A local arm has a `Reply` of the method's own return type; a
/// served arm has one `Reply<(Response, Body)>` for every method, because a peer is
/// owed the service's typed `Response`. For four kinds that is a value the closure
/// builds and `turn` sends. For a `#[defer]` method the handler needs a
/// `Reply<$ret>`, and the answer may arrive when no generated code is running — so
/// the conversion is installed into the channel with
/// [`Reply::wrapping`](crate::Reply::wrapping), which is the only reason that method
/// exists.
///
/// `$body` is the `BodyReader` beside the request. It is dropped for every kind but
/// `send`, which reads it inside the turn.
#[doc(hidden)]
#[macro_export]
macro_rules! __keylink_served_arm {
    ([defer], rpc, $actor:ty, $state:expr, $hooks:expr, $origin:expr, $declared:expr,
        $reply:expr, $body:expr, $method:ident, $resp:ident, $variant:ident,
        ($( $arg:ident ),*)) => {
        $crate::turn_deferred(
            $state,
            $hooks,
            $origin,
            $declared,
            // Only generated code can name `Response`, and the parked answer
            // arrives when none is running. So the wrapping travels with the
            // channel instead of being applied at a call site.
            $crate::Reply::wrapping($reply, |__value| {
                ($resp::$variant(__value), $crate::Body::empty())
            }),
            move |__from: $crate::Origin, __on: &mut $actor, __reply| {
                ::std::boxed::Box::pin(async move {
                    ::core::mem::drop($body);
                    __on.$method(__from.caller(), __reply $(, $arg )*).await
                })
            },
        )
        .await
    };
    ([defer], $kind:ident, $actor:ty, $state:expr, $hooks:expr, $origin:expr, $declared:expr,
        $reply:expr, $body:expr, $method:ident, $resp:ident, $variant:ident,
        ($( $arg:ident ),*)) => {
        $crate::__keylink_served_arm!(
            [], $kind, $actor, $state, $hooks, $origin, $declared, $reply, $body, $method,
            $resp, $variant, ($( $arg ),*)
        )
    };
    ([], send, $actor:ty, $state:expr, $hooks:expr, $origin:expr, $declared:expr,
        $reply:expr, $body:expr, $method:ident, $resp:ident, $variant:ident,
        ($( $arg:ident ),*)) => {
        $crate::turn(
            $state, $hooks, $origin, $declared, $reply,
            move |__from: $crate::Origin, __on: &mut $actor| {
                ::std::boxed::Box::pin(async move {
                    (
                        $resp::$variant(
                            __on.$method(__from.caller() $(, $arg )*, $body).await,
                        ),
                        $crate::Body::empty(),
                    )
                })
            },
        )
        .await
    };
    ([], recv, $actor:ty, $state:expr, $hooks:expr, $origin:expr, $declared:expr,
        $reply:expr, $body:expr, $method:ident, $resp:ident, $variant:ident,
        ($( $arg:ident ),*)) => {
        $crate::turn(
            $state, $hooks, $origin, $declared, $reply,
            move |__from: $crate::Origin, __on: &mut $actor| {
                ::std::boxed::Box::pin(async move {
                    ::core::mem::drop($body);
                    let (__value, __out) = __on.$method(__from.caller() $(, $arg )*).await;
                    ($resp::$variant(__value), __out)
                })
            },
        )
        .await
    };
    // The pair goes into the response WHOLE, and the grant is recorded from there
    // rather than here. `serve_one` is the only place that holds both the
    // `AdmittedPeer` and the store, and it reads the id back off this response
    // through `Granting`.
    ([], blob, $actor:ty, $state:expr, $hooks:expr, $origin:expr, $declared:expr,
        $reply:expr, $body:expr, $method:ident, $resp:ident, $variant:ident,
        ($( $arg:ident ),*)) => {
        $crate::turn(
            $state, $hooks, $origin, $declared, $reply,
            move |__from: $crate::Origin, __on: &mut $actor| {
                ::std::boxed::Box::pin(async move {
                    ::core::mem::drop($body);
                    let (__value, __id) = __on.$method(__from.caller() $(, $arg )*).await;
                    ($resp::$variant((__value, __id)), $crate::Body::empty())
                })
            },
        )
        .await
    };
    // `rpc` and `tell` are identical, because the served chain is typed
    // `(Response, Body)` for every kind: a `tell` still builds a response and
    // `serve_one_tell` drops it, there being no stream to write it to.
    ([], $kind:ident, $actor:ty, $state:expr, $hooks:expr, $origin:expr, $declared:expr,
        $reply:expr, $body:expr, $method:ident, $resp:ident, $variant:ident,
        ($( $arg:ident ),*)) => {
        $crate::turn(
            $state, $hooks, $origin, $declared, $reply,
            move |__from: $crate::Origin, __on: &mut $actor| {
                ::std::boxed::Box::pin(async move {
                    ::core::mem::drop($body);
                    (
                        $resp::$variant(__on.$method(__from.caller() $(, $arg )*).await),
                        $crate::Body::empty(),
                    )
                })
            },
        )
        .await
    };
}

/// Whether `#[defer]` means anything on this kind, and the one place it is refused.
///
/// Expanded once per declaration, inside the generated module. Emitting nothing is
/// the common case; the four refusals name what a deferred reply would have to be
/// for that kind, because "cannot defer" on its own sends an author looking for a
/// workaround rather than for the right method kind.
#[doc(hidden)]
#[macro_export]
macro_rules! __keylink_defer_check {
    ([], $kind:ident, $rpc:ident) => {};
    ([defer], rpc, $rpc:ident) => {};
    ([defer], tell, $rpc:ident) => {
        ::core::compile_error!(::core::concat!(
            "`#[defer]` on `tell ",
            ::core::stringify!($rpc),
            "`, and a `tell` has \
             no reply channel to defer. Nothing is sent back, so there is nothing \
             to answer later and no caller waiting to be answered. Make it an \
             `rpc` — a deferred `rpc` returning `()` is a `tell` whose caller \
             waits, which is what a deferred answer needs."
        ));
    };
    ([defer], send, $rpc:ident) => {
        ::core::compile_error!(::core::concat!(
            "`#[defer]` on `send ",
            ::core::stringify!($rpc),
            "`, and a `send` \
             reads its upload INSIDE the turn. Deferring the reply does not \
             release the `BodyReader`, so the turn would still hold the actor for \
             as long as the peer keeps writing - RFC-04 s10.4 - and a `send` \
             cannot be served as an actor at all. Put the upload on a second \
             service and defer the `rpc` that follows it."
        ));
    };
    ([defer], recv, $rpc:ident) => {
        ::core::compile_error!(::core::concat!(
            "`#[defer]` on `recv ",
            ::core::stringify!($rpc),
            "`, and a `recv` \
             answers with a `Body` the connection drains AFTER the turn ends. The \
             reply is already half-deferred by the transport, and a parked one \
             would leave a stream open with nothing scheduled to write it. Return \
             the payload from the turn, or hand out a `blob` address instead."
        ));
    };
    ([defer], feed, $rpc:ident) => {
        ::core::compile_error!(::core::concat!(
            "`#[defer]` on `feed ",
            ::core::stringify!($rpc),
            "`, and a `feed` has \
             no reply channel to defer - and no caller to defer one TO. An item \
             arrived off a stream nobody is waiting on, which is why the handler \
             takes no `Caller` and returns nothing. If something is waiting for an \
             answer, that something called: make it an `rpc`, and `#[defer]` it if \
             the answer comes from a later turn."
        ));
    };
    ([defer], blob, $rpc:ident) => {
        ::core::compile_error!(::core::concat!(
            "`#[defer]` on `blob ",
            ::core::stringify!($rpc),
            "`, and a `blob` \
             answers with a content address for bytes this node ALREADY holds. \
             There is nothing to wait for: if the content does not exist yet, the \
             address does not either. Defer an `rpc` that returns the address once \
             the content has been written."
        ));
    };
}

/// One `Actor`-trait method, shaped by the rpc kind and by `#[defer]`. `&mut self`,
/// because the task owns the state instead of sharing it.
///
/// The first parameter is the interpreted-attribute group the muncher built: `[]`
/// or `[defer]`. Three arms handle it and the five kind arms below are untouched —
/// a deferred `rpc` gets its own signature, and `#[defer]` on any other kind falls
/// back to that kind's ordinary signature, because `__keylink_defer_check!` is
/// already emitting the one error that says why. Generating a *deferring* signature
/// for a kind whose mailbox arm cannot honour it would bury that error under a
/// trait/mailbox mismatch.
#[doc(hidden)]
#[macro_export]
macro_rules! __keylink_actor_method {
    // A deferred `rpc`: the reply channel is an argument and the return is the
    // token proving it was consumed.
    ([defer], [$( #[$meta:meta] )*], rpc, $name:ident, ($( $arg:ident : $ty:ty ),*), $ret:ty) => {
        $( #[$meta] )*
        #[doc = concat!("Answer the `", stringify!($name), "` rpc — now, or from a later turn.")]
        ///
        /// `#[defer]`. The reply channel is an argument, and
        /// [`Replied`](keylink::Replied) is the proof that it was used: there is no
        /// way to build one except [`Reply::send`](keylink::Reply::send), which
        /// answers this turn, or [`Reply::later`](keylink::Reply::later), which
        /// hands back a [`Deferred`](keylink::Deferred) to answer from any turn
        /// after it. So forgetting to answer is a compile error rather than a
        /// caller that waits forever.
        ///
        /// `send` does not write to the channel: the value goes back to the turn,
        /// which sends it **after** the observer chain — so an answer given here is
        /// committed exactly like an ordinary method's return value, and an
        /// observer that refuses withholds it. A parked answer is the turn's output
        /// no longer, and is not gated.
        ///
        /// Nothing about the wire changes. A caller — local or remote — calls this
        /// like any other `rpc` and blocks until the answer arrives.
        fn $name(
            &mut self,
            cx: $crate::Caller<'_>,
            reply: $crate::Reply<$ret>
            $(, $arg: $ty )*
        ) -> impl ::core::future::Future<Output = $crate::Replied<$ret>> + Send;
    };
    // `#[defer]` on a kind that cannot defer. Refused by `__keylink_defer_check!`;
    // emit the ordinary signature so that is the only error.
    ([defer], $attrs:tt, $kind:ident, $name:ident, $args:tt, $ret:ty) => {
        $crate::__keylink_actor_method!([], $attrs, $kind, $name, $args, $ret);
    };
    ([], [$( #[$meta:meta] )*], rpc, $name:ident, ($( $arg:ident : $ty:ty ),*), $ret:ty) => {
        $( #[$meta] )*
        #[doc = concat!("Answer the `", stringify!($name), "` rpc.")]
        fn $name(&mut self, cx: $crate::Caller<'_> $(, $arg: $ty )*)
            -> impl ::core::future::Future<Output = $ret> + Send;
    };
    ([], [$( #[$meta:meta] )*], tell, $name:ident, ($( $arg:ident : $ty:ty ),*), $ret:ty) => {
        $( #[$meta] )*
        #[doc = concat!("Handle the `", stringify!($name), "` tell. Nothing is sent back.")]
        fn $name(&mut self, cx: $crate::Caller<'_> $(, $arg: $ty )*)
            -> impl ::core::future::Future<Output = $ret> + Send;
    };
    ([], [$( #[$meta:meta] )*], send, $name:ident, ($( $arg:ident : $ty:ty ),*), $ret:ty) => {
        $( #[$meta] )*
        #[doc = concat!("Answer the `", stringify!($name), "` rpc, reading the payload the caller streams up.")]
        ///
        /// The body is read inside the turn, so
        /// [`abort`](keylink::System::abort) can truncate it.
        fn $name(&mut self, cx: $crate::Caller<'_> $(, $arg: $ty )*, body: $crate::BodyReader)
            -> impl ::core::future::Future<Output = $ret> + Send;
    };
    ([], [$( #[$meta:meta] )*], recv, $name:ident, ($( $arg:ident : $ty:ty ),*), $ret:ty) => {
        $( #[$meta] )*
        #[doc = concat!("Answer the `", stringify!($name), "` rpc, returning a payload to stream back.")]
        ///
        /// The body is drained by the connection after the turn ends, so
        /// [`abort`](keylink::System::abort) cannot truncate it.
        fn $name(&mut self, cx: $crate::Caller<'_> $(, $arg: $ty )*)
            -> impl ::core::future::Future<Output = ($ret, $crate::Body)> + Send;
    };
    ([], [$( #[$meta:meta] )*], blob, $name:ident, ($( $arg:ident : $ty:ty ),*), $ret:ty) => {
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

/// One method on the typed actor handle, shaped by the rpc kind.
#[doc(hidden)]
#[macro_export]
macro_rules! __keylink_actor_client_method {
    // No actor handle method for `tell`, and the reason changed completely
    // under it — so here is a new one rather than a stale one.
    //
    // **The old reason is gone.** It was that the actor system reached its
    // registry over a bidirectional `ActorClient::call`, because the reply was
    // what carried the schema-mismatch refusal, and a `tell` has no reply. There
    // is no schema-mismatch refusal any more: drift is refused at the handshake,
    // and a keyed `tell` really would work — `RpcClient::tell` writes a
    // `WireRequest::Call` on a unidirectional stream and `serve_one_tell` walks
    // the same dispatch a call does.
    //
    // It stays absent for a reason of its own. A keyed message can be refused
    // for four things a singleton `tell` cannot: a malformed id, an id nothing
    // is mounted under, a registry at its capacity, and a queue budget that
    // expired before a permit arrived. All four are decided AFTER the bytes
    // reach the wire, and all four are reported on the reply — so a keyed `tell`
    // would be fire-and-forget into a registry that can refuse four ways the
    // caller can never learn about, including "you addressed an actor that does
    // not exist", which is the mistake a caller most wants to hear about.
    //
    // A caller who wants fire-and-forget to a keyed actor writes an `rpc`
    // returning `()` and drops the result, which is honest about the round trip
    // it pays for. Absent and documented, rather than present and lying.
    ([$( #[$meta:meta] )*], tell, $name:ident, $req:ident, $resp:ident, $variant:ident, $extract:ident,
        ($( $arg:ident : $ty:ty ),*), $ret:ty) => {};
    ([$( #[$meta:meta] )*], rpc, $name:ident, $req:ident, $resp:ident, $variant:ident, $extract:ident,
        ($( $arg:ident : $ty:ty ),*), $ret:ty) => {
        $( #[$meta] )*
        #[doc = concat!("Call the `", stringify!($variant), "` rpc on this actor.")]
        pub async fn $name(&self $(, $arg: $ty )*) -> $crate::Result<$ret> {
            let (response, _body) = self
                .client
                .send_to(&self.actor, $req::$variant { $( $arg ),* }, $crate::Body::empty())
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
                .send_to(&self.actor, $req::$variant { $( $arg ),* }, body)
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
                .send_to(&self.actor, $req::$variant { $( $arg ),* }, $crate::Body::empty())
                .await?;
            Ok((response.$extract()?, body))
        }
    };
    // Present, and it was absent for years. The reason it was absent is worth
    // keeping, because it is a good example of a limitation that looked
    // structural and was not.
    //
    // `serve_one` records a grant by reading the address off the reply through
    // `Granting`. On the keyed path that reply used to be an opaque `ActorReply`
    // — a payload the system never decoded — so the address genuinely sat where
    // nothing could see it, no grant was recorded, and any handle built here
    // would have fetched content nobody was granted. Emitting a method that
    // failed at run time was worse than emitting nothing, so nothing is what was
    // emitted, and `tests/blobs.rs` pinned BOTH halves: the method did not exist,
    // and the address reachable the hard way was refused on fetch.
    //
    // A keyed actor answers with the service's own typed `Response` now. The
    // generated `impl Granting for Response` is the one `serve_one` reads, it is
    // the same impl the singleton path uses, and the grant is recorded before the
    // reply is written exactly as it is there. The limitation was a consequence
    // of payload opacity rather than of keyed addressing, which is what removing
    // the opacity demonstrates.
    ([$( #[$meta:meta] )*], blob, $name:ident, $req:ident, $resp:ident, $variant:ident, $extract:ident,
        ($( $arg:ident : $ty:ty ),*), $ret:ty) => {
        $( #[$meta] )*
        #[doc = concat!("Call the `", stringify!($variant), "` rpc on this actor, returning its reply and a handle to the content it named.")]
        ///
        /// The call site names no ALPN, no second dial and no hash: a
        /// [`BlobHandle`](keylink::BlobHandle) is a content address plus the
        /// node that holds it, and it opens the blobs connection itself.
        pub async fn $name(&self $(, $arg: $ty )*)
            -> $crate::Result<($ret, $crate::BlobHandle)> {
            let (response, _body) = self
                .client
                .send_to(&self.actor, $req::$variant { $( $arg ),* }, $crate::Body::empty())
                .await?;
            let (value, id) = response.$extract()?;
            Ok((value, self.client.blob_handle(id)))
        }
    };
}

/// One method on a local `Handle`, shaped by the rpc kind.
///
/// Every arm builds one variant of the service's own `Mail` and hands it to the
/// mailbox. Nothing here is serialized, no `Request` variant is built and no
/// `Response` is taken apart — the variant carries this method's own arguments
/// and a `Reply` of this method's own return type, so there is no encode step to
/// fail and no "wrong variant" to extract.
///
/// **These arms used to call the trait method directly**, bypassing the mailbox
/// loop's dispatch entirely, and `__actor_turn`'s comment admitted it in those
/// words — *"there **are** two dispatchers, and the second one is
/// `__keylink_local_method!`"* — which it now carries in the past tense instead,
/// because the sentence outlived the arms it described by one wave. It argued the
/// split had stopped mattering because the caller became an argument. It had not.
/// A turn's *policy* — the observer chain reached through the loop's own reply
/// channel, the panic guard, the marks a served call makes — was reachable only
/// from the loop, so anything a later kind wanted to do to a turn had to be
/// written twice and compiled fine when it was written once.
///
/// The `body: ()` on every arm is not padding. Only a `send` handler reads an
/// upload inside its turn, so only `send`'s variant carries a `BodyReader`; the
/// other four carry a unit, and `Handle` has no `send` method to build one from
/// anyway.
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
                .ask(move |__from, __reply| Mail::$variant {
                    origin: __from,
                    $( $arg, )*
                    body: (),
                    reply: __reply,
                })
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
                .notify(move |__from, __reply| Mail::$variant {
                    origin: __from,
                    $( $arg, )*
                    body: (),
                    reply: __reply,
                })
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
                .ask(move |__from, __reply| Mail::$variant {
                    origin: __from,
                    $( $arg, )*
                    body: (),
                    reply: __reply,
                })
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
        // TWO feeds, and two is the number that matters: one feed's `POLLERS`
        // table has nothing to rotate among, so the modulo arithmetic in
        // `Feeds::poll_next` is only exercised from two up. It also means the
        // service reaching `SpawnableUnfed::CHECK` below is a fed one.
        feed Ticks(tick: u64);
        feed Fills(fill: String);
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
        /// `representability.rs` closes it for `Origin`.
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
            // No `Caller` and no return, which is the whole shape of a feed
            // handler. That this compiles against the generated trait is the
            // assertion: a signature with either would be `E0053`.
            async fn on_ticks(&mut self, _tick: u64) {}
            async fn on_fills(&mut self, _fill: String) {}
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
