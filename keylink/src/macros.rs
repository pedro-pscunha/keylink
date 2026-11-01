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
/// | `blob` | `async fn f(&self, a: A) -> (R, Grant)`   | `f(a) -> (R, BlobHandle)`            |
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
/// refuses it at startup. Returning the [`Grant`](crate::Grant) is what
/// grants it to the caller: a [`BlobId`](crate::BlobId) grants that one blob, and
/// a [`SeqId`](crate::SeqId) grants the sequence and every blob it lists. No
/// other peer can fetch it, and nothing else in the store becomes reachable.
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
/// use keylink::{Allowlist, BodyReader, Caller, Servable};
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
/// **Watched break.** Changed `ServableAsActor::<A>::CHECK`'s assertion
/// condition to `true` with `scripts/break-once.sh`. This fence then failed with
/// "Test compiled successfully, but it's marked `compile_fail`". Before the
/// [`Servable`](crate::Servable) import above, the same break left this fence green because method
/// lookup failed first.
///
/// And at the other site that serves one, which for a long time it was not.
/// `CHECK` is an associated const on a generic type, so it is evaluated where it
/// is *instantiated*. The former inherent `Handle::service` held the only
/// instantiation, so the
/// identical service was refused there and mounted into a keyed
/// [`Registry`](crate::Registry) without a word — over an ALPN, reachable by any
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
/// `macro_rules!` cannot compare arbitrary identifier fragments or their
/// `snake_case` spellings. Distinct spellings such as `FooBar` and `Foo_Bar`
/// can therefore still collide in a generated method or field and are reported
/// by Rust's duplicate-item diagnostics. RPC and feed declarations use separate
/// generated namespaces; the explicit taken names below are checked at the
/// surface that owns them.
///
/// ### Generated namespaces reserve only their structural names
///
/// Caller capabilities such as `status`, `close`, `reserve`, and `stop` live on
/// extension traits. User operations may therefore use those ordinary words.
/// `connect` remains inherent on the generated clients and is still reserved.
///
/// Wire-only names remain structural: `__Status` is the internal status variant,
/// `Served` is the mailbox envelope, and `Feeds::at` is the feed cursor. Rust
/// keywords are also rejected after snake-casing. `At` remains legal because
/// `RegistryClient::at` and an operation on `ActorRef` occupy different types.
/// These checks use direct declaration-site diagnostics; they are implementation
/// guards rather than compile-fail name tests.
///
/// ## `#[defer]`: one attribute keylink reads, and the rest it forwards
///
/// An `rpc` marked `#[defer]` is handed its own reply channel and may answer from
/// a later turn. Nothing about the wire changes — a deferred method is an ordinary
/// `rpc` to the caller, which blocks on its stream exactly as today, and the ALPN
/// does not move. See [`Reply`](crate::Reply) for the handler shape and
/// [`Replied`](crate::Replied) for why forgetting to answer is a compile error.
///
/// `#[defer]` is the only attribute that changes declaration semantics. The
/// muncher also records `#[cfg]`/`#[cfg_attr]` separately so disabled declarations
/// disappear from generated match arms and expressions; all other attributes are
/// forwarded verbatim to the sites that take one. Reading these attributes at all
/// needs a token-muncher: `service!`'s matcher captures attributes as
/// `#[$meta:meta]`, and a `meta` fragment is opaque to further matching. So the
/// declarations are munched a token at a time *before* the fragment forms, with
/// interpreted attributes accumulated separately from pass-through ones. See
/// [`__keylink_service_munch`](crate::__keylink_service_munch).
///
/// Write `#[defer]` directly. `#[cfg_attr(..., defer)]` is not supported:
/// `macro_rules!` sees the wrapper before configuration expansion, and keylink does
/// not branch the generated `Actor` method signature on that payload. When the
/// handler shape differs by configuration, put complete `service!` declarations
/// and their implementations behind complementary `#[cfg]` attributes instead.
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
/// Wire it at spawn with `setup(state).observe(observer).feeds(Feeds::new(stream)).actor()`, or
/// terminate with `.mountable()` for a keyed actor.
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
/// // `setup(Book).feeds(orders::Feeds::new(stream)).actor()` is the door.
/// let _ = orders::actor(Book);
/// ```
///
/// The `rpc` in both fences is not padding: a `service!` of one method generates an
/// `into_<rpc>` whose catch-all arm is `unreachable_patterns`, and a probe should not
/// be able to fail for that instead. In the second, the `impl Actor` block is not
/// padding either — without it the fence would pass on the missing impl rather than
/// on the refusal, which is the shape of a guard that proves nothing.
///
/// ## Contract windows
///
/// Put `version N;` immediately after `service Name;` (omitted means 1), and put
/// `previous N { hash PIN; keyed_hash KEYED_PIN; ... }` blocks after every declaration and feed, newest
/// first. A block repeats that version's wire declarations in their original
/// order, with `changed` before the kinds whose conversions the application writes.
/// Attributes are preserved just as on current declarations: serde metadata
/// can change the wire hash, and cfg/cfg_attr gates the contract and conversion
/// arms together. `#[defer]` is consumed because it changes no wire shape.
/// Feeds remain local and do not belong in a frozen wire block.
/// Unmarked type changes fail in the generated plain conversion arms.
///
/// Each block generates `version_N::{Request, Response, Step, Convert, VERSION}`.
/// Implement all four `Convert` methods for `Step`; generated arms carry plain
/// methods and `__Status`, and application conversions carry changed, new and
/// removed methods. The handler, authorization and ledger see only current types.
/// `window()` lists the build's versions; the `Wire::version` method names the
/// connection's negotiated version. Downgrades may lose fields: this is the
/// application's policy, not optional-field compatibility in postcard. Blob
/// metadata may convert, but the Content kind and root must stay unchanged; an address change
/// is `UnsupportedResponse`, or `Forgotten` during stored-reply recovery.
///
/// Prefer `crate::` paths in a block, not `super::`: frozen declarations resolve
/// one module deeper. Qualify names that collide with generated `Request`,
/// `Response`, `Step`, `Convert`, `VERSION` or `version_N` items. Keylink's own
/// wire types are hashed into every version, so a window covers application skew
/// only within the same keylink wire revision.
///
/// A frozen declaration must still hash to its pin:
///
/// ```compile_fail
/// # mod frozen { keylink::service! { service PinExample; version 2; rpc Ping() -> u64; } }
/// keylink::service! {
///     service PinExample;
///     version 3;
///     rpc Ping() -> u64;
///     previous 2 { hash frozen::pin_example::VERSION.hash() ^ 1; keyed_hash frozen::pin_example::KEYED_VERSION.hash(); rpc Ping() -> u64; }
/// }
/// # impl pin_example::version_2::Convert for pin_example::version_2::Step {
/// # fn upgrade_request(_: pin_example::version_2::Request) -> Result<pin_example::Request, keylink::Unsupported> { unreachable!() }
/// # fn downgrade_request(_: pin_example::Request) -> Result<pin_example::version_2::Request, keylink::Unsupported> { unreachable!() }
/// # fn upgrade_response(_: pin_example::version_2::Response) -> Result<pin_example::Response, keylink::Unsupported> { unreachable!() }
/// # fn downgrade_response(_: pin_example::Response) -> Result<pin_example::version_2::Response, keylink::Unsupported> { unreachable!() }
/// # }
/// # fn main() {}
/// ```
///
/// Each older label must be lower than the one above it:
///
/// ```compile_fail
/// # mod frozen { keylink::service! { service PinExample; version 4; rpc Ping() -> u64; } }
/// keylink::service! {
///     service PinExample;
///     version 3;
///     rpc Ping() -> u64;
///     previous 4 { hash frozen::pin_example::VERSION.hash(); keyed_hash frozen::pin_example::KEYED_VERSION.hash(); rpc Ping() -> u64; }
/// }
/// # impl pin_example::version_4::Convert for pin_example::version_4::Step {
/// # fn upgrade_request(_: pin_example::version_4::Request) -> Result<pin_example::Request, keylink::Unsupported> { unreachable!() }
/// # fn downgrade_request(_: pin_example::Request) -> Result<pin_example::version_4::Request, keylink::Unsupported> { unreachable!() }
/// # fn upgrade_response(_: pin_example::version_4::Response) -> Result<pin_example::Response, keylink::Unsupported> { unreachable!() }
/// # fn downgrade_response(_: pin_example::Response) -> Result<pin_example::version_4::Response, keylink::Unsupported> { unreachable!() }
/// # }
/// # fn main() {}
/// ```
///
/// The singleton pin can match while the keyed pin refuses a different shape:
///
/// ```compile_fail
/// # mod frozen { keylink::service! { service PinExample; version 2; rpc Ping() -> u64; } }
/// keylink::service! {
///     service PinExample;
///     version 3;
///     rpc Ping() -> u64;
///     previous 2 { hash frozen::pin_example::VERSION.hash(); keyed_hash frozen::pin_example::KEYED_VERSION.hash() ^ 1; rpc Ping() -> u64; }
/// }
/// # impl pin_example::version_2::Convert for pin_example::version_2::Step {
/// # fn upgrade_request(_: pin_example::version_2::Request) -> Result<pin_example::Request, keylink::Unsupported> { unreachable!() }
/// # fn downgrade_request(_: pin_example::Request) -> Result<pin_example::version_2::Request, keylink::Unsupported> { unreachable!() }
/// # fn upgrade_response(_: pin_example::version_2::Response) -> Result<pin_example::Response, keylink::Unsupported> { unreachable!() }
/// # fn downgrade_response(_: pin_example::Response) -> Result<pin_example::version_2::Response, keylink::Unsupported> { unreachable!() }
/// # }
/// # fn main() {}
/// ```
///
/// A caller's assertion macro cannot disable the pin or label guards:
///
/// ```compile_fail
/// # #[allow(unused_macros)]
/// # macro_rules! assert { ($($ignored:tt)*) => { () }; }
/// # mod frozen { keylink::service! { service PinExample; version 2; rpc Ping() -> u64; } }
/// keylink::service! {
///     service PinExample;
///     version 3;
///     rpc Ping() -> u64;
///     previous 2 { hash frozen::pin_example::VERSION.hash() ^ 1; keyed_hash frozen::pin_example::KEYED_VERSION.hash(); rpc Ping() -> u64; }
/// }
/// # impl pin_example::version_2::Convert for pin_example::version_2::Step {
/// # fn upgrade_request(_: pin_example::version_2::Request) -> Result<pin_example::Request, keylink::Unsupported> { unreachable!() }
/// # fn downgrade_request(_: pin_example::Request) -> Result<pin_example::version_2::Request, keylink::Unsupported> { unreachable!() }
/// # fn upgrade_response(_: pin_example::version_2::Response) -> Result<pin_example::Response, keylink::Unsupported> { unreachable!() }
/// # fn downgrade_response(_: pin_example::Response) -> Result<pin_example::version_2::Response, keylink::Unsupported> { unreachable!() }
/// # }
/// # fn main() {}
/// ```
///
/// ```compile_fail
/// # #[allow(unused_macros)]
/// # macro_rules! assert { ($($ignored:tt)*) => { () }; }
/// # mod frozen { keylink::service! { service PinExample; version 4; rpc Ping() -> u64; } }
/// keylink::service! {
///     service PinExample;
///     version 3;
///     rpc Ping() -> u64;
///     previous 4 { hash frozen::pin_example::VERSION.hash(); keyed_hash frozen::pin_example::KEYED_VERSION.hash(); rpc Ping() -> u64; }
/// }
/// # impl pin_example::version_4::Convert for pin_example::version_4::Step {
/// # fn upgrade_request(_: pin_example::version_4::Request) -> Result<pin_example::Request, keylink::Unsupported> { unreachable!() }
/// # fn downgrade_request(_: pin_example::Request) -> Result<pin_example::version_4::Request, keylink::Unsupported> { unreachable!() }
/// # fn upgrade_response(_: pin_example::version_4::Response) -> Result<pin_example::Response, keylink::Unsupported> { unreachable!() }
/// # fn downgrade_response(_: pin_example::Response) -> Result<pin_example::version_4::Response, keylink::Unsupported> { unreachable!() }
/// # }
/// # fn main() {}
/// ```
///
/// The label and singleton guards were watched red with `scripts/break-once.sh`: replacing the
/// hash assertion's condition with `true` made the first fence compile, and
/// replacing the descending-label assertion's condition with `true` made the
/// second compile. Each isolated run reported 0 passed, 1 failed, with
/// "Test compiled successfully, but it's marked `compile_fail`." Both edits
/// were restored. The hidden `main` keeps the frozen module at crate scope;
/// without it a fence can fail on name resolution instead of on its guard.
///
/// The independent keyed pin was also watched red: replacing its assertion's
/// condition with `true` made the third fence compile (0 passed, 1 failed).
/// The singleton pin matched throughout. Restored the keyed assertion.
///
/// Both assertion-shadowing fixtures were watched red independently: removing
/// the core qualification from the corresponding guard let the caller's
/// const-compatible no-op compile the invalid contract (0 passed, 1 failed each).
/// Restored both qualified assertions.
///
/// A window holds at most 64 versions, current included. Construction refuses
/// the 65th entry, rather than publishing a probe its own reader would reject:
///
/// ```compile_fail
/// # #![recursion_limit = "2048"]
/// # mod frozen { keylink::service! { service Wide; version 1; rpc Ping() -> u8; } }
/// # macro_rules! window {
/// # ($($old:literal),*) => {
/// keylink::service! {
///     service Wide; version 65;
///     rpc Ping() -> u8;
/// # $(previous $old { hash frozen::wide::VERSION.hash(); keyed_hash frozen::wide::KEYED_VERSION.hash(); rpc Ping() -> u8; })*
/// }
/// # keylink::pastey::paste! { $(
/// # impl wide::[<version_ $old>]::Convert for wide::[<version_ $old>]::Step {
/// # fn upgrade_request(_: wide::[<version_ $old>]::Request) -> Result<<wide::[<version_ $old>]::Step as keylink::Hop>::NewerRequest, keylink::Unsupported> { unreachable!() }
/// # fn downgrade_request(_: <wide::[<version_ $old>]::Step as keylink::Hop>::NewerRequest) -> Result<wide::[<version_ $old>]::Request, keylink::Unsupported> { unreachable!() }
/// # fn upgrade_response(_: wide::[<version_ $old>]::Response) -> Result<<wide::[<version_ $old>]::Step as keylink::Hop>::NewerResponse, keylink::Unsupported> { unreachable!() }
/// # fn downgrade_response(_: <wide::[<version_ $old>]::Step as keylink::Hop>::NewerResponse) -> Result<wide::[<version_ $old>]::Response, keylink::Unsupported> { unreachable!() }
/// # }
/// # )* }
/// # };
/// # }
/// # window!(64,63,62,61,60,59,58,57,56,55,54,53,52,51,50,49,48,47,46,45,44,43,42,41,40,39,38,37,36,35,34,33,32,31,30,29,28,27,26,25,24,23,22,21,20,19,18,17,16,15,14,13,12,11,10,9,8,7,6,5,4,3,2,1);
/// # fn main() {}
/// ```
///
/// The oversized-window fence was watched red by allowing 65 in the shared
/// count guard: the complete fixture compiled (0 passed, 1 failed). All frozen
/// conversions were present, so the failure cannot come from missing impls.
/// Restored the 64-version construction limit.
///
/// The calling crate needs `serde`, `postcard-schema` and `tokio` as
/// dependencies — the generated code derives against them by path.
#[macro_export]
macro_rules! service {
    ( service $svc:ident; version $version:literal; $( $rest:tt )+ ) => {
        $crate::__keylink_service_munch! {
            @svc [ $svc $version ] @done [ ] @feeds [ ] @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )+ ]
        }
    };
    ( service $svc:ident; $( $rest:tt )+ ) => {
        $crate::__keylink_service_munch! {
            @svc [ $svc 1 ]
            @done [ ]
            @feeds [ ]
            @attrs [ ]
            @cfg [ ]
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
/// about which list a declaration is in.** A rejected declaration does not count as
/// a wire declaration. If it was the only wire-shaped declaration beside feeds,
/// diagnostics deliberately report both the declaration error and the independent
/// feeds-only service-shape error. The list separation is checked by the compiler
/// at every generated site rather than being a claim thirteen macros have to keep
/// agreeing about.
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
/// Split the payload of a `cfg_attr` while retaining only its nested `cfg`
/// attributes for generated non-item syntax. The original attribute remains in
/// the declaration metadata, where Rust applies every payload normally.
#[doc(hidden)]
#[macro_export]
macro_rules! __keylink_cfg_attr {
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @guard $guard:meta @payload [ cfg_attr($condition:meta, $( $nested:tt )*) , $( $payload:tt )* ]
        @after [ $( $after:tt )* ] @rest [ $( $rest:tt )* ]
    ) => {
        $crate::__keylink_cfg_attr! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ $( $attrs )* ] @cfg [ $( $cfg )* ] @defer [ $( $seen )* ]
            @guard all($guard, $condition) @payload [ $( $nested )* ]
            @after [
                @guard $guard @payload [ $( $payload )* ]
                @after [ $( $after )* ]
            ]
            @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @guard $guard:meta @payload [ cfg_attr($condition:meta, $( $nested:tt )*) ]
        @after [ $( $after:tt )* ] @rest [ $( $rest:tt )* ]
    ) => {
        $crate::__keylink_cfg_attr! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ $( $attrs )* ] @cfg [ $( $cfg )* ] @defer [ $( $seen )* ]
            @guard all($guard, $condition) @payload [ $( $nested )* ]
            @after [ $( $after )* ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @guard $guard:meta @payload [ cfg($predicate:meta) , $( $payload:tt )* ]
        @after [ $( $after:tt )* ] @rest [ $( $rest:tt )* ]
    ) => {
        $crate::__keylink_cfg_attr! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ $( $attrs )* ]
            @cfg [ $( $cfg )* #[cfg(any(not($guard), $predicate))] ]
            @defer [ $( $seen )* ] @guard $guard @payload [ $( $payload )* ]
            @after [ $( $after )* ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @guard $guard:meta @payload [ cfg($predicate:meta) ]
        @after [ $( $after:tt )* ] @rest [ $( $rest:tt )* ]
    ) => {
        $crate::__keylink_cfg_attr! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ $( $attrs )* ]
            @cfg [ $( $cfg )* #[cfg(any(not($guard), $predicate))] ]
            @defer [ $( $seen )* ] @guard $guard @payload [ ]
            @after [ $( $after )* ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @guard $guard:meta @payload [ $other:meta , $( $payload:tt )* ]
        @after [ $( $after:tt )* ] @rest [ $( $rest:tt )* ]
    ) => {
        $crate::__keylink_cfg_attr! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ $( $attrs )* ] @cfg [ $( $cfg )* ] @defer [ $( $seen )* ]
            @guard $guard @payload [ $( $payload )* ]
            @after [ $( $after )* ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @guard $guard:meta @payload [ $other:meta ]
        @after [ $( $after:tt )* ] @rest [ $( $rest:tt )* ]
    ) => {
        $crate::__keylink_cfg_attr! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ $( $attrs )* ] @cfg [ $( $cfg )* ] @defer [ $( $seen )* ]
            @guard $guard @payload [ ] @after [ $( $after )* ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @guard $guard:meta @payload [ ]
        @after [ @guard $next_guard:meta @payload [ $( $next_payload:tt )* ]
                 @after [ $( $after:tt )* ] ] @rest [ $( $rest:tt )* ]
    ) => {
        $crate::__keylink_cfg_attr! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ $( $attrs )* ] @cfg [ $( $cfg )* ] @defer [ $( $seen )* ]
            @guard $next_guard @payload [ $( $next_payload )* ]
            @after [ $( $after )* ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @guard $guard:meta @payload [ ] @after [ ] @rest [ $( $rest:tt )* ]
    ) => {
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ $( $attrs )* ] @cfg [ $( $cfg )* ] @defer [ $( $seen )* ]
            @rest [ $( $rest )* ]
        }
    };
}

#[doc(hidden)]
#[macro_export]
macro_rules! __keylink_service_munch {
    // Shared cfg_attr parsing resumes the frozen declaration parser here.
    (@svc [@previous $svc:tt $upper:tt $newer:tt $label:tt $pin:tt]
        @done [$($all:tt)*] @feeds [$($plain:tt)*] @attrs [$($attrs:tt)*] @cfg [$($cfg:tt)*] @defer [] @rest [$($rest:tt)*]) => {
        $crate::__keylink_previous! { @parse $svc $upper $newer $label $pin @attrs [$($attrs)*] @cfg [$($cfg)*] @all [$($all)*] @plain [$($plain)*] @rest [$($rest)*] }
    };

    (
        @svc [ $svc:ident $version:literal ] @done [ $( $done:tt )+ ] @feeds [ $( $feeds:tt )* ]
        @attrs [ ] @cfg [ ] @defer [ ]
        @rest [ $( previous $older:literal { $( $old:tt )* } )+ ]
    ) => {
        $crate::__keylink_service! {
            $svc;
            @version [ $version ] @previous [ $( previous $older { $( $old )* } )+ ]
            @wire [ $( $done )+ ] @feeds [ $( $feeds )* ]
        }
    };

    // `#[defer]` changes semantics and is consumed. Configuration attributes are
    // recorded separately below so they can gate generated non-item syntax.
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $_seen:tt )* ]
        @rest [ #[defer] $( $rest:tt )* ]
    ) => {
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ $( $attrs )* ] @cfg [ $( $cfg )* ] @defer [ defer ]
            @rest [ $( $rest )* ]
        }
    };
    // Configuration attributes must also gate generated match arms, fields,
    // and expressions. Keep a second list so those sites do not receive doc
    // or serde attributes that Rust rejects there.
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ #[cfg($($condition:tt)*)] $( $rest:tt )* ]
    ) => {
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ $( $attrs )* #[cfg($($condition)*)] ]
            @cfg [ $( $cfg )* #[cfg($($condition)*)] ] @defer [ $( $seen )* ]
            @rest [ $( $rest )* ]
        }
    };
    // Keep the complete `cfg_attr` for generated items, and derive one `cfg`
    // metadata attribute for every nested `cfg` payload. A `cfg_attr` may carry
    // more than one payload, and a later `cfg` still gates generated match arms,
    // fields and expressions even when an earlier payload is `doc` or `serde`.
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ #[cfg_attr($condition:meta, $($payload:tt)*)] $( $rest:tt )* ]
    ) => {
        $crate::__keylink_cfg_attr! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ $( $attrs )* #[cfg_attr($condition, $($payload)*)] ]
            @cfg [ $( $cfg )* ] @defer [ $( $seen )* ]
            @guard $condition @payload [ $($payload)* ]
            @after [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ #[cfg_attr($($condition:tt)*)] $( $rest:tt )* ]
    ) => {
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ $( $attrs )* #[cfg_attr($($condition)*)] ]
            @cfg [ $( $cfg )* ] @defer [ $( $seen )* ]
            @rest [ $( $rest )* ]
        }
    };

    // Everything else, kept in order and forwarded verbatim.
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ #[$meta:meta] $( $rest:tt )* ]
    ) => {
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ $( $attrs )* #[$meta] ]
            @cfg [ $( $cfg )* ] @defer [ $( $seen )* ]
            @rest [ $( $rest )* ]
        }
    };
    // As with wire variants, `Self`/`self` cannot survive as a generated
    // enum variant. Diagnose them before the paste expansion parses `Feed`.
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ feed Self ( $arg:ident : $ty:ty ) ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], feed, Self);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], Self, feed, Self);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ feed self ( $arg:ident : $ty:ty ) ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], feed, self);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], self, feed, self);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };

    // `Self`/`self` cannot be generated as an enum variant or method name.
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ $kind:ident Self ( $( $args:tt )* ) -> $ret:ty ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], $kind, Self);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], Self, $kind, Self);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ $kind:ident self ( $( $args:tt )* ) -> $ret:ty ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], $kind, self);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], self, $kind, self);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };

    // A wire declaration named `Request` or `Response` collides with the two
    // enums every service generates, and the collision is silent at the
    // declaration: it surfaces as `error: ambiguous associated item` pointing at
    // the `service!` invocation, under `#[deny(ambiguous_associated_items)]`,
    // which is a `future_incompatible` lint and therefore a warning today in
    // some editions and a hard error later. Neither name appears in the
    // reserved-name section above, so the first person to write
    // `rpc Request(..)` reads an error about an associated type they never
    // wrote.
    //
    // A `feed` of either name is NOT rejected, and that is measured rather than
    // assumed: a feed reaches no `Request` or `Response` enum, so
    // `feed Request(v: u64)` compiles clean today and this arm would break
    // working code for a collision that does not exist.
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ $kind:ident Request ( $( $args:tt )* ) -> $ret:ty ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_wire_name_check_item!(
            [ $( $attrs )* ], Request, $kind, Request
        );
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ $kind:ident Response ( $( $args:tt )* ) -> $ret:ty ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_wire_name_check_item!(
            [ $( $attrs )* ], Response, $kind, Response
        );
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };

    // Feed fields are not rawized, so every CamelCase spelling whose snake-case
    // result is a keyword is rejected before `Feeds` is emitted.
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ feed As ( $arg:ident : $ty:ty ) ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], feed, As);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], As, feed, As);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ feed Break ( $arg:ident : $ty:ty ) ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], feed, Break);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], Break, feed, Break);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ feed Const ( $arg:ident : $ty:ty ) ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], feed, Const);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], Const, feed, Const);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ feed Continue ( $arg:ident : $ty:ty ) ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], feed, Continue);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], Continue, feed, Continue);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ feed Else ( $arg:ident : $ty:ty ) ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], feed, Else);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], Else, feed, Else);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ feed Enum ( $arg:ident : $ty:ty ) ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], feed, Enum);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], Enum, feed, Enum);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ feed Extern ( $arg:ident : $ty:ty ) ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], feed, Extern);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], Extern, feed, Extern);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ feed False ( $arg:ident : $ty:ty ) ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], feed, False);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], False, feed, False);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ feed Fn ( $arg:ident : $ty:ty ) ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], feed, Fn);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], Fn, feed, Fn);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ feed For ( $arg:ident : $ty:ty ) ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], feed, For);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], For, feed, For);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ feed If ( $arg:ident : $ty:ty ) ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], feed, If);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], If, feed, If);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ feed Impl ( $arg:ident : $ty:ty ) ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], feed, Impl);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], Impl, feed, Impl);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ feed In ( $arg:ident : $ty:ty ) ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], feed, In);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], In, feed, In);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ feed Let ( $arg:ident : $ty:ty ) ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], feed, Let);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], Let, feed, Let);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ feed Loop ( $arg:ident : $ty:ty ) ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], feed, Loop);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], Loop, feed, Loop);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ feed Match ( $arg:ident : $ty:ty ) ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], feed, Match);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], Match, feed, Match);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ feed Mod ( $arg:ident : $ty:ty ) ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], feed, Mod);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], Mod, feed, Mod);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ feed Move ( $arg:ident : $ty:ty ) ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], feed, Move);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], Move, feed, Move);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ feed Mut ( $arg:ident : $ty:ty ) ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], feed, Mut);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], Mut, feed, Mut);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ feed Pub ( $arg:ident : $ty:ty ) ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], feed, Pub);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], Pub, feed, Pub);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ feed Ref ( $arg:ident : $ty:ty ) ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], feed, Ref);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], Ref, feed, Ref);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ feed Return ( $arg:ident : $ty:ty ) ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], feed, Return);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], Return, feed, Return);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ feed Static ( $arg:ident : $ty:ty ) ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], feed, Static);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], Static, feed, Static);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ feed Struct ( $arg:ident : $ty:ty ) ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], feed, Struct);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], Struct, feed, Struct);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ feed Trait ( $arg:ident : $ty:ty ) ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], feed, Trait);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], Trait, feed, Trait);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ feed True ( $arg:ident : $ty:ty ) ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], feed, True);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], True, feed, True);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ feed Type ( $arg:ident : $ty:ty ) ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], feed, Type);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], Type, feed, Type);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ feed Unsafe ( $arg:ident : $ty:ty ) ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], feed, Unsafe);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], Unsafe, feed, Unsafe);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ feed Use ( $arg:ident : $ty:ty ) ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], feed, Use);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], Use, feed, Use);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ feed Where ( $arg:ident : $ty:ty ) ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], feed, Where);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], Where, feed, Where);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ feed While ( $arg:ident : $ty:ty ) ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], feed, While);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], While, feed, While);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ feed Async ( $arg:ident : $ty:ty ) ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], feed, Async);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], Async, feed, Async);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ feed Await ( $arg:ident : $ty:ty ) ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], feed, Await);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], Await, feed, Await);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ feed Dyn ( $arg:ident : $ty:ty ) ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], feed, Dyn);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], Dyn, feed, Dyn);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ feed Abstract ( $arg:ident : $ty:ty ) ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], feed, Abstract);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], Abstract, feed, Abstract);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ feed Become ( $arg:ident : $ty:ty ) ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], feed, Become);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], Become, feed, Become);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ feed Box ( $arg:ident : $ty:ty ) ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], feed, Box);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], Box, feed, Box);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ feed Do ( $arg:ident : $ty:ty ) ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], feed, Do);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], Do, feed, Do);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ feed Final ( $arg:ident : $ty:ty ) ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], feed, Final);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], Final, feed, Final);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ feed Macro ( $arg:ident : $ty:ty ) ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], feed, Macro);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], Macro, feed, Macro);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ feed Override ( $arg:ident : $ty:ty ) ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], feed, Override);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], Override, feed, Override);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ feed Priv ( $arg:ident : $ty:ty ) ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], feed, Priv);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], Priv, feed, Priv);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ feed Typeof ( $arg:ident : $ty:ty ) ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], feed, Typeof);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], Typeof, feed, Typeof);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ feed Unsized ( $arg:ident : $ty:ty ) ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], feed, Unsized);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], Unsized, feed, Unsized);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ feed Virtual ( $arg:ident : $ty:ty ) ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], feed, Virtual);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], Virtual, feed, Virtual);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ feed Yield ( $arg:ident : $ty:ty ) ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], feed, Yield);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], Yield, feed, Yield);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ feed Try ( $arg:ident : $ty:ty ) ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], feed, Try);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], Try, feed, Try);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ feed Gen ( $arg:ident : $ty:ty ) ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], feed, Gen);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], Gen, feed, Gen);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };

    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ feed r#As ( $arg:ident : $ty:ty ) ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], feed, r#As);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], as, feed, r#As);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ feed r#as ( $arg:ident : $ty:ty ) ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], feed, r#as);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], as, feed, r#as);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ feed r#Break ( $arg:ident : $ty:ty ) ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], feed, r#Break);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], break, feed, r#Break);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ feed r#break ( $arg:ident : $ty:ty ) ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], feed, r#break);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], break, feed, r#break);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ feed r#Const ( $arg:ident : $ty:ty ) ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], feed, r#Const);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], const, feed, r#Const);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ feed r#const ( $arg:ident : $ty:ty ) ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], feed, r#const);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], const, feed, r#const);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ feed r#Continue ( $arg:ident : $ty:ty ) ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], feed, r#Continue);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], continue, feed, r#Continue);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ feed r#continue ( $arg:ident : $ty:ty ) ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], feed, r#continue);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], continue, feed, r#continue);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ feed r#Else ( $arg:ident : $ty:ty ) ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], feed, r#Else);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], else, feed, r#Else);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ feed r#else ( $arg:ident : $ty:ty ) ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], feed, r#else);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], else, feed, r#else);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ feed r#Enum ( $arg:ident : $ty:ty ) ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], feed, r#Enum);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], enum, feed, r#Enum);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ feed r#enum ( $arg:ident : $ty:ty ) ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], feed, r#enum);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], enum, feed, r#enum);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ feed r#Extern ( $arg:ident : $ty:ty ) ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], feed, r#Extern);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], extern, feed, r#Extern);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ feed r#extern ( $arg:ident : $ty:ty ) ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], feed, r#extern);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], extern, feed, r#extern);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ feed r#False ( $arg:ident : $ty:ty ) ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], feed, r#False);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], false, feed, r#False);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ feed r#false ( $arg:ident : $ty:ty ) ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], feed, r#false);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], false, feed, r#false);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ feed r#Fn ( $arg:ident : $ty:ty ) ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], feed, r#Fn);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], fn, feed, r#Fn);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ feed r#fn ( $arg:ident : $ty:ty ) ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], feed, r#fn);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], fn, feed, r#fn);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ feed r#For ( $arg:ident : $ty:ty ) ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], feed, r#For);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], for, feed, r#For);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ feed r#for ( $arg:ident : $ty:ty ) ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], feed, r#for);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], for, feed, r#for);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ feed r#If ( $arg:ident : $ty:ty ) ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], feed, r#If);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], if, feed, r#If);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ feed r#if ( $arg:ident : $ty:ty ) ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], feed, r#if);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], if, feed, r#if);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ feed r#Impl ( $arg:ident : $ty:ty ) ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], feed, r#Impl);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], impl, feed, r#Impl);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ feed r#impl ( $arg:ident : $ty:ty ) ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], feed, r#impl);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], impl, feed, r#impl);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ feed r#In ( $arg:ident : $ty:ty ) ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], feed, r#In);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], in, feed, r#In);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ feed r#in ( $arg:ident : $ty:ty ) ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], feed, r#in);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], in, feed, r#in);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ feed r#Let ( $arg:ident : $ty:ty ) ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], feed, r#Let);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], let, feed, r#Let);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ feed r#let ( $arg:ident : $ty:ty ) ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], feed, r#let);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], let, feed, r#let);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ feed r#Loop ( $arg:ident : $ty:ty ) ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], feed, r#Loop);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], loop, feed, r#Loop);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ feed r#loop ( $arg:ident : $ty:ty ) ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], feed, r#loop);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], loop, feed, r#loop);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ feed r#Match ( $arg:ident : $ty:ty ) ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], feed, r#Match);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], match, feed, r#Match);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ feed r#match ( $arg:ident : $ty:ty ) ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], feed, r#match);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], match, feed, r#match);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ feed r#Mod ( $arg:ident : $ty:ty ) ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], feed, r#Mod);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], mod, feed, r#Mod);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ feed r#mod ( $arg:ident : $ty:ty ) ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], feed, r#mod);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], mod, feed, r#mod);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ feed r#Move ( $arg:ident : $ty:ty ) ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], feed, r#Move);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], move, feed, r#Move);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ feed r#move ( $arg:ident : $ty:ty ) ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], feed, r#move);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], move, feed, r#move);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ feed r#Mut ( $arg:ident : $ty:ty ) ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], feed, r#Mut);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], mut, feed, r#Mut);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ feed r#mut ( $arg:ident : $ty:ty ) ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], feed, r#mut);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], mut, feed, r#mut);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ feed r#Pub ( $arg:ident : $ty:ty ) ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], feed, r#Pub);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], pub, feed, r#Pub);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ feed r#pub ( $arg:ident : $ty:ty ) ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], feed, r#pub);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], pub, feed, r#pub);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ feed r#Ref ( $arg:ident : $ty:ty ) ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], feed, r#Ref);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], ref, feed, r#Ref);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ feed r#ref ( $arg:ident : $ty:ty ) ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], feed, r#ref);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], ref, feed, r#ref);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ feed r#Return ( $arg:ident : $ty:ty ) ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], feed, r#Return);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], return, feed, r#Return);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ feed r#return ( $arg:ident : $ty:ty ) ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], feed, r#return);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], return, feed, r#return);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ feed r#Static ( $arg:ident : $ty:ty ) ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], feed, r#Static);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], static, feed, r#Static);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ feed r#static ( $arg:ident : $ty:ty ) ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], feed, r#static);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], static, feed, r#static);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ feed r#Struct ( $arg:ident : $ty:ty ) ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], feed, r#Struct);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], struct, feed, r#Struct);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ feed r#struct ( $arg:ident : $ty:ty ) ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], feed, r#struct);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], struct, feed, r#struct);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ feed r#Trait ( $arg:ident : $ty:ty ) ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], feed, r#Trait);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], trait, feed, r#Trait);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ feed r#trait ( $arg:ident : $ty:ty ) ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], feed, r#trait);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], trait, feed, r#trait);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ feed r#True ( $arg:ident : $ty:ty ) ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], feed, r#True);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], true, feed, r#True);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ feed r#true ( $arg:ident : $ty:ty ) ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], feed, r#true);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], true, feed, r#true);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ feed r#Type ( $arg:ident : $ty:ty ) ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], feed, r#Type);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], type, feed, r#Type);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ feed r#type ( $arg:ident : $ty:ty ) ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], feed, r#type);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], type, feed, r#type);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ feed r#Unsafe ( $arg:ident : $ty:ty ) ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], feed, r#Unsafe);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], unsafe, feed, r#Unsafe);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ feed r#unsafe ( $arg:ident : $ty:ty ) ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], feed, r#unsafe);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], unsafe, feed, r#unsafe);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ feed r#Use ( $arg:ident : $ty:ty ) ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], feed, r#Use);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], use, feed, r#Use);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ feed r#use ( $arg:ident : $ty:ty ) ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], feed, r#use);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], use, feed, r#use);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ feed r#Where ( $arg:ident : $ty:ty ) ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], feed, r#Where);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], where, feed, r#Where);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ feed r#where ( $arg:ident : $ty:ty ) ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], feed, r#where);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], where, feed, r#where);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ feed r#While ( $arg:ident : $ty:ty ) ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], feed, r#While);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], while, feed, r#While);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ feed r#while ( $arg:ident : $ty:ty ) ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], feed, r#while);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], while, feed, r#while);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ feed r#Async ( $arg:ident : $ty:ty ) ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], feed, r#Async);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], async, feed, r#Async);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ feed r#async ( $arg:ident : $ty:ty ) ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], feed, r#async);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], async, feed, r#async);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ feed r#Await ( $arg:ident : $ty:ty ) ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], feed, r#Await);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], await, feed, r#Await);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ feed r#await ( $arg:ident : $ty:ty ) ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], feed, r#await);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], await, feed, r#await);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ feed r#Dyn ( $arg:ident : $ty:ty ) ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], feed, r#Dyn);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], dyn, feed, r#Dyn);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ feed r#dyn ( $arg:ident : $ty:ty ) ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], feed, r#dyn);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], dyn, feed, r#dyn);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ feed r#Abstract ( $arg:ident : $ty:ty ) ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], feed, r#Abstract);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], abstract, feed, r#Abstract);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ feed r#abstract ( $arg:ident : $ty:ty ) ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], feed, r#abstract);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], abstract, feed, r#abstract);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ feed r#Become ( $arg:ident : $ty:ty ) ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], feed, r#Become);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], become, feed, r#Become);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ feed r#become ( $arg:ident : $ty:ty ) ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], feed, r#become);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], become, feed, r#become);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ feed r#Box ( $arg:ident : $ty:ty ) ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], feed, r#Box);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], box, feed, r#Box);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ feed r#box ( $arg:ident : $ty:ty ) ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], feed, r#box);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], box, feed, r#box);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ feed r#Do ( $arg:ident : $ty:ty ) ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], feed, r#Do);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], do, feed, r#Do);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ feed r#do ( $arg:ident : $ty:ty ) ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], feed, r#do);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], do, feed, r#do);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ feed r#Final ( $arg:ident : $ty:ty ) ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], feed, r#Final);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], final, feed, r#Final);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ feed r#final ( $arg:ident : $ty:ty ) ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], feed, r#final);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], final, feed, r#final);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ feed r#Macro ( $arg:ident : $ty:ty ) ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], feed, r#Macro);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], macro, feed, r#Macro);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ feed r#macro ( $arg:ident : $ty:ty ) ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], feed, r#macro);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], macro, feed, r#macro);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ feed r#Override ( $arg:ident : $ty:ty ) ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], feed, r#Override);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], override, feed, r#Override);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ feed r#override ( $arg:ident : $ty:ty ) ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], feed, r#override);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], override, feed, r#override);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ feed r#Priv ( $arg:ident : $ty:ty ) ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], feed, r#Priv);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], priv, feed, r#Priv);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ feed r#priv ( $arg:ident : $ty:ty ) ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], feed, r#priv);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], priv, feed, r#priv);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ feed r#Typeof ( $arg:ident : $ty:ty ) ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], feed, r#Typeof);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], typeof, feed, r#Typeof);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ feed r#typeof ( $arg:ident : $ty:ty ) ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], feed, r#typeof);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], typeof, feed, r#typeof);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ feed r#Unsized ( $arg:ident : $ty:ty ) ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], feed, r#Unsized);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], unsized, feed, r#Unsized);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ feed r#unsized ( $arg:ident : $ty:ty ) ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], feed, r#unsized);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], unsized, feed, r#unsized);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ feed r#Virtual ( $arg:ident : $ty:ty ) ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], feed, r#Virtual);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], virtual, feed, r#Virtual);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ feed r#virtual ( $arg:ident : $ty:ty ) ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], feed, r#virtual);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], virtual, feed, r#virtual);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ feed r#Yield ( $arg:ident : $ty:ty ) ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], feed, r#Yield);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], yield, feed, r#Yield);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ feed r#yield ( $arg:ident : $ty:ty ) ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], feed, r#yield);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], yield, feed, r#yield);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ feed r#Try ( $arg:ident : $ty:ty ) ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], feed, r#Try);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], try, feed, r#Try);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ feed r#try ( $arg:ident : $ty:ty ) ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], feed, r#try);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], try, feed, r#try);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ feed r#Gen ( $arg:ident : $ty:ty ) ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], feed, r#Gen);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], gen, feed, r#Gen);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ feed r#gen ( $arg:ident : $ty:ty ) ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], feed, r#gen);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], gen, feed, r#gen);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };

    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ feed Crate ( $arg:ident : $ty:ty ) ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], feed, Crate);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], Crate, feed, Crate);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ feed Super ( $arg:ident : $ty:ty ) ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], feed, Super);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], Super, feed, Super);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };

    // `Feeds::at` is the bundle's cursor, so discard the declaration after the
    // direct diagnostic instead of letting its field collide with that cursor.
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ feed At ( $arg:ident : $ty:ty ) ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], feed, At);
        $crate::__keylink_generated_name_check_module_item!([
            $( $attrs )*
        ], feeds, at, At, feed);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ feed at ( $arg:ident : $ty:ty ) ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], feed, at);
        $crate::__keylink_generated_name_check_module_item!([
            $( $attrs )*
        ], feeds, at, at, feed);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };

    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ feed r#At ( $arg:ident : $ty:ty ) ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], feed, r#At);
        $crate::__keylink_generated_name_check_module_item!([
            $( $attrs )*
        ], feeds, at, r#At, feed);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ feed r#at ( $arg:ident : $ty:ty ) ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], feed, r#at);
        $crate::__keylink_generated_name_check_module_item!([
            $( $attrs )*
        ], feeds, at, r#at, feed);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };


    // Feed names that are keywords still need a direct diagnostic before the
    // generic feed arm emits an unrawized `Feeds` field. The outer matcher accepts
    // these tokens; the dedicated arms below discard them before generated syntax.
    //
    // The valid one-argument feed arm is below the keyword arms and before the wire
    // arm. That order is load-bearing: `feed Fills(f: F) -> ();` would otherwise
    // match the wire arm and fail inside `__keylink_response_ty!` with "no rule
    // expected `feed`", pointing at a macro the author did not write.
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ feed as ( $arg:ident : $ty:ty ) ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], feed, as);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], as, feed, as);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ feed break ( $arg:ident : $ty:ty ) ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], feed, break);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], break, feed, break);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ feed const ( $arg:ident : $ty:ty ) ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], feed, const);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], const, feed, const);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ feed continue ( $arg:ident : $ty:ty ) ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], feed, continue);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], continue, feed, continue);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ feed else ( $arg:ident : $ty:ty ) ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], feed, else);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], else, feed, else);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ feed enum ( $arg:ident : $ty:ty ) ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], feed, enum);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], enum, feed, enum);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ feed extern ( $arg:ident : $ty:ty ) ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], feed, extern);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], extern, feed, extern);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ feed false ( $arg:ident : $ty:ty ) ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], feed, false);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], false, feed, false);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ feed fn ( $arg:ident : $ty:ty ) ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], feed, fn);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], fn, feed, fn);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ feed for ( $arg:ident : $ty:ty ) ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], feed, for);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], for, feed, for);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ feed if ( $arg:ident : $ty:ty ) ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], feed, if);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], if, feed, if);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ feed impl ( $arg:ident : $ty:ty ) ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], feed, impl);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], impl, feed, impl);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ feed in ( $arg:ident : $ty:ty ) ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], feed, in);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], in, feed, in);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ feed let ( $arg:ident : $ty:ty ) ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], feed, let);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], let, feed, let);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ feed loop ( $arg:ident : $ty:ty ) ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], feed, loop);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], loop, feed, loop);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ feed match ( $arg:ident : $ty:ty ) ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], feed, match);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], match, feed, match);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ feed mod ( $arg:ident : $ty:ty ) ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], feed, mod);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], mod, feed, mod);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ feed move ( $arg:ident : $ty:ty ) ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], feed, move);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], move, feed, move);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ feed mut ( $arg:ident : $ty:ty ) ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], feed, mut);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], mut, feed, mut);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ feed pub ( $arg:ident : $ty:ty ) ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], feed, pub);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], pub, feed, pub);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ feed ref ( $arg:ident : $ty:ty ) ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], feed, ref);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], ref, feed, ref);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ feed return ( $arg:ident : $ty:ty ) ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], feed, return);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], return, feed, return);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ feed static ( $arg:ident : $ty:ty ) ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], feed, static);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], static, feed, static);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ feed struct ( $arg:ident : $ty:ty ) ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], feed, struct);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], struct, feed, struct);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ feed trait ( $arg:ident : $ty:ty ) ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], feed, trait);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], trait, feed, trait);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ feed true ( $arg:ident : $ty:ty ) ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], feed, true);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], true, feed, true);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ feed type ( $arg:ident : $ty:ty ) ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], feed, type);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], type, feed, type);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ feed unsafe ( $arg:ident : $ty:ty ) ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], feed, unsafe);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], unsafe, feed, unsafe);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ feed use ( $arg:ident : $ty:ty ) ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], feed, use);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], use, feed, use);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ feed where ( $arg:ident : $ty:ty ) ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], feed, where);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], where, feed, where);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ feed while ( $arg:ident : $ty:ty ) ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], feed, while);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], while, feed, while);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ feed async ( $arg:ident : $ty:ty ) ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], feed, async);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], async, feed, async);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ feed await ( $arg:ident : $ty:ty ) ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], feed, await);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], await, feed, await);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ feed dyn ( $arg:ident : $ty:ty ) ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], feed, dyn);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], dyn, feed, dyn);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ feed abstract ( $arg:ident : $ty:ty ) ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], feed, abstract);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], abstract, feed, abstract);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ feed become ( $arg:ident : $ty:ty ) ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], feed, become);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], become, feed, become);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ feed box ( $arg:ident : $ty:ty ) ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], feed, box);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], box, feed, box);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ feed do ( $arg:ident : $ty:ty ) ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], feed, do);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], do, feed, do);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ feed final ( $arg:ident : $ty:ty ) ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], feed, final);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], final, feed, final);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ feed macro ( $arg:ident : $ty:ty ) ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], feed, macro);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], macro, feed, macro);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ feed override ( $arg:ident : $ty:ty ) ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], feed, override);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], override, feed, override);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ feed priv ( $arg:ident : $ty:ty ) ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], feed, priv);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], priv, feed, priv);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ feed typeof ( $arg:ident : $ty:ty ) ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], feed, typeof);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], typeof, feed, typeof);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ feed unsized ( $arg:ident : $ty:ty ) ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], feed, unsized);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], unsized, feed, unsized);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ feed virtual ( $arg:ident : $ty:ty ) ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], feed, virtual);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], virtual, feed, virtual);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ feed yield ( $arg:ident : $ty:ty ) ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], feed, yield);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], yield, feed, yield);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ feed try ( $arg:ident : $ty:ty ) ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], feed, try);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], try, feed, try);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ feed gen ( $arg:ident : $ty:ty ) ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], feed, gen);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], gen, feed, gen);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ feed crate ( $arg:ident : $ty:ty ) ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], feed, crate);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], crate, feed, crate);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ feed super ( $arg:ident : $ty:ty ) ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], feed, super);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], super, feed, super);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    // One valid `feed`, into the `@feeds` list. No return type, and exactly one
    // argument. The generated field is emitted later from that list.
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ feed $feed:ident ( $arg:ident : $ty:ty ) ; $( $rest:tt )* ]
    ) => {
        $crate::__keylink_service_munch! {
            @svc $svc
            @done [ $( $done )* ]
            @feeds [
                $( $feeds )*
                { [ $( $attrs )* ] [ $( $cfg )* ] [ $( $seen )* ] feed $feed ( $arg : $ty ) }
            ]
            @attrs [ ] @cfg [ ] @defer [ ]
            @rest [ $( $rest )* ]
        }
    };
    // A `feed` written with a return type.
    (
        @svc $svc:tt @done $done:tt @feeds $feeds:tt @attrs $attrs:tt @cfg $cfg:tt @defer $defer:tt
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
        @svc $svc:tt @done $done:tt @feeds $feeds:tt @attrs $attrs:tt @cfg $cfg:tt @defer $defer:tt
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

    // `crate` and `super` cannot be raw identifiers. Reject their CamelCase
    // declarations before the raw method names are pasted.
    // The same early diagnostic is needed when the declaration token itself is
    // a lowercase keyword; Rust would otherwise report only a parser error.
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ $kind:ident as ( $( $args:tt )* ) -> $ret:ty ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], $kind, as);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], as, $kind, as);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ $kind:ident break ( $( $args:tt )* ) -> $ret:ty ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], $kind, break);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], break, $kind, break);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ $kind:ident const ( $( $args:tt )* ) -> $ret:ty ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], $kind, const);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], const, $kind, const);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ $kind:ident continue ( $( $args:tt )* ) -> $ret:ty ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], $kind, continue);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], continue, $kind, continue);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ $kind:ident else ( $( $args:tt )* ) -> $ret:ty ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], $kind, else);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], else, $kind, else);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ $kind:ident enum ( $( $args:tt )* ) -> $ret:ty ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], $kind, enum);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], enum, $kind, enum);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ $kind:ident extern ( $( $args:tt )* ) -> $ret:ty ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], $kind, extern);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], extern, $kind, extern);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ $kind:ident false ( $( $args:tt )* ) -> $ret:ty ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], $kind, false);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], false, $kind, false);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ $kind:ident fn ( $( $args:tt )* ) -> $ret:ty ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], $kind, fn);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], fn, $kind, fn);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ $kind:ident for ( $( $args:tt )* ) -> $ret:ty ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], $kind, for);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], for, $kind, for);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ $kind:ident if ( $( $args:tt )* ) -> $ret:ty ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], $kind, if);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], if, $kind, if);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ $kind:ident impl ( $( $args:tt )* ) -> $ret:ty ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], $kind, impl);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], impl, $kind, impl);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ $kind:ident in ( $( $args:tt )* ) -> $ret:ty ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], $kind, in);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], in, $kind, in);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ $kind:ident let ( $( $args:tt )* ) -> $ret:ty ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], $kind, let);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], let, $kind, let);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ $kind:ident loop ( $( $args:tt )* ) -> $ret:ty ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], $kind, loop);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], loop, $kind, loop);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ $kind:ident match ( $( $args:tt )* ) -> $ret:ty ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], $kind, match);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], match, $kind, match);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ $kind:ident mod ( $( $args:tt )* ) -> $ret:ty ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], $kind, mod);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], mod, $kind, mod);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ $kind:ident move ( $( $args:tt )* ) -> $ret:ty ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], $kind, move);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], move, $kind, move);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ $kind:ident mut ( $( $args:tt )* ) -> $ret:ty ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], $kind, mut);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], mut, $kind, mut);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ $kind:ident pub ( $( $args:tt )* ) -> $ret:ty ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], $kind, pub);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], pub, $kind, pub);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ $kind:ident ref ( $( $args:tt )* ) -> $ret:ty ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], $kind, ref);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], ref, $kind, ref);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ $kind:ident return ( $( $args:tt )* ) -> $ret:ty ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], $kind, return);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], return, $kind, return);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ $kind:ident static ( $( $args:tt )* ) -> $ret:ty ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], $kind, static);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], static, $kind, static);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ $kind:ident struct ( $( $args:tt )* ) -> $ret:ty ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], $kind, struct);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], struct, $kind, struct);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ $kind:ident trait ( $( $args:tt )* ) -> $ret:ty ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], $kind, trait);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], trait, $kind, trait);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ $kind:ident true ( $( $args:tt )* ) -> $ret:ty ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], $kind, true);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], true, $kind, true);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ $kind:ident type ( $( $args:tt )* ) -> $ret:ty ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], $kind, type);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], type, $kind, type);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ $kind:ident unsafe ( $( $args:tt )* ) -> $ret:ty ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], $kind, unsafe);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], unsafe, $kind, unsafe);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ $kind:ident use ( $( $args:tt )* ) -> $ret:ty ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], $kind, use);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], use, $kind, use);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ $kind:ident where ( $( $args:tt )* ) -> $ret:ty ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], $kind, where);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], where, $kind, where);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ $kind:ident while ( $( $args:tt )* ) -> $ret:ty ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], $kind, while);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], while, $kind, while);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ $kind:ident async ( $( $args:tt )* ) -> $ret:ty ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], $kind, async);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], async, $kind, async);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ $kind:ident await ( $( $args:tt )* ) -> $ret:ty ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], $kind, await);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], await, $kind, await);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ $kind:ident dyn ( $( $args:tt )* ) -> $ret:ty ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], $kind, dyn);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], dyn, $kind, dyn);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ $kind:ident abstract ( $( $args:tt )* ) -> $ret:ty ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], $kind, abstract);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], abstract, $kind, abstract);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ $kind:ident become ( $( $args:tt )* ) -> $ret:ty ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], $kind, become);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], become, $kind, become);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ $kind:ident box ( $( $args:tt )* ) -> $ret:ty ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], $kind, box);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], box, $kind, box);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ $kind:ident do ( $( $args:tt )* ) -> $ret:ty ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], $kind, do);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], do, $kind, do);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ $kind:ident final ( $( $args:tt )* ) -> $ret:ty ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], $kind, final);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], final, $kind, final);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ $kind:ident macro ( $( $args:tt )* ) -> $ret:ty ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], $kind, macro);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], macro, $kind, macro);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ $kind:ident override ( $( $args:tt )* ) -> $ret:ty ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], $kind, override);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], override, $kind, override);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ $kind:ident priv ( $( $args:tt )* ) -> $ret:ty ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], $kind, priv);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], priv, $kind, priv);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ $kind:ident typeof ( $( $args:tt )* ) -> $ret:ty ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], $kind, typeof);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], typeof, $kind, typeof);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ $kind:ident unsized ( $( $args:tt )* ) -> $ret:ty ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], $kind, unsized);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], unsized, $kind, unsized);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ $kind:ident virtual ( $( $args:tt )* ) -> $ret:ty ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], $kind, virtual);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], virtual, $kind, virtual);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ $kind:ident yield ( $( $args:tt )* ) -> $ret:ty ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], $kind, yield);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], yield, $kind, yield);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ $kind:ident try ( $( $args:tt )* ) -> $ret:ty ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], $kind, try);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], try, $kind, try);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ $kind:ident gen ( $( $args:tt )* ) -> $ret:ty ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], $kind, gen);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], gen, $kind, gen);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ $kind:ident crate ( $( $args:tt )* ) -> $ret:ty ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], $kind, crate);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], crate, $kind, crate);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ $kind:ident super ( $( $args:tt )* ) -> $ret:ty ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], $kind, super);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], super, $kind, super);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ $kind:ident r#As ( $( $args:tt )* ) -> $ret:ty ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], $kind, r#As);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], as, $kind, r#As);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ $kind:ident r#as ( $( $args:tt )* ) -> $ret:ty ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], $kind, r#as);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], as, $kind, r#as);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ $kind:ident r#Break ( $( $args:tt )* ) -> $ret:ty ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], $kind, r#Break);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], break, $kind, r#Break);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ $kind:ident r#break ( $( $args:tt )* ) -> $ret:ty ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], $kind, r#break);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], break, $kind, r#break);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ $kind:ident r#Const ( $( $args:tt )* ) -> $ret:ty ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], $kind, r#Const);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], const, $kind, r#Const);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ $kind:ident r#const ( $( $args:tt )* ) -> $ret:ty ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], $kind, r#const);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], const, $kind, r#const);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ $kind:ident r#Continue ( $( $args:tt )* ) -> $ret:ty ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], $kind, r#Continue);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], continue, $kind, r#Continue);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ $kind:ident r#continue ( $( $args:tt )* ) -> $ret:ty ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], $kind, r#continue);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], continue, $kind, r#continue);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ $kind:ident r#Else ( $( $args:tt )* ) -> $ret:ty ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], $kind, r#Else);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], else, $kind, r#Else);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ $kind:ident r#else ( $( $args:tt )* ) -> $ret:ty ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], $kind, r#else);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], else, $kind, r#else);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ $kind:ident r#Enum ( $( $args:tt )* ) -> $ret:ty ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], $kind, r#Enum);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], enum, $kind, r#Enum);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ $kind:ident r#enum ( $( $args:tt )* ) -> $ret:ty ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], $kind, r#enum);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], enum, $kind, r#enum);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ $kind:ident r#Extern ( $( $args:tt )* ) -> $ret:ty ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], $kind, r#Extern);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], extern, $kind, r#Extern);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ $kind:ident r#extern ( $( $args:tt )* ) -> $ret:ty ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], $kind, r#extern);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], extern, $kind, r#extern);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ $kind:ident r#False ( $( $args:tt )* ) -> $ret:ty ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], $kind, r#False);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], false, $kind, r#False);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ $kind:ident r#false ( $( $args:tt )* ) -> $ret:ty ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], $kind, r#false);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], false, $kind, r#false);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ $kind:ident r#Fn ( $( $args:tt )* ) -> $ret:ty ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], $kind, r#Fn);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], fn, $kind, r#Fn);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ $kind:ident r#fn ( $( $args:tt )* ) -> $ret:ty ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], $kind, r#fn);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], fn, $kind, r#fn);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ $kind:ident r#For ( $( $args:tt )* ) -> $ret:ty ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], $kind, r#For);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], for, $kind, r#For);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ $kind:ident r#for ( $( $args:tt )* ) -> $ret:ty ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], $kind, r#for);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], for, $kind, r#for);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ $kind:ident r#If ( $( $args:tt )* ) -> $ret:ty ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], $kind, r#If);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], if, $kind, r#If);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ $kind:ident r#if ( $( $args:tt )* ) -> $ret:ty ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], $kind, r#if);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], if, $kind, r#if);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ $kind:ident r#Impl ( $( $args:tt )* ) -> $ret:ty ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], $kind, r#Impl);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], impl, $kind, r#Impl);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ $kind:ident r#impl ( $( $args:tt )* ) -> $ret:ty ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], $kind, r#impl);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], impl, $kind, r#impl);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ $kind:ident r#In ( $( $args:tt )* ) -> $ret:ty ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], $kind, r#In);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], in, $kind, r#In);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ $kind:ident r#in ( $( $args:tt )* ) -> $ret:ty ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], $kind, r#in);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], in, $kind, r#in);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ $kind:ident r#Let ( $( $args:tt )* ) -> $ret:ty ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], $kind, r#Let);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], let, $kind, r#Let);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ $kind:ident r#let ( $( $args:tt )* ) -> $ret:ty ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], $kind, r#let);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], let, $kind, r#let);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ $kind:ident r#Loop ( $( $args:tt )* ) -> $ret:ty ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], $kind, r#Loop);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], loop, $kind, r#Loop);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ $kind:ident r#loop ( $( $args:tt )* ) -> $ret:ty ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], $kind, r#loop);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], loop, $kind, r#loop);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ $kind:ident r#Match ( $( $args:tt )* ) -> $ret:ty ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], $kind, r#Match);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], match, $kind, r#Match);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ $kind:ident r#match ( $( $args:tt )* ) -> $ret:ty ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], $kind, r#match);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], match, $kind, r#match);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ $kind:ident r#Mod ( $( $args:tt )* ) -> $ret:ty ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], $kind, r#Mod);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], mod, $kind, r#Mod);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ $kind:ident r#mod ( $( $args:tt )* ) -> $ret:ty ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], $kind, r#mod);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], mod, $kind, r#mod);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ $kind:ident r#Move ( $( $args:tt )* ) -> $ret:ty ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], $kind, r#Move);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], move, $kind, r#Move);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ $kind:ident r#move ( $( $args:tt )* ) -> $ret:ty ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], $kind, r#move);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], move, $kind, r#move);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ $kind:ident r#Mut ( $( $args:tt )* ) -> $ret:ty ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], $kind, r#Mut);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], mut, $kind, r#Mut);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ $kind:ident r#mut ( $( $args:tt )* ) -> $ret:ty ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], $kind, r#mut);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], mut, $kind, r#mut);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ $kind:ident r#Pub ( $( $args:tt )* ) -> $ret:ty ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], $kind, r#Pub);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], pub, $kind, r#Pub);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ $kind:ident r#pub ( $( $args:tt )* ) -> $ret:ty ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], $kind, r#pub);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], pub, $kind, r#pub);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ $kind:ident r#Ref ( $( $args:tt )* ) -> $ret:ty ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], $kind, r#Ref);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], ref, $kind, r#Ref);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ $kind:ident r#ref ( $( $args:tt )* ) -> $ret:ty ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], $kind, r#ref);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], ref, $kind, r#ref);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ $kind:ident r#Return ( $( $args:tt )* ) -> $ret:ty ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], $kind, r#Return);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], return, $kind, r#Return);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ $kind:ident r#return ( $( $args:tt )* ) -> $ret:ty ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], $kind, r#return);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], return, $kind, r#return);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ $kind:ident r#Static ( $( $args:tt )* ) -> $ret:ty ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], $kind, r#Static);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], static, $kind, r#Static);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ $kind:ident r#static ( $( $args:tt )* ) -> $ret:ty ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], $kind, r#static);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], static, $kind, r#static);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ $kind:ident r#Struct ( $( $args:tt )* ) -> $ret:ty ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], $kind, r#Struct);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], struct, $kind, r#Struct);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ $kind:ident r#struct ( $( $args:tt )* ) -> $ret:ty ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], $kind, r#struct);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], struct, $kind, r#struct);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ $kind:ident r#Trait ( $( $args:tt )* ) -> $ret:ty ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], $kind, r#Trait);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], trait, $kind, r#Trait);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ $kind:ident r#trait ( $( $args:tt )* ) -> $ret:ty ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], $kind, r#trait);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], trait, $kind, r#trait);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ $kind:ident r#True ( $( $args:tt )* ) -> $ret:ty ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], $kind, r#True);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], true, $kind, r#True);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ $kind:ident r#true ( $( $args:tt )* ) -> $ret:ty ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], $kind, r#true);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], true, $kind, r#true);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ $kind:ident r#Type ( $( $args:tt )* ) -> $ret:ty ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], $kind, r#Type);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], type, $kind, r#Type);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ $kind:ident r#type ( $( $args:tt )* ) -> $ret:ty ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], $kind, r#type);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], type, $kind, r#type);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ $kind:ident r#Unsafe ( $( $args:tt )* ) -> $ret:ty ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], $kind, r#Unsafe);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], unsafe, $kind, r#Unsafe);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ $kind:ident r#unsafe ( $( $args:tt )* ) -> $ret:ty ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], $kind, r#unsafe);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], unsafe, $kind, r#unsafe);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ $kind:ident r#Use ( $( $args:tt )* ) -> $ret:ty ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], $kind, r#Use);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], use, $kind, r#Use);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ $kind:ident r#use ( $( $args:tt )* ) -> $ret:ty ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], $kind, r#use);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], use, $kind, r#use);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ $kind:ident r#Where ( $( $args:tt )* ) -> $ret:ty ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], $kind, r#Where);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], where, $kind, r#Where);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ $kind:ident r#where ( $( $args:tt )* ) -> $ret:ty ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], $kind, r#where);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], where, $kind, r#where);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ $kind:ident r#While ( $( $args:tt )* ) -> $ret:ty ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], $kind, r#While);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], while, $kind, r#While);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ $kind:ident r#while ( $( $args:tt )* ) -> $ret:ty ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], $kind, r#while);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], while, $kind, r#while);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ $kind:ident r#Async ( $( $args:tt )* ) -> $ret:ty ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], $kind, r#Async);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], async, $kind, r#Async);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ $kind:ident r#async ( $( $args:tt )* ) -> $ret:ty ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], $kind, r#async);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], async, $kind, r#async);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ $kind:ident r#Await ( $( $args:tt )* ) -> $ret:ty ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], $kind, r#Await);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], await, $kind, r#Await);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ $kind:ident r#await ( $( $args:tt )* ) -> $ret:ty ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], $kind, r#await);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], await, $kind, r#await);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ $kind:ident r#Dyn ( $( $args:tt )* ) -> $ret:ty ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], $kind, r#Dyn);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], dyn, $kind, r#Dyn);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ $kind:ident r#dyn ( $( $args:tt )* ) -> $ret:ty ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], $kind, r#dyn);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], dyn, $kind, r#dyn);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ $kind:ident r#Abstract ( $( $args:tt )* ) -> $ret:ty ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], $kind, r#Abstract);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], abstract, $kind, r#Abstract);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ $kind:ident r#abstract ( $( $args:tt )* ) -> $ret:ty ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], $kind, r#abstract);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], abstract, $kind, r#abstract);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ $kind:ident r#Become ( $( $args:tt )* ) -> $ret:ty ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], $kind, r#Become);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], become, $kind, r#Become);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ $kind:ident r#become ( $( $args:tt )* ) -> $ret:ty ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], $kind, r#become);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], become, $kind, r#become);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ $kind:ident r#Box ( $( $args:tt )* ) -> $ret:ty ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], $kind, r#Box);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], box, $kind, r#Box);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ $kind:ident r#box ( $( $args:tt )* ) -> $ret:ty ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], $kind, r#box);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], box, $kind, r#box);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ $kind:ident r#Do ( $( $args:tt )* ) -> $ret:ty ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], $kind, r#Do);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], do, $kind, r#Do);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ $kind:ident r#do ( $( $args:tt )* ) -> $ret:ty ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], $kind, r#do);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], do, $kind, r#do);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ $kind:ident r#Final ( $( $args:tt )* ) -> $ret:ty ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], $kind, r#Final);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], final, $kind, r#Final);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ $kind:ident r#final ( $( $args:tt )* ) -> $ret:ty ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], $kind, r#final);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], final, $kind, r#final);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ $kind:ident r#Macro ( $( $args:tt )* ) -> $ret:ty ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], $kind, r#Macro);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], macro, $kind, r#Macro);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ $kind:ident r#macro ( $( $args:tt )* ) -> $ret:ty ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], $kind, r#macro);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], macro, $kind, r#macro);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ $kind:ident r#Override ( $( $args:tt )* ) -> $ret:ty ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], $kind, r#Override);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], override, $kind, r#Override);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ $kind:ident r#override ( $( $args:tt )* ) -> $ret:ty ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], $kind, r#override);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], override, $kind, r#override);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ $kind:ident r#Priv ( $( $args:tt )* ) -> $ret:ty ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], $kind, r#Priv);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], priv, $kind, r#Priv);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ $kind:ident r#priv ( $( $args:tt )* ) -> $ret:ty ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], $kind, r#priv);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], priv, $kind, r#priv);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ $kind:ident r#Typeof ( $( $args:tt )* ) -> $ret:ty ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], $kind, r#Typeof);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], typeof, $kind, r#Typeof);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ $kind:ident r#typeof ( $( $args:tt )* ) -> $ret:ty ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], $kind, r#typeof);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], typeof, $kind, r#typeof);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ $kind:ident r#Unsized ( $( $args:tt )* ) -> $ret:ty ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], $kind, r#Unsized);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], unsized, $kind, r#Unsized);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ $kind:ident r#unsized ( $( $args:tt )* ) -> $ret:ty ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], $kind, r#unsized);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], unsized, $kind, r#unsized);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ $kind:ident r#Virtual ( $( $args:tt )* ) -> $ret:ty ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], $kind, r#Virtual);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], virtual, $kind, r#Virtual);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ $kind:ident r#virtual ( $( $args:tt )* ) -> $ret:ty ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], $kind, r#virtual);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], virtual, $kind, r#virtual);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ $kind:ident r#Yield ( $( $args:tt )* ) -> $ret:ty ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], $kind, r#Yield);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], yield, $kind, r#Yield);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ $kind:ident r#yield ( $( $args:tt )* ) -> $ret:ty ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], $kind, r#yield);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], yield, $kind, r#yield);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ $kind:ident r#Try ( $( $args:tt )* ) -> $ret:ty ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], $kind, r#Try);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], try, $kind, r#Try);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ $kind:ident r#try ( $( $args:tt )* ) -> $ret:ty ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], $kind, r#try);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], try, $kind, r#try);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ $kind:ident r#Gen ( $( $args:tt )* ) -> $ret:ty ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], $kind, r#Gen);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], gen, $kind, r#Gen);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ $kind:ident r#gen ( $( $args:tt )* ) -> $ret:ty ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], $kind, r#gen);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], gen, $kind, r#gen);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };

    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ $kind:ident Crate ( $( $args:tt )* ) -> $ret:ty ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], $kind, Crate);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], Crate, $kind, Crate);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ $kind:ident Super ( $( $args:tt )* ) -> $ret:ty ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], $kind, Super);
        $crate::__keylink_keyword_name_check_item!([ $( $attrs )* ], Super, $kind, Super);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };

    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ $kind:ident __Status ( $( $args:tt )* ) -> $ret:ty ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], $kind, __Status);
        $crate::__keylink_status_name_checks!([ $( $attrs )* ], $kind, __Status);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ $kind:ident Served ( $( $args:tt )* ) -> $ret:ty ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], $kind, Served);
        $crate::__keylink_generated_name_check_module_item!([
            $( $attrs )*
        ], wire, Served, Served, $kind);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ $kind:ident r#__Status ( $( $args:tt )* ) -> $ret:ty ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], $kind, r#__Status);
        $crate::__keylink_status_name_checks!([ $( $attrs )* ], $kind, r#__Status);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ $kind:ident r#Served ( $( $args:tt )* ) -> $ret:ty ; $( $rest:tt )* ]
    ) => {
        $( $cfg )* $crate::__keylink_defer_check!([ $( $seen )* ], $kind, r#Served);
        $crate::__keylink_generated_name_check_module_item!([
            $( $attrs )*
        ], wire, Served, r#Served, $kind);
        $crate::__keylink_service_munch! {
            @svc $svc @done [ $( $done )* ] @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ] @rest [ $( $rest )* ]
        }
    };

    // One complete wire declaration, which closes the attribute run above it.
    (
        @svc $svc:tt @done [ $( $done:tt )* ] @feeds [ $( $feeds:tt )* ]
        @attrs [ $( $attrs:tt )* ] @cfg [ $( $cfg:tt )* ] @defer [ $( $seen:tt )* ]
        @rest [ $kind:ident $rpc:ident ( $( $args:tt )* ) -> $ret:ty ; $( $rest:tt )* ]
    ) => {
        $crate::__keylink_service_munch! {
            @svc $svc
            @done [
                $( $done )*
                { [ $( $attrs )* ] [ $( $cfg )* ] [ $( $seen )* ] $kind $rpc ( $( $args )* ) -> $ret }
            ]
            @feeds [ $( $feeds )* ]
            @attrs [ ] @cfg [ ] @defer [ ]
            @rest [ $( $rest )* ]
        }
    };
    // A declaration rejected above may be the entire body. The diagnostic from
    // that declaration is sufficient; do not add the catch-all grammar error.
    // An actually empty body cannot reach this arm because `service!` requires
    // at least one token after `service Name;`.
    (
        @svc [ $svc:ident $version:literal ] @done [ ] @feeds [ ]
        @attrs [ ] @cfg [ ] @defer [ ] @rest [ ]
    ) => {};

    // Done, with at least one wire declaration and no attribute left dangling.
    (
        @svc [ $svc:ident $version:literal ] @done [ $( $done:tt )+ ] @feeds [ $( $feeds:tt )* ]
        @attrs [ ] @cfg [ ] @defer [ ] @rest [ ]
    ) => {
        $crate::__keylink_service! {
            $svc;
            @version [ $version ] @previous [ ]
            @wire [ $( $done )+ ]
            @feeds [ $( $feeds )* ]
        }
    };
    // Feeds and nothing else. Refused, and the message says what is missing rather
    // than "at least one declaration is required" — there ARE declarations. A
    // disabled feed is removed before this diagnostic, so a cfg-disabled feeds-only
    // service does not fail merely because its declaration was present in source.
    (
        @svc [ $svc:ident $version:literal ] @done [ ]
        @feeds [ $( { [ $( #[$fmeta:meta] )* ] [ $( #[$fcfg:meta] )* ]
                     [ $( $fdefer:tt )? ] feed $feed:ident
                     ( $farg:ident : $fty:ty ) } )+ ]
        @attrs [ ] @cfg [ ] @defer [ ] @rest [ ]
    ) => {
        $( $( #[$fcfg] )*
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
        )); )+
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

/// Invoke one generated external method with its exact transport shape.
#[doc(hidden)]
#[macro_export]
macro_rules! __keylink_external_invoke {
    (rpc, $client:expr, $at:tt, $method:ident, ($( $arg:ident : $ty:ty ),*), $body:expr, $call:expr, $codec:expr) => {{
        let key = match $call {
            $crate::ExternalCall::Rpc { key } => key,
            other => return ::std::boxed::Box::pin(::core::future::ready(
                ::core::result::Result::Err($crate::ExternalError::WrongKind {
                    expected: $crate::ExternalKind::Rpc,
                    actual: other.kind(),
                })
            )),
        };
        #[derive(::serde::Deserialize)]
        #[serde(deny_unknown_fields)]
        struct __ExternalArgs { $( $arg: $ty, )* }
        let __ExternalArgs { $( $arg, )* } = match $codec.decode($body) {
            ::core::result::Result::Ok(args) => args,
            ::core::result::Result::Err(error) => return ::std::boxed::Box::pin(
                ::core::future::ready(::core::result::Result::Err(
                    $crate::ExternalError::Codec(error)
                ))
            ),
        };
        let client = $client.clone();
        let _at = $crate::__keylink_address!(@seed $at);
        $crate::pastey::paste! {
            ::std::boxed::Box::pin(async move {
                let value = match key {
                    ::core::option::Option::Some(key) => {
                        let client = $crate::WithIdempotency::with_idempotency(&client, key);
                        $crate::__keylink_address!(@to client, _at, $at)
                            .[<$method:snake>]($( $arg ),*)
                            .await
                    }
                    ::core::option::Option::None => {
                        $crate::__keylink_address!(@to client, _at, $at)
                            .[<$method:snake>]($( $arg ),*)
                            .await
                    }
                }
                .map_err($crate::ExternalError::Client)?;
                $codec.encode(&value)
                    .map($crate::ExternalOutput::Json)
                    .map_err($crate::ExternalError::Codec)
            })
        }
    }};
    (send, $client:expr, $at:tt, $method:ident, ($( $arg:ident : $ty:ty ),*), $body:expr, $call:expr, $codec:expr) => {{
        let upload = match $call {
            $crate::ExternalCall::Send { body } => body,
            other => return ::std::boxed::Box::pin(::core::future::ready(
                ::core::result::Result::Err($crate::ExternalError::WrongKind {
                    expected: $crate::ExternalKind::Send,
                    actual: other.kind(),
                })
            )),
        };
        #[derive(::serde::Deserialize)]
        #[serde(deny_unknown_fields)]
        struct __ExternalArgs { $( $arg: $ty, )* }
        let __ExternalArgs { $( $arg, )* } = match $codec.decode($body) {
            ::core::result::Result::Ok(args) => args,
            ::core::result::Result::Err(error) => return ::std::boxed::Box::pin(
                ::core::future::ready(::core::result::Result::Err(
                    $crate::ExternalError::Codec(error)
                ))
            ),
        };
        let client = $client.clone();
        let _at = $crate::__keylink_address!(@seed $at);
        $crate::pastey::paste! {
            ::std::boxed::Box::pin(async move {
                let value = $crate::__keylink_address!(@to client, _at, $at)
                    .[<$method:snake>]($( $arg, )* upload)
                    .await
                    .map_err($crate::ExternalError::Client)?;
                $codec.encode(&value)
                    .map($crate::ExternalOutput::Json)
                    .map_err($crate::ExternalError::Codec)
            })
        }
    }};
    (recv, $client:expr, $at:tt, $method:ident, ($( $arg:ident : $ty:ty ),*), $body:expr, $call:expr, $codec:expr) => {{
        match $call {
            $crate::ExternalCall::Recv => {}
            other => return ::std::boxed::Box::pin(::core::future::ready(
                ::core::result::Result::Err($crate::ExternalError::WrongKind {
                    expected: $crate::ExternalKind::Recv,
                    actual: other.kind(),
                })
            )),
        }
        #[derive(::serde::Deserialize)]
        #[serde(deny_unknown_fields)]
        struct __ExternalArgs { $( $arg: $ty, )* }
        let __ExternalArgs { $( $arg, )* } = match $codec.decode($body) {
            ::core::result::Result::Ok(args) => args,
            ::core::result::Result::Err(error) => return ::std::boxed::Box::pin(
                ::core::future::ready(::core::result::Result::Err(
                    $crate::ExternalError::Codec(error)
                ))
            ),
        };
        let client = $client.clone();
        let _at = $crate::__keylink_address!(@seed $at);
        $crate::pastey::paste! {
            ::std::boxed::Box::pin(async move {
                let (value, body) = $crate::__keylink_address!(@to client, _at, $at)
                    .[<$method:snake>]($( $arg ),*)
                    .await
                    .map_err($crate::ExternalError::Client)?;
                let value = $codec.encode(&value).map_err($crate::ExternalError::Codec)?;
                ::core::result::Result::Ok($crate::ExternalOutput::Recv { value, body })
            })
        }
    }};
    (blob, $client:expr, $at:tt, $method:ident, ($( $arg:ident : $ty:ty ),*), $body:expr, $call:expr, $codec:expr) => {{
        match $call {
            $crate::ExternalCall::Blob => {}
            other => return ::std::boxed::Box::pin(::core::future::ready(
                ::core::result::Result::Err($crate::ExternalError::WrongKind {
                    expected: $crate::ExternalKind::Blob,
                    actual: other.kind(),
                })
            )),
        }
        #[derive(::serde::Deserialize)]
        #[serde(deny_unknown_fields)]
        struct __ExternalArgs { $( $arg: $ty, )* }
        let __ExternalArgs { $( $arg, )* } = match $codec.decode($body) {
            ::core::result::Result::Ok(args) => args,
            ::core::result::Result::Err(error) => return ::std::boxed::Box::pin(
                ::core::future::ready(::core::result::Result::Err(
                    $crate::ExternalError::Codec(error)
                ))
            ),
        };
        let client = $client.clone();
        let _at = $crate::__keylink_address!(@seed $at);
        $crate::pastey::paste! {
            ::std::boxed::Box::pin(async move {
                let (value, handle) = $crate::__keylink_address!(@to client, _at, $at)
                    .[<$method:snake>]($( $arg ),*)
                    .await
                    .map_err($crate::ExternalError::Client)?;
                let value = $codec.encode(&value).map_err($crate::ExternalError::Codec)?;
                ::core::result::Result::Ok($crate::ExternalOutput::Blob { value, handle })
            })
        }
    }};
}

/// Where an external invocation is addressed: the singleton client, or one
/// actor on it.
///
/// **This is the entire difference between the two surfaces.** Copying the
/// four invoke arms to add `.at(actor)` was tried and reverted: it duplicated
/// the decode-and-encode scaffolding, and the copy immediately made the
/// registered anchor `HJ3` resolve twice, which is the failure `U10` names and
/// `HJ9` already cost this branch once.
///
/// Two rules, because `client.at(actor)` BORROWS the client and cannot be
/// passed in as an expression: `@seed` evaluates the actor once outside the
/// async block, and `@to` builds the receiver inside it. The singleton form
/// carries `()` through both and never names an actor at all.
#[doc(hidden)]
#[macro_export]
macro_rules! __keylink_address {
    (@seed ()) => {
        ()
    };
    (@seed ($actor:expr)) => {
        $actor.clone()
    };
    (@to $client:expr, $seed:expr, ()) => {
        &$client
    };
    // Moved, not cloned. The `rpc` arm expands this twice, in the two arms of
    // one `match`, and Rust permits moving the same local in exclusive arms —
    // so the clone that looks required is one `String` allocation per keyed
    // call that nothing needs.
    (@to $client:expr, $seed:expr, ($actor:expr)) => {
        $client.at($seed)
    };
}

/// Add one generated declaration to the external inventory when its kind is supported.
#[doc(hidden)]
#[macro_export]
macro_rules! __keylink_external_kind {
    (@value rpc) => { $crate::ExternalKind::Rpc };
    (@value send) => { $crate::ExternalKind::Send };
    (@value recv) => { $crate::ExternalKind::Recv };
    (@value blob) => { $crate::ExternalKind::Blob };
    // `rpc`, `recv` and `blob` join BOTH inventories, `send` only the singleton
    // one, everything else neither. Two accumulators walked once is what keeps
    // the actor list a strict subset without a second declaration list to hold
    // in step — and a macro cannot omit an array element conditionally, because
    // a macro in expression position must expand to exactly one expression.
    (@service [ $svc:ident ] @methods [ $( $methods:tt )* ] @actor [ $( $actor:tt )* ]
     @item [ [ $( #[$cfg:meta] )* ] rpc $method:ident ( $( $arg:ident : $ty:ty ),* ) -> $ret:ty ]
     @rest [ $( $rest:tt )* ]) => {
        $crate::__keylink_external! {
            @service [ $svc ]
            @methods [ $( $methods )* { [ $( #[$cfg] )* ] rpc $method ( $( $arg : $ty ),* ) -> $ret } ]
            @actor [ $( $actor )* { [ $( #[$cfg] )* ] rpc $method ( $( $arg : $ty ),* ) -> $ret } ]
            @rest [ $( $rest )* ]
        }
    };
    (@service [ $svc:ident ] @methods [ $( $methods:tt )* ] @actor [ $( $actor:tt )* ]
     @item [ [ $( #[$cfg:meta] )* ] send $method:ident ( $( $arg:ident : $ty:ty ),* ) -> $ret:ty ]
     @rest [ $( $rest:tt )* ]) => {
        $crate::__keylink_external! {
            @service [ $svc ]
            @methods [ $( $methods )* { [ $( #[$cfg] )* ] send $method ( $( $arg : $ty ),* ) -> $ret } ]
            @actor [ $( $actor )* ]
            @rest [ $( $rest )* ]
        }
    };
    (@service [ $svc:ident ] @methods [ $( $methods:tt )* ] @actor [ $( $actor:tt )* ]
     @item [ [ $( #[$cfg:meta] )* ] recv $method:ident ( $( $arg:ident : $ty:ty ),* ) -> $ret:ty ]
     @rest [ $( $rest:tt )* ]) => {
        $crate::__keylink_external! {
            @service [ $svc ]
            @methods [ $( $methods )* { [ $( #[$cfg] )* ] recv $method ( $( $arg : $ty ),* ) -> $ret } ]
            @actor [ $( $actor )* { [ $( #[$cfg] )* ] recv $method ( $( $arg : $ty ),* ) -> $ret } ]
            @rest [ $( $rest )* ]
        }
    };
    (@service [ $svc:ident ] @methods [ $( $methods:tt )* ] @actor [ $( $actor:tt )* ]
     @item [ [ $( #[$cfg:meta] )* ] blob $method:ident ( $( $arg:ident : $ty:ty ),* ) -> $ret:ty ]
     @rest [ $( $rest:tt )* ]) => {
        $crate::__keylink_external! {
            @service [ $svc ]
            @methods [ $( $methods )* { [ $( #[$cfg] )* ] blob $method ( $( $arg : $ty ),* ) -> $ret } ]
            @actor [ $( $actor )* { [ $( #[$cfg] )* ] blob $method ( $( $arg : $ty ),* ) -> $ret } ]
            @rest [ $( $rest )* ]
        }
    };
    (@service [ $svc:ident ] @methods [ $( $methods:tt )* ] @actor [ $( $actor:tt )* ]
     @item [ [ $( #[$cfg:meta] )* ] $kind:ident $method:ident ( $( $arg:ident : $ty:ty ),* ) -> $ret:ty ]
     @rest [ $( $rest:tt )* ]) => {
        $crate::__keylink_external! {
            @service [ $svc ] @methods [ $( $methods )* ] @actor [ $( $actor )* ]
            @rest [ $( $rest )* ]
        }
    };
}

/// Generate the dependency-neutral external conversion seam.
#[doc(hidden)]
#[macro_export]
macro_rules! __keylink_external {
    (
        @service [ $svc:ident ]
        @methods [ $( { [ $( #[$cfg:meta] )* ] $kind:ident $method:ident
                         ( $( $arg:ident : $ty:ty ),* ) -> $ret:ty } )* ]
        @actor [ $( { [ $( #[$acfg:meta] )* ] $akind:ident $amethod:ident
                         ( $( $aarg:ident : $aty:ty ),* ) -> $aret:ty } )* ]
        @rest [ ]
    ) => {
        /// Hand this service's declaration list to a macro that wants to read it.
        ///
        /// The one seam a documentation generator gets, and it is deliberately
        /// narrow: a callback macro path in, the declarations out, nothing
        /// else. It carries the TYPES rather than their names, because the
        /// only thing a caller can usefully do with a declaration is name its
        /// request and response types in code it generates.
        ///
        /// MODULE-SCOPED, not crate-root exported. Two things stack here and
        /// only one of them is load-bearing: `pub(crate) use` makes it
        /// nameable as `module::__keylink_declarations!` within its own crate,
        /// and the ABSENCE of `#[macro_export]` is what keeps it out of every
        /// other crate — measured, by relaxing the `use` to `pub` and watching
        /// the downstream `compile_fail` in `fixtures/imported-ledger` stay
        /// red.
        ///
        /// That is the orphan story: a downstream crate cannot invoke this, so
        /// it cannot implement a foreign documentation trait for this crate's
        /// `External`.
        ///
        /// The `$d:tt` indirection is how a `macro_rules!` writes another
        /// `macro_rules!` on stable: `$` cannot be escaped inside a macro body,
        /// so the dollar arrives as a token.
        #[doc(hidden)]
        macro_rules! __keylink_declarations_with {
            ( $d:tt ) => {
                #[doc(hidden)]
                macro_rules! __keylink_declarations {
                    ( $d callback:path $d (, $d extra:tt )* ) => {
                        $d callback! {
                            $d ( $d extra, )*
                            service = ::core::stringify!($svc),
                            singleton = [ $(
                                {
                                    cfg = [ $( #[$cfg] )* ],
                                    kind = $kind,
                                    method = ::core::stringify!($method),
                                    args = [ $( $arg : $ty ),* ],
                                    ret = $ret,
                                }
                            )* ],
                            actor = [ $(
                                {
                                    cfg = [ $( #[$acfg] )* ],
                                    kind = $akind,
                                    method = ::core::stringify!($amethod),
                                    args = [ $( $aarg : $aty ),* ],
                                    ret = $aret,
                                }
                            )* ],
                        }
                    };
                }
            };
        }
        __keylink_declarations_with!($);
        #[doc(hidden)]
        pub(crate) use __keylink_declarations;

        /// Marker for this generated singleton service's external conversion seam.
        pub struct External;

        impl $crate::ExternalService for External {
            const SERVICE: &'static str = ::core::stringify!($svc);
            const METHODS: &'static [$crate::ExternalMethod] = &[
                $( $( #[$cfg] )* $crate::ExternalMethod::new(
                    ::core::stringify!($method), $crate::__keylink_external_kind!(@value $kind)
                ), )*
            ];
            type Client = Client;

            fn invoke<C: $crate::ServiceCodec>(
                client: &Self::Client,
                method: &str,
                body: &[u8],
                call: $crate::ExternalCall,
                codec: C,
            ) -> $crate::ExternalFuture<C> {
                let _ = (client, body, &call, &codec);
                match method {
                    $(
                        $( #[$cfg] )*
                        ::core::stringify!($method) => $crate::__keylink_external_invoke!(
                            $kind, client, (), $method, ($( $arg : $ty ),*), body, call, codec
                        ),
                    )*
                    _ => ::std::boxed::Box::pin(::core::future::ready(
                        ::core::result::Result::Err($crate::ExternalError::UnknownMethod)
                    )),
                }
            }
        }

        impl $crate::ExternalActorService for External {
            const SERVICE: &'static str = ::core::stringify!($svc);
            const METHODS: &'static [$crate::ExternalMethod] = &[
                $( $( #[$acfg] )* $crate::ExternalMethod::new(
                    ::core::stringify!($amethod), $crate::__keylink_external_kind!(@value $akind)
                ), )*
            ];
            type Client = RegistryClient;

            fn invoke_at<C: $crate::ServiceCodec>(
                client: &Self::Client,
                actor: &$crate::ActorId,
                method: &str,
                body: &[u8],
                call: $crate::ExternalCall,
                codec: C,
            ) -> $crate::ExternalFuture<C> {
                let _ = (client, actor, body, &call, &codec);
                match method {
                    $(
                        $( #[$acfg] )*
                        // The same four arms the singleton impl calls, told to
                        // address one actor. Nothing about decoding, dispatch
                        // or encoding is written twice.
                        ::core::stringify!($amethod) => $crate::__keylink_external_invoke!(
                            $akind, client, (actor), $amethod,
                            ($( $aarg : $aty ),*), body, call, codec
                        ),
                    )*
                    _ => ::std::boxed::Box::pin(::core::future::ready(
                        ::core::result::Result::Err($crate::ExternalError::UnknownMethod)
                    )),
                }
            }
        }
    };
    (
        @service [ $svc:ident ] @methods [ $( $methods:tt )* ] @actor [ $( $actor:tt )* ]
        @rest [ { [ $( #[$meta:meta] )* ] [ $( #[$cfg:meta] )* ]
                  [ $( $defer:tt )? ] $kind:ident $method:ident
                  ( $( $arg:ident : $ty:ty ),* $(,)? ) -> $ret:ty }
                $( $rest:tt )* ]
    ) => {
        $crate::__keylink_external_kind! {
            @service [ $svc ] @methods [ $( $methods )* ] @actor [ $( $actor )* ]
            @item [ [ $( #[$cfg] )* ] $kind $method ( $( $arg : $ty ),* ) -> $ret ]
            @rest [ $( $rest )* ]
        }
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
        @version [ $version:literal ] @previous [ $( previous $older:literal { $( $old:tt )* } )* ]
        @wire [ $( { [ $( #[$meta:meta] )* ] [ $( #[$cfg:meta] )* ]
             [ $( $defer:tt )? ] $kind:ident $rpc:ident ( $( $arg:ident : $ty:ty ),* $(,)? ) -> $ret:ty } )+ ]
        @feeds [ $( { [ $( #[$fmeta:meta] )* ] [ $( #[$fcfg:meta] )* ]
             [ $( $fdefer:tt )? ] feed $feed:ident ( $farg:ident : $fty:ty ) } )* ]
    ) => {
        $crate::pastey::paste! {
        pub mod [<$svc:snake>] {
            #![allow(unused_imports, dead_code, missing_docs)]
            use super::*;

            /// Namespaced by crate, so two crates can define `Inventory` without
            /// their endpoints colliding.
            const PROTOCOL: &str = concat!(env!("CARGO_PKG_NAME"), "/", stringify!($svc));

            $crate::__keylink_external! {
                @service [ $svc ] @methods [ ] @actor [ ]
                @rest [ $( { [ $( #[$meta] )* ] [ $( #[$cfg] )* ]
                    [ $( $defer )? ] $kind $rpc ( $( $arg : $ty ),* ) -> $ret } )+ ]
            }

            // One check per declaration, and the only place `#[defer]` is refused.
            // The three sites that shape a deferred method fall back to the
            // ordinary signatures for a kind that cannot defer, so a mistake here
            // produces exactly one error — the one that says why — instead of that
            // error plus a service whose trait no longer matches its mailbox.
            $( $( #[$cfg] )* $crate::__keylink_defer_check!([$( $defer )?], [<$kind>], $rpc); )+
            // Check keyword spellings before `pastey` emits them after `fn`.
            // Declaration attributes gate the check along with the declaration.
            $( $crate::__keylink_keyword_name_check_item!([$( #[$cfg] )*], $rpc, rpc, $rpc); )+
            // And once per feed, which is why the feed record carries a `[defer?]`
            // group it otherwise has no use for.
            $( $( #[$fcfg] )* $crate::__keylink_defer_check!([$( $fdefer )?], feed, $feed); )*
            $( $crate::__keylink_keyword_name_check_item!([$( #[$fcfg] )*], $feed, feed, $feed); )*

            $crate::__keylink_contract! { $svc, $version;
                $( { [ $( #[$meta] )* ] [ $( #[$cfg] )* ] [ $( $defer )? ] $kind $rpc ( $( $arg : $ty ),* ) -> $ret } )+
            }
            const _: () = $crate::__check_window_len(1usize $(+ { let _ = $older; 1usize })*);
            #[doc(hidden)] pub type __Toward = $crate::Here<Request, Response>;
            $crate::__keylink_previous! { @chain [$svc] [$version] [super] $( previous $older { $( $old )* } )* }

            /// Every contract this build speaks, newest first.
            pub fn window() -> &'static $crate::Window { lineage().window() }
            #[doc(hidden)]
            pub fn lineage() -> &'static $crate::Lineage<Request, Response> {
                static CELL: ::std::sync::OnceLock<$crate::Lineage<Request, Response>> = ::std::sync::OnceLock::new();
                CELL.get_or_init(|| $crate::Lineage::new::<__Toward>(PROTOCOL, VERSION)
                    $( .previous::<[<version_ $older>]::__Toward>([<version_ $older>]::VERSION) )*)
            }
            #[doc(hidden)]
            pub fn keyed_lineage() -> &'static $crate::Lineage<$crate::Addressed<Request>, Response> {
                static CELL: ::std::sync::OnceLock<$crate::Lineage<$crate::Addressed<Request>, Response>> = ::std::sync::OnceLock::new();
                CELL.get_or_init(|| $crate::Lineage::new::<$crate::Keyed<__Toward>>(KEYED_PROTOCOL, KEYED_VERSION)
                    $( .previous::<$crate::Keyed<[<version_ $older>]::__Toward>>([<version_ $older>]::KEYED_VERSION) )*)
            }

            impl Response {
                /// Which rpc this reply answers. Reported when a hand-rolled
                /// peer answers the wrong question, so the error names both
                /// sides instead of printing a debug dump.
                fn variant_name(&self) -> &'static str {
                    match self {
                        $( $( #[$cfg] )* Self::$rpc(_) => stringify!($rpc), )+
                        Self::__Status(_) => "__Status",
                    }
                }

                $(
                    $( #[$cfg] )*
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

                /// Extract the `__Status` reply, or say what came instead.
                ///
                /// The twin of the `into_*` above, written out rather than
                /// produced by that repetition because `__Status` is declared by
                /// no service and so appears in no declaration list.
                ///
                /// **Not `into___status`**, which is what that repetition would
                /// have spelled: three interior underscores are `non_snake_case`
                /// and the workspace denies warnings. Not `into_status` either —
                /// `rpc Status()` is a declaration this crate promises still
                /// compiles, and it produces exactly that name. The leading
                /// underscores are the convention for a name a service cannot
                /// declare, which is the same reason `__Status` carries them.
                fn __into_status(self) -> $crate::Result<$crate::Serving> {
                    match self {
                        Self::__Status(value) => Ok(value),
                        other => Err($crate::Error::UnexpectedResponse {
                            expected: "__Status",
                            actual: other.variant_name(),
                        }),
                    }
                }
            }

            /// `<crate>/<Service>/<label>/<16-hex schema hash>`.
            pub fn alpn() -> &'static [u8] { lineage().alpn() }

            /// Implement this to serve the protocol. One method per rpc.
            pub trait Handler: Send + Sync + 'static {
                $(
                    $crate::__keylink_handler_method!(
                        [$( #[$cfg] )*],
                        [<$kind>], [<# $rpc:snake>], ($( $arg : $ty ),*), $ret
                    );
                )+
            }

            $(
                $( #[$cfg] )*
                $crate::__keylink_invoke_fn!(
                    [<$kind>],
                    [<__invoke_ $rpc:snake>],
                    Handler,
                    Response,
                    $rpc,
                    [<# $rpc:snake>],
                    ($( $arg : $ty ),*)
                );
            )+

            /// Whether this reply hands out a content address, and which.
            ///
            /// Read by `serve_one`, which is the only place that holds BOTH the
            /// store and the [`AdmittedPeer`](keylink::AdmittedPeer) the grant
            /// must be recorded against. The handler has neither and needs
            /// neither: returning the id is the whole of its part.

            /// Whether any method here serves content, folded at compile time.
            ///
            /// `ServerBuilder::spawn` reads it to refuse a service that
            /// declares blob methods and carries no store, which is why
            /// `ConfigError::MissingBlobStore` is a check that can fire rather
            /// than a flag that is always false.
            const fn __keylink_any(values: &[bool]) -> bool {
                let mut index = 0;
                while index < values.len() {
                    if values[index] {
                        return true;
                    }
                    index += 1;
                }
                false
            }

            const DECLARES_BLOBS: bool = __keylink_any(&[
                false $(, $( #[$cfg] )* $crate::__keylink_is_blob!($kind))+
            ]);

            /// Whether any method here reads an upload inside its turn, folded
            /// the same way. Read by [`Servable::service`](keylink::Servable::service), which refuses to
            /// compile for such a service.
            const DECLARES_SEND: bool = __keylink_any(&[
                false $(, $( #[$cfg] )* $crate::__keylink_is_send!($kind))+
            ]);

            /// Whether this service declares a `feed`.
            ///
            /// Folded from the feed list itself rather than from a per-kind helper,
            /// which is the shape of the whole decision: a feed is a second list,
            /// so "are there any" is a repetition over that list and needs no arm
            /// anywhere else.
            const DECLARES_FEEDS: bool = __keylink_any(&[
                false $(, $( #[$fcfg] )* true)*
            ]);

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
                     tell from a quiet feed. Use `setup(state).feeds(Feeds::new(..)).actor()` \
                     for one actor in this process, or terminate with \
                     `.mountable()` for one mounted in a Registry. `Feeds::new` takes one argument per \
                     declared feed, so forgetting one is an argument count."
                );
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
                            let (__outcome, __body, __entered) = match req {
                                $(
                                    $( #[$cfg] )*
                                    Request::$rpc { $( $arg ),* } => {
                                        __progress.dispatched();
                                        let (__response, __body) = [<__invoke_ $rpc:snake>](
                                            handler.as_ref(), &cx, body $(, $arg )*
                                        ).await;
                                        ($crate::Outcome::Ok(__response), __body, true)
                                    }
                                )+
                                // `__Status` asks which turn is running, and a
                                // service served THIS way runs none: every call
                                // takes its own task, so nothing is ever inside
                                // a serial turn and nothing is ever queued
                                // behind one. `RemoteError::NotAnActor` says
                                // that, rather than a `Serving` with both fields
                                // at rest — which would be true here and
                                // indistinguishable from an idle actor.
                                //
                                // One ALPN carries both ways of serving a
                                // singleton, so a peer cannot know which it
                                // dialled and this arm is the answer rather than
                                // a case that cannot arise.
                                //
                                // `unrun`, and `dispatched` moved into the arms
                                // above it: no handler is entered here, so an
                                // idempotency key claimed for this call is
                                // released rather than settled as work that ran
                                // and never answered.
                                Request::__Status { .. } => {
                                    __progress.unrun();
                                    (
                                        $crate::Outcome::Err(
                                            $crate::RemoteError::NotAnActor,
                                        ),
                                        $crate::Body::empty(),
                                        false,
                                    )
                                }
                            };
                            if __entered {
                                __progress.completed();
                            }
                            (__outcome, __body)
                        })
                    });
                $crate::ServiceBuilder::new(lineage(), allow, dispatch)
                    .__keylink_declares_blobs(DECLARES_BLOBS)
            }

            /// Serve this service alone on `node`.
            ///
            /// For several services on one endpoint, build each with
            /// [`service`] and register them on a single
            /// [`Server`](keylink::Server): one router must own every ALPN.
            ///
            /// **It also decides three serving-side policies and takes no argument
            /// for any of them.** It calls `build()` itself, so a caller never holds
            /// the [`ServiceBuilder`](keylink::ServiceBuilder) and cannot reach
            /// [`limits`](keylink::ServiceBuilder::limits),
            /// [`records`](keylink::ServiceBuilder::records) or
            /// [`keyable`](keylink::ServiceBuilder::keyable). What it picks is
            /// `Keyable::Any`, so every method accepts an idempotency key;
            /// `Records::Memory`, so replies are held in memory; and
            /// [`Limits::default`](keylink::Limits::default) for the ledger's bounds.
            /// To change any of the three, take the `service` plus
            /// [`Server::builder`](keylink::Server::builder) route above — named
            /// there for a different reason, and a reader wanting different bounds
            /// has none to look for it.
            ///
            /// **Not a `Limits` parameter, deliberately, and the reason is this
            /// function's own signature.** Issue #13 lists "adding a parameter to
            /// `serve`" as one of three surfaces that are genuinely breaking once
            /// this crate is published — which is why it asked for a documentation
            /// fix here rather than a signature change. A fourth way to build a
            /// service also costs more than a default a caller can read, but that
            /// argument is about a `Limits` *constructor* and not about this
            /// parameter.
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

            /// `<crate>/<Service>/keyed/<label>/<16-hex schema hash>`.
            pub fn registry_alpn() -> &'static [u8] { keyed_lineage().alpn() }

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
            /// Non-configuration attributes written on a method in `service!`
            /// are deliberately **not** forwarded here. They are written for the
            /// wire enum, and a `#[serde(..)]` re-emitted onto this local enum
            /// would be `cannot find attribute` pointing at this file. A `#[cfg]`
            /// is forwarded so disabled declarations leave no stale match arm.
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
                    $( #[$cfg] )*
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
                            $( #[$cfg] )*
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
                                    [<# $rpc:snake>],
                                    ($( $arg ),*)
                                ),
                        )+
                        Mail::Served { origin, request, body, reply } => match request {
                            $(
                                $( #[$cfg] )*
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
                                        [<# $rpc:snake>],
                                        Response,
                                        $rpc,
                                        ($( $arg ),*)
                                    ),
                            )+
                            // **Nothing in this crate builds this, and that is
                            // the wave's central property rather than an
                            // accident.** `__Status` is answered above BOTH
                            // delivery calls — in `Registry::service` dispatch
                            // closure and in the generated `Servable::service`
                            // implementation's — so it never
                            // becomes mail. It cannot: a question about which
                            // turn is running cannot be answered by a turn,
                            // because that turn would report `busy: true` about
                            // itself and count its own permit in `queued`.
                            //
                            // **Taken rather than unreachable**, which is the
                            // distinction this file's `__Status` doctest already
                            // insists on and the reason the sentence above says
                            // *in this crate*. `Addressed::new` and
                            // `deliver_keyed` are `#[doc(hidden)] pub` and
                            // re-exported at the crate root, and
                            // `Request::__Status {}` is a public variant of a
                            // public enum. Every part ships; what does not exist
                            // is a caller.
                            //
                            // The arm exists because `Mail::Served` carries the
                            // whole `Request` and this match must be exhaustive.
                            // It drops the reply rather than inventing an
                            // answer, which the caller reads as
                            // `Withheld::Dropped` — *a live actor took this and
                            // never answered it* — and which is the truth about
                            // what happened if a door is ever built that reaches
                            // here. A `Serving` folded inside the turn would be
                            // a lie in the one field the type exists for, and
                            // generated code must not panic in somebody else's
                            // crate.
                            //
                            // `unrun` for the reason the other three `__Status`
                            // arms call it, and it is the half a dropped reply
                            // does not cover: this ends a message without
                            // entering it, and silence means `enqueued`, which
                            // **settles** a claimed idempotency key as work that
                            // ran and never answered. `Withheld::Dropped` tells
                            // the caller what happened to the message; `unrun`
                            // tells the ledger what happened to the key.
                            //
                            // Through `Caller::peer` because that is the whole
                            // path there is: an origin holds its `Context`
                            // privately and `caller()` is the borrow it hands
                            // out. No new surface is added for this — a local
                            // origin has no proven key and no queue budget, so
                            // `None` is the correct nothing-to-mark rather than
                            // a case left unhandled.
                            Request::__Status { .. } => {
                                if let ::core::option::Option::Some(__cx) =
                                    origin.caller().peer()
                                {
                                    __cx.progress().unrun();
                                }
                                ::core::mem::drop(reply);
                                ::core::option::Option::None
                            }
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
                    $( #[$fcfg] )* $feed($fty),
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
                            $( #[$fcfg] )*
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
            /// `setup(state).feeds(feeds)`. There is no way to add a feed to
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
            /// spawn, against making `Handle`, `Mountable` and `Registry` generic in one
            /// type parameter per declared feed.
            #[doc(hidden)]
            pub struct Feeds {
                $( $( #[$fcfg] )* [<$feed:snake>]: ::core::pin::Pin<
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
                    $( $( #[$fcfg] )* [<$feed:snake>]: impl $crate::Stream<Item = $fty>
                        + ::core::marker::Send + 'static, )*
                ) -> Self {
                    $( $crate::__keylink_feed_name_check_item!([$( #[$fcfg] )*], [<$feed:snake>], $feed); )*
                    Self {
                        $( $( #[$fcfg] )* [<$feed:snake>]: ::std::boxed::Box::pin([<$feed:snake>]), )*
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
                        $( #[$fcfg] )*
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
            pub type Registry = $crate::Registry<Mail>;

            /// Assembles a [`Registry`]: its eviction policy, its cap, and how it
            /// spawns.
            pub type RegistryBuilder = $crate::RegistryBuilder<Mail>;

            /// A typed address of one actor in this process. Its methods are the
            /// service's declared `rpc`, `tell` and `blob` operations, with no
            /// transport or remote error in their result.
            #[derive(Clone, Debug)]
            pub struct LocalRef<'a> {
                system: &'a Registry,
                actor: $crate::ActorId,
            }

            impl LocalRef<'_> {
                $(
                    $crate::__keylink_keyed_local_method!(
                        [$( #[$cfg] )*],
                        [<$kind>],
                        [<# $rpc:snake>],
                        $rpc,
                        ($( $arg : $ty ),*),
                        $ret
                    );
                )+
            }

            /// Address one actor of this service in this process.
            #[must_use]
            pub fn at_local(system: &Registry, actor: $crate::ActorId) -> LocalRef<'_> {
                LocalRef { system, actor }
            }

            /// One live actor of this service, ready to be mounted.
            pub type Mountable = $crate::Mountable<Mail>;

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

            impl $crate::RegistryDispatch for Mail {
                type Request = Request;
                type Response = Response;

                fn service(
                    registry: &$crate::Registry<Self>,
                    allow: $crate::Allowlist,
                ) -> $crate::ServiceBuilder<$crate::Addressed<Self::Request>, Self::Response> {
                    let registry = registry.clone();
                    let dispatch: $crate::Dispatch<$crate::Addressed<Request>, Response> =
                        ::std::sync::Arc::new(move |cx: $crate::Context,
                                                    request: $crate::Addressed<Request>,
                                                    body: $crate::BodyReader| {
                            let registry = registry.clone();
                            ::std::boxed::Box::pin(async move {
                                let __answer = if ::core::matches!(
                                    request.message(),
                                    Request::__Status { .. }
                                ) {
                                    cx.progress().unrun();
                                    $crate::serving_keyed(&registry, &request).map(|__serving| {
                                        (
                                            $crate::Outcome::Ok(Response::__Status(__serving)),
                                            $crate::Body::empty(),
                                        )
                                    })
                                } else {
                                    $crate::deliver_keyed::<Actors>(
                                        &registry,
                                        cx,
                                        request,
                                        body,
                                    )
                                    .await
                                };
                                match __answer {
                                    ::std::result::Result::Ok(__answer) => __answer,
                                    ::std::result::Result::Err(__refusal) => (
                                        $crate::Outcome::Err(__refusal),
                                        $crate::Body::empty(),
                                    ),
                                }
                            })
                        });
                    $crate::ServiceBuilder::new(keyed_lineage(), allow, dispatch)
                        .__keylink_declares_blobs(DECLARES_BLOBS)
                }
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
                        [$( #[$cfg] )*],
                        [<$kind>], [<# $rpc:snake>], ($( $arg : $ty ),*), $ret
                    );
                )+
                $(
                    $( #[$fcfg] )*
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
            /// Hand it to [`Registry::mount`](keylink::Registry::mount) under an
            /// id, or return it from
            /// [`spawn_with`](keylink::RegistryBuilder::spawn_with) to have it
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
            /// `setup(state).observe(observer).feeds(feeds).mountable()` for a keyed
            /// actor that must be observed, or that has a feed — and a service that declares a feed cannot reach *this* door
            /// at all, because `SpawnableUnfed` refuses it at the call site.
            ///
            /// What it no longer is, is a loop. The keyed loop used to read the
            /// queue deadline, mark idempotency progress, guard the panic and render
            /// the refusal itself, in ~150 lines that had to agree with `turns.rs`'s
            /// and twice did not: one spelled the deadline `>` where the other
            /// spelled `>=`, and one rendered a panic bare where the other prefixed
            /// it. Wave 1b made the *dispatch* one path and left two loops around
            /// it; there is one loop now, and this is one call into it.
            ///
            /// Nothing runs until this is mounted:
            /// [`Registry::mount`](keylink::Registry::mount) is what starts the
            /// task, so a `Mountable` that is refused or dropped costs no task at
            /// all. That is where the tokio-runtime panic lives now, and it is
            /// why this function has none.
            #[must_use = "an unmounted actor is dropped, and the state it was \
                          built around goes with it"]
            pub fn spawn<A: Actor>(state: A) -> Mountable {
                // The generated `Servable::service` implementation makes the same
                // refusal, and it did so for longer than
                // it was made here: `CHECK` is an associated const on a generic
                // type, so it is evaluated only where it is INSTANTIATED, and
                // for a long time the one instantiation was in the former inherent
                // `Handle::service`.
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
                __mount(state, ::std::vec::Vec::new(), (), ::core::option::Option::None)
            }

            fn __mount<A: Actor, F: $crate::Feeds<A>>(
                state: A,
                observers: ::std::vec::Vec<::std::boxed::Box<dyn $crate::Observer<A>>>,
                feeds: F,
                backpressure: ::core::option::Option<$crate::Backpressure>,
            ) -> Mountable {
                match backpressure {
                    ::core::option::Option::Some(policy) => {
                        $crate::mount_with_backpressure(
                            move |inbox: Mailbox| async move {
                                $crate::run_keyed(state, observers, feeds, inbox).await
                            },
                            policy,
                        )
                    }
                    ::core::option::Option::None => {
                        $crate::mount(move |inbox: Mailbox| async move {
                            $crate::run_keyed(state, observers, feeds, inbox).await
                        })
                    }
                }
            }

            fn __actor<A: Actor, F: $crate::Feeds<A>>(
                state: A,
                observers: ::std::vec::Vec<::std::boxed::Box<dyn $crate::Observer<A>>>,
                feeds: F,
                backpressure: ::core::option::Option<$crate::Backpressure>,
            ) -> Handle<A> {
                let inner = match backpressure {
                    ::core::option::Option::Some(policy) => {
                        $crate::LocalActor::spawn_deep(state, observers, feeds, policy)
                    }
                    ::core::option::Option::None => {
                        $crate::LocalActor::spawn_fed(state, observers, feeds)
                    }
                };
                Handle { inner }
            }

            /// State-backed setup mode.
            #[doc(hidden)]
            pub struct StateSetup<A, F> {
                state: A,
                observers: ::std::vec::Vec<::std::boxed::Box<dyn $crate::Observer<A>>>,
                feeds: F,
            }

            /// Restarting setup mode.
            #[doc(hidden)]
            pub struct RestartingSetup<N> {
                factory: N,
                limit: $crate::Intensity,
            }

            /// Configure an actor before choosing a local or keyed terminal.
            #[must_use = "a setup does nothing until actor or mountable is called"]
            pub struct Setup<A, Mode, F = ()> {
                mode: Mode,
                backpressure: ::core::option::Option<$crate::Backpressure>,
                marker: ::core::marker::PhantomData<fn() -> (A, F)>,
            }

            /// Configure one state-backed actor.
            #[must_use = "a setup does nothing until actor or mountable is called"]
            pub fn setup<A: Actor>(state: A) -> Setup<A, StateSetup<A, ()>> {
                Setup {
                    mode: StateSetup {
                        state,
                        observers: ::std::vec::Vec::new(),
                        feeds: (),
                    },
                    backpressure: ::core::option::Option::None,
                    marker: ::core::marker::PhantomData,
                }
            }

            /// Configure an actor rebuilt by `factory` after an incarnation fails.
            #[must_use = "a restarting setup does nothing until actor or mountable is called"]
            pub fn restarting<A, N, Fut, F>(
                factory: N,
                limit: $crate::Intensity,
            ) -> Setup<A, RestartingSetup<N>, F>
            where
                A: Actor,
                N: ::core::ops::FnMut() -> Fut + ::core::marker::Send + 'static,
                Fut: ::core::future::Future<
                        Output = ::core::result::Result<$crate::Incarnation<A, F>, $crate::Stop>,
                    > + ::core::marker::Send,
                F: $crate::Feeds<A>,
            {
                Setup {
                    mode: RestartingSetup { factory, limit },
                    backpressure: ::core::option::Option::None,
                    marker: ::core::marker::PhantomData,
                }
            }

            impl<A: Actor, F> Setup<A, StateSetup<A, F>, F> {
                /// Add one observer to each turn.
                #[must_use = "the configured observer is lost unless the returned setup is used"]
                pub fn observe<O>(mut self, observer: O) -> Self
                where
                    O: $crate::Observer<A> + 'static,
                {
                    self.mode.observers.push(::std::boxed::Box::new(observer));
                    self
                }

                /// Wire this service's declared feeds.
                #[must_use = "the configured feeds are lost unless the returned setup is used"]
                pub fn feeds(
                    self,
                    feeds: Feeds,
                ) -> Setup<A, StateSetup<A, Feeds>, Feeds> {
                    Setup {
                        mode: StateSetup {
                            state: self.mode.state,
                            observers: self.mode.observers,
                            feeds,
                        },
                        backpressure: self.backpressure,
                        marker: ::core::marker::PhantomData,
                    }
                }

                /// Set this actor's mailbox depth and overflow policy.
                #[must_use = "the mailbox policy is lost unless the returned setup is used"]
                pub fn backpressure(mut self, backpressure: $crate::Backpressure) -> Self {
                    self.backpressure = ::core::option::Option::Some(backpressure);
                    self
                }
            }

            impl<A: Actor> Setup<A, StateSetup<A, ()>> {
                /// Start the configured actor locally.
                #[must_use]
                pub fn actor(self) -> Handle<A> {
                    let () = SpawnableUnfed::<A>::CHECK;
                    let StateSetup { state, observers, feeds } = self.mode;
                    __actor(state, observers, feeds, self.backpressure)
                }

                /// Build the configured actor for a registry mount.
                #[must_use = "an unmounted actor is dropped"]
                pub fn mountable(self) -> Mountable {
                    let () = ServableAsActor::<A>::CHECK;
                    let () = SpawnableUnfed::<A>::CHECK;
                    let StateSetup { state, observers, feeds } = self.mode;
                    __mount(state, observers, feeds, self.backpressure)
                }
            }

            impl<A: Actor> Setup<A, StateSetup<A, Feeds>, Feeds> {
                /// Start the configured actor locally.
                #[must_use]
                pub fn actor(self) -> Handle<A> {
                    let StateSetup { state, observers, feeds } = self.mode;
                    __actor(state, observers, feeds, self.backpressure)
                }

                /// Build the configured actor for a registry mount.
                #[must_use = "an unmounted actor is dropped"]
                pub fn mountable(self) -> Mountable {
                    let () = ServableAsActor::<A>::CHECK;
                    let StateSetup { state, observers, feeds } = self.mode;
                    __mount(state, observers, feeds, self.backpressure)
                }
            }

            impl<A: Actor, N, F> Setup<A, RestartingSetup<N>, F> {
                /// Set the mailbox policy shared by every incarnation.
                #[must_use = "the mailbox policy is lost unless the returned setup is used"]
                pub fn backpressure(mut self, backpressure: $crate::Backpressure) -> Self {
                    self.backpressure = ::core::option::Option::Some(backpressure);
                    self
                }

                /// Start the supervised actor locally.
                #[must_use]
                pub fn actor<Fut>(self) -> Handle<A>
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
                    let RestartingSetup { factory, limit } = self.mode;
                    let inner = match self.backpressure {
                        ::core::option::Option::Some(policy) => {
                            $crate::LocalActor::spawn_supervised_deep(factory, limit, policy)
                        }
                        ::core::option::Option::None => {
                            $crate::LocalActor::spawn_supervised(factory, limit)
                        }
                    };
                    Handle { inner }
                }

                /// Build the supervised actor for a registry mount.
                #[must_use = "an unmounted actor is dropped"]
                pub fn mountable<Fut>(self) -> Mountable
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
                    let () = ServableAsActor::<A>::CHECK;
                    let RestartingSetup { factory, limit } = self.mode;
                    match self.backpressure {
                        ::core::option::Option::Some(policy) => {
                            $crate::mount_with_backpressure(
                                move |inbox: Mailbox| async move {
                                    $crate::run_keyed_supervised(factory, limit, inbox).await
                                },
                                policy,
                            )
                        }
                        ::core::option::Option::None => {
                            $crate::mount(move |inbox: Mailbox| async move {
                                $crate::run_keyed_supervised(factory, limit, inbox).await
                            })
                        }
                    }
                }
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
            /// let item = warehouse.get_item("ADU-014".into()).await?;   // Err = Unanswered
            /// ```
            ///
            /// To serve the **same** actor to other machines as well, call
            /// [`Servable::service`](keylink::Servable::service). To serve many keyed instances instead, use
            /// [`spawn`] with a [`Registry`](keylink::Registry).
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

            /// A handle to an actor in this process.
            ///
            /// Its methods are the ones written in `service!`, with the same
            /// names, the same argument order and the same success types as
            /// [`Client`]'s. The error type is the difference, and it is the
            /// whole difference: a local call cannot fail the ways a remote one
            /// can, so it never fails with [`Error`](keylink::Error).
            ///
            /// **Two local error types, and they are the two questions a door
            /// asks.** An `rpc` or a `blob` asks *did I get a value* and answers
            /// [`Unanswered`](keylink::Unanswered). A `tell` or
            /// [`Own::stop`](keylink::Own::stop) asks *did this get queued*, has no reply
            /// channel, and answers [`NotQueued`](keylink::NotQueued) — two
            /// states rather than every state a lost reply can be in. One type
            /// for both would advertise on `stop` what `stop` cannot produce.
            ///
            /// That is deliberate and permanent. RFC-04 §5.5: a handle that is
            /// one type in both places must either claim failures the local
            /// case cannot have, or hide failures the remote case does.
            /// `Error::SchemaMismatch` means nothing between two objects in one
            /// process.
            ///
            /// Cheap to clone: every clone names the same actor. The actor runs
            /// until the last handle is dropped, or until [`Own::stop`](keylink::Own::stop)
            /// — **unless it is served**, in which case
            /// [`Servable::service`](keylink::Servable::service) put a clone inside the server and that
            /// one outlives every handle you hold.
            ///
            /// # A panic reaches the caller of an `rpc`
            ///
            /// The actor survives a panicking message, and the caller of that
            /// message gets the panic — resumed with its own payload, close to
            /// the way an ordinary function call would deliver it. Both halves
            /// matter: the first keeps one bad message from costing every later
            /// one, and the second keeps a fault out of
            /// [`Unanswered`](keylink::Unanswered) altogether, so no variant
            /// of it has to mean "something went wrong somewhere".
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
            /// # An actor must not call itself — and it is now REFUSED rather than hung
            ///
            /// A handler that awaits **any** method on a handle to its own actor
            /// would deadlock. Since item M it does not: the message carries the
            /// chain of turns it was sent from, so a send to an actor already on
            /// that chain answers a `Reentrant` instead of being queued —
            /// [`Unanswered::Reentrant`](keylink::Unanswered::Reentrant) from
            /// an `rpc`,
            /// [`NotQueued::Reentrant`](keylink::NotQueued::Reentrant) from a
            /// `tell` or a [`Own::stop`](keylink::Own::stop), which is the same fact through
            /// the door that only queues. The four cases and their mechanisms are
            /// unchanged and worth knowing, because what each one *would* have
            /// done is what decides whether it is refused:
            ///
            /// - **`rpc`, refused always.** The reply cannot be produced until
            ///   the actor takes the next message, and the actor is busy being
            ///   this handler. A free mailbox slot does not save it, so the slot
            ///   is not consulted.
            /// - **`tell`, refused only when the mailbox is FULL.** The actor
            ///   released its permit when it took the message it is running now,
            ///   so a self `tell` into a free slot is legitimate: it runs after
            ///   this turn returns. When the permit is gone the send would block
            ///   with only this actor able to free it, and that is the case that
            ///   is refused. Note the direction — this is the one case that was
            ///   ever load-dependent, and it was *depth 1* that made the block
            ///   likely rather than a deeper mailbox.
            /// - **[`Own::stop`](keylink::Own::stop), refused always.** It takes a permit like
            ///   any message, and then waits for a loop that cannot exit. It
            ///   returns a `Result` for this reason and no other.
            /// - **[`Own::with_state`](keylink::Own::with_state), refused always.** A read is
            ///   a message too, so the one method that never touches `&mut self`
            ///   is refused exactly like an `rpc`.
            ///
            /// An earlier version of this comment said a self `tell` was fine
            /// because it waits for nothing. It waits for a permit.
            ///
            /// **It is not detected by a type, and could not be**: identity here
            /// is per *instance*, and no type can tell one actor's handle from
            /// another's. So it is a runtime refusal with an error position, and
            /// the remedy is unchanged: call the method on `&mut self` instead —
            /// it is the same code without the round trip. If the work must
            /// happen *after* the current turn, spawn a task that holds a handle;
            /// that task is not the actor, starts a fresh chain, and can wait for
            /// a permit safely.
            ///
            /// # The count of actors does not matter: `A → B → A` is refused too
            ///
            /// Nothing above needs the handle to be your **own**. Actor A calling
            /// actor B, whose handler calls A, would hang both for ever, and so
            /// would any longer ring. A handler holds `&mut A` and `A` can hold
            /// another actor's handle, so two collaborating actors each holding
            /// the other's is an ordinary design and not an exotic one — it is the
            /// shape a reader is most likely to build.
            ///
            /// It was **worse than the self-`tell` case above, not milder.** Both
            /// sends would succeed, and for two different reasons: the **opening**
            /// send A→B lands because B is idle with an empty mailbox, and the
            /// **closing** send B→A would land because A's loop released its own
            /// permit when it took the message it is now running. So nothing
            /// complained at either end. What neither caller got is an *answer*,
            /// because the only task that could produce one is the task waiting
            /// for it. A ring therefore needs no contention and hung the first
            /// time it ran — the load-dependence that lets the self-`tell` case
            /// survive testing is absent, and the deadlock was unconditional.
            ///
            /// **The chain closes it**, and closing the ring is the reason it
            /// travels in the message rather than being a flag on the actor: the
            /// closing send B→A carries `[A, B]`, sees A, and is refused. The
            /// opening send A→B is untouched, because B is not on it.
            ///
            /// The remedy across two actors is unchanged, and it is now the way to
            /// make a ring *work* rather than the way to avoid a hang. It is not
            /// "call it on `&mut self`" — there is no `&mut` to reach. It is to
            /// stop being inside the turn: park the reply with
            /// [`Reply::later`](keylink::Reply::later), hand the outbound call to
            /// a spawned task and **return**, so the loop is polling again when
            /// the peer calls back. A spawned task starts a **fresh chain**, so
            /// nothing is refused there either. Parking alone is not enough; see
            /// [`Deferred`](keylink::Deferred), which says which half does the
            /// work.
            ///
            /// `keylink/tests/cycles.rs` builds the two-actor ring and pins the
            /// refusal, the self-call, the supervised path, both halves of the
            /// `tell` rule, and the recovery for the hang no refusal can reach —
            /// a turn that simply never returns, where `abort` frees the *other*
            /// actor and destroys the one it names.
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
                        // Follows `LocalActor`'s own `Debug`, which gained this
                        // in the same commit. A handle printing one of the two
                        // facts its holder can ask for is the drift this avoids.
                        .field("busy", &self.inner.busy())
                        .finish()
                }
            }

            impl<A: Actor> Handle<A> {
                $(
                    $crate::__keylink_local_method!(
                        [$( #[$cfg] )*],
                        [<$kind>],
                        [<# $rpc:snake>],
                        $rpc,
                        ($( $arg : $ty ),*),
                        $ret
                    );
                )+
            }

            impl<A: Actor> $crate::Servable for Handle<A> {
                type Request = Request;
                type Response = Response;

                fn service(
                    &self,
                    allow: $crate::Allowlist,
                ) -> $crate::ServiceBuilder<Self::Request, Self::Response> {
                    let () = ServableAsActor::<A>::CHECK;
                    let actor = self.inner.clone();
                    let dispatch: $crate::Dispatch<Request, Response> =
                        ::std::sync::Arc::new(move |cx: $crate::Context,
                                                    req: Request,
                                                    body: $crate::BodyReader| {
                            let actor = actor.clone();
                            ::std::boxed::Box::pin(async move {
                                if ::core::matches!(&req, Request::__Status { .. }) {
                                    cx.progress().unrun();
                                    return match $crate::serving_unkeyed(&actor) {
                                        ::std::result::Result::Ok(__serving) => (
                                            $crate::Outcome::Ok(Response::__Status(__serving)),
                                            $crate::Body::empty(),
                                        ),
                                        ::std::result::Result::Err(__refusal) => (
                                            $crate::Outcome::Err(__refusal),
                                            $crate::Body::empty(),
                                        ),
                                    };
                                }
                                $crate::deliver_unkeyed(
                                    &actor,
                                    cx,
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
                    $crate::ServiceBuilder::new(lineage(), allow, dispatch)
                        .__keylink_declares_blobs(DECLARES_BLOBS)
                }

            }

            impl<A: Actor> $crate::Own for Handle<A> {
                type State = A;
                type Permit<'a> = Permit<'a, A> where Self: 'a;
                type OwnedPermit = OwnedPermit<A>;

                async fn with_state<T, F>(
                    &self,
                    read: F,
                ) -> ::core::result::Result<T, $crate::Unanswered>
                where
                    T: ::core::marker::Send + 'static,
                    F: ::core::ops::FnOnce(&Self::State) -> T
                        + ::core::marker::Send
                        + 'static,
                {
                    self.inner.read(read).await
                }

                async fn stop(&self) -> ::core::result::Result<(), $crate::NotQueued> {
                    self.inner.stop().await
                }

                fn abort(&self) {
                    self.inner.abort();
                }

                fn is_running(&self) -> bool {
                    self.inner.is_running()
                }

                fn ended(
                    &self,
                ) -> impl ::core::future::Future<Output = $crate::Ended>
                    + ::core::marker::Send
                    + 'static {
                    self.inner.ended()
                }

                fn status(&self) -> $crate::Serving {
                    self.inner.serving()
                }

                async fn reserve(
                    &self,
                ) -> ::core::result::Result<Self::Permit<'_>, $crate::NotQueued> {
                    self.inner.reserve().await.map(|slot| Permit { inner: slot })
                }

                fn try_reserve(
                    &self,
                ) -> ::core::result::Result<
                    ::core::option::Option<Self::Permit<'_>>,
                    $crate::NotQueued,
                > {
                    self.inner
                        .try_reserve()
                        .map(|slot| slot.map(|slot| Permit { inner: slot }))
                }

                async fn reserve_owned(
                    self,
                ) -> ::core::result::Result<Self::OwnedPermit, $crate::NotQueued> {
                    self.inner
                        .reserve_owned()
                        .await
                        .map(|slot| OwnedPermit { inner: slot })
                }
            }

            /// One reserved slot in an actor's mailbox, carrying this service's
            /// own methods.
            ///
            /// Handed out by [`Own::reserve`](keylink::Own::reserve) and
            /// [`Own::try_reserve`](keylink::Own::try_reserve). Every method here takes `self`, because
            /// a slot admits one message: spending it is the whole of what a
            /// permit is for, and there is no second send to make.
            ///
            /// **The methods here carry no underscores**, and the reason is that
            /// this type's namespace holds nothing but the service's own methods:
            /// there is no keylink name for one to collide with, which is exactly
            /// the condition [`Own::reserve`](keylink::Own::reserve) does not meet.
            ///
            /// **The methods differ from [`Handle`]'s in exactly one way — a
            /// `tell` returns [`Queued`](keylink::Queued) directly rather than a
            /// `Result`.** Both of that door's failures are answered before a
            /// permit exists: full is
            /// what the reservation waited out, and closed is what it reported.
            /// An `rpc` still returns a `Result`, because a turn can still lose
            /// its reply after it is queued.
            ///
            /// [`keylink::Permit`] carries what holding one costs — the slot is
            /// held away from every other sender until this is spent or dropped —
            /// and the one deadlock shape a caller has to avoid.
            #[must_use = "a dropped permit returns the slot and sends nothing; \
                          the caller is never told"]
            pub struct Permit<'a, A> {
                inner: $crate::Permit<'a, A, Mail>,
            }

            impl<A: Actor> Permit<'_, A> {
                $(
                    $crate::__keylink_permit_method!(
                        [$( #[$cfg] )*],
                        [<$kind>],
                        [<# $rpc:snake>],
                        $rpc,
                        ($( $arg : $ty ),*),
                        $ret
                    );
                )+
            }

            /// [`Permit`] that owns its sender, so it can be stored or moved into
            /// a spawned task.
            ///
            /// Handed out by [`Own::reserve_owned`](keylink::Own::reserve_owned), and it carries the same
            /// methods for the same reasons. The difference is `'static` and
            /// nothing else.
            #[must_use = "a dropped permit returns the slot and sends nothing; \
                          the caller is never told"]
            pub struct OwnedPermit<A> {
                inner: $crate::OwnedPermit<A, Mail>,
            }

            impl<A: Actor> OwnedPermit<A> {
                $(
                    $crate::__keylink_permit_method!(
                        [$( #[$cfg] )*],
                        [<$kind>],
                        [<# $rpc:snake>],
                        $rpc,
                        ($( $arg : $ty ),*),
                        $ret
                    );
                )+
            }

            /// [`Own::reserve`](keylink::Own::reserve) and [`Own::status`](keylink::Own::status), under the names a
            /// [`Factory`](keylink::compose::Factory) asks a member for.
            ///
            /// **This is the one line `service!` generates because `compose`
            /// exists**, and `compose`'s own module doc says so. It is generated
            /// rather than written once in keylink because the mailbox this
            /// forwards to is [`Handle`]'s private field: a composition outside
            /// this expansion cannot reach it, and a trait is the only way to
            /// name "a member of a factory" without naming the service.
            ///
            /// Nothing new is reachable through it. Both methods forward to the
            /// two above, so a factory and a caller holding the handle directly
            /// cannot be told different things about one actor.
            impl<A: Actor> $crate::compose::Reserving for Handle<A> {
                type Permit<'a> = Permit<'a, A> where Self: 'a;

                fn reserve(&self) -> impl ::core::future::Future<
                    Output = ::core::result::Result<Permit<'_, A>, $crate::NotQueued>,
                > + ::core::marker::Send {
                    $crate::Own::reserve(self)
                }

                fn status(&self) -> $crate::Serving {
                    $crate::Own::status(self)
                }
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
            /// Address one actor with [`RegistryClient::at`], which gives back a
            /// typed handle whose methods are the ones written in `service!`.
            pub struct RegistryClient {
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
            impl ::core::clone::Clone for RegistryClient {
                fn clone(&self) -> Self {
                    Self {
                        inner: self.inner.clone(),
                        idempotency: self.idempotency.clone(),
                    }
                }
            }

            impl ::core::fmt::Debug for RegistryClient {
                fn fmt(&self, f: &mut ::core::fmt::Formatter<'_>) -> ::core::fmt::Result {
                    f.debug_struct("RegistryClient")
                        .field("service", &stringify!($svc))
                        .finish_non_exhaustive()
                }
            }

            impl RegistryClient {
                /// Dial a peer's actor registry for this service, by public key.
                pub async fn connect(
                    node: &$crate::Node,
                    peer: impl Into<$crate::EndpointAddr>,
                ) -> $crate::Result<Self> {
                    Ok(Self {
                        inner: $crate::RpcClient::connect(node, peer, keyed_lineage()).await?,
                        idempotency: ::core::option::Option::None,
                    })
                }

                /// Address one actor of this service on this connection.
                #[must_use]
                pub fn at(&self, actor: $crate::ActorId) -> ActorRef<'_> {
                    ActorRef { client: self, actor }
                }

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

                fn blob_handle(&self, content: $crate::Content) -> $crate::BlobHandle {
                    self.inner.blob_handle(content)
                }
            }

            impl $crate::WithIdempotency for RegistryClient {
                type Output = Self;

                fn with_idempotency(
                    &self,
                    idempotency: impl ::core::convert::Into<$crate::Idempotency>,
                ) -> Self::Output {
                    Self {
                        inner: self.inner.clone(),
                        idempotency: ::core::option::Option::Some(idempotency.into()),
                    }
                }
            }

            impl $crate::Wire for RegistryClient {
                fn version(&self) -> $crate::Version { self.inner.version() }
                async fn key_state(
                    &self,
                    key: $crate::IdempotencyKey,
                ) -> $crate::Result<$crate::KeyState> {
                    self.inner.key_state(key).await
                }

                fn watch(&self) -> $crate::Watch {
                    self.inner.watch()
                }

                async fn close(self) -> $crate::Result<()> {
                    self.inner.close().await
                }
            }

            impl $crate::Dial for RegistryClient {
                async fn dial(
                    node: &$crate::Node,
                    addr: $crate::EndpointAddr,
                ) -> $crate::Result<Self> {
                    Self::connect(node, addr).await
                }
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
                client: &'a RegistryClient,
                actor: $crate::ActorId,
            }

            impl ActorRef<'_> {
                $(
                    $crate::__keylink_actor_client_method!(
                        [$( #[$cfg] )*],
                        [<$kind>],
                        [<# $rpc:snake>],
                        Request,
                        Response,
                        $rpc,
                        [<into_ $rpc:snake>],
                        ($( $arg : $ty ),*),
                        $ret
                    );
                )+
            }

            impl $crate::Status for ActorRef<'_> {
                async fn status(&self) -> $crate::Result<$crate::Serving> {
                    let (__response, _body) = self
                        .client
                        .send_to(
                            &self.actor,
                            Request::__Status {},
                            $crate::Body::empty(),
                        )
                        .await?;
                    __response.__into_status()
                }
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
                        inner: $crate::RpcClient::connect(node, peer, lineage()).await?,
                    })
                }

                $(
                    $crate::__keylink_client_method!(
                        [$( #[$cfg] )*],
                        client,
                        [<$kind>],
                        [<# $rpc:snake>],
                        Request,
                        $rpc,
                        [<into_ $rpc:snake>],
                        ($( $arg : $ty ),*),
                        $ret
                    );
                )+
            }

            impl $crate::WithIdempotency for Client {
                type Output = IdempotentClient;

                fn with_idempotency(
                    &self,
                    idempotency: impl ::core::convert::Into<$crate::Idempotency>,
                ) -> Self::Output {
                    IdempotentClient {
                        inner: self.inner.with_idempotency(idempotency),
                    }
                }
            }

            impl $crate::Wire for Client {
                fn version(&self) -> $crate::Version { self.inner.version() }
                async fn key_state(
                    &self,
                    key: $crate::IdempotencyKey,
                ) -> $crate::Result<$crate::KeyState> {
                    self.inner.key_state(key).await
                }

                fn watch(&self) -> $crate::Watch {
                    self.inner.watch()
                }

                async fn close(self) -> $crate::Result<()> {
                    self.inner.close().await
                }
            }

            impl $crate::Status for Client {
                async fn status(&self) -> $crate::Result<$crate::Serving> {
                    let (__response, _body) = self
                        .inner
                        .call(Request::__Status {}, $crate::Body::empty())
                        .await?;
                    __response.__into_status()
                }
            }

            /// The same connection, scoped to one idempotency key.
            ///
            /// Built by [`WithIdempotency::with_idempotency`](keylink::WithIdempotency::with_idempotency). Its methods are
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
                $(
                    $crate::__keylink_idempotent_client_method!(
                        [$( #[$cfg] )*],
                        [<$kind>],
                        [<# $rpc:snake>],
                        Request,
                        $rpc,
                        [<into_ $rpc:snake>],
                        ($( $arg : $ty ),*),
                        $ret
                    );
                )+
            }

            impl $crate::Wire for IdempotentClient {
                fn version(&self) -> $crate::Version { self.inner.version() }
                async fn key_state(
                    &self,
                    key: $crate::IdempotencyKey,
                ) -> $crate::Result<$crate::KeyState> {
                    self.inner.key_state(key).await
                }

                fn watch(&self) -> $crate::Watch {
                    self.inner.watch()
                }

                async fn close(self) -> $crate::Result<()> {
                    self.inner.close().await
                }
            }

            impl $crate::Status for IdempotentClient {
                async fn status(&self) -> $crate::Result<$crate::Serving> {
                    let (__response, _body) = self
                        .inner
                        .call(Request::__Status {}, $crate::Body::empty())
                        .await?;
                    __response.__into_status()
                }
            }

            /// One function, every surface of this service that can make a call.
            ///
            /// Each method here is one of this service's `rpc` declarations with
            /// **one** error type instead of two:
            /// [`Failure`](keylink::Failure) puts a
            /// [`Withheld`](keylink::Withheld) in the same arm whether a local
            /// mailbox withheld the value or a peer did. Write `impl Calls` where
            /// a function must run against a local actor in a test and a remote
            /// one in production.
            ///
            /// Implemented for `Handle`, `ActorRef`, `Client` and
            /// `IdempotentClient`. `blocking::Client` is not an implementor:
            /// every method here hands back a future, and that type exists for
            /// callers who have no runtime to await one on.
            ///
            /// **The concrete surfaces are untouched.** A `Handle`'s own methods
            /// still answer [`Unanswered`](keylink::Unanswered) and a client's
            /// still answer [`Error`](keylink::Error). This is a second door,
            /// opened by writing the bound — which is the act of saying *I have
            /// chosen not to know which surface I hold.*
            ///
            /// # It carries `rpc`, and the four kinds it drops each argued for it
            ///
            /// `rpc` is the only kind all four surfaces declare. A caller who
            /// cannot find a method here will otherwise add it, and each addition
            /// is refused by a surface in writing:
            ///
            /// - **`tell`** — an `ActorRef` has none, because a keyed `tell` is
            ///   fire-and-forget into a registry that can refuse four ways the
            ///   caller can never learn about, *"you addressed an actor that does
            ///   not exist"* among them. An `IdempotentClient` has none either: a
            ///   client holding a key no longer has a `tell` to call.
            /// - **`send`** and **`recv`** — a `Handle` has neither. Both halves
            ///   of streaming are a QUIC stream, and a local handle names no
            ///   transport to open one on. `ActorRef` also omits `send`: every
            ///   generated service declaring one is rejected by the actor-serving
            ///   doors because upload reads would hold the serial turn.
            /// - **`blob`** — all four have one and they do not agree on the
            ///   answer. A `Handle` gives a [`Grant`](keylink::Grant); a served
            ///   surface gives a [`BlobHandle`](keylink::BlobHandle), which is a
            ///   content address **plus the node that holds it**, and in this
            ///   process the holder is you.
            ///
            /// So a service declaring no `rpc` gets an **empty** trait, and a
            /// service mixing kinds gets a trait whose methods are a subset of
            /// what each of its surfaces can do. Name the concrete surface for
            /// the rest.
            ///
            /// # A handler panic behaves differently by surface
            ///
            /// Against a `Handle` it **unwinds into the calling task**, because
            /// that is what [`ask`](keylink::LocalActor::ask) does with the
            /// payload. Against a served surface it arrives as
            /// `Failure::Turn(Withheld::Panicked { .. })`.
            /// [`Failure`](keylink::Failure) cannot express an unwind, and
            /// [`TurnLost`](keylink::TurnLost) argues at length why it should not
            /// try: `Box<dyn Any + Send>` has no encoding, and
            /// [`Withheld::Panicked`](keylink::Withheld::Panicked) is the same
            /// fault stated to the audience that can act on it.
            ///
            /// # `dyn Calls` does not exist
            ///
            /// Every method returns its future in return position, which cannot
            /// be reached through a trait object — `dyn Calls` is `E0038`.
            /// [`Dial`](keylink::Dial) is the same shape for the same reason: the
            /// `+ Send` is what lets a caller's future cross `tokio::spawn`, and
            /// an `async fn` in a trait does not promise it. Do **not** box the
            /// future to get an object back; that spends an allocation on every
            /// local call to buy a thing a caller who needs one type gets for
            /// free by naming the concrete surface.
            #[diagnostic::on_unimplemented(
                message = "`{Self}` is not a calling surface for this service",
                label = "not a `Calls` surface",
                note = "implemented for `Handle`, `ActorRef`, `Client` and \
                        `IdempotentClient`. `blocking::Client` is deliberately \
                        absent: it is synchronous, and every method here returns \
                        a future."
            )]
            pub trait Calls {
                $(
                    $crate::__keylink_calls_decl!(
                        [$( #[$cfg] )*],
                        [<$kind>],
                        [<# $rpc:snake>],
                        $rpc,
                        ($( $arg : $ty ),*),
                        $ret
                    );
                )+
            }

            impl<A: Actor> Calls for Handle<A> {
                $(
                    $crate::__keylink_calls_impl!(
                        [$( #[$cfg] )*],
                        local_handle,
                        [<$kind>],
                        [<# $rpc:snake>],
                        ($( $arg : $ty ),*),
                        $ret
                    );
                )+
            }

            impl Calls for LocalRef<'_> {
                $(
                    $crate::__keylink_calls_impl!(
                        [$( #[$cfg] )*],
                        local_ref,
                        [<$kind>],
                        [<# $rpc:snake>],
                        ($( $arg : $ty ),*),
                        $ret
                    );
                )+
            }

            impl Calls for ActorRef<'_> {
                $(
                    $crate::__keylink_calls_impl!(
                        [$( #[$cfg] )*],
                        actor_ref,
                        [<$kind>],
                        [<# $rpc:snake>],
                        ($( $arg : $ty ),*),
                        $ret
                    );
                )+
            }

            impl Calls for Client {
                $(
                    $crate::__keylink_calls_impl!(
                        [$( #[$cfg] )*],
                        client,
                        [<$kind>],
                        [<# $rpc:snake>],
                        ($( $arg : $ty ),*),
                        $ret
                    );
                )+
            }

            impl Calls for IdempotentClient {
                $(
                    $crate::__keylink_calls_impl!(
                        [$( #[$cfg] )*],
                        idempotent_client,
                        [<$kind>],
                        [<# $rpc:snake>],
                        ($( $arg : $ty ),*),
                        $ret
                    );
                )+
            }

            // Three surfaces, and `Handle` is the one deliberately missing: it
            // reaches the mailbox directly, so nothing on the `ServiceBuilder`
            // ran. `blocking::Client` is governed in fact and still gets no impl,
            // because a marker on a type that cannot satisfy the `Calls` half of
            // `impl Calls + Governed` is a name nobody can write a bound with.
            impl $crate::GovernedSeal for ActorRef<'_> {}
            impl $crate::Governed for ActorRef<'_> {}
            impl $crate::GovernedSeal for Client {}
            impl $crate::Governed for Client {}
            impl $crate::GovernedSeal for IdempotentClient {}
            impl $crate::Governed for IdempotentClient {}

            // The doc comment lives INSIDE the wrap: attached to the macro
            // invocation instead, it documents nothing and is
            // `unused_doc_comments`.
            $crate::__keylink_if_blocking! {
            /// The same service, for programs that are not async.
            ///
            /// Only unary `rpc` and `tell` methods are generated here. `send` and
            /// `recv` own streaming bodies, and `blob` returns a handle whose fetch
            /// is asynchronous; synchronous wrappers would either buffer data or
            /// hand back a handle that only works inside a runtime.
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

                    $(
                        $crate::__keylink_blocking_method!(
                            [$( #[$cfg] )*],
                            blocking_client,
                            [<$kind>],
                            [<# $rpc:snake>],
                            $rpc,
                            ($( $arg : $ty ),*),
                            $ret
                        );
                    )+
                }

                impl $crate::WithIdempotency for Client {
                    type Output = IdempotentClient;

                    fn with_idempotency(
                        &self,
                        idempotency: impl ::core::convert::Into<$crate::Idempotency>,
                    ) -> Self::Output {
                        IdempotentClient {
                            inner: $crate::WithIdempotency::with_idempotency(
                                &self.inner,
                                idempotency,
                            ),
                            runtime: ::std::sync::Arc::clone(&self.runtime),
                        }
                    }
                }

                impl $crate::BlockingWire for Client {
                    fn version(&self) -> $crate::Version { $crate::Wire::version(&self.inner) }
                    fn key_state(
                        &self,
                        key: $crate::IdempotencyKey,
                    ) -> $crate::Result<$crate::KeyState> {
                        self.runtime.run($crate::Wire::key_state(&self.inner, key))?
                    }

                    fn close(self) -> $crate::Result<()> {
                        self.runtime.run($crate::Wire::close(self.inner))?
                    }
                }

                /// The same connection, scoped to one idempotency key.
                ///
                /// See the async twin,
                /// [`IdempotentClient`](super::IdempotentClient). This blocking,
                /// key-scoped intersection exposes only declared `rpc` methods:
                /// idempotency excludes `tell`, while blocking excludes `send`,
                /// `recv`, and `blob`.
                pub struct IdempotentClient {
                    inner: super::IdempotentClient,
                    runtime: ::std::sync::Arc<$crate::blocking::Runtime>,
                }

                impl IdempotentClient {
                    $(
                        $crate::__keylink_idempotent_blocking_method!(
                            [$( #[$cfg] )*],
                            [<$kind>],
                            [<# $rpc:snake>],
                            $rpc,
                            ($( $arg : $ty ),*),
                            $ret
                        );
                    )+
                }

                impl $crate::BlockingWire for IdempotentClient {
                    fn version(&self) -> $crate::Version { $crate::Wire::version(&self.inner) }
                    fn key_state(
                        &self,
                        key: $crate::IdempotencyKey,
                    ) -> $crate::Result<$crate::KeyState> {
                        self.runtime.run($crate::Wire::key_state(&self.inner, key))?
                    }

                    fn close(self) -> $crate::Result<()> {
                        self.runtime.run($crate::Wire::close(self.inner))?
                    }
                }
            }
            }
        }
        }
    };
}

/// Emit a direct diagnostic when a generated method name lands on a method
/// keylink already owns on that generated surface.
///
/// The check is deliberately surface-specific. A name such as `at` is valid on
/// an actor handle even though it is the addressing door on `RegistryClient`, and
/// `ended` is valid on a client even though it is an observation door on a local
/// handle. A flat reserved-name list would reject valid services and still say
/// nothing about which generated type owns the name.
#[doc(hidden)]
#[macro_export]
macro_rules! __keylink_feed_name_conflict {
    ($surface:literal, $name:literal, $feed:ident) => {
        ::core::compile_error!(::core::concat!(
            "feed `",
            ::core::stringify!($feed),
            "` conflicts with generated `",
            $surface,
            "::",
            $name,
            "`. Rename the feed."
        ));
    };
}

#[doc(hidden)]
#[macro_export]
macro_rules! __keylink_name_conflict {
    ($kind:ident, $surface:literal, $name:literal, $decl:ident) => {
        ::core::compile_error!(::core::concat!(
            ::core::stringify!($kind),
            " `",
            ::core::stringify!($decl),
            "` conflicts with generated `",
            $surface,
            "::",
            $name,
            "`. Rename the ",
            ::core::stringify!($kind),
            "."
        ));
    };
}

/// Check one generated declaration against the methods on one generated surface.
///
/// The fallback is intentional: `macro_rules!` cannot compare identifiers, so
/// the finite generated surface is written here and every other user name is
/// left alone. Calls are made only from emitters that actually generate that
/// surface's method, so omitted capabilities (`send`/`recv` locally and
/// `tell` on an idempotent client) are not falsely reserved.
#[doc(hidden)]
#[macro_export]
macro_rules! __keylink_generated_name_check_item {
    ([$( #[$meta:meta] )*], $surface:ident, $name:ident, $variant:ident, $kind:ident) => {
        $crate::pastey::paste! {
            $( #[$meta] )*
            const [<__keylink_generated_name_check_ $surface _ $variant>]: () = {
                $crate::__keylink_generated_name_check!($surface, $name, $variant, $kind);
            };
        }
    };
}

#[doc(hidden)]
#[macro_export]
macro_rules! __keylink_generated_name_check_module_item {
    ([$( #[$meta:meta] )*], $surface:ident, $name:ident, $variant:ident, $kind:ident) => {
        $( #[$meta] )*
        const _: () = {
            $crate::__keylink_generated_name_check!($surface, $name, $variant, $kind);
        };
    };
}

/// Emit every direct conflict for a declaration before generated methods and
/// matches are expanded. The caller drops the declaration after this check, so
/// `#[cfg]` can remove a conflicting declaration without leaving references to it.
#[doc(hidden)]
#[macro_export]
macro_rules! __keylink_status_name_checks {
    ([$( #[$meta:meta] )*], $kind:ident, $variant:ident) => {
        $crate::__keylink_generated_name_check_module_item!([
            $( #[$meta] )*
        ], local_handle, __status, $variant, $kind);
        $crate::__keylink_generated_name_check_module_item!([
            $( #[$meta] )*
        ], actor_ref, __status, $variant, $kind);
        $crate::__keylink_generated_name_check_module_item!([
            $( #[$meta] )*
        ], client, __status, $variant, $kind);
        $crate::__keylink_generated_name_check_module_item!([
            $( #[$meta] )*
        ], idempotent_client, __status, $variant, $kind);
        $crate::__keylink_generated_name_check_module_item!([
            $( #[$meta] )*
        ], wire, __Status, $variant, $kind);
    };
}

#[doc(hidden)]
#[macro_export]
macro_rules! __keylink_conflicting_method {
    ([$( #[$meta:meta] )*], $surface:ident, rpc, $name:ident, $variant:ident) => {
        $crate::__keylink_generated_name_check_item!([
            $( #[$meta] )*
        ], $surface, $name, $variant, rpc);
    };
    ([$( #[$meta:meta] )*], $surface:ident, tell, $name:ident, $variant:ident) => {
        $crate::__keylink_generated_name_check_item!([
            $( #[$meta] )*
        ], $surface, $name, $variant, tell);
    };
    ([$( #[$meta:meta] )*], $surface:ident, blob, $name:ident, $variant:ident) => {
        $crate::__keylink_generated_name_check_item!([
            $( #[$meta] )*
        ], $surface, $name, $variant, blob);
    };
    ([$( #[$meta:meta] )*], actor_ref, send, $name:ident, $variant:ident) => {
        $crate::__keylink_generated_name_check_item!([
            $( #[$meta] )*
        ], actor_ref, $name, $variant, send);
    };
    ([$( #[$meta:meta] )*], actor_ref, recv, $name:ident, $variant:ident) => {
        $crate::__keylink_generated_name_check_item!([
            $( #[$meta] )*
        ], actor_ref, $name, $variant, recv);
    };
    ([$( #[$meta:meta] )*], client, send, $name:ident, $variant:ident) => {
        $crate::__keylink_generated_name_check_item!([
            $( #[$meta] )*
        ], client, $name, $variant, send);
    };
    ([$( #[$meta:meta] )*], client, recv, $name:ident, $variant:ident) => {
        $crate::__keylink_generated_name_check_item!([
            $( #[$meta] )*
        ], client, $name, $variant, recv);
    };
    ([$( #[$meta:meta] )*], idempotent_client, send, $name:ident, $variant:ident) => {
        $crate::__keylink_generated_name_check_item!([
            $( #[$meta] )*
        ], idempotent_client, $name, $variant, send);
    };
    ([$( #[$meta:meta] )*], idempotent_client, recv, $name:ident, $variant:ident) => {
        $crate::__keylink_generated_name_check_item!([
            $( #[$meta] )*
        ], idempotent_client, $name, $variant, recv);
    };
    ([$( #[$meta:meta] )*], $surface:ident, $kind:ident, $name:ident, $variant:ident) => {};
}

#[doc(hidden)]
#[macro_export]
macro_rules! __keylink_feed_name_check_item {
    ([$( #[$meta:meta] )*], $name:ident, $feed:ident) => {
        $( #[$meta] )*
        const _: () = {
            $crate::__keylink_generated_name_check!(feeds, $name, $feed, feed);
        };
    };
}

#[doc(hidden)]
#[macro_export]
macro_rules! __keylink_keyword_name_check_item {
    ([$( #[$meta:meta] )*], $name:ident, $kind:ident, $decl:ident) => {
        $( #[$meta] )*
        const _: () = {
            $crate::__keylink_keyword_name_check!($name, $kind, $decl);
        };
    };
}

/// Reject declaration names whose snake-case spelling is a Rust keyword before
/// any generated method is parsed. `pastey` can produce a keyword token, but a
/// keyword cannot be used after `fn`, so a method-body check would be too late.
#[doc(hidden)]
#[macro_export]
macro_rules! __keylink_keyword_name_check {
    (as, $kind:ident, $decl:ident) => {
        $crate::__keylink_keyword_name_conflict!(as, $kind, $decl);
    };
    (break, $kind:ident, $decl:ident) => {
        $crate::__keylink_keyword_name_conflict!(break, $kind, $decl);
    };
    (const, $kind:ident, $decl:ident) => {
        $crate::__keylink_keyword_name_conflict!(const, $kind, $decl);
    };
    (continue, $kind:ident, $decl:ident) => {
        $crate::__keylink_keyword_name_conflict!(continue, $kind, $decl);
    };
    (crate, $kind:ident, $decl:ident) => {
        $crate::__keylink_keyword_name_conflict!(crate, $kind, $decl);
    };
    (else, $kind:ident, $decl:ident) => {
        $crate::__keylink_keyword_name_conflict!(else, $kind, $decl);
    };
    (enum, $kind:ident, $decl:ident) => {
        $crate::__keylink_keyword_name_conflict!(enum, $kind, $decl);
    };
    (extern, $kind:ident, $decl:ident) => {
        $crate::__keylink_keyword_name_conflict!(extern, $kind, $decl);
    };
    (false, $kind:ident, $decl:ident) => {
        $crate::__keylink_keyword_name_conflict!(false, $kind, $decl);
    };
    (fn, $kind:ident, $decl:ident) => {
        $crate::__keylink_keyword_name_conflict!(fn, $kind, $decl);
    };
    (for, $kind:ident, $decl:ident) => {
        $crate::__keylink_keyword_name_conflict!(for, $kind, $decl);
    };
    (if, $kind:ident, $decl:ident) => {
        $crate::__keylink_keyword_name_conflict!(if, $kind, $decl);
    };
    (impl, $kind:ident, $decl:ident) => {
        $crate::__keylink_keyword_name_conflict!(impl, $kind, $decl);
    };
    (in, $kind:ident, $decl:ident) => {
        $crate::__keylink_keyword_name_conflict!(in, $kind, $decl);
    };
    (let, $kind:ident, $decl:ident) => {
        $crate::__keylink_keyword_name_conflict!(let, $kind, $decl);
    };
    (loop, $kind:ident, $decl:ident) => {
        $crate::__keylink_keyword_name_conflict!(loop, $kind, $decl);
    };
    (match, $kind:ident, $decl:ident) => {
        $crate::__keylink_keyword_name_conflict!(match, $kind, $decl);
    };
    (mod, $kind:ident, $decl:ident) => {
        $crate::__keylink_keyword_name_conflict!(mod, $kind, $decl);
    };
    (move, $kind:ident, $decl:ident) => {
        $crate::__keylink_keyword_name_conflict!(move, $kind, $decl);
    };
    (mut, $kind:ident, $decl:ident) => {
        $crate::__keylink_keyword_name_conflict!(mut, $kind, $decl);
    };
    (pub, $kind:ident, $decl:ident) => {
        $crate::__keylink_keyword_name_conflict!(pub, $kind, $decl);
    };
    (ref, $kind:ident, $decl:ident) => {
        $crate::__keylink_keyword_name_conflict!(ref, $kind, $decl);
    };
    (return, $kind:ident, $decl:ident) => {
        $crate::__keylink_keyword_name_conflict!(return, $kind, $decl);
    };
    (self, $kind:ident, $decl:ident) => {
        $crate::__keylink_keyword_name_conflict!(self, $kind, $decl);
    };
    (Self, $kind:ident, $decl:ident) => {
        $crate::__keylink_keyword_name_conflict!(Self, $kind, $decl);
    };
    (static, $kind:ident, $decl:ident) => {
        $crate::__keylink_keyword_name_conflict!(static, $kind, $decl);
    };
    (struct, $kind:ident, $decl:ident) => {
        $crate::__keylink_keyword_name_conflict!(struct, $kind, $decl);
    };
    (super, $kind:ident, $decl:ident) => {
        $crate::__keylink_keyword_name_conflict!(super, $kind, $decl);
    };
    (trait, $kind:ident, $decl:ident) => {
        $crate::__keylink_keyword_name_conflict!(trait, $kind, $decl);
    };
    (true, $kind:ident, $decl:ident) => {
        $crate::__keylink_keyword_name_conflict!(true, $kind, $decl);
    };
    (type, $kind:ident, $decl:ident) => {
        $crate::__keylink_keyword_name_conflict!(type, $kind, $decl);
    };
    (unsafe, $kind:ident, $decl:ident) => {
        $crate::__keylink_keyword_name_conflict!(unsafe, $kind, $decl);
    };
    (use, $kind:ident, $decl:ident) => {
        $crate::__keylink_keyword_name_conflict!(use, $kind, $decl);
    };
    (where, $kind:ident, $decl:ident) => {
        $crate::__keylink_keyword_name_conflict!(where, $kind, $decl);
    };
    (while, $kind:ident, $decl:ident) => {
        $crate::__keylink_keyword_name_conflict!(while, $kind, $decl);
    };
    (async, $kind:ident, $decl:ident) => {
        $crate::__keylink_keyword_name_conflict!(async, $kind, $decl);
    };
    (await, $kind:ident, $decl:ident) => {
        $crate::__keylink_keyword_name_conflict!(await, $kind, $decl);
    };
    (dyn, $kind:ident, $decl:ident) => {
        $crate::__keylink_keyword_name_conflict!(dyn, $kind, $decl);
    };
    (abstract, $kind:ident, $decl:ident) => {
        $crate::__keylink_keyword_name_conflict!(abstract, $kind, $decl);
    };
    (become, $kind:ident, $decl:ident) => {
        $crate::__keylink_keyword_name_conflict!(become, $kind, $decl);
    };
    (box, $kind:ident, $decl:ident) => {
        $crate::__keylink_keyword_name_conflict!(box, $kind, $decl);
    };
    (do, $kind:ident, $decl:ident) => {
        $crate::__keylink_keyword_name_conflict!(do, $kind, $decl);
    };
    (final, $kind:ident, $decl:ident) => {
        $crate::__keylink_keyword_name_conflict!(final, $kind, $decl);
    };
    (macro, $kind:ident, $decl:ident) => {
        $crate::__keylink_keyword_name_conflict!(macro, $kind, $decl);
    };
    (override, $kind:ident, $decl:ident) => {
        $crate::__keylink_keyword_name_conflict!(override, $kind, $decl);
    };
    (priv, $kind:ident, $decl:ident) => {
        $crate::__keylink_keyword_name_conflict!(priv, $kind, $decl);
    };
    (typeof, $kind:ident, $decl:ident) => {
        $crate::__keylink_keyword_name_conflict!(typeof, $kind, $decl);
    };
    (unsized, $kind:ident, $decl:ident) => {
        $crate::__keylink_keyword_name_conflict!(unsized, $kind, $decl);
    };
    (virtual, $kind:ident, $decl:ident) => {
        $crate::__keylink_keyword_name_conflict!(virtual, $kind, $decl);
    };
    (yield, $kind:ident, $decl:ident) => {
        $crate::__keylink_keyword_name_conflict!(yield, $kind, $decl);
    };
    (try, $kind:ident, $decl:ident) => {
        $crate::__keylink_keyword_name_conflict!(try, $kind, $decl);
    };
    (gen, $kind:ident, $decl:ident) => {
        $crate::__keylink_keyword_name_conflict!(gen, $kind, $decl);
    };
    (As, $kind:ident, $decl:ident) => {
        $crate::__keylink_keyword_name_conflict!(as, $kind, $decl);
    };
    (Break, $kind:ident, $decl:ident) => {
        $crate::__keylink_keyword_name_conflict!(break, $kind, $decl);
    };
    (Const, $kind:ident, $decl:ident) => {
        $crate::__keylink_keyword_name_conflict!(const, $kind, $decl);
    };
    (Continue, $kind:ident, $decl:ident) => {
        $crate::__keylink_keyword_name_conflict!(continue, $kind, $decl);
    };
    (Crate, $kind:ident, $decl:ident) => {
        $crate::__keylink_keyword_name_conflict!(crate, $kind, $decl);
    };
    (Else, $kind:ident, $decl:ident) => {
        $crate::__keylink_keyword_name_conflict!(else, $kind, $decl);
    };
    (Enum, $kind:ident, $decl:ident) => {
        $crate::__keylink_keyword_name_conflict!(enum, $kind, $decl);
    };
    (Extern, $kind:ident, $decl:ident) => {
        $crate::__keylink_keyword_name_conflict!(extern, $kind, $decl);
    };
    (False, $kind:ident, $decl:ident) => {
        $crate::__keylink_keyword_name_conflict!(false, $kind, $decl);
    };
    (Fn, $kind:ident, $decl:ident) => {
        $crate::__keylink_keyword_name_conflict!(fn, $kind, $decl);
    };
    (For, $kind:ident, $decl:ident) => {
        $crate::__keylink_keyword_name_conflict!(for, $kind, $decl);
    };
    (If, $kind:ident, $decl:ident) => {
        $crate::__keylink_keyword_name_conflict!(if, $kind, $decl);
    };
    (Impl, $kind:ident, $decl:ident) => {
        $crate::__keylink_keyword_name_conflict!(impl, $kind, $decl);
    };
    (In, $kind:ident, $decl:ident) => {
        $crate::__keylink_keyword_name_conflict!(in, $kind, $decl);
    };
    (Let, $kind:ident, $decl:ident) => {
        $crate::__keylink_keyword_name_conflict!(let, $kind, $decl);
    };
    (Loop, $kind:ident, $decl:ident) => {
        $crate::__keylink_keyword_name_conflict!(loop, $kind, $decl);
    };
    (Match, $kind:ident, $decl:ident) => {
        $crate::__keylink_keyword_name_conflict!(match, $kind, $decl);
    };
    (Mod, $kind:ident, $decl:ident) => {
        $crate::__keylink_keyword_name_conflict!(mod, $kind, $decl);
    };
    (Move, $kind:ident, $decl:ident) => {
        $crate::__keylink_keyword_name_conflict!(move, $kind, $decl);
    };
    (Mut, $kind:ident, $decl:ident) => {
        $crate::__keylink_keyword_name_conflict!(mut, $kind, $decl);
    };
    (Pub, $kind:ident, $decl:ident) => {
        $crate::__keylink_keyword_name_conflict!(pub, $kind, $decl);
    };
    (Ref, $kind:ident, $decl:ident) => {
        $crate::__keylink_keyword_name_conflict!(ref, $kind, $decl);
    };
    (Return, $kind:ident, $decl:ident) => {
        $crate::__keylink_keyword_name_conflict!(return, $kind, $decl);
    };
    (Static, $kind:ident, $decl:ident) => {
        $crate::__keylink_keyword_name_conflict!(static, $kind, $decl);
    };
    (Struct, $kind:ident, $decl:ident) => {
        $crate::__keylink_keyword_name_conflict!(struct, $kind, $decl);
    };
    (Super, $kind:ident, $decl:ident) => {
        $crate::__keylink_keyword_name_conflict!(super, $kind, $decl);
    };
    (Trait, $kind:ident, $decl:ident) => {
        $crate::__keylink_keyword_name_conflict!(trait, $kind, $decl);
    };
    (True, $kind:ident, $decl:ident) => {
        $crate::__keylink_keyword_name_conflict!(true, $kind, $decl);
    };
    (Type, $kind:ident, $decl:ident) => {
        $crate::__keylink_keyword_name_conflict!(type, $kind, $decl);
    };
    (Unsafe, $kind:ident, $decl:ident) => {
        $crate::__keylink_keyword_name_conflict!(unsafe, $kind, $decl);
    };
    (Use, $kind:ident, $decl:ident) => {
        $crate::__keylink_keyword_name_conflict!(use, $kind, $decl);
    };
    (Where, $kind:ident, $decl:ident) => {
        $crate::__keylink_keyword_name_conflict!(where, $kind, $decl);
    };
    (While, $kind:ident, $decl:ident) => {
        $crate::__keylink_keyword_name_conflict!(while, $kind, $decl);
    };
    (Async, $kind:ident, $decl:ident) => {
        $crate::__keylink_keyword_name_conflict!(async, $kind, $decl);
    };
    (Await, $kind:ident, $decl:ident) => {
        $crate::__keylink_keyword_name_conflict!(await, $kind, $decl);
    };
    (Dyn, $kind:ident, $decl:ident) => {
        $crate::__keylink_keyword_name_conflict!(dyn, $kind, $decl);
    };
    (Abstract, $kind:ident, $decl:ident) => {
        $crate::__keylink_keyword_name_conflict!(abstract, $kind, $decl);
    };
    (Become, $kind:ident, $decl:ident) => {
        $crate::__keylink_keyword_name_conflict!(become, $kind, $decl);
    };
    (Box, $kind:ident, $decl:ident) => {
        $crate::__keylink_keyword_name_conflict!(box, $kind, $decl);
    };
    (Do, $kind:ident, $decl:ident) => {
        $crate::__keylink_keyword_name_conflict!(do, $kind, $decl);
    };
    (Final, $kind:ident, $decl:ident) => {
        $crate::__keylink_keyword_name_conflict!(final, $kind, $decl);
    };
    (Macro, $kind:ident, $decl:ident) => {
        $crate::__keylink_keyword_name_conflict!(macro, $kind, $decl);
    };
    (Override, $kind:ident, $decl:ident) => {
        $crate::__keylink_keyword_name_conflict!(override, $kind, $decl);
    };
    (Priv, $kind:ident, $decl:ident) => {
        $crate::__keylink_keyword_name_conflict!(priv, $kind, $decl);
    };
    (Typeof, $kind:ident, $decl:ident) => {
        $crate::__keylink_keyword_name_conflict!(typeof, $kind, $decl);
    };
    (Unsized, $kind:ident, $decl:ident) => {
        $crate::__keylink_keyword_name_conflict!(unsized, $kind, $decl);
    };
    (Virtual, $kind:ident, $decl:ident) => {
        $crate::__keylink_keyword_name_conflict!(virtual, $kind, $decl);
    };
    (Yield, $kind:ident, $decl:ident) => {
        $crate::__keylink_keyword_name_conflict!(yield, $kind, $decl);
    };
    (Try, $kind:ident, $decl:ident) => {
        $crate::__keylink_keyword_name_conflict!(try, $kind, $decl);
    };
    (Gen, $kind:ident, $decl:ident) => {
        $crate::__keylink_keyword_name_conflict!(gen, $kind, $decl);
    };
    ($name:ident, $kind:ident, $decl:ident) => {};
}

#[doc(hidden)]
#[macro_export]
macro_rules! __keylink_keyword_name_conflict {
    ($name:ident, $kind:ident, $decl:ident) => {
        ::core::compile_error!(::core::concat!(
            stringify!($kind),
            " `",
            ::core::stringify!($decl),
            "` generates the reserved Rust keyword `",
            ::core::stringify!($name),
            "`. Rename the ",
            stringify!($kind),
            "."
        ));
    };
}

/// Reject a wire declaration whose name is one this service already generates.
///
/// The keyword check above is about a token Rust refuses. This one is about a
/// token Rust accepts and then resolves to the wrong item, so the diagnostic has
/// to say which item rather than which keyword.
#[doc(hidden)]
#[macro_export]
macro_rules! __keylink_wire_name_check_item {
    ([$( #[$meta:meta] )*], $name:ident, $kind:ident, $decl:ident) => {
        $( #[$meta] )*
        const _: () = {
            $crate::__keylink_wire_name_conflict!($name, $kind, $decl);
        };
    };
}

/// The message for the above. Named at the declaration the author wrote, which
/// is the whole point: the error this replaces pointed at `service!` itself.
#[doc(hidden)]
#[macro_export]
macro_rules! __keylink_wire_name_conflict {
    ($name:ident, $kind:ident, $decl:ident) => {
        ::core::compile_error!(::core::concat!(
            stringify!($kind),
            " `",
            ::core::stringify!($decl),
            "` collides with the `",
            ::core::stringify!($name),
            "` enum every service generates. Rename the ",
            stringify!($kind),
            "."
        ));
    };
}

#[doc(hidden)]
#[macro_export]
macro_rules! __keylink_generated_name_check {
    // Wire names are enum variants, not methods. These two are generated into
    // the same enums as user declarations and need the same direct diagnostic.
    (wire, __Status, $rpc:ident, $kind:ident) => {
        $crate::__keylink_name_conflict!($kind, "Request/Response", "__Status", $rpc);
    };
    (wire, Served, $rpc:ident, $kind:ident) => {
        $crate::__keylink_name_conflict!($kind, "Mail", "Served", $rpc);
    };
    (wire, $name:ident, $rpc:ident, $kind:ident) => {};

    // `Feeds` has one generated field of its own, the round-robin cursor.
    // Feed names otherwise form their own enum variants and callback methods,
    // with no built-in method namespace to reserve.
    (feeds, at, $feed:ident, $kind:ident) => {
        $crate::__keylink_feed_name_conflict!("Feeds", "at", $feed);
    };
    (feeds, r#at, $feed:ident, $kind:ident) => {
        $crate::__keylink_feed_name_conflict!("Feeds", "at", $feed);
    };
    (feeds, $name:ident, $rpc:ident, $kind:ident) => {};

    (local_handle, __status, $rpc:ident, $kind:ident) => {
        $crate::__keylink_name_conflict!($kind, "Handle", "__status", $rpc);
    };
    (local_handle, r#__status, $rpc:ident, $kind:ident) => {
        $crate::__keylink_name_conflict!($kind, "Handle", "__status", $rpc);
    };
    (local_handle, $name:ident, $rpc:ident, $kind:ident) => {};

    (local_ref, $name:ident, $rpc:ident, $kind:ident) => {};

    (actor_ref, __status, $rpc:ident, $kind:ident) => {
        $crate::__keylink_name_conflict!($kind, "ActorRef", "__status", $rpc);
    };
    (actor_ref, r#__status, $rpc:ident, $kind:ident) => {
        $crate::__keylink_name_conflict!($kind, "ActorRef", "__status", $rpc);
    };
    (actor_ref, $name:ident, $rpc:ident, $kind:ident) => {};

    (client, connect, $rpc:ident, $kind:ident) => {
        $crate::__keylink_name_conflict!($kind, "Client", "connect", $rpc);
    };
    (client, r#connect, $rpc:ident, $kind:ident) => {
        $crate::__keylink_name_conflict!($kind, "Client", "connect", $rpc);
    };
    (client, __status, $rpc:ident, $kind:ident) => {
        $crate::__keylink_name_conflict!($kind, "Client", "__status", $rpc);
    };
    (client, r#__status, $rpc:ident, $kind:ident) => {
        $crate::__keylink_name_conflict!($kind, "Client", "__status", $rpc);
    };
    (client, $name:ident, $rpc:ident, $kind:ident) => {};

    (idempotent_client, __status, $rpc:ident, $kind:ident) => {
        $crate::__keylink_name_conflict!($kind, "IdempotentClient", "__status", $rpc);
    };
    (idempotent_client, r#__status, $rpc:ident, $kind:ident) => {
        $crate::__keylink_name_conflict!($kind, "IdempotentClient", "__status", $rpc);
    };
    (idempotent_client, $name:ident, $rpc:ident, $kind:ident) => {};

    (blocking_client, connect, $rpc:ident, $kind:ident) => {
        $crate::__keylink_name_conflict!($kind, "blocking::Client", "connect", $rpc);
    };
    (blocking_client, r#connect, $rpc:ident, $kind:ident) => {
        $crate::__keylink_name_conflict!($kind, "blocking::Client", "connect", $rpc);
    };
    (blocking_client, $name:ident, $rpc:ident, $kind:ident) => {};

    (blocking_idempotent_client, $name:ident, $rpc:ident, $kind:ident) => {};

    ($surface:ident, $name:ident, $rpc:ident, $kind:ident) => {};
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
            [$( #[$meta] )*], idempotent_client, $kind, $name, $req, $variant, $extract,
            ($( $arg : $ty ),*), $ret
        );
    };
}

/// The blocking twin of [`__keylink_idempotent_client_method`], and it drops the
/// same kind for the same reason.
#[doc(hidden)]
#[macro_export]
macro_rules! __keylink_idempotent_blocking_method {
    ([$( #[$meta:meta] )*], tell, $name:ident, $variant:ident,
        ($( $arg:ident : $ty:ty ),*), $ret:ty) => {};
    ([$( #[$meta:meta] )*], rpc, $name:ident, $variant:ident,
        ($( $arg:ident : $ty:ty ),*), $ret:ty) => {
        $crate::__keylink_blocking_method!(
            [$( #[$meta] )*], blocking_idempotent_client, rpc, $name, $variant,
            ($( $arg : $ty ),*), $ret
        );
    };
    ([$( #[$meta:meta] )*], $kind:ident, $name:ident, $variant:ident,
        ($( $arg:ident : $ty:ty ),*), $ret:ty) => {
        $crate::__keylink_blocking_method!(
            [$( #[$meta] )*], blocking_idempotent_client, $kind, $name, $variant,
            ($( $arg : $ty ),*), $ret
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
/// content in beside the reply, because a content address IS the
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
        ($ret, $crate::Content)
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

/// One blocking client method. Only unary `rpc` and `tell` kinds produce code;
/// `send`, `recv` and `blob` stay asynchronous.
#[doc(hidden)]
#[macro_export]
macro_rules! __keylink_blocking_method {
    ([$( #[$meta:meta] )*], blocking_client, $kind:ident, connect, $variant:ident,
        ($( $arg:ident : $ty:ty ),*), $ret:ty) => {
        $crate::__keylink_conflicting_method!([$( #[$meta] )*], blocking_client, $kind, connect, $variant);
    };
    ([$( #[$meta:meta] )*], blocking_client, $kind:ident, r#connect, $variant:ident,
        ($( $arg:ident : $ty:ty ),*), $ret:ty) => {
        $crate::__keylink_conflicting_method!([$( #[$meta] )*], blocking_client, $kind, connect, $variant);
    };
    ([$( #[$meta:meta] )*], $surface:ident, tell, $name:ident, $variant:ident,
        ($( $arg:ident : $ty:ty ),*), $ret:ty) => {
        $( #[$meta] )*
        #[doc = concat!("Send the `", stringify!($name), "` tell and return.")]
        ///
        /// Unlike `send` and `recv`, a blocking `tell` is buildable: there is no
        /// stream to hand back and no body to buffer, so the synchronous wrapper
        /// is the whole operation rather than half of one.
        pub fn $name(&self $(, $arg: $ty )*)
            -> ::core::result::Result<$crate::Transmitted, $crate::BlockingError<$crate::TellError>> {
            self.runtime.run_exact(self.inner.$name($( $arg ),*))
        }
    };
    ([$( #[$meta:meta] )*], $surface:ident, rpc, $name:ident, $variant:ident,
        ($( $arg:ident : $ty:ty ),*), $ret:ty) => {
        $( #[$meta] )*
        #[doc = concat!("Call `", stringify!($name), "`, blocking until it answers.")]
        pub fn $name(&self $(, $arg: $ty )*)
            -> ::core::result::Result<$ret, $crate::BlockingError<$crate::Error>> {
            self.runtime.run_exact(self.inner.$name($( $arg ),*))
        }
    };
    ([$( #[$meta:meta] )*], $surface:ident, send, $name:ident, $variant:ident,
        ($( $arg:ident : $ty:ty ),*), $ret:ty) => {};
    ([$( #[$meta:meta] )*], $surface:ident, recv, $name:ident, $variant:ident,
        ($( $arg:ident : $ty:ty ),*), $ret:ty) => {};
    // Absent for the same reason `send` and `recv` are: a `BlobHandle` fetches
    // asynchronously, so a synchronous wrapper would either buffer the whole
    // blob or hand back a handle that only works inside a runtime.
    ([$( #[$meta:meta] )*], $surface:ident, blob, $name:ident, $variant:ident,
        ($( $arg:ident : $ty:ty ),*), $ret:ty) => {};
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
        /// Returning the [`Grant`](keylink::Grant) IS the grant: the
        /// generated dispatch records it against the peer that asked, so the
        /// handler names no ledger and there is no call to forget. A
        /// [`BlobId`](keylink::BlobId) (`id.into()`) grants that one blob, and a
        /// [`SeqId`](keylink::SeqId) (`seq.into()`) grants the sequence and every
        /// blob it lists. Nothing else in the store becomes reachable. The call
        /// answers [`GrantsExhausted`](keylink::RemoteError::GrantsExhausted)
        /// instead when the grant ledger is at its byte budget.
        fn $name(&self, cx: &$crate::Context $(, $arg: $ty )*)
            -> impl ::core::future::Future<Output = ($ret, $crate::Grant)> + Send;
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
            let (value, grant) = handler.$method(cx $(, $arg )*).await;
            ($resp::$variant((value, grant.content())), $crate::Body::empty())
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
        ($ret, $crate::Grant)
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
                    let (__value, __grant) = __on.$method(__from.caller() $(, $arg )*).await;
                    ($resp::$variant((__value, __grant.content())), $crate::Body::empty())
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
        /// [`abort`](keylink::Registry::abort) can truncate it.
        fn $name(&mut self, cx: $crate::Caller<'_> $(, $arg: $ty )*, body: $crate::BodyReader)
            -> impl ::core::future::Future<Output = $ret> + Send;
    };
    ([], [$( #[$meta:meta] )*], recv, $name:ident, ($( $arg:ident : $ty:ty ),*), $ret:ty) => {
        $( #[$meta] )*
        #[doc = concat!("Answer the `", stringify!($name), "` rpc, returning a payload to stream back.")]
        ///
        /// The body is drained by the connection after the turn ends, so
        /// [`abort`](keylink::Registry::abort) cannot truncate it.
        fn $name(&mut self, cx: $crate::Caller<'_> $(, $arg: $ty )*)
            -> impl ::core::future::Future<Output = ($ret, $crate::Body)> + Send;
    };
    ([], [$( #[$meta:meta] )*], blob, $name:ident, ($( $arg:ident : $ty:ty ),*), $ret:ty) => {
        $( #[$meta] )*
        #[doc = concat!("Answer the `", stringify!($name), "` rpc, returning the address of content this node already holds.")]
        ///
        /// Nothing is streamed during the turn: the address is the whole reply,
        /// and the bytes move later on the blobs connection. So
        /// [`abort`](keylink::Registry::abort) has nothing here to truncate.
        fn $name(&mut self, cx: $crate::Caller<'_> $(, $arg: $ty )*)
            -> impl ::core::future::Future<Output = ($ret, $crate::Grant)> + Send;
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
    ([$( #[$meta:meta] )*], $kind:ident, __status, $req:ident, $resp:ident, $variant:ident, $extract:ident,
        ($( $arg:ident : $ty:ty ),*), $ret:ty) => {
        $crate::__keylink_conflicting_method!([$( #[$meta] )*], actor_ref, $kind, __status, $variant);
    };
    ([$( #[$meta:meta] )*], $kind:ident, r#__status, $req:ident, $resp:ident, $variant:ident, $extract:ident,
        ($( $arg:ident : $ty:ty ),*), $ret:ty) => {
        $crate::__keylink_conflicting_method!([$( #[$meta] )*], actor_ref, $kind, __status, $variant);
    };
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
    // Absent: a service declaring `send` cannot pass either actor-serving
    // compile-time door, because its handler would read an upload inside the
    // serial turn. Emitting a caller for an unservable capability is misleading.
    ([$( #[$meta:meta] )*], send, $name:ident, $req:ident, $resp:ident, $variant:ident, $extract:ident,
        ($( $arg:ident : $ty:ty ),*), $ret:ty) => {};
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
            let (value, content) = response.$extract()?;
            Ok((value, self.client.blob_handle(content)))
        }
    };
}

/// One method on a local keyed reference, shaped by the rpc kind.
#[doc(hidden)]
#[macro_export]
macro_rules! __keylink_keyed_local_method {
    ([$( #[$meta:meta] )*], rpc, $name:ident, $variant:ident,
        ($( $arg:ident : $ty:ty ),*), $ret:ty) => {
        $( #[$meta] )*
        #[doc = concat!("Call the `", stringify!($variant), "` rpc on this actor in this process.")]
        pub async fn $name(&self $(, $arg: $ty )*)
            -> ::core::result::Result<$ret, $crate::LocalError> {
            self.system
                .__local_ask(&self.actor, move |__from, __reply| Mail::$variant {
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
        #[doc = concat!("Send the `", stringify!($variant), "` tell to this actor in this process.")]
        pub async fn $name(&self $(, $arg: $ty )*)
            -> ::core::result::Result<$crate::Queued, $crate::LocalTellError> {
            self.system
                .__local_notify(&self.actor, move |__from, __reply| Mail::$variant {
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
        #[doc = concat!("Call the `", stringify!($variant), "` blob rpc on this actor in this process.")]
        pub async fn $name(&self $(, $arg: $ty )*)
            -> ::core::result::Result<($ret, $crate::Grant), $crate::LocalError> {
            self.system
                .__local_ask(&self.actor, move |__from, __reply| Mail::$variant {
                    origin: __from,
                    $( $arg, )*
                    body: (),
                    reply: __reply,
                })
                .await
        }
    };
    // A local keyed reference has no QUIC body stream. Like `Handle`, it does
    // not invent buffering just to imitate `send` or `recv`.
    ([$( #[$meta:meta] )*], send, $name:ident, $variant:ident,
        ($( $arg:ident : $ty:ty ),*), $ret:ty) => {};
    ([$( #[$meta:meta] )*], recv, $name:ident, $variant:ident,
        ($( $arg:ident : $ty:ty ),*), $ret:ty) => {};
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
    ([$( #[$meta:meta] )*], $kind:ident, __status, $variant:ident,
        ($( $arg:ident : $ty:ty ),*), $ret:ty) => {
        $crate::__keylink_conflicting_method!([$( #[$meta] )*], local_handle, $kind, __status, $variant);
    };
    ([$( #[$meta:meta] )*], $kind:ident, r#__status, $variant:ident,
        ($( $arg:ident : $ty:ty ),*), $ret:ty) => {
        $crate::__keylink_conflicting_method!([$( #[$meta] )*], local_handle, $kind, __status, $variant);
    };
    ([$( #[$meta:meta] )*], rpc, $name:ident, $variant:ident,
        ($( $arg:ident : $ty:ty ),*), $ret:ty) => {
        $( #[$meta] )*
        #[doc = concat!("Call the `", stringify!($variant), "` rpc on this actor.")]
        ///
        /// # Errors
        ///
        /// [`Unanswered`](keylink::Unanswered). Its
        /// [`Turn`](keylink::Unanswered::Turn) arm carries a
        /// [`Withheld`](keylink::Withheld), one variant per way the call lost
        /// its value: [`Gone`](keylink::Withheld::Gone) when the actor is gone,
        /// [`Dropped`](keylink::Withheld::Dropped) when a live actor threw this
        /// reply away without answering it,
        /// [`Restarting`](keylink::Withheld::Restarting) when the incarnation
        /// that owed the answer died and a supervisor is starting another —
        /// carrying the ending that caused it —
        /// [`Refused`](keylink::Withheld::Refused) when an
        /// [`Observer`](keylink::Observer) refused the turn that ran — the actor
        /// is alive, and a supervised one may be restarting —
        /// [`ObserverPanicked`](keylink::Withheld::ObserverPanicked) when one
        /// fell over instead of deciding, and
        /// [`Expired`](keylink::Withheld::Expired) when a queueing budget ran
        /// out before the turn started, which a local origin cannot do today.
        /// Plus [`Reentrant`](keylink::Unanswered::Reentrant) when the call was
        /// made from inside this actor's own turn, which is outside `Withheld`
        /// because nothing was queued.
        ///
        /// "One variant per way" is a promise of completeness, so it is the
        /// whole enum: a fault is the one loss missing, and it is missing
        /// because this method resumes the panic in the caller rather than
        /// reporting it.
        ///
        /// # Panics
        ///
        /// With whatever the handler panicked with, after the actor has
        /// survived it.
        pub async fn $name(&self $(, $arg: $ty )*)
            -> ::core::result::Result<$ret, $crate::Unanswered> {
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
        /// Returns [`Queued`](keylink::Queued) rather than the method's declared
        /// type. The proof means the message is in the mailbox — it has not
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
        /// [`NotQueued`](keylink::NotQueued), which is the whole of what a
        /// `tell` can report and the reason it is not
        /// [`Unanswered`](keylink::Unanswered): the sentence above — no reply
        /// channel — is exactly the sentence that says no verdict on the turn
        /// can arrive here.
        ///
        /// [`NotQueued::Closed`](keylink::NotQueued::Closed) when the actor is
        /// gone, and
        /// [`NotQueued::Reentrant`](keylink::NotQueued::Reentrant) for a
        /// self-`tell` into a **full** mailbox — into a free slot it is queued
        /// and runs after the current turn returns.
        pub async fn $name(&self $(, $arg: $ty )*)
            -> ::core::result::Result<$crate::Queued, $crate::NotQueued> {
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
        /// A [`Grant`](keylink::Grant), not a
        /// [`BlobHandle`](keylink::BlobHandle): a handle is an address plus the
        /// node that holds it, and in this process the holder is you. No grant
        /// is recorded either, because a grant is permission for a *peer* and
        /// there is no peer here.
        ///
        /// # Errors
        ///
        /// The same [`Unanswered`](keylink::Unanswered) variants an `rpc`
        /// answers, for the same reasons: this goes through the same `ask`, and
        /// the blob is written by the turn rather than after it.
        pub async fn $name(&self $(, $arg: $ty )*)
            -> ::core::result::Result<($ret, $crate::Grant), $crate::Unanswered> {
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
    // So: absent here, and present the moment the same actor is served through
    // `Servable::service` on its `Handle`, where there IS a stream.
    // Streaming is a transport
    // capability, and this handle names no transport.
    ([$( #[$meta:meta] )*], send, $name:ident, $variant:ident,
        ($( $arg:ident : $ty:ty ),*), $ret:ty) => {};
    ([$( #[$meta:meta] )*], recv, $name:ident, $variant:ident,
        ($( $arg:ident : $ty:ty ),*), $ret:ty) => {};
}

/// One method on a reserved mailbox slot, shaped by the rpc kind.
///
/// [`__keylink_local_method`]'s set with the wait already paid, so the arms differ
/// from it in two ways and only two. Every method takes `self`, because a slot
/// admits one message. A `tell` returns [`Queued`](crate::Queued) directly rather
/// than `Result<Queued, NotQueued>`: that door's two failures are a full mailbox
/// and a closed one, and the reservation answered both before this method existed.
///
/// `send` and `recv` are absent here for the reason they are absent from
/// [`__keylink_local_method`] — a `BodyReader` and a `Body` are a QUIC stream's
/// two halves, and reserving a mailbox slot creates no stream.
#[doc(hidden)]
#[macro_export]
macro_rules! __keylink_permit_method {
    ([$( #[$meta:meta] )*], rpc, $name:ident, $variant:ident,
        ($( $arg:ident : $ty:ty ),*), $ret:ty) => {
        $( #[$meta] )*
        #[doc = concat!("Spend this permit on the `", stringify!($variant), "` rpc.")]
        ///
        /// # Errors
        ///
        /// The [`Unanswered`](keylink::Unanswered) variants the same method on
        /// [`Handle`] answers, minus one:
        /// [`Reentrant`](keylink::Unanswered::Reentrant) cannot arrive, because
        /// that question was asked and answered when the permit was reserved.
        /// What is left is [`Turn`](keylink::Unanswered::Turn) carrying a
        /// [`Withheld`](keylink::Withheld) — a message can be queued and still
        /// lose its answer.
        ///
        /// # Panics
        ///
        /// With whatever the handler panicked with, after the actor has
        /// survived it, exactly as [`Handle`]'s does.
        pub async fn $name(self $(, $arg: $ty )*)
            -> ::core::result::Result<$ret, $crate::Unanswered> {
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
        #[doc = concat!("Spend this permit on the `", stringify!($variant), "` tell.")]
        ///
        /// Returns [`Queued`](keylink::Queued) directly, where the same method
        /// on [`Handle`] can return [`NotQueued`](keylink::NotQueued). Both of
        /// that door's failures are answered before this one is reachable: full
        /// is what the reservation waited out, and closed is what it reported.
        ///
        /// So this cannot be refused, and it still does not mean the turn ran. A
        /// mailbox that closes between the reservation and this call drops the
        /// message unrun — which is what
        /// [`stop`](Handle::stop) already does to a queued one.
        pub fn $name(self $(, $arg: $ty )*) -> $crate::Queued {
            self.inner
                .notify(move |__from, __reply| Mail::$variant {
                    origin: __from,
                    $( $arg, )*
                    body: (),
                    reply: __reply,
                })
        }
    };
    ([$( #[$meta:meta] )*], blob, $name:ident, $variant:ident,
        ($( $arg:ident : $ty:ty ),*), $ret:ty) => {
        $( #[$meta] )*
        #[doc = concat!("Spend this permit on the `", stringify!($variant), "` rpc, returning the address of the content it named.")]
        ///
        /// # Errors
        ///
        /// The same as this permit's `rpc` methods, for the same reason: it goes
        /// through the same door, and the blob is written by the turn rather than
        /// after it.
        pub async fn $name(self $(, $arg: $ty )*)
            -> ::core::result::Result<($ret, $crate::Grant), $crate::Unanswered> {
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
    ([$( #[$meta:meta] )*], send, $name:ident, $variant:ident,
        ($( $arg:ident : $ty:ty ),*), $ret:ty) => {};
    ([$( #[$meta:meta] )*], recv, $name:ident, $variant:ident,
        ($( $arg:ident : $ty:ty ),*), $ret:ty) => {};
}

/// One client method, shaped by the rpc kind.
#[doc(hidden)]
#[macro_export]
macro_rules! __keylink_client_method {
    ([$( #[$meta:meta] )*], client, $kind:ident, connect, $req:ident, $variant:ident, $extract:ident,
        ($( $arg:ident : $ty:ty ),*), $ret:ty) => {
        $crate::__keylink_conflicting_method!([$( #[$meta] )*], client, $kind, connect, $variant);
    };
    ([$( #[$meta:meta] )*], client, $kind:ident, r#connect, $req:ident, $variant:ident, $extract:ident,
        ($( $arg:ident : $ty:ty ),*), $ret:ty) => {
        $crate::__keylink_conflicting_method!([$( #[$meta] )*], client, $kind, connect, $variant);
    };
    ([$( #[$meta:meta] )*], client, $kind:ident, __status, $req:ident, $variant:ident, $extract:ident,
        ($( $arg:ident : $ty:ty ),*), $ret:ty) => {
        $crate::__keylink_conflicting_method!([$( #[$meta] )*], client, $kind, __status, $variant);
        $crate::__keylink_generated_name_check_item!([
            $( #[$meta] )*
        ], wire, __Status, $variant, $kind);
    };
    ([$( #[$meta:meta] )*], client, $kind:ident, r#__status, $req:ident, $variant:ident, $extract:ident,
        ($( $arg:ident : $ty:ty ),*), $ret:ty) => {
        $crate::__keylink_conflicting_method!([$( #[$meta] )*], client, $kind, __status, $variant);
        $crate::__keylink_generated_name_check_item!([
            $( #[$meta] )*
        ], wire, __Status, $variant, $kind);
    };
    ([$( #[$meta:meta] )*], idempotent_client, $kind:ident, __status, $req:ident, $variant:ident, $extract:ident,
        ($( $arg:ident : $ty:ty ),*), $ret:ty) => {
        $crate::__keylink_conflicting_method!([$( #[$meta] )*], idempotent_client, $kind, __status, $variant);
    };
    ([$( #[$meta:meta] )*], idempotent_client, $kind:ident, r#__status, $req:ident, $variant:ident, $extract:ident,
        ($( $arg:ident : $ty:ty ),*), $ret:ty) => {
        $crate::__keylink_conflicting_method!([$( #[$meta] )*], idempotent_client, $kind, __status, $variant);
    };
    ([$( #[$meta:meta] )*], $surface:ident, rpc, $name:ident, $req:ident, $variant:ident, $extract:ident,
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
    ([$( #[$meta:meta] )*], $surface:ident, tell, $name:ident, $req:ident, $variant:ident, $extract:ident,
        ($( $arg:ident : $ty:ty ),*), $ret:ty) => {
        $( #[$meta] )*
        #[doc = concat!("Send the `", stringify!($variant), "` tell and return.")]
        ///
        /// Returns [`Transmitted`](keylink::Transmitted) rather than the method's
        /// declared type: there is no reply stream, so there is nothing to
        /// decode. The proof means the bytes reached the local transport and
        /// nothing more — see
        /// [`RpcClient::tell`](keylink::RpcClient::tell) for what that does and
        /// does not promise.
        pub async fn $name(&self $(, $arg: $ty )*)
            -> ::core::result::Result<$crate::Transmitted, $crate::TellError> {
            self.inner.tell($req::$variant { $( $arg ),* }).await
        }
    };
    ([$( #[$meta:meta] )*], $surface:ident, send, $name:ident, $req:ident, $variant:ident, $extract:ident,
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
    ([$( #[$meta:meta] )*], $surface:ident, recv, $name:ident, $req:ident, $variant:ident, $extract:ident,
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
    ([$( #[$meta:meta] )*], $surface:ident, blob, $name:ident, $req:ident, $variant:ident, $extract:ident,
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
            let (value, content) = response.$extract()?;
            Ok((value, self.inner.blob_handle(content)))
        }
    };
}

/// One `Calls` signature, shaped by the rpc kind.
///
/// Five arms, four of which expand to nothing — the shape
/// [`__keylink_local_method`] already takes for `send` and `recv`. `rpc` is the
/// only kind declared on all four calling surfaces, so it is the only kind a
/// trait over all four can carry. Each of the other four is missing from at
/// least one surface, and every absence is a written refusal with an argument:
/// the `tell` arms of [`__keylink_actor_client_method`] and
/// [`__keylink_idempotent_client_method`], the `send` and `recv` comment in
/// [`__keylink_local_method`], and — for `blob`, which every surface HAS — the
/// [`Grant`](crate::Grant) a local call answers where a served one answers a
/// [`BlobHandle`](crate::BlobHandle). The generated trait's own rustdoc names
/// all four for a reader who cannot find a method on it.
#[doc(hidden)]
#[macro_export]
macro_rules! __keylink_calls_decl {
    ([$( #[$meta:meta] )*], rpc, $name:ident, $variant:ident,
        ($( $arg:ident : $ty:ty ),*), $ret:ty) => {
        $( #[$meta] )*
        #[doc = concat!("Call the `", stringify!($variant), "` rpc on whichever surface this is.")]
        ///
        /// # Errors
        ///
        /// [`Failure`](keylink::Failure). Its
        /// [`Turn`](keylink::Failure::Turn) arm carries the
        /// [`Withheld`](keylink::Withheld) whichever side produced it, which is
        /// the one condition this trait exists to put in one place.
        /// [`Reentrant`](keylink::Failure::Reentrant) reaches you only from a
        /// local handle, and
        /// [`Other`](keylink::Failure::Other) — carrying the whole
        /// [`Error`](keylink::Error), so nothing is erased — only from a served
        /// one.
        fn $name(&self $(, $arg: $ty )*)
            -> impl ::core::future::Future<
                   Output = ::core::result::Result<$ret, $crate::Failure>
               > + ::core::marker::Send;
    };
    ([$( #[$meta:meta] )*], tell, $name:ident, $variant:ident,
        ($( $arg:ident : $ty:ty ),*), $ret:ty) => {};
    ([$( #[$meta:meta] )*], send, $name:ident, $variant:ident,
        ($( $arg:ident : $ty:ty ),*), $ret:ty) => {};
    ([$( #[$meta:meta] )*], recv, $name:ident, $variant:ident,
        ($( $arg:ident : $ty:ty ),*), $ret:ty) => {};
    ([$( #[$meta:meta] )*], blob, $name:ident, $variant:ident,
        ($( $arg:ident : $ty:ty ),*), $ret:ty) => {};
}

#[doc(hidden)]
#[macro_export]
macro_rules! __keylink_calls_conflict {
    ([$( #[$meta:meta] )*], $name:ident, ($( $arg:ident : $ty:ty ),*), $ret:ty) => {
        $( #[$meta] )*
        fn $name(&self $(, $arg: $ty )*)
            -> impl ::core::future::Future<
                   Output = ::core::result::Result<$ret, $crate::Failure>
               > + ::core::marker::Send {
            async move { ::core::panic!("unreachable generated-name conflict") }
        }
    };
}

#[doc(hidden)]
#[macro_export]
macro_rules! __keylink_calls_method {
    ([$( #[$meta:meta] )*], local_handle, __status, ($( $arg:ident : $ty:ty ),*), $ret:ty) => {
        $crate::__keylink_calls_conflict!([$( #[$meta] )*], __status, ($( $arg : $ty ),*), $ret);
    };
    ([$( #[$meta:meta] )*], local_handle, r#__status, ($( $arg:ident : $ty:ty ),*), $ret:ty) => {
        $crate::__keylink_calls_conflict!([$( #[$meta] )*], __status, ($( $arg : $ty ),*), $ret);
    };
    ([$( #[$meta:meta] )*], local_ref, __status, ($( $arg:ident : $ty:ty ),*), $ret:ty) => {
        $crate::__keylink_calls_conflict!([$( #[$meta] )*], __status, ($( $arg : $ty ),*), $ret);
    };
    ([$( #[$meta:meta] )*], local_ref, r#__status, ($( $arg:ident : $ty:ty ),*), $ret:ty) => {
        $crate::__keylink_calls_conflict!([$( #[$meta] )*], __status, ($( $arg : $ty ),*), $ret);
    };
    ([$( #[$meta:meta] )*], actor_ref, __status, ($( $arg:ident : $ty:ty ),*), $ret:ty) => {
        $crate::__keylink_calls_conflict!([$( #[$meta] )*], __status, ($( $arg : $ty ),*), $ret);
    };
    ([$( #[$meta:meta] )*], actor_ref, r#__status, ($( $arg:ident : $ty:ty ),*), $ret:ty) => {
        $crate::__keylink_calls_conflict!([$( #[$meta] )*], __status, ($( $arg : $ty ),*), $ret);
    };
    ([$( #[$meta:meta] )*], client, connect, ($( $arg:ident : $ty:ty ),*), $ret:ty) => {
        $crate::__keylink_calls_conflict!([$( #[$meta] )*], connect, ($( $arg : $ty ),*), $ret);
    };
    ([$( #[$meta:meta] )*], client, r#connect, ($( $arg:ident : $ty:ty ),*), $ret:ty) => {
        $crate::__keylink_calls_conflict!([$( #[$meta] )*], connect, ($( $arg : $ty ),*), $ret);
    };
    ([$( #[$meta:meta] )*], client, __status, ($( $arg:ident : $ty:ty ),*), $ret:ty) => {
        $crate::__keylink_calls_conflict!([$( #[$meta] )*], __status, ($( $arg : $ty ),*), $ret);
    };
    ([$( #[$meta:meta] )*], client, r#__status, ($( $arg:ident : $ty:ty ),*), $ret:ty) => {
        $crate::__keylink_calls_conflict!([$( #[$meta] )*], __status, ($( $arg : $ty ),*), $ret);
    };
    ([$( #[$meta:meta] )*], idempotent_client, __status, ($( $arg:ident : $ty:ty ),*), $ret:ty) => {
        $crate::__keylink_calls_conflict!([$( #[$meta] )*], __status, ($( $arg : $ty ),*), $ret);
    };
    ([$( #[$meta:meta] )*], idempotent_client, r#__status, ($( $arg:ident : $ty:ty ),*), $ret:ty) => {
        $crate::__keylink_calls_conflict!([$( #[$meta] )*], __status, ($( $arg : $ty ),*), $ret);
    };
    ([$( #[$meta:meta] )*], $surface:ident, $name:ident,
        ($( $arg:ident : $ty:ty ),*), $ret:ty) => {
        $( #[$meta] )*
        fn $name(&self $(, $arg: $ty )*)
            -> impl ::core::future::Future<
                   Output = ::core::result::Result<$ret, $crate::Failure>
               > + ::core::marker::Send {
            async move {
                self.$name($( $arg ),*).await.map_err(::core::convert::From::from)
            }
        }
    };
}

/// One `Calls` body, and there is exactly one for all four implementors.
///
/// The body is the surface's **own inherent method**, plus a `From` on the
/// error, so the trait can never disagree with the method a caller reaches
/// without it — it *is* that method.
///
/// **The tempting alternative is routing `Handle` through
/// [`LocalActor::ask_catching`](crate::LocalActor::ask_catching)**, which would
/// make a handler panic behave the same on every surface: an unwind locally
/// becomes a [`Withheld::Panicked`](crate::Withheld::Panicked) the way a served
/// call already reports it. It is refused, because it gives one method name two
/// behaviours on one type — the inherent one outranks the trait one, so the same
/// call would unwind through `handle.settle(..)` and answer through
/// `Calls::settle(&handle, ..)`. The divergence is real either way; the
/// generated trait's rustdoc states it rather than hiding it.
///
/// The `$meta` reaches the impl method as well as the declaration, which reads
/// as duplicated documentation and is not: an attribute that removes a method —
/// a `#[cfg]` on the `rpc` — has to remove it from the trait and from all four
/// impls together, or the expansion is `E0046` at whichever end kept it.
///
/// # Four uses, and the fifth that is not written
///
/// `blocking::Client` gets no impl. It is synchronous, every method here returns
/// a future, and the blocking side has no local surface at all — there is no
/// `blocking::Handle`, so a synchronous twin of this trait would range over two
/// surfaces that are both remote and both governed, which is not the genericity
/// anyone asked for.
///
/// That is an **absence**, so no call can assert it and no break record can
/// substitute a line for it. It is held from a doctest, which compiles as its
/// own crate and is therefore the position a downstream author meets the refusal
/// from.
///
/// ```compile_fail
/// // Fails with `E0277`: `depot::blocking::Client` is not a calling surface for
/// // this service, which is the generated trait's own
/// // `#[diagnostic::on_unimplemented]` message.
/// //
/// // The code is named HERE and deliberately NOT on the fence. rustdoc does not
/// // check the code after `compile_fail`, so an annotated code asserts a
/// // verification it does not perform — this project has shipped five guards
/// // that were satisfiable without proving their claim, and a fence reading
/// // `compile_fail,E0277` is how the sixth would look.
/// //
/// // What stops this passing for the WRONG reason is the FIRST call below.
/// // `depot::Client` is one of the four implementors, so the service, the
/// // generated trait, the bound and the function signature all have to resolve
/// // before the compiler reaches the second call. And `blocking` is a DEFAULT
/// // feature, so `depot::blocking::Client` is a type that exists: the failure
/// // is the missing impl and not a missing name.
/// //
/// // AND `mise run features` RUNS THIS DOCTEST UNDER `--no-default-features`,
/// // where it passes for exactly the wrong reason. Measured with the fence off:
/// // `error[E0433]: cannot find `blocking` in `depot``. A `compile_fail` passes
/// // on any failure, so that run proves nothing about this guard, and the run
/// // that does is the default-feature one — `cargo test -p keylink --doc`.
/// keylink::service! {
///     service Depot;
///
///     rpc Count() -> u64;
/// }
///
/// fn a_calling_surface<C: depot::Calls>(_: &C) {}
///
/// fn refused(asynchronous: &depot::Client, synchronous: &depot::blocking::Client) {
///     a_calling_surface(asynchronous);
///     a_calling_surface(synchronous);
/// }
/// ```
#[doc(hidden)]
#[macro_export]
macro_rules! __keylink_calls_impl {
    ([$( #[$meta:meta] )*], $surface:ident, rpc, $name:ident,
        ($( $arg:ident : $ty:ty ),*), $ret:ty) => {
        $crate::__keylink_calls_method!(
            [$( #[$meta] )*], $surface, $name,
            ($( $arg : $ty ),*), $ret
        );
    };
    ([$( #[$meta:meta] )*], $surface:ident, tell, $name:ident,
        ($( $arg:ident : $ty:ty ),*), $ret:ty) => {};
    ([$( #[$meta:meta] )*], $surface:ident, send, $name:ident,
        ($( $arg:ident : $ty:ty ),*), $ret:ty) => {};
    ([$( #[$meta:meta] )*], $surface:ident, recv, $name:ident,
        ($( $arg:ident : $ty:ty ),*), $ret:ty) => {};
    ([$( #[$meta:meta] )*], $surface:ident, blob, $name:ident,
        ($( $arg:ident : $ty:ty ),*), $ret:ty) => {};
}

/// The only generator of current and frozen wire contracts.
#[doc(hidden)]
#[macro_export]
macro_rules! __keylink_contract {
    ($svc:ident, $label:literal; $( { [ $( #[$meta:meta] )* ] [ $( #[$cfg:meta] )* ]
        [ $( $defer:tt )? ] $kind:ident $rpc:ident ( $( $arg:ident : $ty:ty ),* $(,)? ) -> $ret:ty } )+ ) => {
        $crate::pastey::paste! {
            #[derive(Debug, Clone, ::serde::Serialize, ::serde::Deserialize, ::postcard_schema::Schema)]
            pub enum Request {
                $( $( #[$meta] )* $rpc { $( $arg: $ty ),* }, )+
                /// What the actor answering this service is doing right now.
                ///
                /// Declared by no service and generated onto every contract, so
                /// it moves this service's ALPN once and then never again. The
                /// structural spelling is deliberate: `__Status` is internal
                /// to the wire, while callers reach the operation through the
                /// [`Status`](keylink::Status) trait. A name a user could also
                /// write would be a name a user could collide with.
                ///
                /// **Never delivered.** Both served closures answer it above
                /// their delivery call, from the flag beside the mailbox, and
                /// enqueue nothing. A message asking would queue behind the
                /// turn that wedged the actor.
                __Status {},
            }

            #[derive(Debug, Clone, ::serde::Serialize, ::serde::Deserialize, ::postcard_schema::Schema)]
            pub enum Response {
                $( $( #[$cfg] )* $rpc($crate::__keylink_response_ty!([<$kind>], $ret)), )+
                /// The answer to [`Request::__Status`]. See
                /// [`Serving`](keylink::Serving) for what its two fields do and
                /// do not claim.
                __Status($crate::Serving),
            }

            impl $crate::Rpc for Request {
                const SERVICE: &'static str = stringify!($svc);
                const METHODS: &'static [&'static str] = &[
                    $( $( #[$cfg] )* stringify!($rpc), )+
                    "__Status",
                ];

                fn method(&self) -> &'static str {
                    match self {
                        $( $( #[$cfg] )* Request::$rpc { .. } => stringify!($rpc), )+
                        // The name a layer refuses `__status` by. The generated
                        // method is answered inside the dispatch, and the chain
                        // runs above it, so this is what a per-method
                        // `Middleware` matches on to close the door for a peer
                        // it does not want reading turn state.
                        Request::__Status { .. } => "__Status",
                    }
                }
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


            impl $crate::Granting for Response {
                fn granted(&self) -> ::core::option::Option<$crate::Content> {
                    match self {
                        $( $( #[$cfg] )* Self::$rpc(__value) =>
                            $crate::__keylink_granted_id!([<$kind>], __value), )+
                        // A status read names no content, so there is no grant
                        // for `serve_one` to record against the peer.
                        Self::__Status(_) => ::core::option::Option::None,
                    }
                }
            }

            /// This build's declared contract label and derived shape hash.
            pub const VERSION: $crate::Version = $crate::Version::__new($label, $crate::__schema_hash::<Contract>(PROTOCOL));
            /// The addressed contract's label and derived shape hash.
            pub const KEYED_VERSION: $crate::Version = $crate::Version::__new($label, $crate::__schema_hash::<KeyedContract>(KEYED_PROTOCOL));
        }
    };
}

/// Parse and generate frozen contracts and their adjacent conversion steps.
#[doc(hidden)]
#[macro_export]
macro_rules! __keylink_previous {
    (@chain [$svc:ident] [$upper:literal] [$($newer:tt)*]) => {};
    (@chain [$svc:ident] [$upper:literal] [$($newer:tt)*]
        previous $label:literal { hash $pin:expr; keyed_hash $keyed_pin:expr; $($body:tt)* } $($rest:tt)*) => {
        $crate::__keylink_previous! {
            @parse [$svc] [$upper] [$($newer)*] [$label] [$pin, $keyed_pin]
            @attrs [] @cfg [] @all [] @plain [] @rest [$($body)*]
        }
        $crate::pastey::paste! {
            $crate::__keylink_previous! { @chain [$svc] [$label] [super::[<version_ $label>]] $($rest)* }
        }
    };

    (@parse $svc:tt $upper:tt $newer:tt $label:tt $pin:tt
        @attrs [$($attrs:tt)*] @cfg [$($cfg:tt)*] @all [$($all:tt)*] @plain [$($plain:tt)*]
        @rest [#[defer] $($rest:tt)*]) => {
        $crate::__keylink_previous! { @parse $svc $upper $newer $label $pin @attrs [$($attrs)*] @cfg [$($cfg)*] @all [$($all)*] @plain [$($plain)*] @rest [$($rest)*] }
    };
    (@parse $svc:tt $upper:tt $newer:tt $label:tt $pin:tt
        @attrs [$($attrs:tt)*] @cfg [$($cfg:tt)*] @all [$($all:tt)*] @plain [$($plain:tt)*]
        @rest [#[cfg($($condition:tt)*)] $($rest:tt)*]) => {
        $crate::__keylink_previous! { @parse $svc $upper $newer $label $pin @attrs [$($attrs)* #[cfg($($condition)*)]] @cfg [$($cfg)* #[cfg($($condition)*)]] @all [$($all)*] @plain [$($plain)*] @rest [$($rest)*] }
    };
    (@parse $svc:tt $upper:tt $newer:tt $label:tt $pin:tt
        @attrs [$($attrs:tt)*] @cfg [$($cfg:tt)*] @all [$($all:tt)*] @plain [$($plain:tt)*]
        @rest [#[cfg_attr($condition:meta, $($payload:tt)*)] $($rest:tt)*]) => {
        $crate::__keylink_cfg_attr! {
            @svc [@previous $svc $upper $newer $label $pin] @done [$($all)*] @feeds [$($plain)*]
            @attrs [$($attrs)* #[cfg_attr($condition, $($payload)*)]] @cfg [$($cfg)*] @defer []
            @guard $condition @payload [$($payload)*] @after [] @rest [$($rest)*]
        }
    };
    (@parse $svc:tt $upper:tt $newer:tt $label:tt $pin:tt
        @attrs [$($attrs:tt)*] @cfg [$($cfg:tt)*] @all [$($all:tt)*] @plain [$($plain:tt)*]
        @rest [#[$meta:meta] $($rest:tt)*]) => {
        $crate::__keylink_previous! { @parse $svc $upper $newer $label $pin @attrs [$($attrs)* #[$meta]] @cfg [$($cfg)*] @all [$($all)*] @plain [$($plain)*] @rest [$($rest)*] }
    };
    (@parse $svc:tt $upper:tt $newer:tt $label:tt $pin:tt
        @attrs [$($attrs:tt)*] @cfg [$($cfg:tt)*] @all [$($all:tt)*] @plain [$($plain:tt)*]
        @rest [changed $kind:ident $rpc:ident ($($arg:ident : $ty:ty),* $(,)?) -> $ret:ty; $($rest:tt)*]) => {
        $crate::__keylink_previous! {
            @parse $svc $upper $newer $label $pin @attrs [] @cfg []
            @all [$($all)* { [$($attrs)*] [$($cfg)*] [] $kind $rpc ($($arg : $ty),*) -> $ret }]
            @plain [$($plain)*] @rest [$($rest)*]
        }
    };
    (@parse $svc:tt $upper:tt $newer:tt $label:tt $pin:tt
        @attrs [$($attrs:tt)*] @cfg [$($cfg:tt)*] @all [$($all:tt)*] @plain [$($plain:tt)*]
        @rest [$kind:ident $rpc:ident ($($arg:ident : $ty:ty),* $(,)?) -> $ret:ty; $($rest:tt)*]) => {
        $crate::__keylink_previous! {
            @parse $svc $upper $newer $label $pin @attrs [] @cfg []
            @all [$($all)* { [$($attrs)*] [$($cfg)*] [] $kind $rpc ($($arg : $ty),*) -> $ret }]
            @plain [$($plain)* { [$($cfg)*] $rpc ($($arg),*) }] @rest [$($rest)*]
        }
    };
    (@parse [$svc:ident] [$upper:literal] [$($newer:tt)*] [$label:literal] [$pin:expr, $keyed_pin:expr]
        @attrs [] @cfg [] @all [$($all:tt)+] @plain [$({ [$(#[$pcfg:meta])*] $plain:ident ($($arg:ident),*) })*] @rest []) => {
        $crate::pastey::paste! {
            /// A frozen contract and its conversion step to the next newer shape.
            pub mod [<version_ $label>] {
                #![allow(unused_imports, dead_code, missing_docs)]
                use super::*;
                use $($newer)* as __newer;
                $crate::__keylink_contract! { $svc, $label; $($all)+ }
                const _: () = ::core::assert!($label < $upper, ::core::concat!("previous ", ::core::stringify!($label), " must be lower than ", ::core::stringify!($upper), " in ", ::core::stringify!($svc)));
                const _: () = ::core::assert!(VERSION.hash() == $pin, ::core::concat!("previous ", ::core::stringify!($label), " of ", ::core::stringify!($svc), " does not hash to its pin: the frozen declaration differs from the one that was current, or keylink's own wire types moved since. Print version_", ::core::stringify!($label), "::VERSION to read what this one hashes to."));
                const _: () = ::core::assert!(KEYED_VERSION.hash() == $keyed_pin, ::core::concat!("previous keyed ", ::core::stringify!($label), " of ", ::core::stringify!($svc), " does not hash to its keyed pin: print version_", ::core::stringify!($label), "::KEYED_VERSION to inspect the frozen addressed contract."));
                /// The adjacent conversion step; implement `Convert` for it.
                pub struct Step;
                /// Conversions for changed, added and removed methods.
                #[diagnostic::on_unimplemented(message = "implement this version step's Convert for every changed, added or removed method")]
                pub trait Convert {
                    fn upgrade_request(old: Request) -> ::core::result::Result<__newer::Request, $crate::Unsupported>;
                    fn downgrade_request(new: __newer::Request) -> ::core::result::Result<Request, $crate::Unsupported>;
                    fn upgrade_response(old: Response) -> ::core::result::Result<__newer::Response, $crate::Unsupported>;
                    fn downgrade_response(new: __newer::Response) -> ::core::result::Result<Response, $crate::Unsupported>;
                }
                impl $crate::Hop for Step {
                    type Request = Request;
                    type Response = Response;
                    type NewerRequest = __newer::Request;
                    type NewerResponse = __newer::Response;
                    fn upgrade_request(old: Request) -> ::core::result::Result<__newer::Request, $crate::Unsupported> {
                        #[allow(unreachable_patterns)]
                        match old {
                            $( $(#[$pcfg])* Request::$plain { $($arg),* } => Ok(__newer::Request::$plain { $($arg),* }), )*
                            Request::__Status {} => Ok(__newer::Request::__Status {}),
                            other => <Self as Convert>::upgrade_request(other),
                        }
                    }
                    fn downgrade_request(new: __newer::Request) -> ::core::result::Result<Request, $crate::Unsupported> {
                        #[allow(unreachable_patterns)]
                        match new {
                            $( $(#[$pcfg])* __newer::Request::$plain { $($arg),* } => Ok(Request::$plain { $($arg),* }), )*
                            __newer::Request::__Status {} => Ok(Request::__Status {}),
                            other => <Self as Convert>::downgrade_request(other),
                        }
                    }
                    fn upgrade_response(old: Response) -> ::core::result::Result<__newer::Response, $crate::Unsupported> {
                        #[allow(unreachable_patterns)]
                        match old {
                            $( $(#[$pcfg])* Response::$plain(v) => Ok(__newer::Response::$plain(v)), )*
                            Response::__Status(v) => Ok(__newer::Response::__Status(v)),
                            other => <Self as Convert>::upgrade_response(other),
                        }
                    }
                    fn downgrade_response(new: __newer::Response) -> ::core::result::Result<Response, $crate::Unsupported> {
                        #[allow(unreachable_patterns)]
                        match new {
                            $( $(#[$pcfg])* __newer::Response::$plain(v) => Ok(Response::$plain(v)), )*
                            __newer::Response::__Status(v) => Ok(Response::__Status(v)),
                            other => <Self as Convert>::downgrade_response(other),
                        }
                    }
                }
                #[doc(hidden)] pub type __Toward = (Step, __newer::__Toward);
            }
        }
    };
    ($($other:tt)*) => { ::core::compile_error!("previous blocks require hash PIN; keyed_hash KEYED_PIN; followed by wire declarations, optionally prefixed with changed, and must follow all current declarations"); };
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
            ) -> (u64, crate::Grant) {
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

    /// The `+ Send` that the generated `Calls` trait writes in return position,
    /// asserted on all four implementors.
    ///
    /// A future that is not `Send` cannot cross `tokio::spawn`, so a trait
    /// declaring `impl Future<..> + Send` is unimplementable for any surface
    /// whose own method hands back one that is not. Four surfaces produce that
    /// future from four different machineries — a mailbox, a keyed connection, a
    /// plain one and a key-scoped one — so one of them regressing is a thing this
    /// module exists to make visible, at the commit that does it.
    ///
    /// Nothing here runs, and nothing here is built: an `rpc` future is
    /// type-checked from a reference, so this needs no node, no connection and no
    /// mounted actor. Compiling is the whole assertion.
    mod every_rpc_future_is_send {
        #![allow(dead_code, reason = "the proof is that this module compiles")]

        use super::expansion;

        fn assert_send<T: Send>(_: T) {}

        fn four_surfaces<A: expansion::Actor>(
            handle: &expansion::Handle<A>,
            actor: &expansion::ActorRef<'_>,
            client: &expansion::Client,
            keyed: &expansion::IdempotentClient,
        ) {
            assert_send(handle.ping(1));
            assert_send(actor.ping(1));
            assert_send(client.ping(1));
            assert_send(keyed.ping(1));
        }
    }

    /// The signature the whole permit surface rests on: a future holding a
    /// **borrowed** permit across an await point is `Send`, and only the owned
    /// form is also `'static`.
    ///
    /// It was measured against tokio's own permit on a scratch integration test
    /// that was then deleted — which is a measurement and not a guard, because
    /// nothing was left behind to fail. This is the guard. If the borrowed form
    /// ever stopped being `Send`, [`Own::reserve`](keylink::Own::reserve) would
    /// be unusable from any caller whose own future has to cross a
    /// `tokio::spawn`, and the answer
    /// would be to make every door take the owned form and pay a refcount.
    ///
    /// **The `yield_now` is fidelity to the measured shape, and it is NOT what
    /// makes the assertion bite** — an earlier version of this paragraph claimed
    /// it was. It puts the *permit itself* live across an await point, which is
    /// what the scratch probe held. Deleting it leaves the permit's data inside
    /// `ping`'s own future, which is live across the same await, so the
    /// assertion still covers it. Measured, by deleting the line and asking for
    /// the stricter bound below: `error[E0521]` fires either way.
    ///
    /// **What DOES make it bite** is the pair of bounds. Ask `assert_spawnable`
    /// for the borrowed form and the module stops compiling with
    /// `error[E0521]: borrowed data escapes outside of function` — which is the
    /// `'static` half of the claim, held from the failing side. Watched.
    ///
    /// Nothing here runs and nothing is built: the futures are type-checked from
    /// a reference and dropped. Compiling is the whole assertion.
    mod a_held_permit_crosses_an_await {
        #![allow(dead_code, reason = "the proof is that this module compiles")]

        use super::expansion;

        fn assert_send<T: Send>(_: T) {}

        /// What `tokio::spawn` demands, and what the borrowed form deliberately
        /// does not satisfy.
        fn assert_spawnable<T: Send + 'static>(_: T) {}

        async fn holding_a_borrowed_permit<A: expansion::Actor>(handle: &expansion::Handle<A>) {
            let Ok(permit) = crate::Own::reserve(handle).await else {
                return;
            };
            tokio::task::yield_now().await;
            let _ = permit.ping(1).await;
        }

        async fn holding_an_owned_permit<A: expansion::Actor>(handle: expansion::Handle<A>) {
            let Ok(permit) = crate::Own::reserve_owned(handle).await else {
                return;
            };
            tokio::task::yield_now().await;
            let _ = permit.ping(1).await;
        }

        fn both_forms<A: expansion::Actor + Send + 'static>(handle: &expansion::Handle<A>) {
            assert_send(holding_a_borrowed_permit(handle));
            assert_spawnable(holding_an_owned_permit(handle.clone()));
        }
    }

    /// A [`Factory`](crate::compose::Factory) over **generated** handles, and the
    /// half of it `compose/factory.rs`'s own tests deliberately cannot reach.
    ///
    /// Those tests build members out of [`LocalActor`](crate::LocalActor), by
    /// hand, because the routing rule lives in that file and a probe only this
    /// macro could build would prove the two agree rather than that the routing
    /// holds. What is left over is exactly this: that the generated `Handle`
    /// satisfies [`Reserving`](crate::compose::Reserving), and that a caller's
    /// closure can be handed the generated `Permit` and call a service method on
    /// it. Both are signature claims, so compiling is the whole assertion.
    ///
    /// `assert_send` is not decoration here either. `dispatch`'s future holds a
    /// permit across the closure's own await, so this is the same claim
    /// [`a_held_permit_crosses_an_await`](self::a_held_permit_crosses_an_await)
    /// makes, one composition further out — and a factory that could not cross a
    /// `tokio::spawn` would be unusable from any connection task.
    mod a_factory_dispatches_to_a_generated_handle {
        #![allow(dead_code, reason = "the proof is that this module compiles")]

        use std::num::NonZeroUsize;

        use super::expansion;
        use crate::compose::{Factory, Member, Routing, Sample, Shard};

        fn assert_send<T: Send>(_: T) {}

        /// No turbofish and no key: `Routing::RoundRobin` has to infer
        /// `Routing<()>` from `dispatch`'s own impl block, which is the claim the
        /// `K = ()` default was added for.
        fn unkeyed<A: expansion::Actor>(make: impl FnMut(Member) -> expansion::Handle<A>) {
            let factory = Factory::from_fn(NonZeroUsize::MIN, Routing::RoundRobin, make);
            assert_send(factory.dispatch(|worker| worker.ping(1)));
        }

        /// The mode that reads a load, so [`Reserving::status`] is named too.
        ///
        /// [`Reserving::status`]: crate::compose::Reserving::status
        fn by_load<A: expansion::Actor>(make: impl FnMut(Member) -> expansion::Handle<A>) {
            let factory = Factory::from_fn(
                NonZeroUsize::MIN,
                Routing::LeastLoaded(Sample::TwoOfN),
                make,
            );
            assert_send(factory.dispatch(|worker| worker.ping(1)));
        }

        /// The keyed door, and the only one whose key type has to be inferred
        /// from the `Shard` rather than from the call.
        fn keyed<A: expansion::Actor>(make: impl FnMut(Member) -> expansion::Handle<A>) {
            let factory =
                Factory::from_fn(NonZeroUsize::MIN, Routing::ByKey(Shard::by_hash()), make);
            assert_send(factory.dispatch_keyed(7_u64, |worker| worker.ping(1)));
        }
    }
}
