# keylink — the local user-facing surface

Repo `/Users/pedrocunha/repos/keylink`, `main` at `01195040bfcfff83be7a0af4f69cdba22b644cfb`.
Read-only survey. Every signature below is copied from the file at the line given.

Signatures inside `macros.rs` are **macro source**. They carry `$crate::`, `$ty`, `$ret`
and `[<$rpc:snake>]`. Where the macro form is hard to read, an *expanded* line follows,
marked as such. The expanded lines are derived, not copied.

---

## 1. The `service!` declaration

`service!` is the only public macro. `keylink/src/macros.rs:251-263`:

```rust
251	#[macro_export]
252	macro_rules! service {
253	    ( service $svc:ident; $( $rest:tt )+ ) => {
```

### Grammar, as a user writes it

```
service! {
    service <Name>;                                  // exactly one, first
    [ #[<attr>] ]*  [ #[defer] ]  <kind> <Method> ( <arg>: <Type>, ... ) -> <Ret> ;
    [ #[<attr>] ]*                feed  <Name>   ( <item>: <Type> )              ;
}
```

- `service <Name>;` is mandatory and first. It becomes a module named after the
  snake_case of `<Name>`.
- At least one **non-feed** declaration is required. `macros.rs:431-446` refuses a
  feeds-only service.
- Attributes accumulate onto the declaration that follows them. A dangling attribute
  at the end is refused (`macros.rs:449-457`).
- A malformed body reaches the catch-all at `macros.rs:449`.

### The six declaration kinds

There are **five wire kinds** plus `feed`. `feed` is not a kind — the muncher puts it
in a second list (`macros.rs:295-301`), so it reaches no wire generator.

| Kind | Form | On the wire | Local `Handle` door | Actor trait method |
|---|---|---|---|---|
| `rpc` | `rpc M(a: A) -> R;` | yes | yes | `fn m(&mut self, cx: Caller<'_>, a: A) -> impl Future<Output = R> + Send` |
| `tell` | `tell M(a: A) -> R;` | yes | yes | `fn m(&mut self, cx: Caller<'_>, a: A) -> impl Future<Output = R> + Send` |
| `send` | `send M(a: A) -> R;` | yes | **no** | `fn m(&mut self, cx: Caller<'_>, a: A, body: BodyReader) -> impl Future<Output = R> + Send` |
| `recv` | `recv M(a: A) -> R;` | yes | **no** | `fn m(&mut self, cx: Caller<'_>, a: A) -> impl Future<Output = (R, Body)> + Send` |
| `blob` | `blob M(a: A) -> R;` | yes | yes | `fn m(&mut self, cx: Caller<'_>, a: A) -> impl Future<Output = (R, BlobId)> + Send` |
| `feed` | `feed N(item: T);` | **no** | n/a | `fn on_n(&mut self, item: T) -> impl Future<Output = ()> + Send` |

The kind list the compiler enforces is at `macros.rs:2973-3072` — `__keylink_response_ty!`,
`__keylink_granted_id!`, `__keylink_is_blob!`, `__keylink_is_send!`, each with one arm per
wire kind.

### `feed` — the local-only declaration

Muncher arm, `macros.rs:347-362`:

```rust
350	        @rest [ feed $feed:ident ( $arg:ident : $ty:ty ) ; $( $rest:tt )* ]
```

Three hard constraints, each with its own `compile_error!`:

| Constraint | Refusal site | Message opening |
|---|---|---|
| No return type | `macros.rs:364-381` | ``` `feed <N>` has no return type, because nothing is waiting for one.``` |
| Exactly one argument | `macros.rs:384-400` | ``` `feed <N>` must declare exactly one argument, which is its item.``` |
| Cannot be the only kind | `macros.rs:431-446` | ``` `service <S>` declares only feeds.``` |

A feed produces no `Request` variant, no `Response` variant, no `WireKinds` entry and no
client method, so the ALPN does not move when one is added (`macros.rs:190-193`).

A feed that ends, ends the actor as `Ended::Feed { name }` (`macros.rs:197-199`,
`observe.rs:502-507`).

### `#[defer]` — the one attribute keylink reads

`macros.rs:316-327` consumes it and never forwards it. It is legal on **`rpc` only**.
`__keylink_defer_check!` (`macros.rs:3539-3600`) emits a `compile_error!` for it on
`tell`, `send`, `recv`, `feed` and `blob`.

`#[defer]` changes exactly three of the thirteen helper macros — the `Actor` method, the
mailbox arm and the served arm (`macros.rs:465-471`). A deferred method's wire types,
client methods, blocking method and local `Handle` method are byte-for-byte an ordinary
`rpc`'s.

Every other `#[<attr>]` is forwarded verbatim to nine sites (`macros.rs:270-272`): the
`Request` variant, the `Actor`, `Handler`, `Client`, `ActorRef` and blocking methods, and
the local `Handle` method.

---

## 2. The `Actor` trait a user implements

`macros.rs:1312-1330`:

```rust
1312	            pub trait Actor: Send + 'static {
1313	                $(
1314	                    $crate::__keylink_actor_method!(
1315	                        [$( $defer )?],
1316	                        [$( #[$meta] )*],
1317	                        [<$kind>], [<$rpc:snake>], ($( $arg : $ty ),*), $ret
1318	                    );
1319	                )+
1320	                $(
1321	                    $( #[$fmeta] )*
1322	                    #[doc = concat!("Handle one item from the `", stringify!($feed), "` feed.")]
1327	                    fn [<on_ $feed:snake>](&mut self, $farg: $fty)
1328	                        -> impl ::core::future::Future<Output = ()> + ::core::marker::Send;
1329	                )*
1330	            }
```

Method names are `snake_case` of the declared name. Feed handlers are `on_<snake>`.

### Per-kind method, verbatim from `__keylink_actor_method!` (`macros.rs:3613-3688`)

**`#[defer] rpc`** — `macros.rs:3616-3642`:

```rust
3636	        fn $name(
3637	            &mut self,
3638	            cx: $crate::Caller<'_>,
3639	            reply: $crate::Reply<$ret>
3640	            $(, $arg: $ty )*
3641	        ) -> impl ::core::future::Future<Output = $crate::Replied<$ret>> + Send;
```

**`rpc`** — `macros.rs:3648-3653`:

```rust
3651	        fn $name(&mut self, cx: $crate::Caller<'_> $(, $arg: $ty )*)
3652	            -> impl ::core::future::Future<Output = $ret> + Send;
```

**`tell`** — `macros.rs:3654-3659`:

```rust
3657	        fn $name(&mut self, cx: $crate::Caller<'_> $(, $arg: $ty )*)
3658	            -> impl ::core::future::Future<Output = $ret> + Send;
```

**`send`** — `macros.rs:3660-3668`:

```rust
3666	        fn $name(&mut self, cx: $crate::Caller<'_> $(, $arg: $ty )*, body: $crate::BodyReader)
3667	            -> impl ::core::future::Future<Output = $ret> + Send;
```

**`recv`** — `macros.rs:3669-3677`:

```rust
3675	        fn $name(&mut self, cx: $crate::Caller<'_> $(, $arg: $ty )*)
3676	            -> impl ::core::future::Future<Output = ($ret, $crate::Body)> + Send;
```

**`blob`** — `macros.rs:3678-3687`:

```rust
3685	        fn $name(&mut self, cx: $crate::Caller<'_> $(, $arg: $ty )*)
3686	            -> impl ::core::future::Future<Output = ($ret, $crate::BlobId)> + Send;
```

There is a fallback arm at `macros.rs:3645-3647` that maps `[defer]` on a non-`rpc` kind
to the ordinary arm. It is only reachable if `__keylink_defer_check!` stops refusing.

### What a handler receives

| Parameter | Type | Source | Present on |
|---|---|---|---|
| `&mut self` | the actor state | — | every kind |
| `cx` | `Caller<'_>` | `middleware.rs:388-397` | every wire kind; **not** on `on_*` |
| `reply` | `Reply<$ret>` | `turns.rs:427` | `#[defer] rpc` only |
| `body` | `BodyReader` | `body.rs` | `send` only |
| args | as declared | — | every kind |

`Caller` — `middleware.rs:388-412`:

```rust
388	#[derive(Debug, Clone, Copy)]
389	pub enum Caller<'a> {
394	    Local,
396	    Peer(&'a Context),
397	}
399	impl<'a> Caller<'a> {
405	    #[must_use]
406	    pub fn peer(self) -> Option<&'a Context> {
```

`Reply<T>` — `turns.rs:427`, with two doors:

```rust
476	    pub fn send(self, value: T) -> Replied<T> {
491	    pub fn later(self) -> (Deferred<T>, Replied<T>) {
```

Both are `#[must_use]` with the same reason (`turns.rs:474-475`, `489-490`): *"the token
is the proof that this reply was answered, and a `#[defer]` handler has to return it"*.

`Deferred<T>` — `turns.rs:722`, one door:

```rust
759	    pub fn send(self, value: T)
760	    where
761	        T: Send + 'static,
762	    {
```

`Deferred` is `#[must_use]` (`turns.rs:720-721`): *"dropping a deferred reply answers the
caller `Dropped`; that is sometimes right, so say so with `drop`"*.

`Replied<T>` — `turns.rs:645`, `pub struct Replied<T>(Settled<T>);`. It has no public
methods. It is a proof token only.

---

## 3. The generated `Handle<A>`

`macros.rs:1970-1972`:

```rust
1970	            pub struct Handle<A> {
1971	                inner: $crate::LocalActor<A, Mail>,
1972	            }
```

`inner` is **private**. `Clone` at `macros.rs:1974`, `Debug` at `macros.rs:1980` (fields:
`service`, `running`, `busy`).

All inherent methods are in `impl<A: Actor> Handle<A>` at `macros.rs:1993`.

### Inherent methods

| Method | Line | Signature | Returns | What it does |
|---|---|---|---|---|
| `service` | 2138 | `pub fn service(&self, allow: $crate::Allowlist) -> $crate::ServiceBuilder<Request, Response>` | builder | Builds a served front for this local actor. Asserts `ServableAsActor::<A>::CHECK` at 2145. |
| `serve` | 2233 | `pub fn serve(&self, node: &$crate::Node, allow: $crate::Allowlist) -> $crate::Result<$crate::Server>` | `Result<Server>` | `Server::builder(node).service(self.service(allow).build()).spawn()`. |
| `with_state` | 2275 | `pub async fn with_state<T, F>(&self, read: F) -> ::core::result::Result<T, $crate::Undelivered>` where `T: Send + 'static, F: FnOnce(&A) -> T + Send + 'static` | `Result<T, Undelivered>` | Reads state on a turn. Delegates to `self.inner.read(read)`. |
| `stop` | 2311 | `pub async fn stop(&self) -> ::core::result::Result<(), $crate::NotQueued>` | `Result<(), NotQueued>` | Queues a stop. Delegates to `self.inner.stop()`. |
| `abort` | 2326 | `pub fn abort(&self)` | `()` | Cancels the task. Delegates to `self.inner.abort()`. |
| `is_running` | 2333 | `#[must_use] pub fn is_running(&self) -> bool` | `bool` | Delegates. |
| `is_busy` | 2344 | `#[must_use] pub fn is_busy(&self) -> bool` | `bool` | Delegates. |
| `buffered` | 2354 | `#[must_use] pub fn buffered(&self) -> u64` | `u64` | Delegates. |
| `ended` | 2378 | `pub fn ended(&self) -> impl ::core::future::Future<Output = $crate::Ended> + ::core::marker::Send + 'static` | `Ended` | Resolves when the actor ends. Delegates. |

Verbatim, `macros.rs:2275-2281`:

```rust
2275	                pub async fn with_state<T, F>(
2276	                    &self,
2277	                    read: F,
2278	                ) -> ::core::result::Result<T, $crate::Undelivered>
2279	                where
2280	                    T: ::core::marker::Send + 'static,
2281	                    F: ::core::ops::FnOnce(&A) -> T + ::core::marker::Send + 'static,
```

`macros.rs:2378-2379`:

```rust
2378	                pub fn ended(&self) -> impl ::core::future::Future<Output = $crate::Ended>
2379	                    + ::core::marker::Send + 'static {
```

### Per-method doors, from `__keylink_local_method!` (`macros.rs:3820-3958`)

Emitted at `macros.rs:2383-2392` inside the same `impl`.

**`rpc`** (and `#[defer] rpc`, identically) — `macros.rs:3851-3852`:

```rust
3851	        pub async fn $name(&self $(, $arg: $ty )*)
3852	            -> ::core::result::Result<$ret, $crate::Undelivered> {
```
Body calls `self.inner.ask(..)`.

**`tell`** — `macros.rs:3894-3895`:

```rust
3894	        pub async fn $name(&self $(, $arg: $ty )*)
3895	            -> ::core::result::Result<(), $crate::NotQueued> {
```
Body calls `self.inner.notify(..)`. Note the declared `-> $ret` is **discarded** here.

**`blob`** — `macros.rs:3922-3923`:

```rust
3922	        pub async fn $name(&self $(, $arg: $ty )*)
3923	            -> ::core::result::Result<($ret, $crate::BlobId), $crate::Undelivered> {
```
Body calls `self.inner.ask(..)`.

**`send` and `recv` generate nothing** — `macros.rs:3954-3957`:

```rust
3954	    ([$( #[$meta:meta] )*], send, $name:ident, $variant:ident,
3955	        ($( $arg:ident : $ty:ty ),*), $ret:ty) => {};
3956	    ([$( #[$meta:meta] )*], recv, $name:ident, $variant:ident,
3957	        ($( $arg:ident : $ty:ty ),*), $ret:ty) => {};
```

### Error type per door

| Door | Error |
|---|---|
| `rpc`, `blob`, `with_state` | `Undelivered` |
| `tell`, `stop` | `NotQueued` |
| `serve` | `keylink::Error` (via `Result`) |
| `abort`, `is_running`, `is_busy`, `buffered`, `service` | infallible |
| `ended` | infallible — yields `Ended` |

---

## 4. `LocalActor<A, M>` — `keylink/src/turns.rs:3495`

```rust
3495	pub struct LocalActor<A, M> {
```

`Clone` at `turns.rs:4466`, `Debug` at `turns.rs:4478`. Three inherent `impl` blocks with
different bounds.

### `impl<A: Send + 'static, M: Turn<A>> LocalActor<A, M>` — `turns.rs:3539` (constructors)

```rust
3548	    pub fn spawn(state: A) -> Self {
3564	    pub fn spawn_observed(state: A, hooks: Vec<Box<dyn Observer<A>>>) -> Self {
3582	    pub fn spawn_fed<F: Feeds<A>>(state: A, hooks: Vec<Box<dyn Observer<A>>>, feeds: F) -> Self {
3637	    pub fn spawn_deep<F: Feeds<A>>(
3638	        state: A,
3639	        hooks: Vec<Box<dyn Observer<A>>>,
3640	        feeds: F,
3641	        depth: NonZeroUsize,
3642	    ) -> Self {
3739	    pub fn spawn_supervised<N, Fut, F>(factory: N, limit: Intensity) -> Self
3740	    where
3741	        N: FnMut() -> Fut + Send + 'static,
3742	        Fut: Future<Output = Result<Incarnation<A, F>, Stop>> + Send,
3743	        F: Feeds<A>,
3744	    {
3765	    pub fn spawn_supervised_deep<N, Fut, F>(
3766	        factory: N,
3767	        limit: Intensity,
3768	        depth: NonZeroUsize,
3769	    ) -> Self
3770	    where
3771	        N: FnMut() -> Fut + Send + 'static,
3772	        Fut: Future<Output = Result<Incarnation<A, F>, Stop>> + Send,
3773	        F: Feeds<A>,
3774	    {
```

### `impl<A, M: Send + 'static> LocalActor<A, M>` — `turns.rs:3816` (sending)

```rust
3891	    pub async fn ask<T>(&self, mail: impl FnOnce(Origin, Reply<T>) -> M) -> Result<T, Undelivered>
3892	    where
3893	        T: Send + 'static,
3894	    {
3941	    pub async fn ask_catching<T>(
3942	        &self,
3943	        mail: impl FnOnce(Origin, Reply<T>) -> M,
3944	    ) -> Result<T, TurnLost>
3945	    where
3946	        T: Send + 'static,
3947	    {
4061	    pub async fn notify<T>(&self, mail: impl FnOnce(Origin, Reply<T>) -> M) -> Result<(), NotQueued>
4062	    where
4063	        T: Send + 'static,
4064	    {
```

### `impl<A, M> LocalActor<A, M>` — `turns.rs:4081` (state and lifecycle)

```rust
4150	    pub async fn read<T, F>(&self, read: F) -> Result<T, Undelivered>
4151	    where
4152	        T: Send + 'static,
4153	        F: FnOnce(&A) -> T + Send + 'static,
4154	    {
4194	    pub fn ended(&self) -> impl Future<Output = Ended> + Send + 'static {
4272	    pub async fn stop(&self) -> Result<(), NotQueued> {
4327	    pub fn abort(&self) {
4337	    pub fn is_running(&self) -> bool {
4373	    pub fn is_busy(&self) -> bool {
4400	    pub fn buffered(&self) -> u64 {
```

### Reachability through `Handle`

`Handle`'s `inner` field is private (`macros.rs:1971`) and there is no `Deref`, no
`as_inner`, no `into_inner`. In fact `macros.rs:4120-4121` asserts the absence of `Deref`:

```rust
4120	        impl<T: ::core::ops::Deref> HandleDoesNotDeref for T {}
4121	        impl<A> HandleDoesNotDeref for expansion::Handle<A> {}
```

| `LocalActor` method | Reachable from `Handle`? | How |
|---|---|---|
| `spawn` | no | — |
| `spawn_observed` | no | — |
| `spawn_fed` | **indirectly** | `actor`, `actor_with`, `actor_wired` all call it |
| `spawn_deep` | **indirectly** | `actor_deep` |
| `spawn_supervised` | **indirectly** | `supervised` |
| `spawn_supervised_deep` | **indirectly** | `supervised_deep` |
| `ask` | **indirectly** | every generated `rpc` and `blob` door |
| `ask_catching` | **NO — unreachable** | no generated door and no inherent method uses it |
| `notify` | **indirectly** | every generated `tell` door |
| `read` | yes | `Handle::with_state` |
| `ended` | yes | `Handle::ended` |
| `stop` | yes | `Handle::stop` |
| `abort` | yes | `Handle::abort` |
| `is_running` | yes | `Handle::is_running` |
| `is_busy` | yes | `Handle::is_busy` |
| `buffered` | yes | `Handle::buffered` |

**`ask_catching` and therefore `TurnLost` are unreachable to a `Handle` user.** See §8.

`spawn_observed` has no door of its own: `actor_with` passes observers to `spawn_fed`
with `()` as the feed (`macros.rs:1635`).

---

## 5. Every spawn door

### Generated, module-level — local (`-> Handle<A>`)

| Door | Line | Signature | Delegates to |
|---|---|---|---|
| `actor` | 1602 | `pub fn actor<A: Actor>(state: A) -> Handle<A>` | `LocalActor::spawn_fed(state, Vec::new(), ())` |
| `actor_with` | 1630 | see below | `LocalActor::spawn_fed(state, observers, ())` |
| `actor_wired` | 1667 | see below | `LocalActor::spawn_fed(state, observers, feeds)` |
| `actor_deep` | 1714 | see below | `LocalActor::spawn_deep(state, observers, feeds, depth)` |
| `supervised` | 1765 | see below | `LocalActor::spawn_supervised(factory, limit)` |
| `supervised_deep` | 1799 | see below | `LocalActor::spawn_supervised_deep(factory, limit, depth)` |

All six are `#[must_use]`.

```rust
1630	            pub fn actor_with<A: Actor>(
1631	                state: A,
1632	                observers: ::std::vec::Vec<::std::boxed::Box<dyn $crate::Observer<A>>>,
1633	            ) -> Handle<A> {

1667	            pub fn actor_wired<A: Actor>(
1668	                state: A,
1669	                observers: ::std::vec::Vec<::std::boxed::Box<dyn $crate::Observer<A>>>,
1670	                feeds: Feeds,
1671	            ) -> Handle<A> {

1714	            pub fn actor_deep<A: Actor>(
1715	                state: A,
1716	                observers: ::std::vec::Vec<::std::boxed::Box<dyn $crate::Observer<A>>>,
1717	                feeds: Feeds,
1718	                depth: ::core::num::NonZeroUsize,
1719	            ) -> Handle<A> {

1765	            pub fn supervised<A: Actor, N, Fut, F>(
1766	                factory: N,
1767	                limit: $crate::Intensity,
1768	            ) -> Handle<A>
1769	            where
1770	                N: ::core::ops::FnMut() -> Fut + ::core::marker::Send + 'static,
1771	                Fut: ::core::future::Future<
1772	                        Output = ::core::result::Result<
1773	                            $crate::Incarnation<A, F>,
1774	                            $crate::Stop,
1775	                        >,
1776	                    > + ::core::marker::Send,
1777	                F: $crate::Feeds<A>,
1778	            {

1799	            pub fn supervised_deep<A: Actor, N, Fut, F>(
1800	                factory: N,
1801	                limit: $crate::Intensity,
1802	                depth: ::core::num::NonZeroUsize,
1803	            ) -> Handle<A>
1804	            where
1805	                N: ::core::ops::FnMut() -> Fut + ::core::marker::Send + 'static,
1806	                Fut: ::core::future::Future<
1807	                        Output = ::core::result::Result<
1808	                            $crate::Incarnation<A, F>,
1809	                            $crate::Stop,
1810	                        >,
1811	                    > + ::core::marker::Send,
1812	                F: $crate::Feeds<A>,
1813	            {
```

### Generated, module-level — mounted into a `System` (`-> Mounted`)

| Door | Line | Signature | Delegates to |
|---|---|---|---|
| `spawn` | 1368 | `pub fn spawn<A: Actor>(state: A) -> Mounted` | `__mount(state, Vec::new(), ())` |
| `spawn_wired` | 1463 | see below | `__mount(state, observers, feeds)` |
| `spawn_supervised` | 1560 | see below | `mount(.. run_keyed_supervised(factory, limit, inbox) ..)` |

```rust
1463	            pub fn spawn_wired<A: Actor>(
1464	                state: A,
1465	                observers: ::std::vec::Vec<::std::boxed::Box<dyn $crate::Observer<A>>>,
1466	                feeds: Feeds,
1467	            ) -> Mounted {

1560	            pub fn spawn_supervised<A: Actor, N, Fut, F>(
1561	                factory: N,
1562	                limit: $crate::Intensity,
1563	            ) -> Mounted
1564	            where
1565	                N: ::core::ops::FnMut() -> Fut + ::core::marker::Send + 'static,
1566	                Fut: ::core::future::Future<
1567	                        Output = ::core::result::Result<
1568	                            $crate::Incarnation<A, F>,
1569	                            $crate::Stop,
1570	                        >,
1571	                    > + ::core::marker::Send,
1572	                F: $crate::Feeds<A>,
1573	            {
```

`__mount` is private — `macros.rs:1478-1489`:

```rust
1478	            fn __mount<A: Actor, F: $crate::Feeds<A>>(
1479	                state: A,
1480	                observers: ::std::vec::Vec<::std::boxed::Box<dyn $crate::Observer<A>>>,
1481	                feeds: F,
1482	            ) -> Mounted {
```

All three `Mounted` doors carry a distinct `#[must_use =]` reason:

- `spawn` (1366): *"an unmounted actor is dropped, and the state it was built around goes with it"*
- `spawn_wired` (1461): *"an unmounted actor is dropped, which stops the task this just started"*
- `spawn_supervised` (1558): *"an unmounted actor is dropped, and the supervisor that would have kept it alive goes with it"*

### The two `const` refusals the doors carry

`ServableAsActor::<A>::CHECK` — asserts `!DECLARES_SEND` (`macros.rs:644-657`).
`SpawnableUnfed::<A>::CHECK` — asserts `!DECLARES_FEEDS` (`macros.rs:673-685`).

| Door | `ServableAsActor` | `SpawnableUnfed` |
|---|---|---|
| `actor` | — | 1603 |
| `actor_with` | — | 1634 |
| `actor_wired` | — | — |
| `actor_deep` | — | — |
| `supervised` | — | — |
| `supervised_deep` | — | — |
| `spawn` | 1383 | 1384 |
| `spawn_wired` | 1468 | — |
| `spawn_supervised` | 1574 | — |
| `Handle::service` | 2145 | — |

`DECLARES_BLOBS` / `DECLARES_SEND` / `DECLARES_FEEDS` are folded at `macros.rs:616-631`.

### Other generated module-level items (non-spawn, for context)

| Item | Line | Signature |
|---|---|---|
| `alpn` | 567 | `pub fn alpn() -> &'static [u8]` |
| `keyed_alpn` | 814 | `pub fn keyed_alpn() -> &'static [u8]` |
| `service` | 700 | `pub fn service<H: Handler>(handler: H, allow: $crate::Allowlist) -> $crate::ServiceBuilder<Request, Response>` |
| `serve` | 764 | `pub fn serve<H: Handler>(node: &$crate::Node, handler: H, allow: $crate::Allowlist) -> $crate::Result<$crate::Server>` |
| `keyed_service` | 1202 | `pub fn keyed_service(system: &System, allow: $crate::Allowlist) -> $crate::ServiceBuilder<$crate::Addressed<Request>, Response>` |
| `at` | 2550 | `pub fn at(..)` — keyed client constructor |

Type aliases at `macros.rs:1143-1158`, all `#[doc(hidden)]`:

```rust
1143	            pub type Mailbox = $crate::Mailbox<Mail>;
1151	            pub type System = $crate::System<Mail>;
1155	            pub type SystemBuilder = $crate::SystemBuilder<Mail>;
1158	            pub type Mounted = $crate::Mounted<Mail>;
```

### `Feeds` — the generated bundle (`macros.rs:1055-1139`)

```rust
1054	            #[doc(hidden)]
1055	            pub struct Feeds {
1067	                #[must_use]
1068	                pub fn new(
1069	                    $( [<$feed:snake>]: impl $crate::Stream<Item = $fty>
1070	                        + ::core::marker::Send + 'static, )*
1071	                ) -> Self {
```

One argument per declared feed, in declaration order. It implements
`impl<A: Actor> $crate::Feeds<A> for Feeds` (`macros.rs:1118`) with round-robin polling
starting at `self.at` (`macros.rs:1121-1138`), so no feed starves another.

The `Feeds` **struct** is `#[doc(hidden)]`; the `Feeds` **trait** it implements
(`turns.rs:1398`) is also `#[doc(hidden)]`.

---

## 6. The local error types

### `Undelivered` — `turns.rs:3166-3289`

```rust
3166	#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
3167	pub enum Undelivered {
```

| Variant | Line | `#[error]` text |
|---|---|---|
| `Gone` | 3192 | `the actor is no longer running` |
| `Dropped` | 3203 | `the actor is serving and this reply was never answered` |
| `Restarting` | 3215 | `the incarnation that owed this answer is gone and another is coming` |
| `Refused { name: &'static str }` | 3230 | `the {name} observer refused this turn: it ran and was not committed` |
| `ObserverPanicked { name: &'static str }` | 3258 | `the {name} observer panicked during this turn: it ran and was not committed` |
| `Expired` | 3273 | `the queueing budget expired before this turn started: nothing ran` |
| `Reentrant` | 3288 | `this call was made from inside the target actor's own turn: it would deadlock` |

Seven variants. `Copy`, so it carries no payload beyond `&'static str`.

### `NotQueued` — `turns.rs:3352-3410`

```rust
3352	#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
3353	pub enum NotQueued {
```

| Variant | Line | `#[error]` text |
|---|---|---|
| `Closed` | 3386 | `the actor is no longer running` |
| `Reentrant` | 3409 | `this call was made from inside the target actor's own turn: it would deadlock` |

Two variants. Note the name is `Closed`, not `Gone`, for the same fact.

### `TurnLost` — `observe.rs:646-743`

```rust
646	#[derive(Debug, thiserror::Error)]
647	pub enum TurnLost {
```

| Variant | Line | `#[error]` text |
|---|---|---|
| `Gone` | 653 | `the actor is no longer running` |
| `Dropped` | 671 | `the actor is serving and this reply was never answered` |
| `Restarting(Arc<Ended>)` | 684 | `the incarnation that owed this answer is gone and another is coming` |
| `Refused { name: &'static str }` | 687 | `the {name} observer refused this turn: it ran and was not committed` |
| `ObserverPanicked { name: &'static str }` | 705 | `the {name} observer panicked during this turn: it ran and was not committed` |
| `Faulted(Panic)` | 717 | `{0}` |
| `Expired` | 728 | `the queueing budget expired before this turn started: nothing ran` |
| `Reentrant` | 742 | `this call was made from inside the target actor's own turn: it would deadlock` |

Eight variants. It is `Undelivered` plus `Faulted(Panic)`, with `Restarting` carrying
`Arc<Ended>` instead of nothing. Not `Copy`, not `Clone`, not `PartialEq`.

The doc at `observe.rs:729-740` states the three-enum relationship explicitly: *"Three
enums, one fact, because a caller matches the answer of the door it used."*

### `Ended` — `observe.rs:451-540`

```rust
451	#[derive(Debug, Clone)]
452	pub enum Ended {
```

**Not a `thiserror::Error`.** It has a hand-written `impl fmt::Display` at
`observe.rs:547-570`, so there are no `#[error]` attributes to quote.

| Variant | Line | Fields | `Display` output (`observe.rs:550-567`) |
|---|---|---|---|
| `Stopped` | 455 | — | `the actor was stopped` |
| `Vanished` | 458 | — | `the actor's task was cancelled` |
| `Panicked` | 465 | `message: String` | `a turn panicked: {message}` |
| `Observer` | 470 | `name: &'static str`, `error: Arc<dyn Error + Send + Sync>`, `fatal: bool` | `the {name} observer ended this incarnation: {error}` |
| `Feed` | 502 | `name: &'static str` | `the {name} feed ended` |
| `GaveUp` | 522 | `after: u32`, `error: Arc<dyn Error + Send + Sync>` | `gave up after {after} restarts: {error}` |

Six variants.

### Which door returns which

| Door | Error |
|---|---|
| `Handle::<rpc>`, `Handle::<blob>`, `Handle::with_state` | `Undelivered` |
| `Handle::<tell>`, `Handle::stop` | `NotQueued` |
| `Handle::ended` | `Ended` (not an error — the future's `Output`) |
| `LocalActor::ask`, `LocalActor::read` | `Undelivered` |
| `LocalActor::ask_catching` | `TurnLost` |
| `LocalActor::notify`, `LocalActor::stop` | `NotQueued` |
| `LocalActor::ended` | `Ended` |

---

## 7. Observability and lifecycle

### `Observer<A>` — `observe.rs:109-165`

```rust
109	pub trait Observer<A>: Send + 'static {
111	    fn name(&self) -> &'static str;
122	    fn after_turn<'a>(&'a mut self, state: &'a A, turn: TurnInfo<'a>) -> TurnFut<'a>;
162	    fn finish<'a>(&'a mut self, _state: &'a A, _ended: &'a Ended) -> FinishFut<'a> {
163	        Box::pin(async {})
164	    }
165	}
```

`name` and `after_turn` are required. `finish` has a default.

Supporting types:

`turns.rs:305`:

```rust
305	pub type Hooks<A> = [Box<dyn Observer<A>>];
```

`observe.rs:88` and `observe.rs:96`:

```rust
88	pub type TurnFut<'a> = Pin<Box<dyn Future<Output = Result<(), Stop>> + Send + 'a>>;
96	pub type FinishFut<'a> = Pin<Box<dyn Future<Output = ()> + Send + 'a>>;
```

`TurnInfo<'a>` — `observe.rs:206-250`, `#[non_exhaustive]`, `Debug + Clone + Copy`:

```rust
208	pub struct TurnInfo<'a> {
210	    pub outcome: TurnOutcome<'a>,
226	    pub method: &'static str,
227	}
229	impl<'a> TurnInfo<'a> {
245	    #[doc(hidden)]
246	    #[must_use]
247	    pub fn for_test(outcome: TurnOutcome<'a>, method: &'static str) -> Self {
```

`TurnOutcome<'a>` — `observe.rs:253-276`, `Debug + Clone + Copy`:

| Variant | Line | Fields |
|---|---|---|
| `Completed` | 256 | — |
| `Panicked` | 272 | `message: &'a str` |

`Stop` — `observe.rs:318-366`, the veto an observer returns:

```rust
318	pub struct Stop(Box<dyn Error + Send + Sync>, bool);
320	impl Stop {
332	    pub fn new(because: impl Into<Box<dyn Error + Send + Sync>>) -> Self {
358	    pub fn fatal(because: impl Into<Box<dyn Error + Send + Sync>>) -> Self {
364	    pub fn is_fatal(&self) -> bool {
```

### How a user attaches an observer

Only through a spawn door that takes `observers: Vec<Box<dyn Observer<A>>>`:
`actor_with`, `actor_wired`, `actor_deep`, `spawn_wired`, and — via
`Incarnation<A, F> = (A, Vec<Box<dyn Observer<A>>>, F)` (`turns.rs:2474`) — the factory
of `supervised`, `supervised_deep` and `spawn_supervised`.

`actor` and `spawn` pass `Vec::new()` and attach none. There is no `Handle` method to add
an observer after the fact.

### `Feeds` — the actor's second input

`turns.rs:1349-1412`:

```rust
1349	pub enum Fed<T> {
1357	    Item(Box<T>),
1359	    Ended {
1361	        name: &'static str,
1362	    },
1363	}
1397	#[doc(hidden)]
1398	pub trait Feeds<A>: Send + 'static {
1404	    type Item: Turn<A>;
1411	    fn poll_next(&mut self, cx: &mut std::task::Context<'_>) -> std::task::Poll<Fed<Self::Item>>;
1412	}
```

`impl<A: Send + 'static> Feeds<A> for ()` at `turns.rs:1421` is the empty feed — that is
what `actor`, `actor_with` and `spawn` pass.

A user never implements this trait. They call the generated `Feeds::new(stream, ..)`.

### `Inspector` — `console.rs:720-843`

```rust
720	pub struct Inspector {
753	impl Inspector {
765	    pub fn new<M>(system: &System<M>, node: &Node, events: &Events, history: NonZeroUsize) -> Self
766	    where
767	        M: Send + 'static,
768	    {
```

It is exported at `lib.rs:252-255` alongside `Console`, `EndReason`, `EventPage`,
`EventStream`, `Events`, `FollowRefused`, `LifecycleEvent`, `NodeKey`, `NodeStatus`,
`MAX_FOLLOWERS`.

`Inspector::new(&system, &node, &events, HISTORY)` (`console.rs:20`) takes a `System`,
so **it is a served-side facility, not a local-`Handle` one**. It implements
`inspect::Actor` (`console.rs:845`). A `Handle` user has no `System` and cannot build one.

### `compose::*` — `keylink/src/compose/`

Re-exports at `compose/mod.rs:134-139`:

```rust
134	pub use group::{Added, Group, GroupEnded, Membership, Retire, Verdict};
135	pub use keyed::Endings;
136	pub use names::Names;
137	pub use pool::{Member, Pool};
138	pub use schedule::{after, every, Scheduled};
139	pub use topic::{Fanout, Overflow, Subscription, Topic};
```

**`Group`** — `compose/group.rs:784`. Supervision over a set of named actors.

```rust
803	    pub fn new(policy: impl FnMut(&ActorId, &Ended) -> Verdict + Send + 'static) -> Self {
854	    pub fn add(&self, name: ActorId, member: Membership) -> Added {
913	    pub fn remove(&self, name: &ActorId) -> Option<Retire> {
940	    pub fn names(&self) -> Vec<ActorId> {
953	    pub fn ended(&self) -> impl Future<Output = GroupEnded> + Send + 'static {
1010	    pub fn stop(&self) {
```

- `Verdict` (`group.rs:347`): `Forget` (349), `StopAll` (352).
- `GroupEnded` (`group.rs:363`, `#[non_exhaustive]`): `Member { member: ActorId, reason: Ended }` (365).
- `Added` (`group.rs:520`): `Filed` (522), `Displaced(Retire)` (527), `Refused(Retire)` (533); `pub fn is_filed(&self) -> bool` (543).
- `Retire` (`group.rs:457`): `pub async fn retire(self)` (479).
- `Membership` (`group.rs:553`):

```rust
612	    pub fn new<F>(
613	        ended: impl Future<Output = Ended> + Send + 'static,
614	        retire: impl FnOnce() -> F + Send + 'static,
615	    ) -> Self
616	    where
617	        F: Future<Output = ()> + Send + 'static,
618	    {
```

`Membership::new` takes an `ended` future and a `retire` closure, so a `Handle` joins a
`Group` as `Membership::new(handle.ended(), move || async move { handle.stop().await; })`
or similar — the `Handle` itself is never handed over.

**`Names<H>`** — `compose/names.rs:106`. A registry keyed by `ActorId`, generic over the
handle type, self-cleaning on `ended`.

```rust
114	    pub fn new() -> Self {
137	    pub fn register(
138	        &self,
139	        name: ActorId,
140	        handle: H,
141	        ended: impl Future<Output = Ended> + Send + 'static,
142	    ) -> Option<H>
143	    where
144	        H: Send + 'static,
145	    {
186	    pub fn unregister(&self, name: &ActorId) -> Option<H> {
192	    pub fn registered(&self) -> usize {
202	    pub fn names(&self) -> Vec<ActorId> {
224	    pub fn get(&self, name: &ActorId) -> Option<H> {
```

**`Pool<H>`** — `compose/pool.rs:130`. Round-robin over a fixed set of handles.

```rust
95	pub struct Member(usize);
100	    pub fn index(self) -> usize {
154	    pub fn from_fn(count: NonZeroUsize, spawn: impl FnMut(Member) -> H) -> Self {
175	    pub fn pick(&self) -> (Member, &H) {
191	    pub fn members(&self) -> &[H] {
202	    pub fn size(&self) -> NonZeroUsize {
```

**`Topic<T>`** — `compose/topic.rs:193`. Fan-out to subscribers.

```rust
205	    pub fn new(overflow: Overflow) -> Self {
223	    pub fn subscribe(&self) -> Subscription<T> {
236	    pub fn subscribers(&self) -> usize {
280	    pub async fn publish(&self, item: T) -> Fanout {
```

- `Overflow` (`topic.rs:107`): `BlockPublisher` (119), `DropSubscriber` (131).
- `Fanout` (`topic.rs:144`, `Debug + Clone + Copy + PartialEq + Eq`): `pub delivered: usize` (146), `pub dropped: usize` (151), `pub gone: usize` (154).
- `Subscription<T>` (`topic.rs:384`) implements `crate::Stream` (`topic.rs:397`), so it
  plugs straight into a generated `Feeds::new(..)`. That is the join between `compose`
  and `feed`.

**`Endings`** — `compose/keyed.rs:225`. Watch keyed actors by name.

```rust
233	    pub fn new() -> Self {
271	    pub fn watch(&self, name: &ActorId) -> impl Future<Output = Ended> + Send + 'static {
```

**`schedule`** — `compose/schedule.rs`.

```rust
112	pub struct Scheduled {
128	    pub fn detach(mut self) {
138	    pub fn is_finished(&self) -> bool {
178	pub fn after<F, Fut>(delay: Duration, work: F) -> Scheduled
179	where
180	    F: FnOnce() -> Fut + Send + 'static,
181	    Fut: Future<Output = ()> + Send + 'static,
182	{
206	pub fn every<F, Fut>(period: Duration, mut work: F) -> Scheduled
207	where
208	    F: FnMut() -> Fut + Send + 'static,
209	    Fut: Future<Output = ()> + Send + 'static,
210	{
```

`Scheduled` has no `cancel`. `compose/schedule.rs:81` records that `pub fn cancel(self)`
shipped briefly and was removed.

---

## 8. What surprised me

Ordered by how much it changes the local-vs-remote comparison.

**1. `send` and `recv` have no local door, and the gap is deliberate, documented and
guarded.** `__keylink_local_method!` (`macros.rs:3954-3957`) expands to nothing for both.
The reason is written out at `macros.rs:3934-3953`: a `BodyReader` reads a QUIC
`RecvStream` and a `Body` drains into a `SendStream`, and *"neither half exists in this
process"*. The absence is also asserted — `mod no_streaming_on_a_local_handle`
(`macros.rs:4090-4106`) is a shadowing probe that stops compiling if a `store` or `fetch`
inherent method is ever added to `Handle`. `macros.rs:851-861` records the matching fact
on the `Mail` enum: `send` and `recv` get variants nothing builds.

What is still worth flagging for the comparison: the **user-facing diagnostic** is a plain
*"no method named `download`"*, not a `compile_error!` naming the reason. A user who
declares `recv` and implements the handler finds the door missing with no pointer to
`macros.rs:3934`. The two `const` assertions (`ServableAsActor`, `SpawnableUnfed`) show the
crate is willing to spend a `compile_error!` on exactly this shape of mistake elsewhere.

**2. `TurnLost` is unreachable from the local surface.** `LocalActor::ask_catching`
(`turns.rs:3941`) is the only producer, and no generated door and no `Handle` method calls
it. It is the richest of the three error enums — eight variants, the only one carrying
`Faulted(Panic)` and the only one whose `Restarting` carries `Arc<Ended>` — and a `Handle`
user cannot obtain one. `lib.rs:322` exports it regardless.

**3. Three enums say "the actor is no longer running" with two different variant names,
and the odd one out is argued for.** `Undelivered::Gone`, `TurnLost::Gone`,
`NotQueued::Closed`. The `#[error]` text is byte-identical across all three. `Closed` is
deliberate — `turns.rs:3357-3362` argues that this door never reads actor state, so
*"each of them would be an inference wearing the clothes of an observation."*

The residue for the comparison is not a defect but a cost: a caller reading three doors
matches two spellings of one condition, and the identical `Display` text means a log line
cannot tell them apart.

**4. `tell` requires a return type that no caller can read.** The muncher arm
(`macros.rs:402-417`) matches only `$kind $rpc ( .. ) -> $ret ;`, so `tell Note(line: String);`
is a syntax error — a user must write `-> ()`. The `Actor` method does return `$ret`
(`macros.rs:3657-3658`), but the local door returns `Result<(), NotQueued>` and the remote
client returns `Result<()>`. Both discards are documented at `macros.rs:3868-3872`:
*"Returns `()` rather than the method's declared type, exactly as the remote client does,
and for the same reason: a `tell` has no reply channel."*

So the discard is deliberate and symmetric. What the comparison should carry is the
grammar cost: `feed` is allowed to omit `-> Ret` and `tell` is not, though neither
delivers a value to a caller. A `tell` declared `-> u64` compiles, and the handler must
produce a `u64` that nothing anywhere can read.

**5. `Undelivered` has seven variants, `NotQueued` has two — documented, with a
consequence worth stating.** A `tell` caller cannot be told `Refused`,
`ObserverPanicked`, `Expired`, `Dropped` or `Restarting`. `macros.rs:3882-3887` and
`observe.rs:736-738` both give the reason: no reply channel means no verdict on the turn
can arrive. The consequence for the comparison is that an observer vetoing every turn is
**invisible** to a `tell`-only caller — every call returns `Ok(())` while nothing commits.

**6. `Ended` is the only one of the four that is not a `thiserror::Error`.** Hand-written
`Display` at `observe.rs:547`, `#[derive(Debug, Clone)]` only. It has no `source()`, so
`Ended::Observer { error, .. }` and `Ended::GaveUp { error, .. }` — both holding
`Arc<dyn Error + Send + Sync>` — do not chain. The lead should check whether that is
deliberate before treating `Ended` as an error type in the comparison.

**7. `spawn_observed` has no generated door, but it is not dead.**
`LocalActor::spawn(state)` delegates to it (`turns.rs:3549`) and tests call it directly
(`turns.rs:5931`, `turns.rs:6037`). No `service!`-generated door reaches it: `actor_with`
goes to `spawn_fed(state, observers, ())` instead (`macros.rs:1635`). The name is argued
for at `turns.rs:3554-3558` — not `spawn_with`, because `SystemBuilder::spawn_with`
already means a factory for keyed actors.

**8. The `ServableAsActor` / `SpawnableUnfed` checks are asymmetric across siblings.**
`actor` and `actor_with` assert `SpawnableUnfed`; `actor_wired`, `actor_deep`,
`supervised` and `supervised_deep` assert neither. `spawn` asserts both, `spawn_wired`
and `spawn_supervised` assert only `ServableAsActor`. `supervised` returns a `Handle<A>`
and asserts nothing, so a `send`-declaring service reaches `Handle::service` before the
`ServableAsActor` refusal fires (at `macros.rs:2145`) — later than the equivalent `spawn`
path, though still at compile time.

**9. `Handle` has no identity, by design, and `compose` keys on identity.** No `id()`, no
name, no `ActorId`. `ActorRef<'a>` has `pub fn id(&self) -> &$crate::ActorId`
(`macros.rs:2579`), but that is the keyed side, and `macros.rs:2052-2059` states the rule:
*"No type represents a served actor"* — an `ActorId` belongs to an actor mounted in a
`System`, and a local actor is a singleton with no registry entry.

The consequence is real for section 7: `Names<H>` (`compose/names.rs:137`) and `Group`
(`compose/group.rs:854`) both take an `ActorId` as the key. A user putting `Handle`s into
either must invent and carry the `ActorId` themselves, and nothing checks that the invented
name matches anything.

**10. The generated `Feeds` struct carries sixteen lines of doc and then
`#[doc(hidden)]`.** `macros.rs:1038-1055` argues why `new` is a constructor rather than a
builder, why each stream is boxed, and why feeds cannot be added to a running actor — and
`#[doc(hidden)]` on line 1054 means none of it renders. The `Feeds` trait
(`turns.rs:1397`) is hidden too.

`Feeds::new` is nonetheless the documented way to wire a feed: `macros.rs:183-184` tells
the user to call it, and the `SpawnableUnfed` refusal message (`macros.rs:679-683`) names
it as the fix. The type a user is instructed to call does not appear in rustdoc, so the
`compile_error!` points at an API the reader cannot look up.

**11. `Handle::with_state` and `LocalActor::read` are the same door under two names.**
`with_state` (`macros.rs:2275`) is a pure delegation to `read` (`turns.rs:4150`) with an
identical signature, and its own doc says so (`macros.rs:2267`). It is the only renamed
delegation — `Handle` keeps `LocalActor`'s names exactly everywhere else (`stop`, `abort`,
`is_running`, `is_busy`, `buffered`, `ended`). I found no argument in the source for this
one rename, unlike the `Closed` and `spawn_observed` namings, which both carry one.

`macros.rs:2266-2272` also narrows the error set: `with_state` can only return
`Undelivered::Reentrant` or `Undelivered::Gone`, because a read runs no observers and
carries no queueing budget. The other five variants are unreachable through this door.

**12. `Scheduled` cannot be cancelled**, only detached (`compose/schedule.rs:128`) or
polled (`is_finished`, 138). `compose/schedule.rs:81` records that `cancel` shipped and
was withdrawn. Worth knowing before the comparison implies a symmetry with anything on
the remote side that can be aborted.

---

## Appendix — files read

| File | Sections |
|---|---|
| `/Users/pedrocunha/repos/keylink/keylink/src/macros.rs` | 1, 2, 3, 5 |
| `/Users/pedrocunha/repos/keylink/keylink/src/turns.rs` | 2, 4, 6, 7 |
| `/Users/pedrocunha/repos/keylink/keylink/src/observe.rs` | 6, 7 |
| `/Users/pedrocunha/repos/keylink/keylink/src/middleware.rs` | 2 |
| `/Users/pedrocunha/repos/keylink/keylink/src/lib.rs` | export surface |
| `/Users/pedrocunha/repos/keylink/keylink/src/compose/*.rs` | 7 |
| `/Users/pedrocunha/repos/keylink/keylink/src/console.rs` | 7 (`Inspector`) |
