# Proposal — the extension surface and the fan-out policies

|  |  |
|---|---|
| Date | 2026-08-10 |
| Baseline | `origin/main @ 896e869` (includes merged PR 48) |
| Supersedes | `.tmp/ractor-proposal/index.html` (Rev. A, 2026-08-08) |
| Status | Approved after the PR 48 rebase and adversarial plan review |

---

## 0. What this proposes

Six future changes across two areas: two extension-surface changes and four
`compose` fan-out policies.

Two items from the previous revision are no longer proposed work:

- generated permit types and the `Own` capability already exist.
- generated `Feeds` already are the typed local-input extension point.

One rule governs the remaining choices.

> **Runtime data may choose among names the generated contract exposes. An
> unknown name fails at startup; it never becomes a string comparison that can
> fail open during dispatch.**

That is the contract-hash rule — type drift is refused at the handshake —
applied one layer in without inventing a second, unchecked vocabulary.

---

## 1. What already exists

Read from current `origin/main` after merged PR 48. This inventory bounds the
scope: nothing below re-proposes it.

| Capability | Current state | Evidence |
|---|---|---|
| Permit doors, primitive | `LocalActor::{reserve, try_reserve, reserve_owned}` | `keylink/src/turns.rs` |
| Permit doors, generated | Generated handles implement `Own::{reserve, try_reserve, reserve_owned}` and retain generated `Permit` and `OwnedPermit` | `keylink/src/surfaces.rs`, `keylink/src/macros.rs` |
| Factory reservation seam | Generated handles implement `compose::Reserving`, forwarding through `Own` | `keylink/src/macros.rs`, `keylink/src/compose/factory.rs` |
| Mailbox overflow policy | `Backpressure::Park(NonZeroUsize)` and `Shed(NonZeroUsize)` | `keylink/src/turns.rs` |
| `Unbounded` mailbox | Refused by the permit-bearing type shape | `keylink/src/turns.rs` |
| Mailbox depth, local | Primitive `LocalActor::spawn_deep`. Generated `setup` / `restarting` builders select `.backpressure(...)` before `.actor()` | `keylink/src/turns.rs`, `keylink/src/macros.rs` |
| Mailbox depth, keyed | `RegistryBuilder::mailbox_depth` supplies the default. Generated `Setup::backpressure` overrides it per `Mountable` | `keylink/src/actor/registry.rs`, `keylink/src/macros.rs` |
| Worker factory | `Factory::{from_fn, members, dispatch, dispatch_keyed}` | `keylink/src/compose/factory.rs` |
| Routing modes | `RoundRobin`, `FirstAvailable`, `LeastLoaded(Sample)`, `ByKey(Shard<K>)` | `keylink/src/compose/factory.rs` |
| `Routing::Sticky` | Refused in place | `keylink/src/compose/factory.rs` |
| Load signals | `LocalActor::{busy, queued, serving}`. Generated handles expose `Own::status() -> Serving` | `keylink/src/turns.rs`, `keylink/src/surfaces.rs` |
| Hand-written loop escape hatch | Public `Mailbox<M>` | `keylink/src/actor/registry.rs` |
| Runtime peer admission | `Admission`, `DynamicAdmission`, and live revocation | `keylink/src/admission.rs` |
| Behavior-installation seams | `Admission`, `Middleware`, `Observer`, and generated `Feeds` | `keylink/src/admission.rs`, `keylink/src/middleware.rs`, `keylink/src/observe.rs`, `keylink/src/turns.rs` |
| Caller capability traits | `Wire`, `BlockingWire`, `WithIdempotency`, `Status`, `Own`, and `Servable`, curated by `keylink::prelude` | `keylink/src/surfaces.rs`, `keylink/src/lib.rs` |
| Generated calling surfaces | `Handle`, `Client`, `RegistryClient`, `LocalRef`, `ActorRef`, and `Calls` | `keylink/src/macros.rs` |
| Keyed lifecycle observation | `Registry::ended(&id)` | `keylink/src/actor/registry.rs` |
| Fan-out baseline | `Topic<T>`, fixed `IN_FLIGHT = 1`, and `Overflow::{BlockPublisher, DropSubscriber}` | `keylink/src/compose/topic.rs` |

Three consequences close questions rather than open them.

**`Backpressure` remains closed at two arms, by a type.** An unbounded channel's
sender has no permits, so the three reservation doors would each need a
reservation that reserves nothing — a representable but meaningless state.

**`Routing::Sticky` remains refused.** A factory's members are a `Box<[H]>`
built once. There is no resize for affinity to survive, and at a fixed member
count `Sticky` is `ByKey`.

**Caller capabilities are not behavior registries.** `LocalRef` and `ActorRef`
address actors. `Calls` abstracts typed `rpc`. The six traits in
`keylink::prelude` add capabilities to generated caller or serving surfaces.
These types install no behavior. Thus, they do not replace the four behavior
seams in §3.

---

## 2. Invariants this proposal does not touch

- **The wire-contract mechanism.** The generated `Request` and `Response` types
  define the service contract. Feeds and all four `compose` items remain local
  types and enter no generated wire enum (`keylink/src/macros.rs:5178`, `:5807`).
- **Fail-closed peer admission.** `Allowlist` has no `Default`, pinned by
  `HasNoDefault` (`keylink/src/authz.rs:128`; `keylink/src/representability.rs:410`). E1 does
  not overload `Admission` with per-method policy.
- **Nothing is lost in silence.** Every new policy either loses nothing or
  reports loss through a type the consumer must handle.
- **One construction path.** No item adds another local reference, calling
  trait, permit surface, or plugin wrapper around a surface that already exists.

---

## 3. The extension surface

`Admission`, `Middleware`, and `Observer` are runtime trait-object seams around
compile-time types. Generated `Feeds` is the fourth seam, but it cannot be a
trait-object registry: `Feeds<A>` carries `type Item: Turn<A>`, and `Turn<A>`
returns `impl Future` (`keylink/src/turns.rs:1419`, `:332`). The macro also fixes
one stream field per declared feed and a static poller table
(`keylink/src/macros.rs:5887`, `:5919`).

A process-external plugin boundary is still refused. In particular,
`Observer::after_turn` borrows actor state (`keylink/src/observe.rs:126`), which
has no ABI, WASM guest, or socket representation. A dynamic plugin matching
methods by string would also turn a contract change into a rule that quietly
stops matching instead of a build or startup failure.

### E1 — compile data-driven peer/method rules into the existing seams

`Admission` runs once per connection and sees a proven peer. Per-method
authorization belongs in `Middleware`, whose `Context` already carries the peer,
service, method, actor, and call identity. The previous revision incorrectly
combined those two decisions in one `Admission` object.

The file remains data-driven:

```toml
[[peer]]
id    = "k51qzi5uqu5..."
allow = ["GetItem", "Reserve"]

[[peer]]
id    = "k51qzi5uqu9..."
allow = ["GetItem"]
```

The generated `Rpc` implementation gains one associated method set beside its
existing per-request `method()`:

```rust
trait Rpc {
    const SERVICE: &'static str;
    const METHODS: &'static [&'static str];
    fn method(&self) -> &'static str;
    fn actor(&self) -> Option<&ActorId> { None }
}
```

`METHODS` is emitted from the same wire repetition as `Request`. It uses the
macro's existing normalized `$cfg` channel instead of arbitrary item metadata.
It includes generated `__Status`. Thus, a method disabled in this binary is
unknown in this binary. Existing `Rpc::actor` behavior remains intact.
`Addressed<T>` forwards both `T::METHODS` and the actor identity. `METHODS`
has no default, so every hand-written `Rpc` implementation must name its complete
method set.

```rust
let compiled = Rules::parse(&fs::read_to_string("rules.toml")?)?
    .compile::<inventory::Request>()?;
//  ^ Err(UnknownMethod { named: "Reserv", .. }) at startup

let service = compiled
    .install(|allowlist| inventory::service(handler, allowlist))
    .build();
```

The compiled value keeps two separate policies internally:

- the connection-wide peer set for the existing `Admission`.
- typed middleware that compares `Context::method()` only against names already
  validated through `Request::METHODS` and refuses with the existing
  `RemoteError::Unauthorized`.

`Compiled<Req>` and `Authorization<Req>` retain the request type with
`PhantomData<fn() -> Req>`. Authorization implements middleware only for `Req`
and `Addressed<Req>`. Rules compiled for one service therefore cannot authorize a
second service that happens to use the same method name.

`Compiled::install` owns builder creation and applies both policies. The closure
receives the validated allowlist, creates the existing generated service builder,
and `install` adds the matching authorization layer. The two raw projections stay
hidden as implementation details. A coherence probe fails if authorization later
becomes generic over an unrelated request type.

E1 also closes the existing unchecked `Keyable::{Only, Except}` vocabulary.
`ServiceBuilder::keyable` and `build` remain infallible. `build` compares every
selected name with `Req::METHODS` while `Req` is known and stores the first error
on the erased service. Server startup returns `ConfigError` before dispatch. A
cfg-disabled name is unknown. `__Status` is a known method and may appear in the
policy.

No hot-reload protocol is proposed. Re-reading the file and atomically replacing
both projections can be designed later if a real caller needs it. This change is
only startup compilation with fail-closed names.

### E2 — one documentation page, no new behavioral surface

Add a documentation-only `keylink::extend` module linking to the existing root
exports. It adds no re-export aliases, trait, registry, or `Plugin` wrapper.

The page states, per seam, what it sees, when it runs, how refusal is reported,
and what a panic means:

| Seam | Sees | Runs | Refusal / ending | Panic |
|---|---|---|---|---|
| `Admission` | `ProvenPeer` | once per connection, before dispatch | `Admittance` closes or admits the connection | no typed recovery at this seam |
| `Middleware` | `Context`, typed `Request`, body, and `Next` | around dispatch, before the handler or actor delivery | `Outcome::Err(RemoteError)` | not converted into a typed refusal at this seam |
| `Observer` | `&A` plus `TurnInfo { outcome, method, duration }` | after the handler, before the reply, on local and keyed actors | caller sees `Withheld::Refused`; actor reports `Ended::Observer` | caller sees `Withheld::ObserverPanicked`; actor reports `Ended::Observer` or supervision restarts it |
| generated `Feeds` | no caller context; it is a local input | interleaved with the mailbox | stream exhaustion reports `Ended::Feed { name }` | a feed-handler panic is a panicked turn: `Ended::Panicked` or a supervised restart |

That table is the primary deliverable. The page then makes the public taxonomy
explicit:

- **behavior-installation seams:** `Admission`, `Middleware`, `Observer`, and
  generated `Feeds`.
- **caller and serving capability extension traits:** `Wire`, `BlockingWire`,
  `WithIdempotency`, `Status`, `Own`, and `Servable`, available through
  `keylink::prelude`.
- **generated cross-surface calling interface:** per-service `Calls`.
- **concrete generated surfaces:** `Handle`, `Client`, `RegistryClient`,
  `LocalRef`, and `ActorRef`.

The capability traits install no behavior. The page records PR 48's collision
rule. User RPC names own both bare and underscored spellings. Framework
operations remain available through trait dispatch. The page includes explicit
UFCS such as `Own::reserve(&handle)`.

### E3 — closed: generated `Feeds` already are the extension point

There is nothing to build. `Feeds::new` accepts one external
`Stream<Item = T> + Send + 'static` per declared feed, and the generated feed
turn is `Send + 'static`. `T` has no serialization or wire-schema bound.

```rust
let ticks = external.subscribe();
let book = book::setup(Book::new())
    .feeds(book::Feeds::new(ticks))
    .actor();
```

The cost remains the reliable-delivery limit: a feed has no caller, reply,
acknowledgement, or idempotency context. A source with at-least-once semantics
carries its sequence number into actor state. E2 consolidates that existing fact;
E3 is removed from the implementation order.

---

## 4. `compose` — the fan-out policies

Today `Topic` holds one unread item per subscriber through
`const IN_FLIGHT: usize = 1` (`keylink/src/compose/topic.rs:94`).

### C1 — make depth an explicit policy decision

```rust
pub enum Overflow {
    BlockPublisher(NonZeroUsize),
    DropSubscriber(NonZeroUsize),
}
```

`Topic::new` still requires an `Overflow`; neither `Overflow` nor `Topic` gains a
`Default`. Existing callers migrate explicitly to `NonZeroUsize::MIN`, preserving
the current depth of one without introducing a hidden default.

The depth lives inside the policy, so an unbounded topic cannot be written. This
is the same finite-capacity contract as `Backpressure::{Park, Shed}`. The current
mailbox API has no shared numeric maximum, so this change also corrects RFC-07
§8.1: it must not claim fan-out is bounded by a mailbox maximum that does not
exist. A separate maximum-depth policy would have to apply to both mailboxes and
fan-out and is outside this proposal.

A successful publish and `Fanout::delivered` mean that the item was accepted into
the subscriber's bounded channel. They do not mean that the receiver consumed it
or that an actor turn ran. Blocking begins only after that subscriber's configured
depth is full. C1 corrects existing “taken” or “slowest subscriber rate” prose to
that enqueue boundary.

C1 rewrites the module's titled depth-one decision instead of deleting it. The
old argument treated every queue as unbounded. A `NonZeroUsize` depth makes that
state impossible. The new text records three reliable policies over two loud
outcomes and keeps `Overflow` exhaustive for those three policies. It also changes
the cost statement from capacity one to the configured finite capacity.

Add an exact, exhaustive representability probe with no `..` patterns. The
probe binds every depth as `NonZeroUsize`. Thus, a bare `usize` fails to compile.
An added field, such as an unbounded `Option`, also fails to compile.

Two existing slow-subscriber guards migrate to explicit depth one:

- `a_slow_subscriber_is_unsubscribed_and_its_actor_ends_naming_the_feed`.
- `a_slow_subscriber_blocks_the_publisher_instead_of_growing_a_buffer`.

The registered topic-policy anchors are D4 and D5 in `.mise/tasks/anchors`.
They point from `keylink/tests/cycles.rs` to the two `Overflow` match arms.
Re-quote them when the variants gain depth. Preserve those self-publish safety tests
at depth one.

Add separate depth-two behavior guards: two items are accepted and the third
triggers the selected policy. Their red break mutates the production expression
that passes `depth.get()` to the bounded channel, not the test's input. A test
input changed from one to two proves only that the assertion notices different
input. It does not prove that production uses the configured depth.

### C2 — block, then unsubscribe that subscriber

This is a third policy but not a third terminal outcome: an item is delivered, or
the slow subscriber is removed and told through stream termination. The current
"two policies" prose is rewritten to distinguish **three policies over two loud
outcomes**.

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Patience(Duration);

impl Patience {
    pub fn try_after(value: Duration) -> Result<Self, PatienceError>;
}

pub enum Overflow {
    BlockPublisher(NonZeroUsize),
    DropSubscriber(NonZeroUsize),
    BlockThenDrop {
        depth: NonZeroUsize,
        patience: Patience,
    },
}
```

`Patience::try_after` accepts only portable timer values. The duration must be
nonzero, use whole milliseconds, and be no greater than `i32::MAX` milliseconds.
The constructor also rejects a value that the current target clock cannot
represent. Each subscriber deadline repeats the clock check. If a long-lived
value no longer fits, it expires immediately instead of panicking. The clock is
`n0_future::time`, never `std::time::Instant` or `tokio::time`.

The deadline is per subscriber. Publishing remains sequential in subscription
order, so one publish may wait up to `subscriber_count × patience`. Cancellation
still delivers to a prefix. On timeout, the new item is not queued. The publisher
closes and removes the canonical bounded subscriber queue during that subscriber's
iteration. The subscriber drains its already-buffered items and then ends, even
while a later subscriber is still waiting. `Fanout::dropped` increments. A closed
receiver increments `gone`. A successful send increments `delivered`.

`BlockThenDrop` totally orders publishes on one topic with cancellation-safe
publication tickets. A later publisher waits for the current publish to finish or
be canceled. The order state stores no items and no lock guard survives a poll.
Receiver wakes are batched until the current ticket is released. Each caller
retains ownership of its own publish future and item. The subscriber-list and
queue mutexes are never held across an await. Paused-clock guards prove that no
item arrives after a fanout reports the subscriber as dropped.

An outer `timeout(d, topic.publish(x))` remains different: outer cancellation
does not itself unsubscribe the subscriber at which it stopped. If that
subscriber's own patience deadline wins, including equality under the clock's
poll order, the policy removal still occurs.

### C3 — typed loss through `Lagged<T>` and `LossyTopic`

Dropping an item while keeping the subscriber is a different interface because
the stream must report gaps.

```rust
pub enum Lagged<T> {
    Item(T),
    Skipped(NonZeroU64),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BroadcastDepth(NonZeroUsize);

impl BroadcastDepth {
    pub fn try_new(value: NonZeroUsize) -> Result<Self, BroadcastDepthError>;
    pub fn get(self) -> NonZeroUsize;
}

pub enum Lossy {
    DropNewest(NonZeroUsize),
    DropOldest(BroadcastDepth),
}

pub struct LossyTopic<T> { /* ... */ }
pub struct LossySubscription<T> { /* Stream<Item = Lagged<T>> */ }

pub struct LossyFanout {
    pub subscribers: usize,
    pub accepted: usize,
}

impl<T: Clone + Send + 'static> LossyTopic<T> {
    pub fn new(policy: Lossy) -> Self;
    pub fn subscribe(&self) -> LossySubscription<T>;
    pub fn publish(&self, item: T) -> LossyFanout;
}
```

Neither `Lossy` nor `LossyTopic` has a `Default`. The caller names the policy
and depth. `DropNewest` accepts any `NonZeroUsize`. `DropOldest` accepts only a
`BroadcastDepth` that is a power of two and no greater than Tokio's maximum safe
broadcast capacity. Invalid values fail before a `LossyTopic` can exist.

`publish` is synchronous. The absence of `async` guarantees that neither policy
waits for a subscriber. `LossyFanout` is a sibling of `Fanout` because
`Fanout::dropped` means subscriber removal. Lossy delivery keeps subscribers.
`subscribers` counts live receivers at the publication decision. `accepted`
counts receivers that accepted the new item. Under `DropOldest`, every live
receiver accepts it. Loss remains authoritative on each `LossySubscription`
through `Lagged`.

Implementation:

- `DropOldest` uses one `tokio::sync::broadcast` channel and one receiver cursor
  per subscriber. `RecvError::Lagged(u64)` maps directly to
  `Skipped(NonZeroU64)`. `LossySubscription` owns a `Send` stream built with
  `n0_future::stream::unfold` and stores it as
  `Pin<Box<dyn Stream<Item = Lagged<T>> + Send>>`. It does not use
  `n0_future::boxed::BoxStream`, which is non-`Send` in a browser.
- `DropNewest` uses one bounded `mpsc` channel per subscriber. Accepted channel
  entries carry `skipped_before: Option<NonZeroU64>`. A saturating pending counter
  shares one non-async lock with the sender decision. When a send succeeds, the
  entry takes the pending count and resets it. When a full channel rejects the
  item, the counter increases. The receiver emits `Skipped` before a tagged item.
  If no later item arrives, it emits the pending marker after the kept backlog
  drains, including before stream termination.

The ordering is explicit. A `DropNewest` marker follows the backlog that was kept
and precedes the first later accepted item. A `DropOldest` marker precedes the
next surviving item. A handler must match both `Lagged` arms. The shared lock is
the linearization point for a pending marker and a later accepted item.

### C4 — `KeyedTopic<K, T>`

Use a name that cannot be confused with the existing root `keylink::Router`,
which is iroh's protocol router. `KeyedTopic` is keyed fan-out beside `Topic`: an
item reaches only subscribers for its key.

```rust
let ticks: KeyedTopic<Symbol, Arc<Tick>> =
    KeyedTopic::new(Overflow::DropSubscriber(depth));

// in the keyed actor factory
orders::setup(state)
    .feeds(orders::Feeds::new(ticks.route(id.clone())))
    .mountable()

// in the reader task
let symbol = tick.symbol.clone();
let fanout = ticks.publish(&symbol, Arc::new(tick)).await;
```

The routed methods require `K: Eq + Hash + Clone + Send + 'static` and
`T: Clone + Send + 'static`. `route(key)` returns an owned
`KeyedSubscription<K, T>` that is compatible with generated `Feeds::new`.

The key map uses `std::sync::Mutex`. Thus, holding its guard across an await does
not compile on a `Send` publish future. Each entry stores an explicit live-route
count. `route` increments it under the map lock. `KeyedSubscription::drop`
decrements it under the same lock. The drop removes the entry only when the count
reaches zero and `Arc::ptr_eq` confirms that the map still contains the same
entry. In-flight publishes can clone the entry but never change the route count.
A late drop therefore cannot remove a replacement for the same key.

Publishing clones the entry under the map lock, releases the lock, and then
awaits the per-key topic. A blocked publish for one key cannot hold the map or
block another key. Publishing an unknown key performs only a lookup. It returns
`Fanout { delivered: 0, dropped: 0, gone: 0 }`, does not clone the key, does not
allocate, and does not insert an entry. The map is bounded by live routed
subscriptions, not by every key ever published.

It reuses `Overflow` and `Fanout` because the payload and terminal outcomes are
unchanged. Without it, a `Topic` in front of N keyed actors clones
every item N times so N−1 handlers can discard it.

---

## 5. Closed from the previous revision — generated permit doors

The old F1 wave is complete. Generated handles implement the root `Own` trait.
This trait exposes `reserve`, `try_reserve`, and `reserve_owned`. Generated typed
permits remain, as does the `compose::Reserving` adapter.

PR 48 deliberately moved framework operations from generated inherent methods to
traits. A service can own names such as `Reserve`, `TryReserve`, and their
underscored forms. Callers import `keylink::prelude` or use UFCS such as
`Own::reserve(&handle)`. Bare inherent names remain on `LocalActor`. Keylink owns
that namespace.

One stale sentence remains in `keylink/src/compose/pool.rs`: generated handles do
have the nonwaiting primitive through `Own::try_reserve`. The real backlog is a
nonwaiting factory `try_dispatch` seam because `Reserving` intentionally exposes
only waiting `reserve` and `status`. Correct that sentence with E2 rather than
adding another permit surface.

---

## 6. Where keylink wraps tokio

> A wrapper earns its place when it changes **what the compiler checks**. A
> wrapper that changes only **what the call looks like** is a rename.

| Item | Tokio equivalent | Verdict |
|---|---|---|
| `Backpressure::Park/Shed(NonZeroUsize)` | bounded beside unbounded `mpsc` constructors | **Earns it.** An unbounded mailbox cannot be expressed. |
| `LocalActor::{reserve, try_reserve, reserve_owned}` and generated handles through `Own` | `mpsc::Sender` permit methods | **Earns it.** Re-entrancy is refused; full and closed stay distinct; generated handles preserve service typing. |
| Generic and generated permit types | `mpsc::Permit<Mail>` | **Earns it.** Generated `Mail` is private per service, so Tokio's type cannot cross the public interface. |
| `Topic` + `Overflow` | `broadcast` | **Earns it.** Tokio broadcast neither blocks the publisher nor removes a slow subscriber. |
| `LossyTopic::DropOldest` | `broadcast` | **Does not earn a rewrite.** It is an adapter over one broadcast channel. |
| `LossyTopic::DropNewest` | none | **Earns it.** Tokio has no drop-newest channel. |
| `Lagged<T>` | `RecvError::Lagged` | **Earns it.** The gap reaches the typed handler instead of a receive loop that can swallow it. |
| `KeyedTopic<K, T>` | none | **Earns it.** Tokio does not route fan-out by key. |
| `Factory` routing | none | Already built. |

The mailbox itself cannot become a raw Tokio channel. The receiver lives inside
the actor task with the serial loop, panic guard, observers, and fate. A caller
who wants the floor can already use public `Mailbox<M>` and write that loop.

---

## 7. Refused, with the reason recorded

| Idea | Verdict | Reason |
|---|---|---|
| Process-external plugin loader over the four seams | **Refused** | `Observer` borrows actor state, and string-matched typed middleware can fail open after contract change. |
| `Backpressure::Unbounded` | **Refused by type** | Permit doors would need a reservation that reserves nothing. |
| Runtime-registered feed | **Not expressible in the current interface** | `Feeds<A>` has `type Item: Turn<A>` and `Turn<A>` returns `impl Future`. Neither is an object-safe runtime registry. |
| `Routing::Sticky` | **Refused in place** | Fixed members mean no resize for affinity to survive; at fixed N it is `ByKey`. |
| `Coalesce` | **Refused in this proposal** | It adds a reducer and payload semantics neither `Topic` nor `LossyTopic` owns. Publish snapshots, or aggregate before this seam; revisit only for a caller that cannot. |
| Factory `try_dispatch` | **Backlog** | Generated handles have `Own::try_reserve`, but `Reserving` intentionally exposes only waiting reservation. A fast factory door needs a new seam and a real caller. |
| Four-lane priority mailbox | **Refused** | The actor loop cannot use `tokio::select!` on the browser target (`keylink/src/turns.rs:1404`). A lane therefore means hand-written poll ordering. |
| Factory-owned restart or kill | **Refused** | Supervision and composition already compose; kill without restart silently shrinks the pool. |
| Periodic factory sweep | **Refused** | It costs a task per factory, while `compose::every` already lets a caller request one. |

---

## 8. Order of work

Each wave is a commit boundary and names what it invalidates.

| # | Wave | Depends on | Invalidates or updates |
|---|---|---|---|
| 1 | **C1** — depth-bearing `Overflow`, representability probe, and corrected RFC/module invariant | — | private `IN_FLIGHT`, Topic constructors, slow-subscriber guards, `cycles.rs`, and D4/D5 anchor literals |
| 2 | **C2** — portable patience and serialized `BlockThenDrop` | C1 | the "two policies" prose, timeout/accounting tests |
| 3 | **C3** — `BroadcastDepth`, `Lagged`, `LossyTopic`, and `LossySubscription` | C1 | compose export/bounds table, browser guards, and marker-order guards |
| 4 | **C4** — `KeyedTopic<K, T>` with key-entry lifecycle | C1 | generated `Registry`/`setup(...).mountable()` example becomes a concrete crate type |
| 5 | **E1** — generated `Rpc::METHODS`, parsed rules, and separate admission/middleware projections | — | `Rpc` implementors, cfg-gated method-set tests, service-builder example |
| 6 | **E2** — documentation-only `extend` page and stale Pool correction | E1 | documentation only |

C1–C4 are one subsystem. E1 is independent. E2 lands last so its table names the
final extension surface. E3 and F1 are not waves.

---

## 9. Measurements and red guards to add

1. **C1 representability and depth:** exact exhaustive patterns prove every
   arm carries `NonZeroUsize` and no extra unbounded spelling exists. Depth-two
   tests break the production `depth.get()` capacity use. D4/D5 preserve the
   self-publish policy distinction at depth one.
2. **C2 clock and concurrency semantics:** constructor tests reject zero,
   fractional milliseconds, values above `i32::MAX` milliseconds, and values the
   current clock cannot represent. Native tests use
   `#[tokio::test(start_paused = true)]` plus `sleep`, never the repository's
   known-vacuous `advance + yield` pattern. They pin one tick before and exactly
   at the deadline, per-subscriber patience, prefix cancellation, buffered-item
   draining, equality ordering, and `Fanout` accounting. A two-publisher test
   proves prompt terminal removal by channel identity. Constructor boundaries and
   a short policy timeout also run in the existing Chrome target.
3. **C3 adapter behavior:** constructor tests accept powers of two and reject
   three plus values above Tokio's safe maximum. `HasNoDefault` probes cover
   `BroadcastDepth`, `Lossy`, and `LossyTopic`. A compile assertion proves
   `LossySubscription<T>: Stream + Send + 'static`. Native and existing-browser
   guards cover broadcast lag, both marker orders, no marker without loss, and
   surviving items. A deterministic depth-two `A/B/C/D` schedule proves that a
   `DropNewest` marker stays between the kept backlog and later accepted items. A
   private unit seam seeds the skip counter near `u64::MAX`. Its red break
   replaces `saturating_add` with ordinary addition.
4. **C4 lifecycle and allocation:** an isolated allocation test target proves that
   an unknown-key publish allocates nothing. A private clone-bomb and map-length
   guard proves that it does not clone the key or insert an entry. Deterministic
   entry-identity tests pause old cleanup, install a replacement, resume cleanup,
   and prove last-route removal without scheduler races. A blocked publish on key
   A does not delay publication on key B.
5. **E1 names and binding:** exact cfg fixtures prove active declarations present,
   disabled declarations absent, generated `__Status` present, and
   `Addressed<T>` forwards the same slice while preserving `Rpc::actor`.
   Unknown, cfg-disabled, and misspelled rule and `Keyable` names fail at startup.
   A coherence probe prevents authorization compiled for one request type from
   implementing middleware for another. `Compiled::install` tests prove that one
   construction door applies both policies. Dispatch never performs an unchecked
   name lookup.

The former open question about a served full `Shed` mailbox is closed in the
current tree: it renders `RemoteError::MailboxFull`, not `TimedOut`
(`keylink/src/turns.rs:5418`, `keylink/tests/singleton.rs:994`).

None of the remaining items changes a generated `Request` or `Response` schema.
