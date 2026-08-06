# Plan: keylink correctness programme

Executes `docs/goals/2026-08-06-correctness-programme.md`. One integration branch
`correctness-programme`, atomic commits, one PR to `main`.

Read the goal first. This file records only what the goal left to the plan: the
mechanism for each item, the seams it touches, and the decisions the goal
explicitly deferred ("the plan must decide this explicitly, not by default").

## Verified before planning, not assumed

1. **`n0_future::time::Instant` is `tokio::time::Instant` on native.**
   `n0-future-0.3.2/src/time.rs:6-10` re-exports tokio's `Instant`, `sleep`,
   `timeout` for `not(wasm_browser)`. So `tokio::time::pause()` controls it and
   the TTL test needs no sleep. On wasm it is `web_time::Instant`, which is why
   `std::time::Instant` stays banned.
2. **A generic associated const defers its assert to instantiation.** Probed with
   a scratch crate: `impl<A> ServableAsActor<A> { const CHECK: () = assert!(..) }`
   compiles when merely defined and fails with `E0080` naming the concrete `A`
   only when `ServableAsActor::<Concrete>::CHECK` is reached. This is what makes
   item 6 a compile error scoped to *serving* rather than to *declaring*.

## Deviations from the goal, decided here

The goal contradicts itself in four places. Each is resolved below and repeated
in the final report.

**D1 — "the retry receives the real reply of a timed-out turn".**
Success criteria ask for `a_timed_out_actor_turn_records_its_real_reply`. Scope
item 1 says the opposite in bold: *"Explicitly not promised: the retry does not
receive the real reply of a timed-out turn."* Scope wins; the criterion is
restated as `a_timed_out_actor_turn_does_not_record_the_refusal`, which is the
defect actually being fixed.

The goal's stated reason for coupling them — that `attach` needs the same
machinery — **is wrong**, and this plan records why. `attach` waits on a
`Notify` that the *first* caller's guard fires when it settles; the guard never
leaves `serve_one`. Delivering a timed-out turn's real reply genuinely does need
the guard inside `Mail`, settled by the turn. They are not the same machinery,
so "build both or neither" does not bind.

**D2 — item 6's refusal is a compile error, not a `ConfigError`.**
Scope item 6 says compile time, "**not** a runtime `ConfigError`, and **no
opt-out**", and argues it at length. The success criterion says "refused at
startup … returning a `ConfigError`". Scope wins (and is now proven buildable —
see the probe above). Proof moves from a runtime test to a `compile_fail`
doctest, which needs no new dependency.

**D3 — no per-service bounds.** Item 9 says "Bounds become configurable per
service"; the Public interface section says "NO per-service bounds type … The
two crate-level constants stay as they are", with the `keylink::Retention`
collision as the reason. The Public interface section wins: TTL is a crate
constant, and `Ledger::with_limits` stays private and gains a TTL parameter for
tests.

**D4 — `KeyState` has six variants, not five.** The goal lists five and the
ledger has five settled states plus vacant. `Unrecordable` is not `Forgotten`:
one is permanent and knowable up front, the other is an eviction. Merging them
would make `key_state` lie about why. Six variants, exhaustive, no
`#[non_exhaustive]` — the goal's argument for exhaustiveness is unaffected by
the count.

**D5 — item 10 is descoped, under the goal's own permission.** See wave B below.

## Wave A — idempotency correctness

Moves the ALPN once, deliberately. `tests/protocol.rs::the_contract_hash_has_not_moved`
is updated in the wave-A commit with the reason.

### A1 · a layer's refusal never settles a key

Mechanism: a three-state progress marker shared through `Context`.

```rust
// middleware.rs, pub(crate) except for the one doc(hidden) accessor
enum Reached { NotDispatched, Dispatched, Completed }
struct Progress(Arc<AtomicU8>);   // Arc, because Context is cloned by value down the chain
```

`Context` gains `progress: Progress`. `serve_one` builds the `Context`, keeps a
clone of the marker, and reads it after the chain returns:

| marker after the chain | what `serve_one` does |
|---|---|
| `NotDispatched` | `guard.release()` — nothing ran, nothing is owed |
| `Dispatched` | drop the guard → `Interrupted` |
| `Completed` | `guard.record(frame, carries_body)` as today |

**The marking lives in generated dispatch, not in a wrapper around `base`.**
The goal proposed a wrapper installed in `ServiceBuilder::build`. That is
imprecise on the actor path: the wrapper marks `Dispatched` when the *dispatch
closure* is entered, which happens before the message is even in the mailbox, so
a `QueueTimeout` refusal of a message that never ran would settle `Interrupted`.
Generated code can be exact:

- **Handler dispatch** marks `Dispatched` immediately before invoking the
  handler and `Completed` immediately after.
- **Actor dispatch** moves a clone of the marker *into the turn closure*, which
  runs on the actor's task. It marks `Dispatched` as the turn begins and
  `Completed` as it ends. So `Dispatched` means exactly "the turn started", and
  a `Timeout` above it that drops the wait still leaves `Interrupted` — correct,
  because the turn is running and may mutate.

`ServiceBuilder::new` is `#[doc(hidden)]` and documented as generated-code-only;
its doc gains the sentence that the marker is the dispatch's responsibility.

`Guard::release()` is new: settle without recording, returning the slot to
vacant. Byte-for-byte replay is untouched — nothing decodes or re-encodes.

`Outcome::Err`'s doc comment stops saying "by the handler or by a layer above
it" as though the two were interchangeable.

### A5 · running claims are bounded

`Inner` gains `running: usize`; `Bucket` gains `running: usize`. Two caps, both
`pub` constants:

- `MAX_RUNNING_PER_PEER: usize = 128` — above QUIC's 100 concurrent bidi
  streams, so one well-behaved connection never reaches it, and a peer opening
  many connections does.
- `MAX_RUNNING: usize = 4096` — the total, because `Allowlist::Anyone` makes the
  peer count unbounded.

Over either cap, `claim` refuses with a new
`IdempotencyRefusal::TooManyRunning { limit: u32 }`. Not folded into
`max_records`: `evict` returns early when no bucket has settled keys
(`idempotency.rs:284-286`), so a bound counting running claims would stop being
enforced rather than fire.

### A9 · a TTL as a second bound

Slots move into `struct Entry { slot: Slot, at: Instant }` on
`n0_future::time::Instant`, copying `actor.rs`'s `Eviction::Idle`. `RECORD_TTL`
is a crate constant. A settled entry past its TTL is downgraded to a tombstone —
lazily on `claim`, and swept in `evict` so memory is reclaimed without traffic.
`Running` entries have no TTL and are never evicted.

### A7 · eviction leaves a tombstone

Eviction downgrades rather than deletes, so a forgotten key is refused rather
than silently re-executed. `IdempotencyRefusal::Forgotten` is new.

Two-pool eviction, because a tombstone that stayed in the record count would
make `evict` loop forever:

```
Bucket { slots: HashMap<Key, Entry>, held: VecDeque<Key>, spent: VecDeque<Key>, running: usize }
Inner  { peers, held: usize, bytes: usize, spent: usize, running: usize }
```

- `held` — entries with a replayable reply (`Done`). Bounded by `MAX_RECORDS`
  and `MAX_RECORDED_BYTES`. Over either, the oldest of the largest holder is
  downgraded to `Forgotten` (bytes freed, key kept).
- `spent` — `Interrupted`, `Unrecordable`, `Forgotten`: ran, hold no bytes.
  Bounded by a new `MAX_TOMBSTONES: usize = 16_384`, because a key string is
  bounded by `MAX_KEY_LEN`. Over it, the oldest of the largest holder is dropped
  outright and the key becomes `Fresh` again.

Largest-holder-first survives both pools, so a flooding peer still evicts itself.

### A3 · `key_state`, and `attach`

**The wire grows a request direction.** A ledger query is keylink's own
question, not the user's, so it does not touch the user's `Request` enum, does
not spend a `Handler` method, and does not reach the layer chain:

```rust
pub enum WireRequest<Req> { Call(Req), KeyState(IdempotencyKey) }
pub enum WireReply<Resp>  { Call(Outcome<Resp>), KeyState(KeyState) }
pub type  WireResponse<Resp> = WireReply<Resp>;   // the alias `Contract` already names
```

`Envelope<WireRequest<Req>>` up, `Envelope<WireReply<Resp>>` down. `Contract`
names the aliases, so the hashed contract still equals the wire type
(RFC-04 §14.6). Peer scoping is free: the ledger is keyed by the proven peer of
the connection the query arrived on, so a peer cannot name another.

`KeyState` is answered in `serve_one` before the chain, because it is a ledger
read with no handler and no body.

**`attach`.** `Envelope.idempotency` becomes `Option<Idempotency>`:

```rust
pub struct Idempotency { key: IdempotencyKey, attach_for_millis: Option<u64> }
impl Idempotency { fn new(key); fn attaching_for(key, budget); fn key(); fn attach_budget(); }
impl From<IdempotencyKey> for Idempotency {}          // every call site keeps compiling
impl Client { fn with_idempotency(&self, impl Into<Idempotency>) -> Self }
```

`Ledger` gains `claim_or_wait` returning `Running(Arc<Notify>)` where `claim`
returns `Refused(InFlight)`; `claim` is `claim_or_wait` with that arm mapped.
`Slot::Running` carries the `Notify`; `settle` calls `notify_waiters()` before
replacing the slot.

The dropped-wakeup hazard RFC-04 §3.3 named is real — `notify_waiters()` wakes
only *currently registered* waiters — and is answered by registering before the
re-check:

```rust
let notified = notify.notified();
tokio::pin!(notified);
notified.as_mut().enable();            // registered HERE
// re-claim under the lock; if it settled, answer without waiting
// only then await `notified`, bounded by n0_future::time::timeout
```

One wait, then one re-claim; still `Running` after that is `Refused(InFlight)`,
so the loop is bounded by construction. `MAX_ATTACH: Duration` caps a peer that
sends an absurd budget. Only the `Running` arm waits: `Interrupted`,
`Unrecordable` and `Forgotten` refuse immediately.

### RFC-05

Written first in the wave-A commit. Covers: the provenance defect and its fix,
why "idempotency as the innermost layer" is wrong (`rpc.rs:127-132` writes
recorded `Bytes` straight to the stream), the `attach` reversal *with* §3.3's
objection upheld and answered by `enable()`, the four "considered and declined"
items, D1's correction of the goal's own coupling claim, the `NoSuchActor`
replay wart, and the generated-client-trait follow-up.

## Wave B — caller identity and actor safety

### B4 · `cx` first on every generated method

```rust
trait Handler { async fn f(&self,     cx: &Context,     a: A) -> R; }
trait Actor   { async fn f(&mut self, cx: Caller<'_>,   a: A) -> R; }
pub enum Caller<'a> { Local, Peer(&'a Context) }
```

`Actor::caller` is deleted, and with it the reserved name and the
`__keylink_local_method!` hazard its own doc comment describes. `send` keeps
`body: BodyReader` **last**: `fn f(&self, cx: &Context, a: A, body: BodyReader)`.
`Next`/`Dispatch` take `Context` by value, so the generated closure moves it into
the async block and borrows inside — expected, not a defect.

14 files: `demo/src/main.rs`, `keylink/examples/durable_ledger.rs`,
`keylink/src/macros.rs`, eleven under `keylink/tests/`.

### B6 · a `send`-declaring service cannot be served as an actor

`DECLARES_SEND` folds like `DECLARES_BLOBS`. Per-service generated:

```rust
struct ServableAsActor<A>(PhantomData<A>);
impl<A> ServableAsActor<A> {
    const CHECK: () = assert!(!DECLARES_SEND, "…RFC-04 §10.4…");
}
```

`Handle::service` and `Handle::serve` open with `let () = ServableAsActor::<A>::CHECK;`.
Generic in `A`, so it fires only where the actor is actually served — proven by
the probe above. Scoped to `send` alone: `recv` drains after the turn and `blob`
streams later. Proof is a `compile_fail` doctest, no new dependency.

### B8 · `QueueTimeout`

`Context` gains `pub(crate) queue_deadline: Option<Instant>`, set by the
`QueueTimeout` layer before it calls `next`. The generated actor dispatch reads
it and uses a new `LocalActor::ask_catching_before(deadline, turn)`, which

1. bounds the wait for a mailbox permit, and
2. re-checks the deadline **on the actor's task, before the turn begins**.

so it can never refuse a turn that has started. Returns
`Queued<T> { Ran(Result<T, Panic>), Expired }`; `Expired` becomes
`RemoteError::TimedOut` and leaves the progress marker at `NotDispatched`, so
the key stays claimable — the whole point.

On a `Handler`-served builder `QueueTimeout` refuses nothing, and that is not a
lie: a handler never queues, so no message is ever "still queued". Documented in
those words.

`Timeout` is **kept** — on a `Handler` it genuinely cancels. What stops it being
layered on an actor path: **nothing, by decision**, because after A1 it is no
longer unsafe there. It refuses the caller, the turn runs on, and the key is
settled `Interrupted` rather than as the refusal. That is honest and is what the
rewritten `macros.rs:626-649` will say. A marker bound was considered and
rejected: it would split `ServiceBuilder` in two for a layer that is now merely
ineffective rather than wrong.

`macros.rs:626-649` is rewritten in the same commit — wave A makes its
idempotency paragraph false while leaving its load-shedding paragraph true, and
a half-stale warning is worse than none.

### B10 · an abandoned turn — **descoped**

The goal permits this outcome in terms: *"Either put it somewhere only an actor
turn can reach, or do not build it."* Three placements exist and all three cost
more than the item is worth:

- On `Context`: `Context` is shared with `Handler`, where nothing wires the
  signal, so the method would always answer "no" for every handler. The goal
  rejects this itself.
- Inside `Caller`: it stops being `Local | Peer(&Context)` — the exact enum this
  wave exists to introduce — and becomes a struct wrapping an `Origin` enum plus
  a type-erased `&dyn` closed-signal. It would distort the wave's centrepiece.
- A second parameter on every actor method: every user pays, in every signature,
  for a signal few need.

Recorded in the RFC-04 append with this reasoning, so it is answered rather than
forgotten. Success criterion `an_abandoned_turn_observes_its_caller_left` is
reported as descoped, not as passing.

## Wave C — server-initiated messages

RFC-04 §4.4 Design B, no wire change: the reverse contract is an ordinary
`service!` block with its own derived ALPN, and the dialling side runs a
`Server` for it.

```rust
pub struct Subscribers<C: Clone> { … }
impl<C: Clone + Send + 'static> Subscribers<C> {
    pub fn new() -> Self;
    pub fn insert(&self, peer: EndpointId, client: C, departure: Watch);
    pub fn get(&self, peer: EndpointId) -> Option<C>;
    pub fn snapshot(&self) -> Vec<(EndpointId, C)>;
    pub fn len(&self) -> usize;
    pub fn is_empty(&self) -> bool;
}
```

`insert` spawns a task that awaits the `Watch` and removes the entry — which is
the whole reason this is a type rather than a `HashMap` at a call site. It
evicts on `Departure` rather than `Connection::closed`, because `closed()`
borrows a strong handle and a diagnostic that pins its subject open is a leak
wearing a diagnostic's clothes (`watch.rs:35-42`). That comparison is written
into the module doc in the persist crate's style.

`snapshot()` is named for what it is — an eager `Vec`, not a lazy `iter()` — and
`is_empty` ships beside `len` because `clippy::len_without_is_empty` is inside
`clippy::all`, denied at `Cargo.toml:34`.

Requires `RpcClient` and the generated `Client` to be `Clone` (a `Connection` is
a handle; `derive` would wrongly demand `Req: Clone`, so both are hand-written).

Wave D instantiates the same type at `C = Revocation`. One registry, two uses.

## Wave D — runtime admission

```rust
pub enum Admittance { Admitted, Refused, Unavailable(String) }
pub trait Admission: Send + Sync + 'static {
    fn admit(&self, peer: &ProvenPeer) -> Admittance;
    /// Called once per admitted connection, so a policy that can revoke has
    /// something to close. The default drops it; a static policy never revokes.
    fn admitted(&self, _peer: &AdmittedPeer, _live: Revocation) {}
}
pub trait Revocable: Admission {
    #[must_use = "a revocation that closed nothing is usually a mistake"]
    fn replace(&self, next: Allowlist) -> Vec<EndpointId>;
}
pub struct DynamicAdmission { … }   // the one adapter; no Default
```

`Admitted` carries no value: `AdmittedPeer`'s only constructor is
`Allowlist::admit` and that privacy is the invariant, so a trait returning one
could not be implemented outside the crate. keylink mints it at the single call
site in `RpcService::accept`. `Unavailable` exists because a `bool` cannot say
"the policy could not be consulted", and an operator who cannot tell a dead
authorizer from an unlisted peer reaches for `Allowlist::anyone()` during an
incident.

Held as `Arc<dyn Admission>` — dyn-compatible on purpose, unlike
`keylink-persist`'s RPITIT `Snapshots`, because `admit` runs once per connection
and does no I/O, and a generic `A` would leak into every `service!` user's type
errors through the generated `service(&self, allow)`.

`replace` **swaps the snapshot before it closes**: closing first leaves a window
in which a revoked peer reconnects and is admitted by the old list. That
ordering is the correctness property and is tested.

`ServiceBuilder` keeps `service(handler, allow: Allowlist)` and gains
`.admission(Arc<dyn Admission>)`. `Allowlist` becomes the canonical
`Admission` impl. `representability.rs` gains probes for "no blanket permissive
impl" and "`DynamicAdmission` has no `Default`".

What this does not promise, in the persist crate's style: nothing about a peer
already inside a stream; nothing across nodes; no audit trail. One connection
per peer is remembered, so a peer holding several has its most recent closed —
stated rather than discovered.

## Sequencing

A → B → C → D, atomic commits on `correctness-programme`, pushed as waves
complete. Only D depends on C. Review gates: `brooks-review` on A and D
(behaviour- and security-sensitive), the invariant-compliance wave on the whole
diff before the PR. No questions mid-run; assumptions annotated inline and
surfaced in the final report.
