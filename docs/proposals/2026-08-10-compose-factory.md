# Wave 4 — a factory over N handles, and the bookkeeping keylink already owns

Rebuild of the keylink × ractor proposal, Rev. A (2026-08-08, written against
`main @ 99ce53f`). Re-derived in full against `d54f302`.

Status: draft for review. Every recommendation is a proposal, not a decision.

---

## 1. What this is

Rev. A compared keylink against ractor, found one genuine hole, and proposed
twelve changes. It then cut itself to three and called the remaining nine a
backlog with no named caller.

This document keeps the finding and rebuilds the three. It does not re-run the
comparison. Rev. A's thesis survives re-reading and is the reason the design is
small:

> ractor's mailbox is unbounded. Most of its factory — queue depth, discard
> reasons, TTL sweeps, a dynamic limit reaching workers on a 10-second ping —
> exists to rebuild the bound its mailbox threw away. keylink has permits.

Re-verified this pass, directly rather than through an explorer:

| Claim | Where | Result |
|---|---|---|
| ractor's mailbox is unbounded | `ractor/src/concurrency.rs:48` — `tokio::sync::mpsc::unbounded_channel()` | holds |
| ractor's cluster discards the deserializer's error | `ractor_cluster/src/node/node_session.rs` — `let _ = actor.send_serialized(..)` at `:423`, `:440`, `:447`, `:496` | holds, at **four** sites, not the one Rev. A's citation implies |

The hole is unchanged: **`hash(key) % N` onto a fixed pool of workers has no
keylink spelling.** `System` gives a million keys a million actors. `Pool`
gives round-robin and loses the affinity. Nothing gives eight workers, each
holding one expensive thing, with the same tenant landing on the same one.

Scope is **R1, R2, R3**. Those are the only items with a requester, which Rev. A
says itself. §12 records the rest with its reasoning intact and schedules none of
it.

---

## 2. What changed under Rev. A

Three waves merged between `99ce53f` and `d54f302`. Each one moves this design.

### 2.1 The failure vocabulary landed, and `MailboxClosed` is gone

`Withheld` (`observe.rs:966`) is now the crate's one list of *why a call produced
no value*, carried by `TurnLost::Turn`, `Unanswered::Turn` and
`RemoteError::Withheld`. Five `Failed(String)` wire prefixes are deleted.

**Rev. A's entire Layer 0 signature does not compile.** It returns
`Result<Permit<'_, A>, MailboxClosed>` in three places and says Cut 2's error is
"`MailboxClosed` and nothing else". `MailboxClosed` is not a type at `d54f302`:

> It replaced a unit struct called `MailboxClosed`, and the name was the fault
> — `keylink/src/turns.rs:3161`

`MailboxClosed` became `Undelivered` at `4e0fcca`, and `Undelivered` became
`Unanswered` (`turns.rs:3172`). §3.5 rules on what replaces it.

### 2.2 `Calls` landed

`macros.rs:3297` generates a per-service `Calls` trait, implemented for four
surfaces — `Handle<A>` , `ActorRef<'_>`, `Client` and `IdempotentClient`
(`macros.rs:3346` and the three impls above it). It carries `rpc` only, and the
four kinds it drops each argued for it in writing. §3.3 rules on whether a
factory is expressible through it.

### 2.3 `__status` landed, and both depth knobs exist

`Serving { busy: bool, buffered: u64 }` (`observe.rs:774`) is answered by
`LocalActor::serving` (`turns.rs:4392`) and `System::serving`
(`registry.rs:1827`), and a generated `__status` answers it to a peer
**inside the `Dispatch`, without enqueueing** (`turns.rs:4903`,
`actor/remote.rs:611`).

Both mailbox depths are now knobs, which is what Rev. A's sequencing clause was
waiting for:

| Path | Door | Where |
|---|---|---|
| local | `LocalActor::spawn_deep(.., depth: NonZeroUsize)` → `mpsc::channel(depth.get())` | `turns.rs:3597`, channel at `:3614` |
| local, generated | `actor_deep`, `supervised_deep` | `macros.rs:2016`, `macros.rs:2101` |
| keyed | `SystemBuilder::mailbox_depth(NonZeroUsize)` → `Registry::entry` makes the channel | `registry.rs:2198`, channel at `registry.rs:948` |

**The sequencing clause is discharged on both halves.** Rev. A's owner decision
of 2026-08-09 said "do not start before mailbox depth above one is normal on
every path". It is. `DEFAULT_MAILBOX_DEPTH` is still `1` (`registry.rs:82`), so
depth is opt-in — but it is opt-in on both paths, which is the condition the
clause named.

### 2.4 `compose::Pool` is intact, and it makes three refusals

`pool.rs` is 224 lines. Its `# What a pool is not` list has **three** bullets.

Predicate and command:

```
awk '/# What a pool is not/,/^use /' keylink/src/compose/pool.rs \
  | grep -c '^//! - \*\*'
```

→ `3`, being *Not a way to make one actor concurrent* (`:53`), *Not a
supervisor* (`:56`), *Not keyed* (`:74`).

**Rev. A's Table 3 enumerates the wrong three.** It lists *Not a supervisor*,
*Not keyed* and *No queue*, silently dropping *Not a way to make one actor
concurrent* and promoting a section heading to a bullet: "no queue" is the
`# Backpressure: the pool has none, and that is the point` section
(`pool.rs:19-30`), not a refusal in that list. Re-adjudicated against what is
actually there:

| Refusal | Where | Verdict |
|---|---|---|
| Not a way to make one actor concurrent | `pool.rs:53-55` | **Upheld.** A factory is N states too. It does not make one state concurrent and must not read as if it does. |
| Not a supervisor | `pool.rs:56-73` | **Upheld.** Members are built with the generated `supervised()`; a supervised member's mailbox outlives its incarnations, so routing keeps working across a restart. §9 refuses factory-owned restart on this ground. |
| Not keyed | `pool.rs:74-76` | **Overturned, and the overturn still follows.** It says the wanted thing is `System` or `Names`. Both give a key its own actor. Neither gives `hash(key) % N` onto a fixed pool. The two senses of "keyed" are different and only one is expressible. |
| ("no queue", `pool.rs:19-30`) | not a refusal in the list | **Upheld, and it is now load-bearing.** §4 deletes the queued mode, so this argument governs the whole design rather than being an exception to it. |

---

## 3. Rulings

### 3.1 Routing is one closed enum

Five booleans and a closure can express *RoundRobin and Sticky at once*, which is
not a state that exists. One enum, and the hasher is a field on the keyed
variants only:

```rust
/// How a factory chooses which worker handles a dispatch.
#[non_exhaustive]
pub enum Routing<K> {
    /// Cursor. What `Pool::pick` does today (`pool.rs:175`).
    RoundRobin,
    /// Race a permit across every worker; the first slot to open wins.
    FirstAvailable,
    /// Fewest messages outstanding, from `is_busy` + `buffered`.
    LeastLoaded(Sample),
    /// `hash(key) % N`. Pass a hasher, or take the default.
    ByKey(Shard<K>),
}

/// How many workers to read before choosing.
#[non_exhaustive]
pub enum Sample {
    /// Read all N. Exact, O(N) relaxed loads.
    All,
    /// Two at random, take the better. O(1), and it is what stops many
    /// concurrent dispatchers piling onto the same "least loaded" worker.
    TwoOfN,
}
```

Four variants, not five. §3.2 refuses the fifth.

**`#[non_exhaustive]`, and the crate argues both ways — the difference is who
matches.** `compose::Overflow` is deliberately *not* sealed:

> Not `#[non_exhaustive]`: a caller constructs one and a caller matches one, so
> sealing it would cost every call site a `_ =>` arm for a third policy that
> this module argues must not exist.
> — `compose/topic.rs:103-105`

A caller constructs a `Routing` and never matches one; the factory matches it,
inside this crate. `#[non_exhaustive]` on an enum restricts matching and not
construction, so it costs a caller nothing here and makes a fifth variant
additive. That is the opposite conclusion from `Overflow` and it comes from the
same rule.

**No `Router` trait.** A trait puts a third type parameter on `Factory` and
cascades it through every construction site, for extensibility nobody has asked
for. Adding a variant later is non-breaking on an enum this crate owns.

### 3.2 `Sticky` is refused from this cut

Rev. A's `Sticky` is *"a worker already holding this key keeps it, even after a
resize"*. It needs an in-flight key→worker map with counts — the only shared
mutable state in the design — and it weakens the property §1 gets free: a
composition is a plain value over N handles, so there is no routing state to
race.

The instruction was: write the serialization story before it is built, or refuse
the mode. **It is refused, because at a fixed worker count `Sticky` and `ByKey`
are the same function.**

- The factory has no resize. `Pool::size()` returns `NonZeroUsize` and
  `pool.rs:197-200` says a pool "is built from one and holds that many forever".
  Live reconfiguration is Rev. A's R12, which has no caller and is not scheduled
  (§12).
- A worker *restart* is not a resize. A supervised member's mailbox outlives its
  incarnations (`pool.rs:63-65`), so membership does not change and `hash(key) %
  N` is unmoved.
- At fixed N, `hash(key) % N` is total, deterministic and stable. `Sticky` adds
  a lock, a map and an eviction question, and buys a behaviour difference that
  cannot occur.

So the mode is bought back by whatever ships a resize, and the serialization
story is written then, against the resize that makes it necessary. §9 records
the refusal with the exact condition that re-opens it.

**This departs from the brief, which said to hold five modes.** It is the same
brief's ponytail rule and its "or refuse the mode" clause applied to a mode whose
only distinguishing input does not exist.

### 3.3 The factory does not implement `Calls`

Refused, on three grounds, each from `macros.rs:3240-3300`:

1. **No `Calls` method has a key parameter.** `Calls` carries the service's own
   `rpc` signatures. `Factory::dispatch(key, |w| w.method(..))` takes a key that
   no generated signature has room for. `ByKey` is unwritable through it.
2. **The routing mode is a runtime field, not a type parameter.** A
   `Factory<A>: Calls` impl would be correct for `RoundRobin`, `FirstAvailable`
   and `LeastLoaded` and wrong for `ByKey`. One type cannot implement a trait for
   three of its own enum's variants — the impl would have to mis-route or panic
   on the fourth, which is exactly the illegal state §3.1 removes.
3. **`dyn Calls` does not exist** (`E0038`, `macros.rs:3285`), so implementing it
   buys no erasure to pay for the other two.

A caller that wants a surface-agnostic function keeps writing it against
`Calls` and passes one worker's `Handle`. A caller that wants routing calls
`dispatch`.

### 3.4 `LeastLoaded`'s deferral is overturned

Rev. A deferred `LeastLoaded` on one sentence, and the sentence is now false.

`LocalActor::buffered()` (`turns.rs:4367`) still documents itself at
`turns.rs:4346` as:

> Zero or one at the local mailbox depth, which is a constant and not a knob

**The depth is a knob** (§2.3): `spawn_deep` takes a `NonZeroUsize` and builds
`mpsc::channel(depth.get())`, and `buffered()` computes
`max_capacity() - capacity()`, which ranges over that depth. `Serving::buffered`
(`observe.rs:788`) carries no such claim and gives a peer the same gradient.

So `LeastLoaded` ships in the cut. It is the only routing mode needing **no new
primitive** — `is_busy` (`turns.rs:4340`) and `buffered` (`turns.rs:4367`) are
both public and both `#[must_use]` today, and `Serving` folds them
(`turns.rs:4392`).

**`turns.rs:4346` is a live doc defect and this proposal fixes it in commit 1**,
whether or not the rest is accepted. Its sibling paragraph already states the
correct caveat — the local channel also carries a `read` and the stop request, so
the number is a predicate across the two sides and not a count.

### 3.5 The error type is `NotQueued`

`MailboxClosed` is gone (§2.1). The correct replacement is not a new type. It is
the enum every door that only asks to be queued already answers:

```rust
pub enum NotQueued { Closed, Reentrant }   // turns.rs:3314
```

`reserve` sits at exactly the point `notify` does — `turns.rs:4190` says
`NotQueued::Reentrant` is answered "before the permit is touched" — so reserving
from inside the target actor's own turn is the same deadlock, refused the same
way. An error type without `Reentrant` would make `reserve` the one door that
deadlocks where its neighbours refuse.

```rust
impl<A> Handle<A> {
    pub async fn reserve(&self) -> Result<Permit<'_, A>, NotQueued>;
    pub fn try_reserve(&self) -> Result<Option<Permit<'_, A>>, NotQueued>;
    pub async fn reserve_owned(self) -> Result<OwnedPermit<A>, NotQueued>;
}
```

`Ok(None)` from `try_reserve` means *full right now*. That differs from tokio,
whose `try_reserve` returns `Result<Permit, TrySendError<()>>`
(`bounded.rs:1356`) and folds "full" and "closed" into one error. Splitting them
is the crate's own habit: full is a schedule and closed is a fate.

**`reserve_owned` ships, on a reason that is not the one Rev. A gives.** Rev. A
justifies it by the queued mode, which §4 deletes. The reason that survives is
narrower and holds inside `direct`: `Permit<'_, A>` borrows the handle, so it
cannot cross `tokio::spawn` and cannot be stored. `Handle<A>` is `Clone`, so
`reserve_owned` costs one refcount, and tokio already ships it
(`bounded.rs:1265`, verified at the pinned 1.53.1). Adding it later regenerates
the macro output a second time.

### 3.6 Cut 3's served refusal is a new `RemoteError` variant

A served call into a full shed mailbox must render as an explicit overload and
never as `TimedOut` — a retry policy reads those two differently: a timeout
invites the same retry, an overload asks for backoff.

**Not a `Withheld` variant.** `Withheld` is why a call produced no value *on
every path that has one* — a turn that ran and lost. A shed happens before the
turn. The crate has already made this exact ruling for the neighbouring case:

> Outside `Withheld` because there is no turn: the refusal happens before the
> message reaches the mailbox, so nothing ran and nothing was withheld.
> — `observe.rs:885-886`, on `Reentrant`

**Not `RemoteError::AtCapacity` either, which already exists.**
`AtCapacity { capacity }` (`reply.rs:168`) means the *registry* is full — too
many actors — and it has a producer (`actor/remote.rs:377`, from
`RouteError::AtCapacity`, tested at `remote.rs:824-827`). Folding "this actor's
mailbox is full" into it conflates two different facts about two different
bounds.

So: **a new variant appended to `RemoteError`**, after `NotAnActor`
(`reply.rs:234`). `RemoteError` is `#[non_exhaustive]` (`reply.rs:73`), derives
`postcard_schema::Schema`, and `reply.rs:55` says it is in every service's
contract — so a new variant moves **both** ALPN pins.

**Append, never insert**, and the enum's own doc says why (`reply.rs:58-68`): an
append and an insert move the hash equally, so the handshake refuses a drifted
peer either way; what separates them is that an insert renumbers every variant
after it, so the wire meaning of codes both sides already agreed on changes
underneath them and the failure is a misdecode rather than a refusal.

**The local half costs no ALPN.** `NotQueued` derives no `Serialize` and no
`Schema` (`turns.rs:3313`), so it never reaches a contract. It gains a third
variant for "full", and its own doc anticipated exactly that:

> the two decisions above are the whole of what `send` does before the message is
> in the channel, so a third variant is a change to that function rather than a
> variant added beside it — `turns.rs:3310-3312`

Shed *is* a change to that function. That paragraph is a description of the
work, not an objection to it, and it must be amended in the same commit.

### 3.7 `FirstAvailable` is sound and is not FIFO

`reserve` is **not** documented cancel-safe. Verified against the pinned
tokio 1.53.1 (`Cargo.lock`), at
`src/sync/mpsc/bounded.rs:1086-1091` — the `# Cancel safety` heading is at
`:1087` and the sentence spans `:1089-1091`:

> This channel uses a queue to ensure that calls to `send` and `reserve`
> complete in the order they were requested.  Cancelling a call to `reserve`
> makes you lose your place in the queue.

Rev. A's citation is exact. What tokio *does* guarantee is capacity
conservation: `Drop for Acquire` (`src/sync/batch_semaphore.rs:686-708`) removes
the node from the wait list and returns any already-assigned permits via
`add_permits_locked`. A dropped racer leaks nothing.

So `FirstAvailable` is sound and **not FIFO across dispatchers**. Each dispatch
enqueues N waiters and cancels N−1; every cancellation takes the wait-list lock,
and a freed slot can be assigned to a waiter that has already left. That belongs
in `FirstAvailable`'s rustdoc, not in a footnote.

**The test measures the property at risk, not the one tokio promises.** A
"race 32 reserves 10,000 times, assert the pool never loses capacity" test
asserts tokio's own guarantee. The test to write is: saturate all N workers, run
M concurrent dispatchers, and bound the spread of per-dispatcher wait time. §7
names it and marks it as one to write.

---

## 4. What ponytail deleted before it was written

The thesis is that ractor's factory rebuilds a bound its mailbox threw away. A
design that re-adds bookkeeping a semaphore already provides fails its own
argument. Deleted from Rev. A, with the reason:

| Deleted | Rev. A had it as | Why it goes |
|---|---|---|
| `Factory::queued` and the whole second builder type | Layer 1, mode 2 | A second admission point in front of N mailboxes that are already admission points. `pool.rs:26-30` argues it: work in a pool's own queue is work no member has admitted, so the caller is told "accepted" by something that cannot run it. The permit is the bound. |
| `depth(256)`, `WhenFull::{DropOldest, DropNewest}` | Layer 1 | Queue depth is the thing a permit already is. |
| `.ttl(Duration)` and its clock | Layer 1 | A TTL on a queue that does not exist. `QueueTimeout` (`middleware.rs:538`) is the caller's deadline and already ships. |
| `.rate(Rate::new(..))`, leaky bucket, ticker | Layer 1 / R11 | Every ticker is a task per factory. §9 refuses the periodic sweep on the same ground. |
| `Refused { Full, Expired, Draining, RateLimited, Gone }` | Layer 1 | Four of five variants describe states only the queue can produce. |
| `Rejected { why, job }` | Layer 1 | Nothing sheds in `direct`, so nothing hands a payload back. |
| `.on_refused(closure)` | Layer 1 | No refusal to report. |
| `report_stuck_after(..)` | Layer 4 / R6 | A clock, a ticker and a task per factory, for a signal `LocalActor` does not carry — Rev. A concedes it is invention on the local path. Backlog, §12. |
| `Sticky(Shard<K>)` and its in-flight key map | Layer 2 | §3.2. The only shared mutable state in the design, bought for a resize that does not exist. |

What is left is three things: a permit made addressable, a value holding N
handles and a routing rule, and one mailbox policy. There is no background task,
no second queue, no clock and no discard policy anywhere in this proposal.

---

## 5. The shape

### 5.1 Cut 1 — the permit, made addressable

`pool.rs:39-43` names this as its own missing piece:

> a `try_pick` across N mailboxes needs a `try_reserve` the handle does not have.
> That is a real gap and it is named here rather than hidden behind a claim of
> fairness

```rust
impl<A> Handle<A> {
    /// Wait for a mailbox slot. Backpressure, made addressable.
    pub async fn reserve(&self) -> Result<Permit<'_, A>, NotQueued>;

    /// The same, without waiting. `Ok(None)` = this actor is full right now.
    pub fn try_reserve(&self) -> Result<Option<Permit<'_, A>>, NotQueued>;

    /// Owned, so a permit can be stored or cross `tokio::spawn`.
    /// `Handle<A>` is `Clone`, so this costs one refcount.
    pub async fn reserve_owned(self) -> Result<OwnedPermit<A>, NotQueued>;
}

/// A `Permit` carries the same generated methods. Sending cannot wait again.
#[must_use = "a dropped permit returns the slot and sends nothing; \
              the caller is never told"]
impl Permit<'_, Warehouse> {
    pub async fn get_item(self, id: u64) -> Result<Option<Item>, Unanswered>;
}
```

### 5.2 Cut 2 — `Factory::direct`

```rust
let ingest = Factory::direct(workers(8), |m| parse::actor(Parser::new(m)))
    .routing(Routing::ByKey(Shard::by_hash()))
    .build();

let parsed = ingest.dispatch(tenant_id, |w| w.parse(blob)).await?;
// Err is `NotQueued`. `direct` never sheds and holds no queue.
```

One constructor, so there is no second builder type and no combination to make
unwritable. `Factory` is a plain value over N handles — `Clone` by `Arc`, like
`Pool` (`pool.rs:207-215`) — carrying a `Routing<K>` and nothing else mutable
except `RoundRobin`'s cursor, which is the `AtomicUsize` `pool.rs:124` already
holds.

**Ordering, stated where a caller meets it.** `ByKey` guarantees two dispatches
with the same key reach the same actor. It does **not** guarantee they are
handled in the order you sent them — a serial mailbox gives serialization, not
sequencing. Await the first reply, or carry your own sequence number.

### 5.3 Cut 3 — `Backpressure::Shed`

```rust
// The depth lives INSIDE the policy, so "unbounded with a depth" is unwritable.
pub enum Backpressure {
    Park(NonZeroUsize),      // today. The default. Unchanged.
    Shed(NonZeroUsize),      // try_send. A queue-only door -> Err(NotQueued::Full)
    Unbounded,               // you have to say it out loud
}
```

Not `Mailbox::park(..)`: `Mailbox<M>` (`registry.rs:216`) is the receiver handed
to a hand-written loop, and hanging a policy constructor on it gives it a
meaning it does not have. `Backpressure` names what the thing governs.

**`compose/mod.rs` does not need amending, and Rev. A says it does.** Rev. A
quotes `compose/mod.rs:41-44` as *"There is no third option."* The sentence is at
`compose/mod.rs:51-54` and reads:

> **There is no third option that hides.** A pattern here either applies
> backpressure to whoever is producing, or ends the consumer and says so.
> Nothing is silently buffered, and nothing is silently dropped: `Topic`'s drop
> policy ends an actor, which is the loudest signal the crate has.

The two dropped words carry the whole argument. Shed does not hide — the caller
is answered at once with a reason. The paragraph forbids *silence*, not a third
arm, and this design is inside it rather than against it. What **does** need
amending is `registry.rs:2110-2117` — "Nothing in this crate calls `try_send`,
and there is **no `unbounded` spelling — permanently**" — which Cut 3 makes false
in both halves.

**One precedent, and it is narrower than it looks.** `console.rs:470` does
`self.to.try_send(event)` and drops a diagnostic event on a full channel;
`console.rs:421-424` scopes it, and `representability.rs:1039-1041` excludes
diagnostic sinks from the mailbox rule by name. `topic.rs:300` is not a second
precedent — its `Full` arm returns `false` from a `retain`, which removes the
subscriber and ends its stream, and `compose/mod.rs:54` holds that up as the
crate's example of *not* shedding. So shed on an actor's own mailbox is new here
and argues from its own merits.

**Three guards, and Rev. A miscounts them.** Measured at `d54f302`:
`representability.rs` holds **two** probe functions —
`no_mailbox_can_be_asked_to_be_unbounded` (`:1056`), which shadows *three*
method names (`unbounded`, `unbounded_mailbox`, `without_backpressure`), and
`no_local_mailbox_can_be_asked_to_be_unbounded` (`:1092`), which pins
`LocalActor::spawn_deep` and `LocalActor::spawn_supervised_deep` to
`NonZeroUsize`. The `rg 'unbounded_channel'` check at `:1038` is prose
describing a grep that **no gate runs**. Replace the two probes rather than
delete them — probe that unbounded cannot be reached without naming it, and that
it is not the default. A deleted record is a guard the gate stops counting.

---

## 6. The work, in commits

Four commits, serially. Commit 2 is the only one that pays an ALPN.

| # | Commit | ALPN pin | anchors registry | intra-doc links |
|---|---|---|---|---|
| 1 | `reserve` / `try_reserve` / `reserve_owned` on `Handle` and `LocalActor`, plus the `turns.rs:4346` doc fix | **no** | no | few — new `[`Permit`]`, `[`NotQueued`]`, `[`OwnedPermit`]` targets in `turns.rs` and `macros.rs` |
| 2 | `Backpressure`, `NotQueued::Full`, the new `RemoteError` variant, the `registry.rs:2110-2117` amendment, the two `representability.rs` probes replaced | **BOTH** | predicate — see below | few |
| 3 | `compose::Factory`, `Routing`, `Sample`, `Shard` | **no** | no | moderate — a new module, and every link out of it |
| 4 | the guards, with their break records | **no** | **`RECORD_BLOCKS` + one row per breakable guard** | few |

### Commit 1 — nothing reaches the wire

`Permit<'_, A>` regenerates the per-method set on a second type, so macro output
grows. Rev. A sizes this as "roughly doubles — this is M, not S". **NOT
ESTABLISHED**; see §11.

The `turns.rs:4346` fix is one sentence and belongs here rather than in commit 3,
because commit 3's `LeastLoaded` rustdoc cites the corrected version.

### Commit 2 — the pin move, deliberately one commit

One new `RemoteError` variant, appended after `NotAnActor` (`reply.rs:234`).
Both pins move together because `RemoteError` is in every service's contract.

**Read both literals off a failing run. Do not compute either.**

```
cargo test -p keylink --test protocol the_contract_hash_has_not_moved
cargo test -p keylink --test protocol the_inspect_contract_hash_has_not_moved
```

Current values, which this commit replaces:

- `keylink/tests/protocol.rs:1285` — `keylink/Probe/4697a79e29347129`
- `keylink/tests/protocol.rs:1447` — `keylink/Inspect/e7579f60b94c0ea0`

**The test names carry a leading `the_`** — `the_contract_hash_has_not_moved`
(`protocol.rs:1284`) and `the_inspect_contract_hash_has_not_moved`
(`protocol.rs:1446`). A nextest filter on the shorter name runs zero tests and
exits without reporting a failure, which reads as a pass.

One coordinated redeploy. Two nodes across this commit do not misdecode — the
handshake refuses them — but they do not talk, so both sides move together.

The `registry.rs:2110-2117` paragraph and `turns.rs:3310-3312` are both amended
here, in the commit that makes them false, not before and not after.

### Commit 3 — the factory

`keylink/src/compose/factory.rs`, `mod factory;` and a `pub use` in
`compose/mod.rs` beside `pool` (`compose/mod.rs:137`). The module table at
`compose/mod.rs:9-16` gains a row, and the backpressure table at
`compose/mod.rs:40-49` gains one too — the entry for `Factory` says *the caller
waits, on the chosen member's own mailbox; the factory holds no queue at all*,
which is `Pool`'s row with one word changed.

**Every `Pool` backpressure sentence becomes conditional once Cut 3 ships.**
`pool.rs:21-24` says the wait "is the ordinary mailbox permit on that one
member". Over a `Shed` member it is not a wait. `Topic`'s `BlockPublisher`
(`topic.rs:119`) assumes park for the same reason. Each needs one sentence naming
what it does over a shed or unbounded member. That is in commit 2, with the
policy that makes it true.

### Commit 4 — the guards, and each needs a record

Every guard added here needs a break record, and **`RECORD_BLOCKS` must be read
off the failing run of `mise run anchors`, never computed.** It is `183` today
at `.mise/tasks/anchors:3436`, and it has drifted three times.

**Nothing in the gate replays a break record.** `mise run replay-anchors` is a
gate step only in its `--selftest` arm, which feeds the classifier synthetic
transcripts and runs no cargo (`mise.toml`, the `replay-anchors` entry).
`scripts/replay-fate-breaks.sh` is a script, not a task, and its own header says
"Not part of any gate: it edits the tree." So §7 lists a **manual replay** as a
verification step.

`mise run pre-commit` filters for Rust. A commit touching only
`.mise/tasks/anchors` is skipped by the hook that owns the registry, so commit 4
must carry Rust in the same commit or be gated by hand.

---

## 7. What is measured, and by what

| Claim | Proof |
|---|---|
| `reserve` answers `Reentrant` from inside the actor's own turn | `a_reserve_from_inside_a_turn_is_refused_before_the_permit` — **does not exist; commit 4 writes it.** Same door as `notify` (`turns.rs:4190`). |
| A dropped racer returns its slot | `racing_reserves_conserves_capacity` — **does not exist; commit 4 writes it.** Red-test it by leaking a permit on purpose: a capacity test that stays green under that edit asserts nothing. |
| `FirstAvailable` bounds per-dispatcher wait-time spread | `first_available_bounds_the_spread_of_dispatcher_wait` — **does not exist; commit 4 writes it.** Saturate all N workers, run M concurrent dispatchers, bound the spread. This is the property at risk; capacity is the one tokio already promises. |
| `ByKey` sends one key to one worker | `the_same_key_reaches_the_same_worker` — **does not exist; commit 4 writes it.** Assert the `Member` reported by the actor itself (`pool.rs:90-93`'s argument: the composition's claim and the actor's claim are compared rather than one taken on trust). |
| `LeastLoaded` reads a real gradient above depth 1 | `least_loaded_separates_workers_at_depth_above_one` — **does not exist; commit 4 writes it.** Build members with `actor_deep` (`macros.rs:2016`); at `DEFAULT_MAILBOX_DEPTH` the assertion is vacuous, so the depth is the control. |
| A full shed mailbox answers a peer an overload, never `TimedOut` | `a_shed_mailbox_answers_an_overload_and_not_a_timeout` — **does not exist; commit 4 writes it.** Assert the variant, not the `Display` text. |
| Both pins moved once, in commit 2 | `the_contract_hash_has_not_moved` (`protocol.rs:1284`) and `the_inspect_contract_hash_has_not_moved` (`protocol.rs:1446`) — **both exist.** Literals read off the failing run. |
| `RemoteError` gained exactly one variant | `awk 'NR>=74 && /^}/{exit} /^    [A-Z][A-Za-z]+/{n++} END{print n}' keylink/src/reply.rs` → `10` today, `11` after. |
| `Routing` has four variants and no boolean | Predicate: no `bool` field on `Routing` or `Factory`; `rg -n 'bool' keylink/src/compose/factory.rs` names none. |
| Nothing regressed | `mise run gate` — **18 steps**, counted by `awk '/^\[tasks.gate\]/,/^\]/' mise.toml \| grep -c '{ task ='`. |
| The new break records actually replay | **`scripts/replay-fate-breaks.sh`, by hand, on a clean worktree.** No gate step replays a record. |

---

## 8. Blast radius

Source: `keylink/src/turns.rs` (`LocalActor`, `NotQueued`, `buffered`'s doc),
`keylink/src/macros.rs` (generated `Handle`, `Permit`), `keylink/src/reply.rs`
(`RemoteError`), `keylink/src/actor/registry.rs` (`SystemBuilder`, the
`try_send` / `unbounded` paragraph), `keylink/src/representability.rs` (two
probes), `keylink/src/compose/mod.rs` and a new `keylink/src/compose/factory.rs`,
`keylink/src/compose/pool.rs` and `keylink/src/compose/topic.rs` (one
conditional sentence each).

Tests: `keylink/tests/protocol.rs` (both pins), `keylink/tests/compose.rs`,
`keylink/tests/tell.rs`, `keylink/tests/actors.rs`.
`keylink/tests/actor_memory.rs` is at risk from commit 1 — it asserts a *slope*,
and `SLOPE_SLACK_OVER_A_KILOBYTE` is 13 bytes over a kilobyte of `Mail`
(`actor_memory.rs:365`). A macro that regenerates the per-method set on a second
type does not obviously move the per-actor figure, but it is the budget most
likely to notice.

Registry: `.mise/tasks/anchors` (`RECORD_BLOCKS`, plus whatever `mise run
anchors` names).

Docs: `docs/rfc-04-actors.md` and `docs/rfc-05-idempotency.md` both carry pin
histories and need the append commit 2 pays for. `docs/rfc-07-the-turn-model.md`
cites `keylink/Probe/d3fe34471921164e` in three places as "unchanged and green",
which is already stale at `d54f302` and is not this proposal's to fix.

**Three known holes in `mise run docs`, all of which this work can fall into:**

1. `mise run docs` **cannot see a link to a `#[doc(hidden)]` item** — it ships as
   a dead raw href. Ten exist. `Withheld`'s own rustdoc (`observe.rs:946-949`)
   documents the pattern: it names two `#[doc(hidden)]` items in prose precisely
   because "rustdoc renders a link to a hidden item as a dead `href` and
   `-D warnings` does not see it".
2. A `#[cfg(test)] mod` is not reached, flag or no flag, and `keylink/tests/*.rs`
   is unreachable either way (`mise.toml`, the `docs` task's holes 1 and 2). A
   broken link in a break record inside a unit-test module is invisible.
3. **Adding an import reddens `rustdoc::redundant_explicit_links` on lines
   nobody touched.** Where a label and a target name different items, keep the
   target.

wasm: `mod compose` is unconditional, and nothing in this proposal reaches a
clock, `tokio::select!` or `std::time::Instant`. Four of the eighteen gate steps
are wasm, so a red one there reads as a link problem rather than a design one.

---

## 9. Refused, so nobody re-derives them

- **`Routing::Sticky` and its in-flight key map.** §3.2. The only shared mutable
  state in the design, and at a fixed worker count it is `ByKey`. **Re-opens
  exactly when a resize ships**, and the serialization story is written against
  that resize rather than in advance of it.
- **`Factory::queued`, and every knob on it.** §4. A second admission point in
  front of N mailboxes that are already admission points; `pool.rs:26-30`
  carries the argument.
- **`Factory: Calls`.** §3.3. No `Calls` method has a key, the impl would be
  correct for three variants and wrong for one, and `dyn Calls` is `E0038`.
- **A `Router` trait.** §3.1. A third type parameter cascading through every
  construction site, for extensibility nobody has asked for.
- **Reusing `RemoteError::AtCapacity` for a shed.** §3.6. It means the registry
  is full and it has a producer (`actor/remote.rs:377`). Two bounds, two facts.
- **A `Withheld` variant for a shed.** §3.6. `observe.rs:885-886`'s ruling on
  `Reentrant`: no turn ran, so nothing was withheld.
- **A periodic sweep tick.** Every tick is a task per factory, which makes
  `build()` panic outside a runtime and moves `actor_memory.rs`'s slope.
  `compose::every` (`compose/mod.rs:138`) already exists — a caller who wants a
  sweep composes one.
- **Factory-owned restart, and a kill on a stuck worker.** `pool.rs:56-73` keeps
  the *not a supervisor* refusal, and killing without restarting is worse than
  either: the pool shrinks silently, and `pool.rs:57-59` already describes what
  follows — `pick` keeps handing back a handle to a stopped actor. Aborting also
  drops the task, which drops `A`, which drops every `Deferred<T>` parked in that
  state.
- **A four-lane priority mailbox.** `turns.rs:1393` rules out ractor's mechanism
  outright — "`tokio::select!` in this loop would not compile for a browser at
  all" — so a lane means hand-written poll ordering, not a cheap copy.
- **Name-string wire dispatch and shared-secret auth.** ractor_cluster's
  foundation, and both are strictly worse than the ALPN-as-type-hash and iroh's
  per-peer keys. Nothing here erodes them.
- **Retry inside a `Drop` impl.** Hidden control flow in a destructor is the
  opposite of failing loudly.

---

## 10. Rev. A claims that do not hold against `d54f302`

Every one measured this pass. Line numbers are the current ones.

| # | Rev. A said | Measured at `d54f302` |
|---|---|---|
| 1 | The API returns `Result<.., MailboxClosed>`; Cut 2's error is "`MailboxClosed` and nothing else"; `pool.rs:57-60` documents `MailboxClosed` | **`MailboxClosed` is not a type.** `turns.rs:3161-3172` records it becoming `Undelivered` at `4e0fcca` and then `Unanswered`. `pool.rs:57-60` names `Unanswered` and `NotQueued`. The whole Layer 0 signature does not compile. |
| 2 | *"There is no third option."* at `compose/mod.rs:41-44`, so shed must amend it first | The sentence is at **`compose/mod.rs:51-52`** and reads *"There is no third option **that hides**."* The dropped words carry the argument. Shed does not hide, so the paragraph does not forbid it and needs no amendment. `compose/mod.rs:41-44` is the backpressure **table**. |
| 3 | Pool makes three refusals: not a supervisor, not keyed, no queue | The list has three bullets and *"no queue"* is not one of them. It is *not a way to make one actor concurrent* (`:53`), *not a supervisor* (`:56`), *not keyed* (`:74`). "No queue" is the section at `pool.rs:19-30`. |
| 4 | `console.rs:543` does `try_send`; `console.rs:494` scopes it | `console.rs:470`; scoped at `console.rs:421-424`. |
| 5 | `registry.rs:2051` says "Nothing in this crate calls `try_send`"; `registry.rs:1922` carries the unbounded quote | Both are the same paragraph, at **`registry.rs:2110-2117`**. |
| 6 | `buffered()` at `turns.rs:4292`, `is_busy()` at `turns.rs:4265` | `turns.rs:4367` and `turns.rs:4340`. |
| 7 | `buffered`'s "zero or one, a constant and not a knob" defers `LeastLoaded` | **The sentence is false at `d54f302`** and still on the page at `turns.rs:4346`. `spawn_deep` (`turns.rs:3597`) takes a `NonZeroUsize` and builds `mpsc::channel(depth.get())` at `:3614`. The deferral is overturned; the sentence is a live defect. |
| 8 | `spawn_deep` at `turns.rs:3538`; `actor_deep` at `macros.rs:1721` | `turns.rs:3597`; `macros.rs:2016`. `supervised_deep` is `macros.rs:2101`. (The wave-4 grounding note's `3637` / `1714` / `1799` are also wrong.) |
| 9 | `compose::Overflow` re-exported at `compose/mod.rs:108` | `compose/mod.rs:139`. The enum itself at `topic.rs:107` and its not-`#[non_exhaustive]` argument at `:103-105` are both **correct**. |
| 10 | `registry.rs:1160-1190` carries `ActorStatus`'s `busy` and `idle_for` | `pub struct ActorStatus` at `registry.rs:1215`, `idle_for` at `:1228`. |
| 11 | `registry.rs:2105-2111` is the drain budget | That span is the mailbox-depth paragraph. `async fn drain(entry, budget: Duration)` is `registry.rs:1124`; the budget's own doc is `registry.rs:480-520`. |
| 12 | `turns.rs:1280` rules out `tokio::select!` for a browser | `turns.rs:1393`. |
| 13 | `TurnInfo` at `observe.rs:208-227` carries no timing | Struct at `observe.rs:211-230`. **The substance holds**: exactly two fields, `outcome` and `method`. |
| 14 | "Three guards get touched, not two" in `representability.rs` | Two probe **functions** (`:1056`, `:1092`), between them three shadowed method names and two signature pins. The `rg` at `:1038` is prose describing a grep **no gate runs**. Neither "two" nor "three" is the shape. |
| 15 | The `actor_memory.rs` slope "took three attempts to get to 13 bytes" | Two separate facts spliced. The three attempts (`actor_memory.rs:272`) were to land a **budget at 256**. The 13 bytes is `SLOPE_SLACK_OVER_A_KILOBYTE` at `:365`. |
| 16 | `Handle::is_busy` / `buffered` are the load signal, "both already public" | **Holds**, and it is now stronger: `Serving` (`observe.rs:774`) folds both and a peer reads the same value through `__status` without enqueueing. |
| 17 | tokio 1.53.1 `bounded.rs:1086-1091` says cancelling `reserve` loses your place | **Holds exactly.** Verified against the pinned source. `reserve_owned` at `bounded.rs:1265` and `Drop for Acquire` at `batch_semaphore.rs:686-708` also hold. |
| 18 | ractor's cluster drops mismatched messages silently, `node_session.rs:423-447` | **Holds, and understates it.** Four `let _ = actor.send_serialized(..)` sites: `:423`, `:440`, `:447`, `:496`. Rev. A rated this Med-High from an explorer; it is now read directly. |

**One defect found while checking, unrelated to whether this proposal is
accepted.** `reply.rs:67-68` says of `Withheld`, `AtCapacity`, `BadActorId` and
`FollowRefused`: *"Nothing produces the four yet."* At least two now do —
`RemoteError::AtCapacity` from `actor/remote.rs:377` (tested at `:824-827`), and
`RemoteError::Withheld` on both served paths (`reply.rs:109-114` says so four
lines above). Fix the sentence whether or not this proposal ships.

---

## 11. What I could not establish

- **Whether `Permit` roughly doubles macro output.** Rev. A sizes Cut 1 as "M,
  not S" on this. Not measured. **Experiment:** `cargo expand -p demo` before and
  after commit 1, diff the line counts of the generated `Handle` and `Permit`
  impls, and run `cargo test -p keylink --test actor_memory` for the slope.
- ~~**Whether `Factory::dispatch(&self, ..)` can hand out a borrowed `Permit` and
  still return a `Send + 'static` future.**~~ **MEASURED at `d54f302`, and Cut 2's
  shape holds.** A scratch integration test compiled two futures against tokio's
  own `Permit`, which is what keylink's would wrap:
  - `async fn holds_borrowed(tx: &Sender<u64>)` — takes `tx.reserve().await`,
    holds the permit **across a `yield_now().await`**, then sends. Passed to
    `fn assert_send<T: Send>(_: T)`: **compiles.** So a future holding a borrowed
    permit across an await point **is `Send`**.
  - `async fn holds_owned(tx: Sender<u64>)` — `reserve_owned`, same shape.
    Compiles, and a real `tokio::spawn(holds_owned(tx)).await` completes and the
    value arrives, so it is `Send + 'static`.

  **The distinction is `'static`, not `Send`.** A borrowed permit cannot cross
  `tokio::spawn`, and it does not need to: `dispatch`'s future is awaited in
  place by the caller. `+ Send` alone is what `macros.rs:3287-3291` requires, and
  the borrowed form has it. **§5.2 keeps the borrowed signature**; `reserve_owned`
  stays for callers who spawn, which is the reason §4 kept it after deleting the
  queued mode that first motivated it.

  The probe was deleted after measuring. Re-assert it in commit 1 as a kept
  compile-only guard — a probe deleted is a measurement, not a guard, and this
  signature is the one the whole design rests on.
- **Whether `FirstAvailable`'s wait-time spread is bounded at all** on this
  workload, or only bounded in the limit. §7 names the test; nothing has been
  run. The rustdoc must not promise a bound the test has not produced.
- **What `Shard<K>`'s default hasher should be.** `ByKey` needs a stable hash
  across process restarts if a caller's per-worker cache is warmed from disk.
  `DefaultHasher` is not stable across releases. Not decided here.
- **Nothing was compiled, benchmarked or red-tested.** No design on this page has
  been built.

---

## 12. Backlog, recorded and not scheduled

Rev. A's R4–R12, with its reasoning intact and no schedule. None has a named
caller; §1 of Rev. A says so and calls the line below R3 a backlog rather than a
plan. Order is Rev. A's, by value per unit of risk.

| # | Item | Rev. A's reasoning, preserved |
|---|---|---|
| R4 | `Reply::detach(future)` | Closes three deferred-reply defects with one method, from parts that already exist. S / Low / High. |
| R5 | Drain sentinel | Replaces a `Duration` budget keylink's own docs call uncheckable (`registry.rs:1124`). Becomes a prerequisite if `Backpressure::Unbounded` ships, because "drain an unknown number of turns in a fixed `Duration`" is then unanswerable. M / Med / High. |
| R6 | Stuck-actor detection | Nothing in keylink handles a hung actor. **Size it against the local path, not the keyed one** — `ActorStatus` carries `busy` and `idle_for` (`registry.rs:1215-1228`), and `LocalActor` carries no clock at all. Default to reporting, never killing (§9). M / Low / High. |
| R7 | Caller-gone signal on `Reply` | ractor's `is_closed()`. Lets a handler skip work nobody is waiting for. Nearly free. S / Low / High. |
| R8 | `enqueue_` / `Pending<T>` | Independently validated: ractor's `Job.accepted` port is the same design, arrived at separately. M / Low / Med. |
| R9 | Timing on `TurnInfo` | It has exactly two fields (`observe.rs:211-230`), so an observer sees no time at all, not even the turn's own duration. S / Low / Med. |
| R10 | Capability-narrowed handle | ractor's `DerivedActorRef`. keylink authorizes per method on the wire and not at all locally. M / Med / Med. |
| R11 | Rate limiting | keylink has none. Rev. A folds it into the queued mode, which §4 deletes — so it has no home and needs its own design if it is ever wanted. S / Low / Med. |
| R12 | Live reconfiguration | Changes worker count on a running factory. **This is what buys `Routing::Sticky` back** (§3.2 and §9). Lowest priority; do it only once the factory has users. M / Med / Low. |
| — | A priority queue in the factory | Deferred by Rev. A and it has nowhere to live: it exists only inside the queued mode. If it ever ships, carry priority on the call — `dispatch_with(Priority::High, key, job)` — rather than deriving it from the key through a trait, and state the property ractor does not: a steady flood of high priority starves low priority forever, so low-priority work needs a TTL to die visibly. |

---

`keylink × ractor` · rebuilt 2026-08-10 against `keylink main @ d54f302` ·
tokio pinned at 1.53.1 · ractor read at `~/repos/ext/ractor`
