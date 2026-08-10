# Wave 3 — what a peer may learn, and what it may not do

> **Current testing note (2026-08-10):** Any ALPN literal-pin step below is
> historical and is superseded by `CONTRIBUTING.md`'s **The wire contract**.
> The literal-pin tests are ignored until keylink has a stable version. Do not
> run or update them, and do not order work around them. The active contract
> property tests remain in the gate.

Plan for `docs/goals/2026-08-09-peer-administration.md`, **revision 1**, written
against `factory` at `2dd3eb4`.

The goal declares itself **"blocked on a security design that does not exist
yet"** and names three things to settle. §2 settles all three. §12 records what
was refused, so nobody re-derives it. §13 lists seven claims measured against
the tree, with the measurement for each: five of the goal's that do not hold as
written, one of the goal's that does, and one of the crate's own.

**This plan asserts no totals.** Wave 1's plan gate returned 7 Critical and 16
Major, and its merge gate returned 4 more Critical; none was arithmetic. Every
count in that plan grew on every measurement while each partial enumeration
added up correctly. So each figure below is either a **measurement at `2dd3eb4`
for sizing**, labelled as such, or a **predicate with the command that proves
it**. Where a figure is load-bearing and I could not measure it, §14 says so
rather than guessing.

## 1. What this is

A local `Handle` can ask four questions about an actor. A peer can ask none of
them on the service it is already talking to.

| Question | Local, today | Peer, today |
|---|---|---|
| Am I in a turn? | `Handle::is_busy` (`macros.rs:2345`) | nothing |
| How deep is the queue? | `Handle::buffered` (`macros.rs:2355`) | nothing |
| Is it running? | `Handle::is_running` (`macros.rs:2334`) | nothing |
| Why did it end? | `Handle::ended` (`macros.rs:2379`) | nothing |

`Inspect` answers the first three for **keyed** actors on a node, behind its own
ALPN and its own `Allowlist`, and only for a node that wired an `Inspector`.

This wave closes the first two on the service the peer is already connected to,
and it **refuses** the other two on that surface, for two different measured
reasons. It ships **no destructive verb**. §2.2 says why, and the reason is not
the one the goal gives.

## 2. Three rulings

### 2.1 The reads go on the service contract. `WireRequest` gains nothing.

The goal makes the symmetry case: `WireRequest::KeyState` is answered *"without
entering the handler and without running a layer"*, so a read placed beside it
is ungoverned on both sides — symmetric — while a read on the service contract
is governed for a peer and ungoverned for a `Handle`.

**Measured, the symmetry argument transplants a justification that does not
transfer.** Read `KeyState`'s own sentence to the end (`reply.rs:231-235`):

> Answered without entering the handler and without running a layer, because
> there is nothing to authorize past the connection's own admission: **a peer
> can only ever ask about its own keys.**

That is a **containment** argument, not a symmetry one. `Registry`'s
`key_state` takes the peer as a parameter — `key_state(&self, peer: EndpointId,
key: &IdempotencyKey)` (`idempotency.rs:1876`) — and looks up per peer. The
query cannot reach anything the asking peer did not put there. `KeyState` is not
ungoverned; it is **governed by construction**, which is why it needs no layer.

`__status` has no containment. It reports a fact about the node's actor rather
than about the caller's own data, so it is a question that needs a policy.
Placed on `WireRequest` it becomes a door that **every service in the fleet**
answers, for **every admitted peer**, permanently. That is fail-open in the
exact shape `Allowlist`'s missing `Default` exists to prevent.

**The decisive measurement is code, and not a doc comment.** A `WireRequest` arm
is answered **before the layer chain**. `rpc.rs:525-527` is the whole of it:

```rust
WireRequest::KeyState(key) => {
    return answer_key_state::<Resp>(&mut send, keys.ledger.as_deref(), peer, &key).await;
}
```

`serve_one` matches the variant and **returns**. It never calls `next`, so no
`Middleware` below it sees the request. A read placed there is not "ungoverned
by default" — it is **unreachable by governance**, and no node can opt in. Cite
this line rather than the doc comment at `reply.rs:231-235`: the comment records
what the design intends, and the `return` is what enforces it.

**So `__status` must go where a layer reaches it.** On the service contract the
chain runs above the `Dispatch` (`rpc.rs:1608` wraps each layer around it), so a
`Middleware` refuses `__status` per method and per peer, exactly as `PerMethod`
(`tests/actors.rs:1787-1814`) refuses one keyed method today.

**Corroborating, and not decisive: create-by-id.** `Registry::route`
(`registry.rs:771-791`) returns `NoSuchActor` **only when `self.factory` is
`None`** (`:778-780`); with a factory it takes the write lock and **inserts a
new entry**. So on a system built with `SystemBuilder::spawn_with` a call naming
an unknown id creates the actor. This argues for **neither placement**, in
either direction:

- **A `WireRequest` read need not insert.** `Registry::status`
  (`registry.rs:840-867`) reads through `self.read()`, and `Registry::touch`
  (`registry.rs:869-876`) answers `None` for an id with no entry. A
  `WireRequest::Status(ActorId)` written over either creates nothing.
- **The chosen placement's ordinary path does insert.** A contract `rpc` reaches
  `deliver_to_actor` (`macros.rs:1213`) and therefore `route`. `__status` itself
  will not, because §3's served arm answers **above** that call — which is a
  property of the mechanism, not of the placement.

Create-by-id is a fact about `route`, not about the door. It stays in the record
because §13.6 turns on it, and because the RFC must not claim otherwise.

**The goal's hazard framing is also inverted.** It says a contract read is
"governed for a peer and ungoverned for a `Handle`, which is goal 2's hazard
reappearing". A `Handle` is in-process. `Handle::ended` and `Handle::is_busy`
are already ungoverned and no allowlist has ever applied in-process. Goal 2's
hazard is a policy reachable one way and not the other by two callers who both
crossed the wire. A local caller did not cross it. Governed for a peer and
ungoverned in-process is not an asymmetry; it is the trust boundary.

**Ruling: `__status()` is generated onto each service's contract. `WireRequest`
keeps its two variants.**

**Say this in the RFC append, or the next revision re-derives the void
argument:** both placements move the ALPN. `wire.rs:52-79` says a change to
keylink's own wire types "moves the ALPN for every service in the fleet at
once", and adding a generated method to every contract does the same. The ALPN
**discriminates nothing here** and is not evidence for either choice — the same
trap wave 1 §2.1 had to disarm for `#[non_exhaustive]`.

### 2.2 No destructive verb ships. And the blocker is not the one the goal names.

The goal's objection is sound as far as it goes: `Allowlist` is
`Only(BTreeSet<EndpointId>) | Anyone` (`authz.rs:133-138`) with **no per-method
dimension of its own**, attached per service, so a destructive verb on a
business contract means one list admits both `place_order` and `shutdown`, on
upgrade, with nobody editing a list. Its recommended fix is a separate admin
service with its own ALPN and its own `Allowlist`.

**That fix does not work, and the reason is the goal's own premise.** An admin
service carrying a read (`__ended`) and a destroy (`shutdown`) has **one
allowlist admitting both** — the identical defect, moved one service over.
`Allowlist` is the thing with no per-method dimension, and moving the verbs to a
second service does not give it one.

**A per-method dimension does exist, and the blocker is that it is
caller-supplied.** `Middleware::handle` takes a `Context`
(`middleware.rs:584-590`); `tests/actors.rs:1787-1814` is a layer that reads
`cx.method()` and refuses one method with `RemoteError::Unauthorized`, green at
`tests/actors.rs:1823`. §2.1 rests on that same mechanism. So the dimension is
reachable — **by a node that writes the layer**.

**The asymmetry between the two is fail-open, and that is the real blocker.**
`Allowlist` has deliberately no `Default` (`authz.rs:130-131`: *"Admitting
everyone is a decision you have to type out, so it can never be what you get by
forgetting"*), so a **forgotten allowlist admits nobody** and the service does
not build. A **forgotten `Middleware` refuses nothing**, and the service builds
and serves. So the gate every served method carries today fails **closed** on
omission, and a destructive verb guarded only by a layer the operator must
remember to write fails **open** on the same omission. Closing that gap means
giving `Allowlist` the dimension, which is outside this goal's radius.

Two further measured costs, stated so the follow-on goal starts from them:

- **The precedent's own example places `anyone()` one line above
  `only([operator])`.** `console.rs:26-27` reads
  `.service(journal::keyed_service(&system, Allowlist::anyone()).build())` and
  then `.service(inspector.service(Allowlist::only([operator])).build())`. That
  is the file a new admin service is copied from. A copied wiring line carrying
  `anyone()` hands a remote kill switch to the world. `Allowlist` having no
  `Default` refuses the *forgotten* case; it does not refuse the *copied* one.
- **The first release of the vehicle would also be the first release of the
  weapon.** There is no deployed experience of an admin ALPN to draw on.

**Ruling: `shutdown`, `abort`, `evict_idle` and `mount` ship on no peer surface
in this wave.** Their absence stays a stated property, not an oversight — see
§9's residual note, which must also carry §13.6's correction that a peer *can*
already cause an actor to exist.

### 2.3 No reservation mechanism ships. Two doctests ship instead.

The goal budgets a per-method `const _: () = assert!(...)` because
`macro_rules!` cannot compare idents. Measured, the mechanism would cost more
than it buys and would guard the wrong set.

**How the names are actually derived.** `service!` runs through
`pastey::paste!` (opened at `macros.rs:489`) and spells every generated method
`[<$rpc:snake>]` (`macros.rs:578, 590, 948, 965, 2388, 2588`).
`macros.rs:126` states the rule:
*"RPC names are written in `CamelCase` ...; the generated methods are
`snake_case`."* `macros.rs:3326-3330` records that the wire name and the trait
name are carried as **two parameters** rather than one derivation, "because
`pastey` can go from `Reserve` to `reserve` and nothing can reliably go back".

**So a collision is a duplicate inherent method in one `impl` block —
`error[E0592]` — and the colliding set is surface-dependent.** Measured at
`2dd3eb4`:

| Surface | Declared at | Names already taken |
|---|---|---|
| `Handle<A>` | `macros.rs:1995` | `service`:2140, `serve`:2234, `with_state`:2276, `stop`:2312, `abort`:2327, `is_running`:2334, `is_busy`:2345, `buffered`:2355, `ended`:2379 |
| `Client` | `macros.rs:2627, 2789` | `connect`, `with_idempotency`, `key_state`, `watch`, `close` |
| `KeyedClient` | `macros.rs:2448-2551` | those five, plus `at`:2551 |
| `IdempotentClient` | `macros.rs:2736-2882` | `key_state`, `watch`, `close` |
| `ActorRef<'_>` | `macros.rs:2580` | `id`:2580 |

**This is a measurement for sizing, not a total.** The predicate is: every
`pub fn` in a generated `impl` block that the per-method expansion also lands
in. The command is
`rg -n '^\s+pub (async )?fn [a-z_]+' keylink/src/macros.rs` read against the
`impl` headers above it.

A flat reserved list would refuse `at` on a `Handle`, where it is legal, and
`ended` on a `Client`, where it is legal. A surface-correct list is five lists.
And every form of the check lands **in generated code, one expansion per
`service!` in the world** — the exact cost `registry.rs:198-201` refuses when it
declines to widen `mount`'s closure for the same kind of gain.

**Ruling: no reservation mechanism. The guard is a `compile_fail,E0592`
doctest** — doctest **A** below — which is the house form. `turns.rs:412-420`,
`observe.rs:185-193`, `actor/remote.rs:166-175` and `registry.rs:1402-1416` all
pin an error code, and
`registry.rs:1416` argues why: *"`compile_fail` says only that it failed"*.
Gated, because `[tasks.test]` runs `cargo test --workspace --doc` explicitly and
`test` is in the gate.

**The name this wave adds must be one a user cannot produce.** `__status` follows
the crate's existing internal convention (`__invoke_$rpc:snake`,
`macros.rs:586`). Whether any legal `CamelCase` ident can make `pastey`'s
`:snake` emit `__status` is **not established** — see §14.1.

**One doctest cannot answer both questions, so there are two.** A
`compile_fail,E0592` over a **known** collision stays green whether or not a
legal ident reaches `__status`. It proves that one collision is refused, and
says nothing about which names collide. Split it:

- **A — a collision is refused, with the code pinned.** `compile_fail,E0592` on
  a known collision: declare `rpc IsBusy()` against `Handle::is_busy`
  (`macros.rs:2345`). This is §2.3's guard, and it is the house form.
- **B — is `__status` producible at all?** Declare `rpc __Status()` and
  **record the outcome**. `__Status` is a legal Rust ident, so the declaration
  either compiles or it does not, and the run is the measurement. **If B
  compiles, `__status` is producible by a user and the chosen name must
  change.** §14.1's answer comes from B, and never from A.

## 3. The shape

```rust
// keylink/src/observe.rs — beside EndReason and Withheld, the both-sides vocabulary
/// What a caller may learn about an actor that is still there.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Schema)]
pub struct Serving {
    /// Inside a turn right now.
    pub busy: bool,
    /// Queued and not yet taken.
    ///
    /// **Comparable as a predicate, not as a count.** `turns.rs:4351-4356`:
    /// a keyed mailbox carries only the service's own messages, while a
    /// `LocalActor`'s channel also carries a `read` and the stop request.
    /// "Will the next caller block" is exact either way; the number is not.
    pub buffered: u64,
}
```

Two fields, and the two that were dropped were dropped for different reasons:

- **`id` and `idle_for` are not narrowed, they are unreachable.** `ActorStatus`
  (`registry.rs:1190`) has four fields — `id`:1192, `idle_for`:1203,
  `busy`:1214, `buffered`:1242 — and is built **only** by `Registry::status`
  (`registry.rs:840-867`), which maps the keyed entry map. A singleton has no
  registry entry at all; `console.rs:82-85` states exactly this about the
  `Inspector` and refuses inventing a second flag for actors with no registry as
  *"exactly the parallel fact this crate refuses"*. `idle_for` reads
  `entry.last_seen`, which exists nowhere else.
- **`is_running` is dropped, and the `Result` is the liveness answer** — but not
  for the goal's stated reason. See §13.4.

**`Serving` lives in `observe.rs`.** Wave 1 made that module the shared
vocabulary and made it wire-aware, and it already holds `EndReason`
(`observe.rs:592`) and `Withheld` (`observe.rs:870`) with `Schema` derives.
Splitting the both-sides vocabulary across two modules is what wave 1 §2.2
refused.

**`ActorStatus` is not touched.** It keeps four fields and stays `Inspect`'s
answer. Two types, because they answer two questions: `ActorStatus` is *every
actor on this node, to an operator*; `Serving` is *the actor you just
addressed, to its own caller*.

### How `__status` is answered, and why it cannot be a message

**`__status` is answered inside the `Dispatch`, and it enqueues nothing.** This
is the wave's central mechanism, and `macros.rs:2341-2343` is why it has to be.
That doc states the constraint on `Handle::is_busy`:

> This separates them, and it is the question you cannot ask the actor: a
> message asking would queue behind the turn that wedged it.

A `__status` **delivered as a message** is that forbidden question. It queues
behind the wedged turn. If it ever ran it would answer `busy: true` for every
caller — the actor is inside a turn, namely the one answering the question —
while its own mailbox permit made `buffered` count itself. The method would
report the state of asking rather than the state of the actor.

So the **generated served arm intercepts it, before the delivery call**. There
are **two** such calls, one per serving path, and therefore two arms rather than
one:

- **keyed** — `macros.rs:1213` calls `deliver_to_actor` inside `keyed_service`'s
  `Dispatch` closure (`macros.rs:1202-1226`). The arm answers from the registry
  entry: the two facts `Registry::status` reads (`entry.busy.running()` at
  `registry.rs:849`, the `max_capacity`/`capacity` difference at `:858-864`),
  through a **non-inserting** reader. `Registry::touch` (`registry.rs:869-876`)
  answers `None` for an id with no entry, where `route` would insert one.
- **singleton** — `macros.rs:2170` calls `deliver_to_singleton` inside
  `service()`'s `Dispatch` closure (`macros.rs:2140-2185`). The arm answers from
  the `LocalActor` flag that `Handle::is_busy` and `Handle::buffered` already
  forward to (`macros.rs:2345`, `:2355`, over `LocalActor::buffered` at
  `turns.rs:4367`).

Those two are the whole set. The predicate is
`rg -n 'deliver_to_actor|deliver_to_singleton' keylink/src/macros.rs` read
against the `Dispatch` closures around the hits — every other hit in that file
is prose.

**Governance is unchanged by the interception.** The layer chain runs above the
`Dispatch` (`rpc.rs:1608`), so a `Middleware` refuses `__status` per peer and
per method exactly as it refuses any other. What the interception removes is the
mailbox, not the allowlist and not the layer.

**§14.6 records the part this plan could not establish**: whether either arm
fits where its `Dispatch` closure already stands, or whether the served side
needs a new seam.

### `Inspect` after the wave

```rust
crate::service! {
    service Inspect;
    rpc Actors() -> Vec<ActorStatus>;
    rpc Node()   -> NodeStatus;
    /// Why the actor under `id` ended, if this log still holds the event.
    rpc Ended(id: ActorId) -> Option<EndReason>;   // NEW
    #[defer]
    rpc Follow(after: u64) -> ::core::result::Result<EventPage, FollowRefused>;
    feed Events(event: LifecycleEvent);
}
```

**`__ended()` is not on the service contract, and the reason is mechanical
rather than political.** A question about an actor that has **ended** cannot be
answered by that actor's own handler — the one state the method exists to report
is the state in which the contract's methods are not served. On the keyed path
a stopped actor is unrouted and the call answers `NoSuchActor`
(`registry.rs:1551-1554`); on the singleton path wave 1 routed the
`Standing::Gone` arm to `Withheld::Gone`, which is what the call gets *instead
of* reaching a handler.

So the fact moves to where it is already recorded. `LifecycleEvent::Ended { id,
why: EndReason }` (`console.rs:164`) is already in the `Inspector`'s bounded log
(`console.rs:661`, `:709-714`), behind an allowlist `console.rs:38-45` already
calls a secrets boundary. `rpc Ended(id)` is a lookup over that `VecDeque`:
**no new ALPN, no new allowlist decision, no new state, no widening of
`Inspector`'s erasure, and no new memory** — `history` already bounds it.

`Inspector` keeps `actors: Arc<dyn Fn() -> Option<Vec<ActorStatus>> + Send +
Sync>` (`console.rs:649`) exactly as it is. The erasure that
`registry.rs:1776-1798` records — `Weak`, `Option` for "system gone", and the
leak that made it `Weak` — is **not touched by this wave**. Stated because the
goal's brief predicts it must be widened, and it must not.

**Its limit is one the module already declares.** `console.rs:100-102`: *"No
history before the `Inspector` existed."* `Ended(id)` answers `None` for an
actor whose ending fell out of the ring, and `None` is not "still running". That
ambiguity is real and is the price of not adding state. Record it on the method.

**What this wave therefore does NOT close:** an ordinary peer of an ordinary
service still cannot ask why an actor ended. Only an operator on `Inspect` can,
and only inside the history window. That is the wave's declared scope, not an
omission.

## 4. The work, in commits

Every `file:line` above was measured at `2dd3eb4` and describes the tree
**before** commit 1. Pointers are not rewritten in place as the wave proceeds —
a pointer edited in place stops showing what was true when the instruction was
written. Append to the table below instead.

`lint-fast` is `cargo clippy -p keylink --lib` — **library
target only** — so a green `pre-commit` is not proof a commit compiles. Verify
each commit with `cargo check --workspace --all-targets`, `mise run test` and
`mise run docs` before it lands.

| # | Commit | ALPN pin | anchors registry | rustdoc links |
|---|---|---|---|---|
| 1 | `Serving`, and the two bodies that produce it | **no** | no | few |
| 2 | `__status` on every contract **+** `Inspect::Ended` | **BOTH** | predicate — see below | see below |
| 3 | the guards, with their break records | no | **`RECORD_BLOCKS` + one row per breakable guard** | few |
| 4 | RFC / README / `authz.rs` appends | no | no | no |

### Commit 1 — `Serving`, and nothing reaches it from the wire yet

`observe.rs` gains the type; `lib.rs` re-exports it beside `EndReason`
(`lib.rs:323`); `turns.rs` gains a `LocalActor` body folding the two facts
`Handle::is_busy` and `Handle::buffered` already forward to (`macros.rs:2345`,
`:2355`, over `LocalActor::buffered` at `turns.rs:4367`); `registry.rs` gains a keyed
body reading the same two facts `Registry::status` reads (`entry.busy.running()`
at `:849`, the `max_capacity`/`capacity` difference at `:858-864`).

**It does not move an ALPN**, because no contract reaches `Serving` yet. That is
the whole reason it is its own commit: it makes commit 2's pin move attributable
to one cause.

**`keylink/tests/api.rs` does not redden, and the plan said it would.**
`api.rs:182` is a **doc line** inside a rustdoc block describing an anchor that
already rotted; it is not an assertion. The file's list of names is the `use
keylink::{..}` at `api.rs:47-50`, and `api.rs:42-43` states the claim: *"There
is nothing to assert at run time. This test passing means it compiled."* A
crate **gaining** a name breaks no named import, so adding `pub use Serving` to
`lib.rs` leaves `api.rs` green. Expect no red here, and do not read one as the
guard working.

**`keylink/tests/persist.rs:1265` already declares `struct Serving`.** Its
import is a named list (`persist.rs:30-33`) rather than a glob, so a new
`pub use` in `lib.rs` collides with nothing today and nothing goes red. The
consequence is for whoever writes in that file next: a test there that wants the
new type must spell it `keylink::Serving` or alias it on import.

### Commit 2 — the pin move, and it is deliberately one commit

Carries: the `__status` expansion on `Handle`, `Client`, `KeyedClient`,
`IdempotentClient` and `ActorRef`; the served arm that answers it; `Inspect`'s
new `Ended` rpc and the `Inspector` handler that reads its own log; both pinned
literals.

**The served arms are the two §3 specifies** — one above `deliver_to_actor`
(`macros.rs:1213`) and one above `deliver_to_singleton` (`macros.rs:2170`) —
answering from the registry entry and from the `LocalActor` flag, and
**enqueuing nothing**. This is the wave's central mechanism rather than a detail
of this commit: an arm that instead delivers a message answers `busy: true`
unconditionally and counts itself in `buffered`. If a `Dispatch` closure cannot
take its arm where it stands, that is §14.6, and it is a design question rather
than a merge conflict.

**Not split, because `Inspect` is a service too.** `__status` lands on every
contract, `Inspect`'s included, so a commit adding `__status` already moves
`keylink/Inspect`. Adding `Ended` in a second commit moves the same literal a
second time — two failing runs and two edits of one const, against a budget of
one coordinated move.

Read both new literals off **one failing run** of:

```
cargo test -p keylink --test protocol contract_hash_has_not_moved
```

The current pins are `keylink/Probe/75cb158fa0f636c8` (`protocol.rs:1251`) and
`keylink/Inspect/f62a21fb09137ace` (`protocol.rs:1380`). **Confirm the run
reports two failures before reading any literal** — a filter that reaches one
test looks identical to a pin that did not move. Use `cargo test` and **not** a
nextest `-E 'test(...)'` filterset: a filterset matching nothing exits non-zero
having run zero tests, which reads exactly like a caught defect. **Never compute
a hash.**

Two documentation copies of those literals move with them:
`docs/rfc-07-the-turn-model.md:4782-4783` and
`docs/rfc-08-serving-side-keys.md:958-959`. **No gate reads either file**, so
nothing catches a survivor.

**The anchors answer is a predicate, never a list.** Rows quote source literals,
and this commit edits `macros.rs`, `console.rs`, `turns.rs` and `registry.rs`.
Three registered rows target `macros.rs` — `R25` (`anchors:550`), `R28`
(`anchors:635`), `F1` (`anchors:1124`) — and are the likeliest to need
re-quoting, but the set comes off `mise run anchors`, not off this sentence.
Count anchor uniqueness **after `cargo fmt`**: the formatter owns the line breaks
a record quotes.

**Twenty-eight files declare a `service!` block** outside `keylink/src`, in
three regions — `demo/src/main.rs`, **two under `keylink/examples/`**
(`durable_ledger.rs:30` and `reliable_call.rs:54`), and 25 under
`keylink/tests/` — measured at `2dd3eb4` for sizing. All of them regenerate;
only `protocol.rs` pins a literal, and `api.rs` is compile-only. The
verification is `mise run test` green **and `mise run examples`
green**, not this count.

**The examples are run, not built.** `[tasks.examples]` names each example on
its own `cargo run` line, and its comment records why: *"`cargo test
--workspace` compiles the examples and never executes one"*. An example whose
`service!` regenerated into something that compiles and then panics is red in
the `examples` gate step and nowhere earlier.

**Intra-doc links.** Do not carry a number. The verification is `mise run docs`
exiting 0 under `-D warnings`, which is the only gate that sees a broken link.
Budget for `rustdoc::redundant_explicit_links`, which `[tasks.docs]` keeps on and
**does not allow**.

### Commit 3 — the guards, and each needs a record

Four guards, and none is free:

1. **Doctest A — `compile_fail,E0592`** on `service!`, proving a declared `rpc`
   whose snake form collides with an existing inherent method is refused, with
   the code pinned. Per §2.3.
2. **Doctest B — `rpc __Status()`**, whose **outcome is the measurement**. Which
   form it takes — a plain `rust` doctest or a `compile_fail` one — is decided
   by running it once, not by this document. Per §2.3 and §14.1. A doctest that
   records an outcome has nothing to break, so it carries **no break record**.
3. **One `Serving`, both sides** — a value-equality assertion that a local
   caller and a peer read the same `Serving` for one keyed actor in one state.
4. **A wedged actor answers `__status` during its turn** — the assertion that
   §3's interception is real: `busy: true`, **returned while the turn holding
   the actor is still running**. A `__status` that queues cannot pass this,
   which is what makes it the mechanism's guard rather than its restatement.

**None of the four exists.** §6 lists all four, with the `Inspect::Ended` round
trip, as the tests somebody must write.

**Each breakable guard needs a break record**, and a record moves
`RECORD_BLOCKS` —
**`.mise/tasks/anchors:3318`, value `176`, measured at `2dd3eb4`**. Wave 1's
plan cites `177` at `anchors:3239`; that plan is a **dated record** and is not
renumbered, so cite this figure and its commit rather than reconciling the two.
**Read the new value off the failing run — never compute it.**

`break-once.sh:219` — `── selected=1` is the only reading a record supports.
`0` means nothing ran. **`?` means no totals line: an abort, or a tree that did
not compile**, which reads like a caught defect and is not one.

### Commit 4 — docs only, and `pre-commit` will skip it

`pre-commit`'s filter looks for Rust. A commit touching only `.md` files runs
**no** `anchors` step, so the gate that pins the registry does not fire on the
commit most likely to rot a quoted literal. **Run `mise run anchors` by hand on
commit 4**, or fold any registry edit into commit 3.

## 5. What is measured, and by what

| Claim | Proof |
|---|---|
| A peer reads the same `Serving` a `Handle` does | `a_local_caller_and_a_peer_read_the_same_serving` — **does not exist; commit 3 writes it**. Compares the `Serving` a `Handle` on the serving node reads against the `Serving` a peer client reads over the wire, **for one keyed actor on the keyed path**, asserted for `busy` in both states. **Never compare a keyed `Serving` against a singleton one by value**: §3 documents `buffered` as a predicate rather than a count, and the two channels carry different traffic (`turns.rs:4351-4356`) |
| `__status` answers while the actor is wedged | `a_wedged_actor_answers_status_during_its_turn` — **does not exist; commit 3 writes it**. `busy: true`, returned **before the turn holding the actor completes**. This is the §3 interception; a `__status` that enqueues cannot pass it |
| A colliding `rpc` is refused, with the code pinned | doctest **A** of §2.3 — `compile_fail,E0592` on `rpc IsBusy()` against `Handle::is_busy` — **does not exist; commit 3 writes it**. Unannotated `compile_fail` passes for the wrong reason (`registry.rs:1416`) |
| `__status` is a name a user cannot produce | doctest **B** of §2.3 — `rpc __Status()`, outcome recorded — **does not exist; commit 3 writes it**. A green **A** is no evidence for this row |
| `Ended(id)` answers from the log and adds no state | `Inspector` gains no field. Predicate: `git diff` on the `Inspector` struct (`console.rs:647` to its closing brace, `impl Inspector` opening at `:680`) shows no added struct member |
| `Inspector`'s erasure is unchanged | `console.rs:649`'s type is byte-identical after the wave, and `System::weak_status` (`registry.rs:1790`) gains no sibling |
| `WireRequest` gained nothing | `WireRequest` still has exactly `Call` and `KeyState` (`reply.rs:227-236`) |
| No destructive verb reaches a peer | `rg -n 'shutdown\|abort\|evict_idle\|mount' keylink/src/macros.rs` names no generated client or served arm |
| The hash moved once | both pins updated in commit 2 from one failing run showing **two** failures, and the release note names **both** tests, as `CONTRIBUTING.md:199-201` already does |
| Nothing regressed | `mise run gate` (every declared step), `mise run test`, `mise run anchors`, `mise run replay-anchors`, `mise run docs` |
| The new break records actually replay | **`scripts/replay-fate-breaks.sh`, run by hand.** Nothing in the gate replays a break record — `replay-anchors` self-tests the classifier and `[tasks.gate]` says the replaying arm "is in no list and cannot be" |

## 6. Tests this wave asks somebody to write

Named separately because wave 1 shipped a success criterion citing a test that
three documents referenced and nobody had written.

1. `a_local_caller_and_a_peer_read_the_same_serving` — new, `keylink/tests/`.
   One **keyed** actor, per §5's row.
2. `a_wedged_actor_answers_status_during_its_turn` — new, `keylink/tests/`. The
   guard on §3's interception, and the only test that can fail if `__status`
   becomes a message.
3. Doctest **A**, `compile_fail,E0592` on `service!` — new,
   `keylink/src/macros.rs`.
4. Doctest **B**, `rpc __Status()` with its outcome recorded — new,
   `keylink/src/macros.rs`. It answers §14.1.
5. An `Inspect::Ended` round trip that also asserts the `None` answer for an id
   whose event has fallen out of the ring — new, `keylink/tests/inspect.rs`.

Every other proof in §5 is a predicate over an existing command.

## 7. Blast radius

Source: `keylink/src/observe.rs`, `lib.rs`, `macros.rs`, `turns.rs`,
`actor/registry.rs`, `console.rs`.
Tests: `keylink/tests/protocol.rs` (both pins), `keylink/tests/inspect.rs`,
`keylink/tests/persist.rs` (its `struct Serving` at `:1265` takes the name; see
commit 1). `keylink/tests/api.rs` regenerates and is expected to stay green.
Examples: `keylink/examples/durable_ledger.rs` and
`keylink/examples/reliable_call.rs` each declare a `service!` block, and **gate
step 11 runs them** rather than building them.
Registry: `.mise/tasks/anchors` (`RECORD_BLOCKS`, plus whatever `mise run
anchors` names).
Docs: `docs/rfc-07-the-turn-model.md`, `docs/rfc-08-serving-side-keys.md`,
`README.md`, `keylink/src/authz.rs` (§9).

**`keylink-persist/` declares no `service!` block** — measured at `2dd3eb4`.
`demo/src/main.rs` declares one; its ALPN moves and nothing pins it.

## 8. wasm

Four gate steps are `wasm`, `wasm-guards`, `wasm-web`,
`wasm-browser`. The dimension that matters is `--no-default-features`
compilation of `keylink` **and** `keylink-persist`. `mod observe` and `mod
console` are unconditional in `lib.rs`, so `Serving` and the `Inspect` change
stay inside the wasm build with no `#[cfg]` change. `Serving` holds a `bool` and
a `u64` and reaches no clock, so none of the forbidden APIs
(`tokio::select!`, `std::time::Instant`, `tokio::task_local!`) is involved.
Stated so a red step 13 reads as a link problem rather than a design one.

**`u64` and never `usize`**, matching `ActorStatus::buffered`
(`registry.rs:1242`) and its `try_from`-not-`as` construction
(`registry.rs:858-864`): a diagnostic must not be the thing that panics, and the
wire width must not depend on the target.

## 9. Docs

**Appends, not rewrites.** One append per affected file, saying what moved and
at which commit.

RFC-07 and RFC-08 must carry:

- **The `WireRequest` ruling on its two reasons**, per §2.1, in this order.
  First, `KeyState` is **governed by construction** — `key_state` takes the peer
  as a parameter, so the query reaches nothing the asking peer did not put
  there. Second, and decisively, a `WireRequest` arm is **answered before the
  chain**: `rpc.rs:525-527` returns without calling `next`, so no `Middleware`
  sees such a door and **no node can opt in**. Cite that line, not the doc
  comment at `reply.rs:231-235`. Written without the containment sentence, the
  next author reads "KeyState is ungoverned" and puts the next read there.
- **The create-by-id measurement as corroborating**, per §2.1, and **not** as
  the ruling's ground. `route` inserts rather than refusing when a factory is
  set (`registry.rs:778-791`). Do **not** write that an ungoverned status door
  is necessarily an ungoverned creation door: `Registry::status`
  (`:840-867`) and `Registry::touch` (`:869-876`) are non-inserting readers a
  `WireRequest` arm could have used, and the chosen placement's ordinary path
  reaches `route` and inserts. The sentence as first drafted was false about the
  rejected option and silent about the chosen one.
- **How `__status` is answered**, per §3: inside the `Dispatch`, above
  `deliver_to_actor`, enqueuing nothing — because `macros.rs:2341-2343` says a
  message asking this question queues behind the turn that wedged the actor.
- **Why no destructive verb ships**, per §2.2 — with the correction that a
  separate admin service does **not** fix the goal's own objection, because the
  missing dimension is `Allowlist`'s, and with the real blocker: a per-method
  dimension **exists** via `Context` on `Middleware::handle`
  (`middleware.rs:584-590`, green at `tests/actors.rs:1823`), but it is
  caller-supplied. A forgotten `Allowlist` admits nobody; a forgotten
  `Middleware` refuses nothing. That asymmetry is fail-open.
- **Why `__ended` is not on the service contract**, per §3: the state it reports
  is the state in which the contract is not served.
- **The residual `mount` note**, corrected per §13.6: a peer **can** already
  cause an actor to exist; only handing over a pre-built `Mounted<M>` is
  impossible.

`keylink/src/authz.rs` gains a sentence on `Allowlist` stating what a service's
list now admits, including `Serving`. It is documentation, not a mechanism.

**`keylink/src/console.rs:38-45` is corrected**, per §13.7, and the wording is
constrained: the sentence was **overstated when written**, because the restart
path has always rendered an ending to any admitted peer. Do not write that wave
1 introduced the exposure — that reading points at reverting a typed variant to
a string, which is strictly less governed than what the tree has now. State what
the allowlist *does* uniquely gate: `LifecycleEvent::Faulted` and the event
stream.

**The RFC append carries §15's preconditions verbatim**, and not the bare
claim. Write: *an ordinary peer of an ordinary service reads a panic message out
of `err.to_string()` only when `retryable` grants the restart
(`turns.rs:2760-2777`) **and** the budget grants it too (`turns.rs:2856`). Both
must hold.* Written without them, a reader concludes that every service leaks on
every fault, and the follow-up goal in §15 sizes the wrong problem.

`README.md` — **no gate reads it**, so nothing catches a survivor. Grep it for
the pinned literals and for the four-question table before calling commit 4
done.

## 10. Refused, so nobody re-derives them

- **A peer door for `System::contains`.** Refused for **privacy**, and the
  argument is at `reply.rs:95-97` — *"telling them apart would leak that an id
  was once real"* — inside `NoSuchActor`, not at `reply.rs:79-81`, which is
  `TimedOut`. `contains` (`registry.rs:1712`) answers exactly the question
  `NoSuchActor` refuses to answer, so a peer door for it undoes that decision in
  one line. The privacy reason is unrelated to the hash and survives any ALPN
  move.
- **`__status` on `WireRequest`.** §2.1. Not because of the ALPN.
- **`__ended` on the service contract.** §3. Mechanically unanswerable.
- **A redacted second `EndReason` for peers.** A parallel fact, refused on
  `console.rs:84-85`'s ground.
- **A separate admin service.** §2.2 — it does not fix the goal's own objection.
- **A per-method reservation mechanism.** §2.3 — surface-dependent, and it lands
  in generated code once per `service!` in the world.
- **Widening `Inspector`'s erasure.** §3. It stays
  `Fn() -> Option<Vec<ActorStatus>>`, `Weak`, and the leak on record
  (`registry.rs:1776-1786`) stays fixed.
- **Retaining endings in the registry.** `report_ended` (declared at
  `registry.rs:702`; `:1706` is a call site, not the declaration) reports and
  does not keep. A retained map needs an eviction policy and is unbounded
  without one. The `Inspector`'s ring is already bounded.
- **`is_running` on `Serving`.** §13.4 — dropped, and the goal's reason for
  dropping it is conditional.

## 11. Order, and what may run in parallel

**Commits 1 → 2 → 3 → 4, serially.** Commit 2 touches `macros.rs`,
`console.rs`, `turns.rs` and `registry.rs`; commit 3's break records quote lines
commit 2 writes. There is no disjoint pair. Serial costs minutes; parallel buys
conflicts in the largest files in the crate.

## 12. What this plan changed from the goal

The goal recommends a separate admin service and budgets a reservation
mechanism. This plan ships neither, and refuses one read the goal treats as
shippable. §2.2 and §2.3 carry the measurements. §13 carries the claims that do
not hold.

## 13. Claims that do not hold against `2dd3eb4`

1. **"`reply.rs:79-81` refuses the distinction for a privacy reason."**
   `reply.rs:79-84` is `RemoteError::TimedOut`. The privacy argument is
   `reply.rs:95-97`. Wave 1 appended four `RemoteError` variants and moved the
   lines.

2. **"a user declaring `rpc Status()` gets a duplicate-definition error pointing
   inside `macros.rs`."** It compiles. `service!` derives `[<$rpc:snake>]`
   (`macros.rs:590, 2388`), so `rpc Status()` generates `status()`, and no
   generated surface defines `status` today. The colliding names are the ones in
   §2.3's table, and the error is `E0592`.

3. **"`status()` must not return `ActorStatus`. Two of its four fields (`id`,
   `idle_for`) do not exist for a singleton."** Understated. `ActorStatus` is
   built **only** by `Registry::status` (`registry.rs:840-867`) over the keyed
   entry map, so a singleton yields **no row at all**. The question has no
   field-level answer; the whole struct is unreachable. `console.rs:82-85` states
   the same fact about the `Inspector`.

4. **"drop `is_running` entirely ... remotely a stopped actor answers
   `NoSuchActor`, so `running: false` can never arrive."** The conclusion holds;
   the premise is conditional and the field does not exist. `ActorStatus` has no
   `is_running` — it is a `Handle` method (`macros.rs:2334`). And
   `Registry::route` returns `NoSuchActor` **only when `self.factory` is `None`**
   (`registry.rs:778-780`); with a `spawn_with` factory it creates the actor
   instead.

5. **"There is no reservation mechanism today."** True of `macros.rs`. The crate
   has one a layer up: `RESERVED_ALPNS` (`rpc.rs:1648`), refused at
   `rpc.rs:1785`. Recorded so the next sweep does not claim there is no
   precedent for refusing a name.

6. **"A peer cannot cause an actor to exist" — the goal calls this false, and it
   is right.** Confirmed: `deliver_to_actor` (`actor/remote.rs:438`) calls
   `system.route(&actor)` at `:454`, and `route` inserts from `self.factory`
   (`registry.rs:778-791`). The layer chain runs first (`rpc.rs:1608`), so a
   `Middleware` can already refuse a create-by-id per method and per peer.

7. **`console.rs:38-45` is false, and it was overstated when written — wave 1 did
   not make it so.** The sentence claims the `Inspect` allowlist *"is the only
   thing between a peer and whatever this node's panics say"*. Measured at
   `0119504`, the commit before the branch, three citations:

   - `turns.rs:4618` — `Standing::Restarting(reason) => RemoteError::Failed(format!("{RESTARTING}{reason}"))`
   - `observe.rs:552` — `Self::Panicked { message } => write!(f, "a turn panicked: {message}")`
   - `reply.rs:189` — `pub const RESTARTING: &'static str = "restarting: ";`

   `reason` is the `Arc<Ended>`, so its `Display` carried the panic message into
   a `Failed(String)` on **every service**, for any admitted peer, before wave 1
   existed. **The crate said so itself**: `observe.rs:545-546` at `0119504`
   reads *"`turns.rs` puts one of these behind a `"restarting: "` prefix on the
   served path"*, and `:557-559` adds *"it is also the sentence a peer reads
   about a restart."* The exposure was documented two files away from the
   sentence that denied it.

   **What wave 1 changed is the carrier, not the class.** That text moved from
   an unhashed string prefix — outside the contract, where the handshake could
   not protect it — into `Withheld::Restarting { reason: EndReason }` (the
   variant opens at `observe.rs:928`; `:950` is the `reason` field), a typed
   variant inside the hashed graph. Strictly better governed, same information.
   `observe.rs:931-943` scopes which `EndReason` shapes reach it: `Stopped`,
   `Vanished`, `GaveUp` and `Observer { fatal: true }` cannot, so **three** can
   — `Panicked`, `Feed`, and `Observer { fatal: false }`. Of those three,
   `Panicked { message }` and `Observer { error }` are the two carrying
   arbitrary text; `Feed { name }` (`observe.rs:618-621`) carries a
   subscription name.

   **Write the commit-4 correction as "overstated when written", never as "wave
   1 introduced a leak."** The second wording invites reverting a typed variant
   back to a string, which would make the exposure *less* governed. What
   `Inspect`'s allowlist uniquely gates is `LifecycleEvent::Faulted` and the
   event stream — not `EndReason` as a class.

   **`Inspect::Ended(id)` adds no information class and no availability.** A
   peer already admitted to `Inspect` reads every retained ending today.
   `EventPage` carries `events: Vec<LifecycleEvent>` (`console.rs:296`);
   `LifecycleEvent::Ended { id, why: EndReason }` (`console.rs:160-165`) is one
   of them; and `Follow(0)` hands back the whole ring — `page_after`
   (`console.rs:726-731`) collects **every** event with `seq > after`, and
   `console.rs:767-768` gates only on `after < self.seq`. So `Ended(id)`
   returns a **subset** of what `Follow(0)` already returns, to the **same
   allowlist**. It is a lookup convenience, not a disclosure, and that is a
   stronger safety argument than "widens availability" — which was the claim
   here and was false.

   Whether an ending should carry free text to an ordinary peer at all is a
   real decision with its own cost, and it is **out of this wave's scope** —
   see §15.

## 14. What I could not establish

1. **Whether `pastey`'s `:snake` emits `__status` from `__Status`, or from any
   other legal `CamelCase` ident.** The chosen name depends on it. **The
   measurement is doctest B** of §2.3 — declare `rpc __Status()` and record what
   the compiler does. It is **not** doctest A: a `compile_fail,E0592` over a
   known collision stays green either way. If a user ident *can* reach
   `__status`, §2.3's ruling stands and the chosen name must change.
2. **Whether a singleton can be wired to an `Events` sink**, and therefore
   whether `Inspect::Ended` can answer for one at all. `console.rs:17` shows the
   keyed wiring; `turns.rs:3514` names `spawn_observed` for the singleton side.
   Unverified. If it cannot, `Ended(id)` is keyed-only and the method doc must
   say so.
3. **Which anchors rows commit 2 rots.** Comes off `mise run anchors`. §4 names
   the three rows targeting `macros.rs` as likeliest and asserts nothing.
4. **How many intra-doc links break.** Not measured, deliberately. `mise run
   docs` is the only thing that sees one.
5. **Whether `mise run docs` reports a link to the new `#[doc(hidden)]`
   expansion sites.** `[tasks.docs]` records three known holes in that gate;
   a link to a `#[doc(hidden)]` item ships as a dead raw href with no warning.
   Any `[`__status`]` link written in commit 2 is in that class and must be
   checked by opening the rendered page, not by reading a green gate.
6. **Whether the served arms can intercept `__status` without a new seam.** §3's
   mechanism assumes both `Dispatch` closures — `keyed_service`
   (`macros.rs:1202-1226`) and `service()` (`macros.rs:2140-2185`) — can take an
   arm above their delivery call. That the two generated call sites are
   `macros.rs:1213` and `:2170` was measured; that an arm fits above either was
   **not**. If the served side needs a new seam, the wave's central mechanism is
   a design change rather than a match arm. **Experiment:** write both arms first
   and run `cargo check --workspace --all-targets` before anything else in
   commit 2 lands.
7. **Whether an unsupervised actor can reach `Standing::Restarting` at all.**
   §15's path needs `retryable` (`turns.rs:2760-2777`) **and** the budget
   (`turns.rs:2856`) to grant. Where `self.budget` comes from for an actor
   nobody supervised was **not read**, so how often §15's path is live is
   unmeasured. **Experiment:** read the constructor that sets `Lent`'s `budget`
   and record whether it grants on the unsupervised path. Until then §15 is an
   upper bound on reachability, and never a frequency.

## 15. Named follow-up, deliberately not solved here

**Should an ending carry free text to an ordinary peer at all?**

`Withheld::Restarting { reason: EndReason }` reaches every service's contract,
and `EndReason::Panicked` and `EndReason::Observer { fatal: false }` carry a
panic message and the `Display` of an arbitrary `dyn Error` — which is where a
path, a key name or a row id most often lives (`console.rs:41-45`). §13.7
establishes that this predates wave 1 and that wave 1 improved its governance.

**The path, for an ordinary peer on an ordinary service** — no `Inspect`, no
operator, no allowlist beyond the service's own:

1. The peer is admitted by the service's `Allowlist` and calls any method.
2. The turn panics, so the incarnation ends as `Ended::Panicked { message }`.
3. `Lent::ending` (`turns.rs:2855-2861`) asks `retryable(reason)`
   (`turns.rs:2760-2777`), which answers `true` for `Panicked`.
4. The **same line** asks the restart budget: `self.budget.spend(..)`
   (`turns.rs:2856`).
5. Both grant, so it settles `Standing::Restarting(Arc::new(reason))`.
6. The served path renders that standing as `Withheld::Restarting { reason }`,
   whose `#[error]` text interpolates `{reason}` (`observe.rs:927`). The peer's
   `err.to_string()` therefore carries `EndReason::Panicked`'s `Display`
   (`observe.rs:557`) — the panic message.

**Two preconditions must BOTH hold, and this is not "every service leaks".**
Steps 3 and 4 are a conjunction. `retryable` refuses `Stopped`, `Vanished`,
`GaveUp` and `Observer { fatal: true }` outright (`turns.rs:2764-2772`), and
`observe.rs:931-943` records the same bound from the other side: those four are
representable in the field and unreachable in it. A budget that does not grant
settles `Standing::Gone` instead (`turns.rs:2859-2860`), and the peer reads
`Withheld::Gone`, which carries nothing. **§14.7 records that the budget's own
source on an unsupervised path was not read**, so the frequency of this path is
unmeasured. State the preconditions wherever the path is stated. Without them
the follow-up sizes the wrong problem.

It is still an open question, and it is **not this wave's**. Answering it means
deciding whether `EndReason` needs a peer-facing shape that carries the variant
without the text, which is a second `EndReason` — the parallel fact
`console.rs:84-85` refuses — or whether the service `Allowlist` is the right
place to draw the line and the answer is documentation. Both have real cost, and
neither is reachable from this goal's radius.

**Do not solve it inside this wave**, and do not let commit 4's correction to
`console.rs:38-45` drift into solving it. The correction states what is true.
This section states what is still undecided.
