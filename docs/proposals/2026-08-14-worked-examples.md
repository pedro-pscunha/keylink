# Proposal: four worked examples that put the library under load

Status: proposed, 2026-08-14.

## Why

The repository has two runnable examples. `reliable_call` shows one call made
effectively-once. `durable_ledger` shows one actor that survives its process.
Both are correct and both are small.

Neither example reaches the surfaces that carry the most risk. Nothing runnable
today uses a keyed actor registry over the wire, a `feed`, a `tell`, a
`#[defer]`, a `blob`, a supervision restart, an `Allowlist` that names a peer,
or a fan-out with a stated backpressure policy. Those surfaces have unit tests.
A unit test proves that a mechanism works. It does not prove that a person can
compose four mechanisms into a program that a reader understands.

An example in this repository is not documentation. `mise run gate` runs every
example and the assertions inside it, so an example is a test that a reader can
read. Four programs that each solve a real problem will find what the unit tests
cannot: awkward types, missing accessors, rules that only a maintainer knows,
and error messages that arrive too late.

## What each example must satisfy

These rules come from the repository, not from taste.

1. The example must run in `mise run gate`. Add its command to `[tasks.examples]`
   in `mise.toml`. Nothing else enumerates the examples directory.
2. The example must terminate and exit 0. It must carry assertions. A print
   statement proves nothing.
3. The example must be deterministic. Bind every node with `Node::bind_local`,
   which uses no relay and no discovery. Sequence turns with a channel or a
   semaphore, never with a sleep.
4. A `keylink` example must compile with `--no-default-features`, or it must
   declare `required-features` in `keylink/Cargo.toml`.
5. `cargo clippy --all-targets --all-features -- -D warnings` must pass.
6. Each doc heading must fit on one line, and each doc code fence must close.
7. The file must not contain the six strings that the anchors task counts.
8. Add the file to the example list in `README.md`.
9. The example must add no dependency. `keylink` carries `tokio`, `serde`,
   `postcard-schema` and `n0-future`. It carries no `tokio-stream` and no
   `futures`. Build a `feed` stream with
   `n0_future::stream::poll_fn(move |cx| rx.poll_recv(cx))`, which is what
   `keylink/tests/feeds.rs:168` does.

## The four examples, easiest first

| # | Name | Field | New surfaces | Size |
|---|---|---|---|---|
| 1 | `payment_approval` | payment operations | `Allowlist::only`, `#[defer]`, `Reply`, `Replied` | ~200 lines |
| 2 | `firmware_depot` | device fleets | `blob`, `Blobs`, `send`, `tell`, `Transmitted` | ~220 lines |
| 3 | `match_room` | multiplayer games | keyed `Registry`, `Eviction::Idle`, `restarting`, `feed` | ~300 lines |
| 4 | `trading_desk` | markets | `Topic`, `LossyTopic`, `KeyedTopic`, actor-to-singleton calls | ~360 lines |

---

## 1. `payment_approval`

**The problem.** A treasury pays suppliers. A client requests a payment. The
treasury pays only after a second person approves the request. The client
retries when its connection drops. The treasury must never pay twice.

**The shape.** One `service!` block, served as a local actor on one node. Two
client nodes: one that the allowlist names, and one that it does not.

```rust
keylink::service! {
    service Treasury;

    /// Parked until a second approver signs. The caller waits on one call.
    #[defer]
    rpc Request(payee: String, cents: u64) -> Settlement;

    /// The second signature. Answers every request it releases.
    rpc Approve(reference: String) -> u32;

    /// What the treasury has actually paid, so a test can count executions.
    rpc Paid() -> Vec<Settlement>;
}
```

`#[defer]` is the right tool and not a decoration. An approval takes a second
person, so the handler cannot answer inside its own turn. The handler receives a
`Reply<Settlement>`, stores it, and returns `Replied`. A later `approve` turn
answers it. The caller sees one ordinary `rpc` and blocks on one stream.

`#[defer]` works on the `Actor` trait only, so this example serves a local actor
through `Servable::serve`. It mounts no registry and addresses no key, which is
what the request asked for.

**What it proves.**

- An unlisted peer cannot connect. `Allowlist::only([client.id()])` names one
  public key. The second node's dial fails, and the example prints why.
- A retry under the same `IdempotencyKey` returns the first settlement. `paid()`
  reports one payment, not two.
- The same call without a key pays twice. The example runs both and asserts the
  difference, so the key is measured rather than recommended.
- `key_state` reports `Fresh` before the call and `Done` after it.

**What it will test.** Whether a parked `Reply` and an idempotency record
compose. The record is written when the reply is sent, and the reply is sent
from a later turn. If a repeat arrives while the first request is still parked,
the caller must meet `InFlight` or must attach, and this example will show
which.

---

## 2. `firmware_depot`

**The problem.** A depot serves firmware images to devices in the field. Many
devices need the same image. A device with a broken connection must resume its
download rather than restart it. A device uploads a crash dump after a failed
boot. An operator asks a device to reboot and accepts that the message can be
lost.

**The shape.** One `service!` block, served as a concurrent `Handler`. A
`Blobs` store holds the images.

```rust
keylink::service! {
    service Depot;

    /// The image, content addressed. Two devices asking for the same channel
    /// receive the same bytes and the store holds one copy.
    blob Image(channel: String) -> Release;

    /// The crash dump, streamed up beside the request.
    send Diagnostics(device: String) -> Receipt;

    /// Fire and forget. The reply proves transmission and nothing more.
    tell Reboot(device: String) -> ();
}
```

A `blob` service must receive a store with `.with_blobs(&blobs)`, or the server
refuses at startup. This service declares a `send`, so it cannot be served as an
actor. Both facts are compile-time or startup rules that a reader meets here for
the first time.

**What it proves.**

- Two devices fetch the same channel and receive the same `BlobId`. The store
  holds one copy. Content addressing is measured by an identity, not asserted.
- A device resumes an interrupted download. `fetch_into(&store)` carries resume
  and deduplication, and `copy_to` carries neither, because `copy_to` has no
  local store. The example runs both and prints the two byte counts. That
  difference is the reason a device holds a store at all.
- `tell` returns `Transmitted`. The example states, in the same breath, that the
  device may never reboot, and it shows the `rpc` that a caller uses when the
  answer matters.

**What it will test.** Whether the grant model survives composition. Returning a
`BlobId` grants that exact `(peer, hash)` pair, and
`the_grant_covers_the_returned_address_and_nothing_else` already pins that for
one call. This example asks the harder question: a device that fetches channel
`stable`, then fetches `beta`, holds two grants at once, and `MAX_GRANTS`
evicts the oldest. The example will show what a device sees when its grant is
evicted under it.

---

## 3. `match_room`

**The problem.** A game server runs many matches at once. Each match holds two
players. A round resolves only after both players move. A player who does not
move within the round time forfeits. An abandoned match must free its memory. A
malformed move must not take the server down.

**The shape.** One `service!` block, mounted many times into a keyed
`Registry`. Each `ActorId` is a match. Two remote players address the same match
through `RegistryClient::at`.

```rust
keylink::service! {
    service Match;

    /// Parked until the opponent moves. Both callers are answered together.
    #[defer]
    rpc Move(player: Seat, choice: Choice) -> Round;

    /// The board, for an operator or a reconnecting player.
    rpc Board() -> Board;

    /// The round clock. A tick that finds one seat still parked forfeits it.
    feed Clock(tick: u64);
}
```

This is the second use of `#[defer]`, and it is a different use. In
`payment_approval` a later call releases one parked reply. Here the second
caller's own turn releases both. Two callers block on one event, which is the
case that a single-reply model cannot express.

**What it proves.**

- Two remote clients address two different matches by id and never see each
  other's board.
- A `feed` item is a turn. The clock tick runs under the same panic guard and
  the same observer chain as a call.
- `Eviction::Idle` removes a match that nobody has addressed. The example reads
  `Registry::status` before and after.
- A handler that panics does not stop the server. `restarting` rebuilds the
  actor within its limit, and the caller reads a `Failure` rather than silence.
- A restart that kills a parked `Reply` answers the waiting caller with
  `Withheld::Restarting`. That is the one loss a supervisor cannot hide, and the
  caller must be able to tell it from `Withheld::Gone`.

**What it will test.** Holding two `Reply` values across turns. The actor must
own both, answer both, and answer neither twice. `Replied` makes forgetting a
compile error. This example will show whether it makes *double* answering a
compile error as well, or only a runtime one.

---

## 4. `trading_desk`

**The problem.** Trading robots watch a market feed. Each robot decides on its
own and sends orders. One execution desk owns the exchange session and executes
every order in sequence, because the venue accepts one session. Fills return to
the robot that placed the order. A slow robot must not stall the market feed for
the others. A lost fill is not acceptable. A stale price is worthless.

**The shape.** This is the largest example, and every part of it is forced by a
rule rather than chosen.

- Robots are keyed actors in a `Registry`, one `ActorId` per robot.
- The desk is a singleton `Handle`, held locally. It is the "one session"
  constraint expressed as a type: one actor, serial turns, no second writer.
- A robot calls the desk from inside its own turn. The chain runs one way, so
  no turn is reentrant, and `submit` answers with an acknowledgement.
- **The desk must not call the robot back.** A call back would close the ring,
  and `Registry::ask` refuses any target already on the chain with
  `LocalError::Reentrant`. So fills reach the robot on a second `feed`.
- **The desk must not publish the fill from inside its own turn either.** A
  `Topic` is not an actor, so no chain reaches it, and a publish from inside a
  turn hangs under `Overflow::BlockPublisher`. `keylink/tests/cycles.rs:50-53`
  states that. The venue task publishes fills, and a spawned task starts a
  fresh chain. This is the rule that decides the whole layout.
- An operator on a third node reads a robot's position and halts it over the
  wire. `ActorRef` carries no `tell`, so `Halt` is an `rpc` that returns `()`.

```rust
keylink::service! {
    service Robot;

    /// The market. A price the robot never saw is cheaper than a stale one.
    feed Ticks(tick: Lagged<Tick>);
    /// The venue's answer, out of band because neither call nor publish can
    /// come from inside the desk's own turn.
    feed Fills(fill: Fill);

    /// What this robot holds, read by an operator over the wire.
    rpc Position() -> Position;
    /// Stop trading. An `rpc` and not a `tell`, because `ActorRef` has no `tell`.
    rpc Halt() -> ();
}
```

The market feed carries `Lagged<Tick>` and not `Tick`. A `LossySubscription`
yields `Lagged::Item` or `Lagged::Skipped(n)`, so the robot must match both.
Loss becomes a value the actor reads, rather than a silence it cannot see.

**Two streams, two backpressure policies, and the reason for each.** This is
the point of the example.

| stream | policy | why |
|---|---|---|
| market data | `LossyTopic` with `Lossy::DropOldest` | A slow robot receives the newest price and an exact skip count. The publisher never waits. A stale price is worse than no price. |
| fills | `Topic` with `Overflow::BlockPublisher` | A fill is a position change. Dropping one makes the robot's book wrong. The venue task waits instead. |

`Lossy::DropOldest` takes a `BroadcastDepth`, which refuses any depth that is
not a power of two.

**What it proves.**

- Total filled quantity equals total submitted quantity. The desk executes each
  order once.
- A deliberately slow robot loses ticks and loses no fills. The example prints
  the lag count that the lossy subscription reports.
- A halted robot submits nothing more, and the assertion is a count rather than
  a state flag.
- The operator reads a position over a real QUIC connection.

**What it will test.** The composition rules, at the one place where three of
them meet. A robot calls out from inside a turn, the desk answers in its own
turn, and a third task publishes back into the robot's feed. The reentrancy
guard covers the first edge and not the third, because a `Topic` is not an
actor and no chain reaches it.

It will also test a limit the guard does not cover. `actor/remote.rs:472-476`
builds the chain for a keyed delivery and never checks it, so **a ring closed
through a `RegistryClient` is not detected, even back to the same node.** The
in-process ring is refused, and the remote ring hangs. This example holds both
shapes side by side, so a reader meets the boundary rather than the rule.

---

## Order of work

Build them in the order above. Each one adds surfaces to the one before it, so a
failure in the later example is easier to place.

After each example:

1. Add its command to `[tasks.examples]` in `mise.toml`.
2. Add its link to the example line in `README.md`.
3. Run `mise run examples`, `mise run lint` and `mise run fmt-check`.
4. Record what the example found. A defect, a missing accessor and an unclear
   error message are all results.

The findings are the deliverable. Four programs that compile prove less than one
program that found a rule nobody had written down.

---

## What was built, and what it found

Six examples now run in `mise run gate`, and every finding below is fixed in
keylink itself.

| example | shape | what it adds |
|---|---|---|
| `payment_approval` | one file, `required-features = ["persist-file"]` | allowlist, `#[defer]`, idempotency, **durable state**, and the restart that keeps the payments and loses the replay record |
| `firmware_depot` | one file | `blob`, `send`, `tell` |
| `match_room` | one file | keyed `Registry`, two callers on one `#[defer]`, `feed`, `Eviction::Idle` |
| `trading_desk` | **a directory** | one scenario against `impl Calls`, run **in process and over QUIC**, plus two fan-out policies |
| `substation` | one file | `restarting`, and what a parked reply reads when its incarnation dies |

`trading_desk` is a directory because it holds two deployments of one scenario,
not because 239 lines is a large file. `cargo` discovers `examples/<name>/main.rs`
as example `<name>`, and a sibling directory with no `main.rs` is not a target at
all, so `#[path]` shares a module between examples.

### The local-versus-served result

The scenario is written once against `impl robot::Calls` and produces the same
numbers on both surfaces: 136 filled for the paced robot, 59 for the stalled
one, 195 at the desk. The stalled robot's split between processed and skipped is
a timing outcome and is deliberately not compared; what is compared is the
invariant under it, that the two counts sum to the number published.

### Fixes applied to keylink

| # | Finding | Fix |
|---|---|---|
| 1 | `rpc Request` / `rpc Response` fail with `ambiguous associated item` pointing at the macro | Two muncher arms and a named `compile_error!`: *"rpc `Request` collides with the `Request` enum every service generates."* A `feed` of either name still compiles, which was measured rather than assumed |
| 2 | An admission refusal is indistinguishable from a dead link without depending on `iroh` | [`Error::refusal`] returns `Option<Refusal>`, with `Denied` meaning stop and `Unavailable` meaning try later |
| 3 | A received `BlobId` had no public route to a fetch | `BlobHandle::new` is public and documented, including that an address is not a capability |
| 4 | Every `NonZeroUsize` knob costs an `unwrap` a careful crate cannot write | `keylink::depth!(4)`, where `depth!(0)` is a compile error |
| 5 | `Registry::stop` discarded its `Option<Ended>` silently | `#[must_use]`. It immediately found six real discard sites, including two in `keylink/tests/actors.rs` |
| 6 | `Transmitted` printed `Transmitted { _private: () }` | Hand-written `Debug` on both `Transmitted` and `Queued` |

### Finding 7, found while splitting `trading_desk`, and still open

`Calls` is implemented for `Handle`, `ActorRef`, `Client` and
`IdempotentClient`, and **not** for `LocalRef`. So the one surface a
surface-generic function cannot reach is the in-process *keyed* actor. That is
why `trading_desk`'s local deployment uses singletons rather than a local
registry, and the file says so.

### Finding 1 — `rpc Request` and `rpc Response` do not compile

```
error: ambiguous associated item
note: `Request` could refer to the variant defined here
```

The error points at the `service!` invocation and not at the declaration line,
and it is a `future_incompatible` deny rather than an ordinary error. The
reserved-name section at `keylink/src/macros.rs:144-151` lists `__Status`,
`Served`, `Feeds::at` and Rust keywords. It does not list these two.

Measured: `Request` and `Response` both fail. `Client`, `Handler`, `Actor`,
`Feeds`, `Registry` and `State` all compile clean as `rpc` names. So the
collision is exactly the two generated enum names, and the fix is one more line
in the reserved-name list plus a `compile_error!` that names the declaration.

### Finding 2 — an admission refusal is indistinguishable from a dead link

An unlisted peer's first call fails with `transport: connection lost`. The dial
itself succeeds, because admission runs one layer above the handshake, so a live
connection is not proof of access.

Telling "the policy refused me" apart from "the network died" means walking the
error's source chain to an `iroh::endpoint::ConnectionError::ApplicationClosed`
frame and comparing its reason bytes. `keylink/src/lib.rs:363` re-exports
`Router`, `EndpointAddr`, `EndpointId` and `SecretKey`, and not
`ConnectionError`. A downstream crate must therefore depend on `iroh` directly
to act on the difference. `payment_approval.rs` does exactly that and says so.

### Finding 3 — a `BlobId` travels, and only a hidden constructor spends it

`BlobId` is public and `Serialize`, so one peer can hand another a content
address out of band. The only way to turn that address back into a fetch is
`BlobHandle::new`, which is `#[doc(hidden)]` and documents itself as "Public
only because generated `blob` client code names it." There is no supported
public route from a received `BlobId` to a `BlobHandle`.

### Finding 4 — `Transmitted`'s `Debug` shows a private field

`println!("{sent:?}")` on a `tell`'s proof prints `Transmitted { _private: () }`.
A hand-written `Debug` that prints `Transmitted` would cost two lines.

### What held up under load

Nothing else broke. The reentrancy rules, the two fan-out policies, the keyed
registry, `#[defer]` with two callers on one event, eviction, and the blob
deduplication all behaved exactly as documented, and the numbers are
hand-checkable — `trading_desk` fills 136 for the paced robot, which is
`1 + 2 + … + 16`, and the stalled robot's 5 processed plus 11 skipped is the
same 16.
