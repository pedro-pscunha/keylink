# Bolder than the goal: a second pass on `2026-08-09-one-service-one-trait.md`

Read-only. Repo `/Users/pedrocunha/repos/keylink`, `main` at `0119504`.
Every claim below cites `file:line`. Where I could not run a mechanism, I mark the
idea **UNVERIFIED**.

---

## 0. The headline, before the detail

Three findings change what the plan must do. Read these first.

1. **`Withheld` is already a type in `turns.rs`.** `turns.rs:866` declares a
   private `enum Withheld { Vetoed { name, fatal }, Panicked { name } }`. Its
   `Panicked` arm means **the observer panicked** (`turns.rs:1020`, `1038`). The
   goal's public `Withheld::Panicked` means **the handler panicked**. Two enums,
   one file, one shared variant name, inverted meaning — and that exact confusion
   is what `RemoteError::OBSERVER_PANICKED` was added to delete
   (`reply.rs:163-170`). RFC-06 records the private one at
   `docs/rfc-06-observers.md:1243`, so the name is on an append-only record.
   **One of the two must be renamed before any code is written.**

2. **`Failure` does not exist.** The goal's trait bound is
   `type Error: Into<Failure>` and its motivating example returns
   `Result<u64, Failure>` (goal lines 161, 170). `rg 'pub (enum|struct|type)
   Failure\b'` over `keylink/`, `demo/` and `keylink-persist/` returns nothing.
   Worse, the only type that satisfies the bound is a **union of `Undelivered`
   and `Error`** — which the goal's own Invariants forbid at line 267: *"Trait
   error is an associated type, not one unified enum."* **The goal contradicts
   itself, and the contradiction is on the one line the whole goal exists for.**
   §3.1 gives two honest resolutions.

3. **The lifted constraint bankrupts three DELIBERATE rows, not several.** Of the
   seven `D` rows in `seam.md`, three fall (L3, L4, L8). Four survive, and three
   of those four were never bought with the hash at all (§1). The large harvest
   from the lifted constraint is not in the `D` column — it is the **whole prefix
   regime** plus `AtCapacity`, which the goal already claims as items 9 and 10.

---

## Ranking

| Rank | Item | Where |
|---|---|---|
| **must close** | Rename one of the two `Withheld` enums | §0.1 |
| **must close** | Define `Failure`, or drop it and bound on `std::error::Error` | §3.1 |
| **must close** | Delete the five prefixes; type every `Failed` keylink builds | goal 9, 10 |
| **must close** | `deliver_to_local` → `deliver_to_singleton` (N5) | §2.1 |
| **must close** | `NodeStatus::lost` → `NodeStatus::dropped`, not the other way | §2.2 |
| **must close** | Put `__status` and `__ended` in `WireRequest`, not the service contract | §3.2 |
| **should close** | `L3`/`L4`/`L8` — peer-reachable `stop`, `abort`, ending reason | §1.1 |
| **should close** | Local `send`/`recv` via a second `BodyReader` arm, **not** a `FrameReader` rewrite | §2.3 |
| **should close** | `at` becomes `KeyedClient::at` | goal 6 |
| **should close** | Extend the twin guard to `NotQueued::Closed` (N3) | §2.5 |
| **should close** | Rename `Undelivered`; its first doc line is false of 4 of its 7 variants | §2.4 |
| **should close** | `#[diagnostic::on_unimplemented]` on the generated trait | §2.6 |
| **should close** | A peer cannot tell "the factory refused me" from "no factory" | §4.2 |
| **could close** | `blob`: trait returns `BlobId`; `fetch` becomes its own door | §3.5 |
| **could close** | Blocking `send` (cheap). Blocking `recv`/`blob` cost two new types | §3.6 |
| **could close** | Merge `Undelivered` into `Withheld` entirely — a real fork, argued both ways | §2.4 |
| **would be a lie** | Keyed `tell` (goal item 4) — the name, not the capability | §3.3 |
| **would be a lie** | `IdempotentClient` folded into a field (goal item 6) | §3.4 |
| **would be a lie** | `status()` returning `ActorStatus` on a `Handle` | §3.2 |
| **would be a lie** | `ended()` returning `Ended` remotely | §3.2 |
| **would be a lie** | A peer door for `System::contains` | §2.7 |
| **not a defect** | `ServableAsActor::CHECK` at `svc::service` (goal item 12) | §5.1 |
| **not a defect** | `SpawnableUnfed` "same shape" (goal item 12) | §5.1 |
| **leave** | `L11`, `R3`, `R4`, `R16`, N6, N7, N8 | §1.2, §2.8 |

---

## 1. Every DELIBERATE row, re-audited against the lifted constraint

### 1.1 The three that fall

**L3 — `stop` has no peer door. L4 — `abort` has no peer door.**
The argument, quoted from `seam.md:120`: *"A peer never holds the actor's
lifetime."* `Server::shutdown` stops accepting and does not drop the actor
(`macros.rs:2219-2226`).

**It does not survive.** The sentence is a statement of the current shape, not a
reason for it. The `Allowlist` already gates per method (`macros.rs:2140`,
`admission.rs:318`), so authority to end an actor is expressible today. The only
real cost was the wire, and the wire is now paid for.

What closing it buys: an operator with a client can drain and replace an actor
without shell access to the serving host. What it costs: one `WireRequest` arm
each, and a deployment where a mis-scoped `Allowlist` lets a peer stop
production. **State that risk in the rustdoc.** The goal covers this at item 8.

One split the goal does not make: `shutdown` and `abort` reach **both** surfaces,
because the connection task is separate from the actor task — `Handle::serve`
puts a handle clone inside the server (`macros.rs:2238-2240`), so the server
answers after the actor ends. `evict_idle` reaches **only** the keyed surface,
because it is a `System` method and a singleton has no registry. Item 8 lumps
three verbs that split two ways.

**L8 — a peer cannot learn why the actor ended.**
The argument, quoted from `reply.rs:177-180`: *"A peer has no `ended` to fall
back on, so this prefix is the whole of what it learns."*

**It does not survive, and `seam.md:378` already says why**: that sentence is an
observation of the consequence, not a defence of it. The mechanism is built:
`EndReason` (`console.rs:184`) is a wire-ready flattening of `Ended` with one
conversion and no wildcard arm (`console.rs:230-252`). Closing it costs one
`WireRequest` arm and reuses an existing type. The goal covers this at item 7 —
but it puts the door in the wrong place and names the wrong return type. See
§3.2.

### 1.2 The four that survive, and three of them never depended on the hash

**L11 — mailbox depth is not settable by a peer.** RFC-04 §6.2
(`docs/rfc-04-actors.md:826`). The RFC sentence *"nothing to configure"* is
already overtaken locally: `SystemBuilder::mailbox_depth` exists
(`actor/registry.rs:2138`) and `actor_deep` takes a depth (`macros.rs:1714`). So
the RFC is stale. **The refusal still survives, on a different ground**: depth is
a serving-side deployment decision, like a pool size. A peer that sets it
degrades every other caller of that node. Leave it. Note in the RFC append that
the reason changed.

**R3 — no local idempotency key.** `macros.rs:2640`: *"The key is a transport
concern rather than a domain one."* There is no transport in one process.
**Survives on physics.** Reclassify **D → E**.

**R4 — no local `key_state`.** `macros.rs:2666-2668`. The ledger is scoped to the
connection it arrived on (`reply.rs:20-23`). No connection, no ledger.
**Survives on physics.** Reclassify **D → E**.

**R16 — no keyed `tell`.** The refusal at `macros.rs:3705-3716` names four
refusals decided **after** the bytes reach the wire, all reported on the reply.
**The hash appears nowhere in that argument.** The comment even records that the
old ALPN-shaped reason is already dead. So the lifted constraint does not touch
it. It survives exactly as written, and goal item 4 overturns it with the
mechanism the refusal already prices. See §3.3.

### 1.3 The half-row that falls, and it is load-bearing

**L2's `D` half — a local caller has no value form of a fault.** The stated
reason is completeness of `Undelivered` (`macros.rs:3842-3845`).

`Withheld` dissolves that reason: once `Withheld::Panicked { message }` exists,
the value form is in the type and the argument is spent. The mechanism is
already built — `LocalActor::ask_catching` answers `TurnLost::Faulted(Panic)`
(`turns.rs:3941-3944`) and the generated `Handle` does not expose it.

**The goal calls exposing `ask_catching` "adjacent, cheap" (item 12). It is
not adjacent — success criterion 2 depends on it.** That criterion asks for
`Withheld` to be byte-identical on both sides "for all seven conditions". A
local `Handle::ask` **resumes** the panic (`turns.rs:3905`, `panic.rs:97`), so
`Withheld::Panicked` has no local producer through that door. Only
`ask_catching` produces it. Move item 12's `ask_catching` clause into item 1.

### 1.4 What the lifted constraint really buys

RFC-06 §6's surviving bullet, quoted from `docs/rfc-06-observers.md:630-635`:

> ~~No new `RemoteError` variant, because that moves the wire contract.~~ **The
> rule survives; the reason it was listed does not.**

Read the mechanism: the reason it was listed **was** the wire cost. The bullet
records that Wave 9 met the constraint by a prefix rather than by argument. That
is a budget note, not a principle. **With the budget granted, this bullet is
bankrupt in full**, and with it:

- the five prefixes (`reply.rs:154`, `161`, `181`, `189`, `206`);
- `AtCapacity` as a `Failed` string — the argument at `actor/remote.rs:345-348`
  is two clauses, and its second clause is *"Changing that would move every
  service's ALPN"*. Bankrupt. Its first clause (*"not something the caller can
  act on differently"*) is **false**: a caller backs off on `AtCapacity` and
  never retries on a malformed id;
- the malformed-`ActorId` refusal (`actor/remote.rs:434`);
- the console subscription refusal (`console.rs:1031`);
- `FollowRefused::{Ahead, Crowded}` flattened to text.

Goal items 9 and 10 claim all of these. Agreed, and this is the largest true win
in the goal.

**One measurement that changes how you sell it.** `rg 'starts_with'` over
`keylink/ demo/ keylink-persist/` finds exactly **two** live matches on a served
failure, and both are inside guards (`turns.rs:4860`,
`actor/remote.rs:808`). No production call site in this workspace matches a
prefix. So deleting the prefix regime buys this tree almost nothing and buys
**downstream peers** everything. Success criterion 3 will pass the moment the two
guards are deleted, which is not evidence the work landed. Say so in the ledger.

---

## 2. What neither the audit nor the goal proposes

### 2.1 `deliver_to_local` is the served path (N5) — must close

`turns.rs:4680` answers **a peer's** request on the singleton path. Its twin is
`actor/remote.rs:425`'s `deliver_to_actor`, the keyed path. Both are served. The
name says the opposite of what the function does, and its own body proves it:
`Origin::peer(cx)` at `turns.rs:4704`.

Rename to `deliver_to_singleton` and `deliver_to_keyed`. Both are
`#[doc(hidden)] pub`, so no user code names them. Cost: one identifier carve-out
edit per break record that names them. This is the cheapest true finding in the
audit and the goal omits it.

### 2.2 `NodeStatus::lost` is worse than "two counters, one name" — must close

Goal item 11 says two counters share one name and asks for a rename. Read the
mechanism at `console.rs:307-317`, `345-352` and `353-367`. There are **three
names for two quantities, cross-wired**:

| Quantity | Named | Named |
|---|---|---|
| Never sequenced — `Events::record` found the channel full | `NodeStatus::lost` (317) | `EventPage::dropped` (367) |
| Sequenced, then pushed out of the ring before this console read it | — | `EventPage::lost` (352) |

`EventPage::dropped`'s own doc says *"The same reading `NodeStatus::lost`
returns"* (`console.rs:356`). So the collision is not symmetric, and the rename
has only one correct direction: **`NodeStatus::lost` → `NodeStatus::dropped`.**

Renaming `EventPage::lost` instead breaks the loop `Console`'s documentation
tells a reader to write — `if page.lost > 0 { .. }` (`console.rs:334`) — and
leaves `EventPage` disagreeing with itself. The goal does not name a direction.
Name it.

### 2.3 Local `send`/`recv` without rewriting the transport — should close

Goal item 3 buys local `send`/`recv` by generalising `FrameReader` over
`AsyncRead`. The crate priced that option itself and called it a rewrite *"to
give a local caller a convenience"* (`macros.rs:3947-3949`).

**The price is worse than the goal states.** `FrameReader` holds a `RecvStream`
by value (`wire.rs:178-181`), and public `BodyReader` holds a `FrameReader`
(`body.rs:135-139`). `BodyReader` is a handler parameter and a `recv` return
type, so it cannot take a generic parameter without that parameter reaching every
handler signature. The only way out is type erasure inside `FrameReader`, which
puts a virtual call on the **success path of every remote call**. The goal
forbids performance work at line 113. This change would spend performance, in
the hot path, silently.

**There is a cheaper mechanism, and it costs the remote path nothing.** Give
`BodyReader` a second arm:

```rust
enum Frames { Wire(FrameReader), Local(Body) }   // BodyReader.frames
```

`Body` already holds `Source::Bytes(Bytes)` or
`Source::Reader(Pin<Box<dyn AsyncRead + Send>>)` (`body.rs:37-43`), so a local
`send` moves the caller's `Body` straight into the handler's `BodyReader` with no
transport, no buffering and no new generic. Backpressure is real, because the
handler pulls. A 4 GiB upload is never resident, so the crate's own objection at
`macros.rs:3945-3947` does not apply to this construction.

Cost: one enum arm and one `match` per `next_chunk`. Blast radius: `body.rs`
only. **UNVERIFIED** — I did not compile it.

This also forces a re-argument the goal must not skip. `macros.rs:3952-3953`
says *"Streaming is a transport capability, and this handle names no
transport."* A `Body::from_reader(File)` drained by a local handler is streaming
with no transport, so that sentence becomes false. Append the re-argument to
RFC-04. Do not delete the sentence quietly.

### 2.4 `Undelivered`'s own first line is false of four of its seven variants

`turns.rs:3146`: *"A local call that never entered the mailbox, and why."*

Check it variant by variant against each variant's own doc:

| Variant | Entered the mailbox? | Line |
|---|---|---|
| `Gone` | no | 3191 |
| `Dropped` | **yes, and it ran** | 3202 |
| `Restarting` | **yes** | 3214 |
| `Refused` | **yes — "It ran and was not committed"** | 3229 |
| `ObserverPanicked` | **yes — "It ran"** | 3257 |
| `Expired` | no — "It never started" | 3272 |
| `Reentrant` | no | 3287 |

Four of seven. This is N2 recurring: the type was renamed once already because
its name outgrew its variants (`turns.rs:3153-3160`), and it has outgrown the
replacement in the same way. The doors ask *"did I get a value"*
(`turns.rs:3295-3296`), so the honest name is about the answer, not about the
mailbox. **Rename it while the contract is moving.** It is a local type, so this
costs no ALPN.

**And now the fork the lead asked about: can one of the three enums disappear?**

After the extraction the shapes are:

- `Withheld` = 7 conditions, on the wire
- `Undelivered` = `{ Turn(Withheld), Reentrant }`
- `NotQueued` = `{ Closed, Reentrant }`
- `TurnLost` = 8, local, carries `Arc<Ended>` and `Panic`

`NotQueued` **cannot** go. Its doors have no reply channel — `stop` sends a
request that is not a turn and `notify` drops the receiving half before it sends
(`turns.rs:3300-3305`). Merging it would put all seven `Withheld` conditions on a
public `stop`, which is the representable-but-impossible defect the goal exists
to remove. The argument gets **stronger** after the extraction, not weaker.

`TurnLost` **cannot** go either, and the crate already settled the pattern.
`TurnLost` differs from `Withheld` in exactly two places: `Restarting(Arc<Ended>)`
against a rendered `String`, and `Faulted(Panic)` against `Panicked { message }`.
Both differences are "the local side holds the live object, the wire holds its
rendering". That is precisely `Ended` against `EndReason`, and `console.rs:170-182`
argues the pattern is correct and names what keeps the two from drifting: one
conversion, no wildcard arm, an exhaustive source. **Add
`From<&TurnLost> for Withheld` in that exact shape.**

`Undelivered` **can** go, and here is the mechanism. If `Withheld` absorbs
`Reentrant`, then `Undelivered ≡ Withheld` and `ask` returns
`Result<T, Withheld>`. The objection is that a peer can never receive
`Withheld::Reentrant`. Read `turns.rs:4696-4702`: the re-entrancy arm of the
served path is written as the general case **on purpose**, against the day a
local door delivers a served call inline, and the alternative is called *"a hole
that door falls into silently"*. The crate has already accepted an unreachable
wire-adjacent variant twice, for that reason, in `TurnLost::Expired`
(`observe.rs:721-726`) and `Undelivered::Expired` (`turns.rs:3265-3271`).

Two consequences make the merge attractive. It deletes a public type instead of
adding one. And the twin guard
`every_turn_lost_except_faulted_has_one_undelivered_twin_with_the_same_words`
loses its exception: `Withheld::Panicked { message }` **is** `Faulted`'s
flattening, so the guard covers 8 of 8 instead of 7 of 8.

**My recommendation is still to keep the wrapper**, and the reason is a
distinction the merge destroys. `Reentrant` is a caller **defect** — you
deadlocked yourself, and the fix is to edit your code. The seven are operational
**conditions** — the actor panicked, an observer refused, a supervisor is
restarting. One enum holding both tells a caller to `match` a bug beside an
outage. That is the same argument `NotQueued`'s doc makes about `stop`
(`turns.rs:3300-3305`), applied one level out.

So: **three enums stay, `Withheld` is a fourth, and the win is four conversions
with no wildcard arm** rather than fewer types. Rename the wrapper (above) and
the shape reads honestly.

### 2.5 Extend the twin guard to the third enum (N3) — should close

`turns.rs:3366-3374` says one sentence lives on three enums and the guard holds
**two** of the three, with the third *"kept by hand"*. `NotQueued::Closed` is the
one outside. Extend the guard. The goal names the guard in Context (line 30) as a
thing to reuse and never says to widen it.

### 2.6 Make the compiler say what a user must otherwise know — should close

The lead asked for anything a user must *know* rather than be *told*. This is the
biggest one, and it is cheap.

`rust-version = "1.91"` (`keylink/Cargo.toml:9`), so
`#[diagnostic::on_unimplemented]` is available. Today the crate has zero uses
(`rg 'on_unimplemented' keylink/src/` is empty) while it carries two hand-built
compile-time refusals with long messages — `ServableAsActor::CHECK`
(`macros.rs:645-656`) and `SpawnableUnfed::CHECK` (`macros.rs:674-684`).

That machinery becomes necessary again the moment the trait lands, because **not
every surface can implement the generated trait**:

- `Handle` cannot, for a service declaring `send`/`recv`, unless §2.3 lands.
- `blocking::Client` cannot, for a service declaring `recv` or `blob`
  (`macros.rs:3095-3100`).

A missing impl is already a compile error. `#[diagnostic::on_unimplemented]`
turns it into a sentence that names the physical reason. Put the same words the
two `CHECK` constants already carry.

**On "one trait" itself.** A single trait plus a conditional impl needs the
`macro_rules!` expansion to branch on whether the kind list contains `send` —
`macro_rules!` can fold a `const bool` (`macros.rs:622-623`) but cannot emit an
`impl` from one. **UNVERIFIED** whether a sealed-bound trick expresses it.

The shape that is certainly expressible with the machinery already in the file is
**two traits**: `svc::Calls` (`rpc`, `tell`, `blob` — every surface) and
`svc::Streams` (`send`, `recv` — the two remote surfaces). Per-kind macro arms
emit each method into one trait or the other, which is exactly how
`__keylink_local_method!` already dispatches by kind (`macros.rs:3821`, `3863`,
`3906`, `3954`). It costs the goal its title and buys a caller a **more** precise
bound. State the trade rather than forcing one trait and discovering it cannot be
emitted.

**One limit to write down either way.** The trait uses `-> impl Future` in return
position, so it is **not object-safe**. `Box<dyn Orders>` is impossible. A caller
switching local against remote from configuration at run time cannot use this
trait. A caller switching at compile time — the goal's stated case, local in a
test and remote in production — can. Put that sentence in the trait's rustdoc.

### 2.7 One thing a bold sweep will grab and must not — would be a lie

`System::contains` (`actor/registry.rs:1711`) and `System::len` (`1717`) have no
peer door, and `seam.md:322-323` classes both **I**. A bold reading says close
them.

**Do not.** `reply.rs:79-81` refuses the distinction on a ground that has nothing
to do with the hash: *"Deliberately does NOT distinguish 'never existed' from
'was stopped' ... telling them apart would leak that an id was once real."* A
`__contains(id)` door leaks exactly that. `__len()` is defensible on its own
(it names no id), but it is a fleet-size disclosure to any allowed peer. Leave
both, and record why in the same append that closes item 8 — otherwise the next
sweep re-proposes them.

### 2.8 The naming findings, ruled one by one

| # | Ruling | Reason |
|---|---|---|
| N1 | close — goal 13 | The argument names `MailboxClosed`, a type that no longer exists |
| N2 | close — §2.4 | And it is live again, not historical |
| N3 | close — §2.5 | Widen the guard, do not rename |
| N4 | already fixed | `macros.rs:2061-2083` records the correction in the past tense |
| N5 | **must close** — §2.1 | The name states the opposite of the mechanism |
| N6 | **leave** | Four names for four different facts. Merging them is the collapse. Write one doc table instead |
| N7 | **leave** | `ended` asks about the actor, `watch` asks about the connection. Two questions |
| N8 | **leave** | `close` is a connection, `stop` is graceful, `abort` is destructive |
| N9 | see §3.3 | `tell` means three things and goal item 4 adds a fourth |
| N10 | see §3.5 | Unify by splitting the question, not by unifying the return type |
| N11 | close — goal 9, 10 | The prefixes are variants wearing a `String` |
| N12 | close — goal 9 | Same move, and `AtCapacity` needs `u64`, not `usize` |

---

## 3. Where the goal is wrong to close something

The user made truthful the binding half. These are the places the goal trades it.

### 3.1 `Failure` — the trait bound contradicts the goal's own invariant

Stated at §0.2. The resolution matters, so here are the two honest ones.

**(a) The plain one.** `type Error: std::error::Error + Send + Sync + 'static`,
and the generic function returns `Result<u64, Box<dyn Error + Send + Sync>>`.
Generic code genuinely cannot know what can fail, and saying so in the type is
the truth RFC-04 §5.5 states. Adds no type to the crate. Costs the generic caller
every ability to `match`.

**(b) The one I recommend.**

```rust
pub enum Failure {
    Withheld(Withheld),                       // the conditions both sides share
    Other(Box<dyn Error + Send + Sync>),      // everything only one side has
}
```

The seven shared conditions stay typed and matchable. Everything else stays
opaque, so **no impossible arm is forced on anybody**. A local caller keeps
matching `Undelivered` on the inherent method and never sees `Failure` unless it
asks to be generic. This satisfies the goal's invariant at line 267 literally:
the trait error is still an associated type, and `Failure` is only the target of
`Into`.

Either way, **the goal must not ship the current text.** `Into<Failure>` with no
`Failure` is an invented mechanism.

### 3.2 The two reserved doors: right idea, wrong home, wrong types

**Wrong home.** Goal item 7 puts `__status()` and `__ended()` *"on the service's
own contract"*. The crate has already written the rule against that, and it has a
precedent that does the opposite. `reply.rs:15-23`:

> That question has no handler, no body and no layer to run through — it is a
> read of keylink's own bookkeeping — so it is a second arm of the request type
> rather than a method on the user's service. Keeping it here means a service's
> `Request` enum stays exactly the methods its author wrote.

`WireRequest::KeyState(IdempotencyKey)` (`reply.rs:231`) is that second arm.
`__status` and `__ended` are the same kind of question: keylink's own
bookkeeping, no handler, no body. Put them in `WireRequest` and `WireResponse`.
Consequences of getting this right: a service's `Request` enum stays the author's
methods; the two doors do not run the caller's `Middleware`; and a reserved
`__`-prefixed method name cannot collide with a user's own declaration.

**Wrong type, one.** Goal line 164 declares
`fn ended(&self) -> impl Future<Output = Result<Ended, Self::Error>>`. `Ended`
holds `Arc<dyn Error + Send + Sync>` (`observe.rs:477`, `538`). A trait object is
not data, so `Ended` has no `Serialize` and no `Schema` — `console.rs:173-176`
says exactly this. **The signature is unimplementable for `Client`.** It must
return `EndReason`.

That has a consequence the goal must decide: the local `Handle::ended()` returns
the richer `Ended` (`macros.rs:2378`) and returns it **infallibly**, with no
`Result`. If the trait method is also called `ended`, the same call spelled two
ways yields two types, because Rust resolves the inherent method first.
**Give the trait method a different name** — `ending()` — or accept the footgun
in writing.

**Wrong type, two — and this one is a lie.** Goal line 163 declares
`fn status(&self) -> Result<ActorStatus, Self::Error>`. `ActorStatus` has four
fields (`actor/registry.rs:1189-1241`): `id: ActorId`, `idle_for: Duration`,
`busy: bool`, `buffered: u64`. A singleton `Handle<A>` **has no `ActorId`** —
`deliver_to_local` uses the service name as a stand-in and says so at
`turns.rs:4727-4729` — and it has no last-routed clock, because `idle_for` is a
registry reading tied to `Eviction::Idle::after`. Implementing this on `Handle`
means fabricating two of four fields.

The honest shape is a narrower shared type:

```rust
pub struct Liveness { pub busy: bool, pub buffered: u64 }
```

with `ActorStatus` keeping `id` and `idle_for` beside it. Both facts are real on
every surface.

And drop `is_running` from the trait entirely. Remotely, a stopped actor answers
`NoSuchActor` (`turns.rs:4730-4737`), never `running: false` — so a `false` can
never arrive and the field is representable-but-impossible. **The `Result` is the
liveness answer.** Make `Handle::status()` return `Err(Gone)` when
`is_running()` is false, and the two sides agree for a real reason.

### 3.3 Keyed `tell` — the capability is right, the name is a lie

`RpcClient::tell` opens a **unidirectional** stream, writes, finishes
(`rpc.rs:2178-2188`). There is no return path. So a remote `tell` is
structurally incapable of learning anything, and `Ok(())` means only that the
bytes reached QUIC.

For a keyed `tell` to return a routing acknowledgement it must use a
**bidirectional** stream, which is what an `rpc` does. So goal item 4 generates
an `rpc` and names it `tell`. The refusal already priced exactly that trade and
chose the other side (`macros.rs:3705-3716`):

> A caller who wants fire-and-forget to a keyed actor writes an `rpc` returning
> `()` and drops the result, which is honest about the round trip it pays for.
> Absent and documented, rather than present and lying.

`tell` would then mean four things: queued and ordered (local), written to a uni
stream (remote unkeyed), a full round trip (keyed) — and N9 already calls three a
finding.

**Close the capability under a different verb.** Give `ActorRef` a method whose
name says it waits — `deliver` — returning `Result<()>`. A keyed caller then
learns the four post-wire refusals the comment lists, which is the whole value,
and no existing `tell` changes meaning.

If you want one uniform verb in the trait, the uniform semantic is *"the message
was accepted into the mailbox"*. Local `tell` already answers that. The remote
unkeyed `tell` is the odd one out, and making it uniform costs it a round trip —
a latency regression the goal forbids at line 113. So: **`tell` stays out of the
generated trait, and `deliver` goes in on all four surfaces.** That closes more
than item 4 does and it lies about nothing.

### 3.4 Folding `IdempotentClient` into a field re-introduces a fixed defect

Goal item 6: *"fold `IdempotentClient` into `Client` (idempotency is a field, not
a type)"*. `macros.rs:2707-2718` records what the type is for:

> Its methods are `Client`'s, **minus every `tell`**: a key promises the work
> runs at most once, and the ledger keeps that promise by recording the reply and
> answering a repeat with it, so a method with no reply has nothing to record and
> could only discard the key.
>
> It used to be `Client` itself, and `client.with_idempotency(key).some_tell(..)`
> therefore compiled and sent no key at all.

`IdempotentClient` is a **typestate**, and `rust-illegal-states` §3 — a skill the
goal itself loads at line 45 — is the name for it. A field restores the silent
key-discarding call.

**The honest way to delete the type.** Make the state a type parameter rather
than a second name:

```rust
pub struct Client<K = Unkeyed> { inner: .., _k: PhantomData<K> }
impl Client<Unkeyed> { /* tell methods emitted here only */ }
impl<K> Client<K>    { /* rpc, send, recv, blob */ }
```

`client.with_idempotency(key)` returns `Client<Keyed>`, which has no `tell`, and
the compile error survives. This satisfies the goal's own success criterion
literally — `rg 'pub struct IdempotentClient' keylink/src/macros.rs` is empty —
while keeping the guarantee. Cost: a generic parameter in every `Client` mention,
and two trait impls instead of one. **UNVERIFIED** in `macro_rules!` expansion,
but the per-kind arms that already emit `tell` separately
(`macros.rs:3863`) are the hook.

`at` becoming `KeyedClient::at` is mechanical and correct. Keep that half.

### 3.5 `blob` returning one type — unify by splitting the question

Goal item 3 asks for one `blob` return type. Both directions lie:

- `BlobHandle` locally would name a holder node that does not exist.
  `macros.rs:3911-3915`: *"a handle is an address plus the node that holds it,
  and in this process the holder is you. No grant is recorded either, because a
  grant is permission for a peer and there is no peer here."*
- `BlobId` remotely throws away the grant and the holder, which are the whole
  reason a peer can fetch.

The difference is not the return type. It is that a remote caller needs a second
step and a local caller does not. **Split the question**: the trait method
answers `(R, BlobId)` — the address, which both sides genuinely have — and the
remote surfaces expose `fetch(&self, id: BlobId) -> BlobHandle` as its own door.
One method, one meaning, on every surface, and the extra remote step is visible
instead of hidden in a type. Ranked **could close**, because it moves a public
return type for a naming win.

### 3.6 Blocking 5/5 pulls against item 6

`blocking::Client` omits `send`, `recv` and `blob` (`macros.rs:3095-3100`), and
the stated reason for `blob` is *"a synchronous wrapper would either buffer the
whole blob or hand back a handle that only works inside a runtime."*

- **`send` is cheap.** It takes a `Body`, which is a value (`body.rs:29-31`), and
  the whole operation completes inside `self.runtime.run(..)`. Close it.
- **`recv` and `blob` are not.** Both hand back a handle — `BodyReader`,
  `BlobHandle` — that is only drivable inside a runtime. Closing them honestly
  needs `blocking::BodyReader` and `blocking::BlobHandle`, each holding the
  runtime. That is **two new types**, against item 6's collapse of the client
  types (6 → 4, re-derived at §7.5). The goal states both targets and never
  states that they collide.

Recommendation: close blocking `send`. Leave `recv` and `blob` absent, and let
the missing trait impl plus `#[diagnostic::on_unimplemented]` say why (§2.6).
Item 5's "2/5 → 5/5" becomes "2/5 → 3/5, and the two absences are compiler
errors with reasons".

---

## 4. The two the goal declares impossible

### 4.1 `with_state` remotely — genuinely closed

The closure is `FnOnce(&A) -> T`, synchronous, over a borrow of the state
(`macros.rs:2275-2282`, doc at `2257-2261`). There is no `A` on the peer's
machine and a Rust closure is not data. That half is physics, and the goal is
right.

I checked three near-misses and all three collapse:

1. **A serializable projection door.** Any projection a peer can name must be
   declared in advance, and a declared projection with a name and a return type
   **is an `rpc`**. `with_state`'s own doc already gives that answer for the
   mutating case: *"Use a declared method for that"* (`macros.rs:2260-2261`).
2. **A `#[project]` attribute generating a `Snapshot` type plus a reserved rpc.**
   New machinery, new hashed entries, and it collapses to (1) with extra syntax.
3. **Sending the closure as bytes** — a wasm module or an expression language.
   That is an interpreter inside keylink, and it would take `&Snapshot`, not
   `&A`. Refuse.

**Ruling: closed. One action, not zero** — `with_state`'s rustdoc gives the
"declare a method" answer only for the mutating case. Extend the same sentence to
the projection case, so a peer-side reader is not left with a bare "impossible".

### 4.2 `mount` remotely — closed as stated, and the goal understates both halves

`System::mount` takes `Mounted<M>` (`actor/registry.rs:1486`), a spawned local
task. A peer cannot send a running task. Correct.

**The goal understates what already works.** `SystemBuilder::spawn_with`
(`actor/registry.rs:1888`) builds an actor **by id, lazily, on first message**.
So creation-by-id over the wire is not a near-miss — it ships today. A peer calls
`at(client, id).some_rpc(..)` and the factory builds the actor. Say that in the
residual documentation, because "mount is impossible" reads as "a peer cannot
cause an actor to exist", which is false.

**And it misses a real gap on that same path.** `deliver_to_actor` answers
`NoSuchActor` for *"nothing is mounted and no factory built one"*
(`actor/remote.rs:415-416`). Those are two facts. A factory that returned `None`
for this id **refused** the caller. No factory at all means the node does not
create actors lazily. A peer acts differently on each: fix the id, against stop
asking. Today both arrive as one variant.

Closing it is one more arm on the single authorised move, and it removes an
inference rather than adding a feature. **Ranked should close, and the goal does
not list it.**

Caution: keep it away from `NoSuchActor`'s privacy refusal (§2.7). *"The factory
declined to build this id"* leaks nothing about whether the id was ever real. It
is a statement about the factory, not about history.

---

## 5. Two claims in the goal that the tree does not support

### 5.1 Goal item 12's first two clauses are non-defects

**Claim:** *"`ServableAsActor::CHECK` fires at three of four doors (a service is
refused as an actor and accepted as a handler)."*

`ServableAsActor::<A>::CHECK` is instantiated at **four** sites: `spawn`
(`macros.rs:1383`), `spawn_wired` (`1468`), `spawn_supervised` (`1574`) and
`Handle::service` (`2145`). `Handle::serve` reaches it through
`self.service(allow)` (`macros.rs:2239`). Six local `actor*`/`supervised*` doors
do not instantiate it and do not need to, because a purely local actor is never
served and generates no local `send` method at all (`macros.rs:3954-3957`).

The one door that does not fire it — `svc::service(handler, allow)`
(`macros.rs:700`) — is **documented as intentionally exempt**, at
`macros.rs:638-641`:

> A plain `const` would be evaluated where it is written, and would refuse every
> service that merely *declares* a `send` method — including every one served the
> concurrent way, **where a `send` is perfectly safe**.

The constant refuses `send` for an actor because a `send` handler reads its
upload inside the **serial turn** (`macros.rs:645-656`). A concurrent handler has
no serial turn. **Not a defect. Do not "fix" it** — doing so refuses a shape the
crate supports on purpose.

**Claim:** *"same shape for `SpawnableUnfed`."*

`SpawnableUnfed::<A>::CHECK` fires at exactly the three doors that wire no feeds:
`spawn` (`1384`), `actor` (`1603`), `actor_with` (`1634`). Every other spawn door
takes a `Feeds` argument or an `F: Feeds<A>` bound — `spawn_wired`,
`spawn_supervised`, `actor_wired`, `actor_deep`, `supervised`, `supervised_deep`
— so instantiating the check there would be wrong. Coverage is **3 of 3**.

The audit's sentence in `.tmp/surf/REPORT.md:134` — *"which `spawn` instantiates
and `spawn_wired` does not"* — is a false finding, and the goal inherited it.
Drop both clauses from item 12 and keep the other three
(`keyed_service` twin, `ask_catching`, the `Expired` ruling).

### 5.2 Two type errors in the goal's Public-interface block

- `AtCapacity { capacity: usize }` (goal line 154). `usize` **has no wire
  schema**, and the crate states why at `actor/registry.rs:1228-1240`:
  `postcard_schema` implements `Schema` for no pointer-sized integer, because
  keylink builds for `wasm32-unknown-unknown` and for 64-bit natives. Must be
  `u64`, exactly like `ActorStatus::buffered`.
- `Result<Ended, ..>` (goal line 164). Covered at §3.2 — must be `EndReason`.

### 5.3 One success criterion that can pass for the wrong reason

Criterion 2 asks that `Withheld` be identical on both sides *"for all seven
conditions"*, asserting equality of values.

Two of the seven have no local producer today. `Expired` is documented
unreachable from a local call (`turns.rs:3265-3267`, `observe.rs:721-726`).
`Panicked` is unreachable through `Handle::ask`, which resumes the panic
(`turns.rs:3905`) — it needs `ask_catching` (§1.3). A test that **constructs**
both values by hand satisfies the criterion and proves nothing.

Word the criterion so it names the door each condition is driven through, and
mark the ones that are constructed rather than driven. Otherwise the proof is a
tautology.

---

## 6. What I did not check

- I compiled nothing. Every proposal marked **UNVERIFIED** is a reading of the
  mechanism, not a run.
- I did not read `local.md` or `remote.md` in full. Where I cite them, I cite the
  source file they point at instead.
- I did not measure the cost of the `BodyReader` second arm (§2.3) against the
  `FrameReader` erasure. I argue the erasure touches the remote hot path and the
  second arm does not. That is a reading of `wire.rs:178-181` and
  `body.rs:135-139`, not a benchmark.
- I did not check whether the `Client<K>` typestate (§3.4) or the conditional
  trait impl (§2.6) is expressible in this crate's `macro_rules!` expansion. Both
  are marked UNVERIFIED for that reason.

---

## 7. Addendum: the served axis, and a second hazard

Written after the lead relayed the brooks pass. §7.5 corrects two numbers.

### 7.1 The served-vs-not-served axis holds, and I agree it is a hazard

I checked the mechanism independently and it is exactly as the brooks pass
states. `Middleware` wraps a `Dispatch` (`rpc.rs:1390`). Only `Handle::service`
builds one (`macros.rs:2147`). `__keylink_local_method!` calls
`self.inner.ask(..)` and reaches the mailbox directly (`macros.rs:3853-3860`).
Both callers share one mailbox, and the crate says so at `macros.rs:2035-2036`.
My own §7.4-equivalent reading is in `seam.md:427-449`, which reaches the same
place: **the real boundary is served against not-served.**

**One refinement about what is new, because it decides how bad this is.** A
`Handle` bypasses those three today, and the crate documents it. What the trait
adds is not a bypass. It is the loss of *visibility*: today the author of a
function writes `Handle` or `Client` in the signature and a reviewer reads the
boundary off the type. After the trait, the surface is chosen by whoever calls
`settle` — possibly in another crate, possibly in a test fixture — and neither
`settle`'s author nor its reviewer can see whether authorization ran.

**Three faces, and I have seen only two named.**

1. **Authorization.** The `Allowlist` is checked on the `Dispatch` path
   (`macros.rs:2140`, `admission.rs:318`, `middleware.rs:391-394`). A `Handle`
   never reaches it.
2. **Middleware.** Every layer the operator installed — timeout, rate limit,
   logging, metrics.
3. **The idempotency ledger, which is a correctness hole rather than a security
   one.** `settle(&keyed_client)` is at-most-once, because the ledger records the
   reply and answers a repeat with it (`macros.rs:2710-2713`).
   `settle(&handle)` has no ledger at all, because a local call has no transport
   to key (R3, and it is physics). **The same function delivers two different
   guarantees and signals nothing.** A caller that retries on error turns
   at-most-once into at-least-once by switching surface.

**Ranking: would be a lie**, as the lead puts it — unless the boundary is moved
into the type system. §7.2 does that.

### 7.2 The honest close, and it costs three lines of macro output

Emit a sealed marker trait for the surfaces that pass a `Dispatch`:

```rust
/// Implemented only by surfaces whose calls pass this service's `Middleware`,
/// its `Allowlist` and its idempotency ledger.
///
/// `Handle` does **not** implement it. A local call reaches the mailbox
/// directly (`macros.rs:3853`) even while the same actor is served, so the
/// boundary this trait names is served-against-not-served and not
/// local-against-remote.
pub trait Governed: Calls {}
impl Governed for Client {}
impl Governed for ActorRef<'_> {}
impl Governed for blocking::Client {}
```

Code that must be governed writes `impl Calls + Governed`, and the compiler
refuses a `Handle`. Code that does not care writes `impl Calls` and says so.
This needs no new machinery — the macro already emits a per-surface block for
each of those three types.

Two limits, stated rather than hidden:

- **The permissive case is still the default.** A bare `impl Calls` accepts a
  `Handle`. I considered inverting it, so the plain trait means governed and a
  `Handle` needs an extra bound. That breaks the goal's core use case — one
  signature, local in a test and remote in production — so I do not recommend
  it. Put the default in the trait's first rustdoc paragraph.
- **A `const GOVERNED: bool` is worth adding for logging and assertions, and it
  is not enforcement.** You cannot bound on a const value on stable Rust. Do not
  present the const as a guard.

### 7.3 What the reframing changes in my ranking

| Item | Move | Why |
|---|---|---|
| §2.6 trait split + `on_unimplemented` | **should → must** | The split is no longer only about streaming. It now carries a security boundary |
| §3.2 doors in `WireRequest`, not the contract | **stronger, same rank** | `WireRequest::KeyState` is answered *"without entering the handler and without running a layer"* (`reply.rs:227-230`). `__status`/`__ended` join a category that is ungoverned on **both** sides — symmetric. On the service contract they are governed for a peer and ungoverned for a `Handle`, which is face 1 again, on keylink's own bookkeeping |
| §3.4 `IdempotentClient` as a field | **stronger** | Face 3 makes the idempotency difference invisible as well as untyped |
| Goal item 8 | **gains a caveat** | *"gated by the existing `Allowlist`"* is true **only for peers**. A local `Handle::stop` is not gated and never was. The sentence is true and incomplete |
| Everything else | unchanged | The naming items, `deliver_to_local`, `NodeStatus::lost`, the `BodyReader` mechanism and the two rulings do not touch this axis |

Nothing gets less urgent.

### 7.4 The second hazard: the blocking client fails at run time, not compile time

This is the one the lead asked me to find, and it is the same class — a
convenience that hides a boundary the compiler cannot see.

`blocking::Runtime::run` calls `guard()?` **before anything else**
(`blocking.rs:77-78`) and returns `Error::BlockingInAsync` whenever a tokio
runtime is present (`blocking.rs:76`, `92`). So this compiles and fails at run
time:

```rust
#[tokio::test]
async fn t() { settle(&blocking_client).await; }   // Err(BlockingInAsync)
```

Today that trap does not exist, because the two worlds share no type and nobody
writes a function that takes both. **The trait creates it.**

The crate has already written the rule down, at `keylink/tests/protocol.rs:753-754`:

> Not a `#[tokio::test]` on purpose — this must run on a plain thread, which is
> exactly the situation the blocking API exists for.

That is `#[test] fn a_synchronous_program_can_call_a_service()`
(`protocol.rs:759`), the only test in the tree that drives a
`blocking::Client`.

**Consequence for the goal: success criterion 1 is unsatisfiable as written.** It
asks for one test, `one_function_serves_every_surface`, to pass against
`Handle`, `Client`, `ActorRef` **and** `blocking::Client`. The first three need a
runtime. The fourth needs the **absence** of one. One test function cannot hold
both.

Reword it: two tests — a `#[tokio::test]` over three surfaces and a plain
`#[test]` over the fourth — sharing **one** generic function. The shared function
is the proof; the single test never was.

This is the second independent argument for the two-trait split (§2.6), and the
minimum acceptable alternative is a trait whose first rustdoc line states the
runtime precondition for the blocking implementation.

### 7.5 The two numbers, re-derived rather than accepted

**Gate steps.** I never cited a step count. `grep -n '22' bolder.md` returns only
line-number references. Nothing to correct.

**Client types.** I wrote "six to three". The lead's correction is right, and
here is the derivation rather than the assent.
`rg '^\s*pub struct (Handle|Client|IdempotentClient|KeyedClient|ActorRef)'
keylink/src/macros.rs` returns **seven** generated caller-side structs:

| Type | Line |
|---|---|
| `Handle<A>` | 1970 |
| `KeyedClient` | 2412 |
| `ActorRef<'a>` | 2572 |
| `Client` | 2609 |
| `IdempotentClient` | 2719 |
| `blocking::Client` | 2783 |
| `blocking::IdempotentClient` | 2853 |

Excluding `Handle`, that is **six client types**. Item 6 deletes the two
`IdempotentClient`s, leaving `KeyedClient`, `ActorRef`, `Client` and
`blocking::Client` — **four**. Corrected in §3.6.

**And the correction exposes a defect in the goal.** Item 6's success criterion
names three survivors and proves them with
`rg 'pub struct IdempotentClient' keylink/src/macros.rs` is empty. That grep
passes while **four** client types remain, so the criterion does not measure the
claim it sits under. Either name `ActorRef` as the fourth survivor, or restate
the claim as 6 → 4.

**One more thing the enumeration shows.** `IdempotentClient` is generated
**twice** — at 2719 and at 2853. The blocking one carries the same
`tell`-removal guarantee, so the typestate argument in §3.4 applies twice and
item 6 must delete both.

### 7.6 Status

Finished. The report was written before this addendum and did not route. It is
at `/Users/pedrocunha/repos/keylink/.tmp/goal-review/bolder.md`.
