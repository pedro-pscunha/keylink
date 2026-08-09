# Feasibility of the bold mechanisms

Goal: `docs/goals/2026-08-09-one-service-one-trait.md`. Tree: `main` at `0119504`.
Method: trace the code, cite `file:line`, refuse what I cannot show.

Two claims below are proven by a run, not by reading. Both runs are quoted.

**Verdicts.** Item 1 buildable, cheaper than the goal prices it. Item 2 **refuted
as written** — success criterion 1 is unachievable. Item 3 buildable, proven by a
compile. Item 4 buildable, and the refusal's premise does not reach its
conclusion. Item 5 buildable, and the "once" criterion is ambiguous. Item 6
buildable, and the allowlist concern in the prompt is correct and unstated.

---

## 1. Local `send` / `recv`

**Verdict: buildable. The refusal is right about `AsyncRead` and wrong that only
two paths exist. Four to six days, not a fortnight, and not a day.**

### What the receive half needs from QUIC

`FrameReader` is `{ stream: RecvStream, staged: BytesMut }` at
`keylink/src/wire.rs:178-181`. It is `pub(crate)`.

It calls exactly **one** method on that stream: `read_chunk`, at `wire.rs:239`
and `wire.rs:246`. Nothing else.

I checked for a concrete-type dependency and found none. Every `.finish()` in the
crate runs on a `SendStream` — `rpc.rs:365,378,429,613,2088,2130,2186` and the
`wire.rs` tests. No call site uses `reset()`, `stop()` or a stream id. Every
`.id()` hit resolves to a peer, a node or a handle, never to a stream.

So the receive half depends on one method of `iroh::endpoint::RecvStream`.

### Is `Body` constructible in-process today

**Yes.** `Body::empty`, `from_bytes`, `from_file` and `from_reader` are all `pub`
and touch no stream (`keylink/src/body.rs`). Only `write_to` is transport-bound,
and it is `pub(crate)` at `body.rs:103`.

`BodyReader` is **not** constructible in-process. Its one constructor is
`pub(crate) fn new(frames: FrameReader, ..)` at `body.rs:142`, and a `FrameReader`
demands a `RecvStream`.

The asymmetry is one-sided. The send half is already a local value. The receive
half has no local constructor.

### Why `AsyncRead` is the expensive path

`FrameReader::next_chunk` is documented "Zero-copy: ... the transport's own
buffers handed straight through" (`wire.rs:233-234`). `BodyReader::next_chunk`
repeats that promise (`body.rs`).

`AsyncRead::poll_read` fills a caller-supplied `ReadBuf`. No zero-copy `AsyncRead`
exists. A generic `FrameReader<R: AsyncRead>` therefore allocates and copies every
chunk, **including on the QUIC path**. The generalisation breaks a documented
property of the code it passes through. That is a stronger reason to refuse it
than "would rewrite the transport".

### The path the refusal does not consider

Keep the chunk shape. Swap the source. `FrameReader.stream` becomes a private
two-arm enum: `Quic(RecvStream)`, and an in-process `mpsc::Receiver<Bytes>`.
`next_chunk` and `stage_at_least` match on it. A `Bytes` moves through a channel
by reference count, so zero-copy survives on both arms.

The crate already uses this exact shape twice: `Body`'s own
`Source::{Bytes, Reader}` (`body.rs`), and `Incoming::{Bi, Uni}` at `rpc.rs:73-75`.

**Cost, counted:**

- Types changed internally: **2** (`FrameReader`, `Body`). Public signatures
  changed: **0**.
- `FrameReader::new(recv)` keeps working at all **9** sites — 4 production
  (`rpc.rs:480,517,2090,2132`) and 5 test (`wire.rs:274,293,322,339,378`) — if the
  enum is built inside `new`. Add one `FrameReader::in_process(rx)`.
- `Body::write_to` takes a sink enum instead of `&mut SendStream`. One definition,
  few callers.
- `BodyReader::new` gains a second constructor. Public API unchanged.

**The actor side needs no change.** `Mail::$rpc` already types its body field per
kind — `body: $crate::__keylink_mail_body!([<$kind>])` at `macros.rs:886`, which
is a `BodyReader` for `send` and `()` otherwise. The macro's own doc at
`macros.rs:851-861` states those variants are already "written to be *correct* if
something ever did build one". Only a builder is missing. The two empty arms sit
at `macros.rs:3954-3957`.

**Estimate: 4 to 6 working days including guards.** The local `recv` costs most,
because the handler returns a `Body` the caller reads as a `BodyReader`. A bounded
channel of depth 1 gives the one-chunk peak the module promises.

**wasm caution.** Pick the channel, not `tokio::io::duplex`. `Cargo.toml` declares
`tokio/sync` and states it compiles for the browser.

---

## 2. One trait over four surfaces

**Verdict: REFUTED as written. Success criterion 1 cannot be met by any
implementation.**

### The toolchain permits the trait

`rust-toolchain.toml` pins `channel = "stable"`. `keylink/Cargo.toml` declares
`edition = "2021"` and `rust-version = "1.91"`.

1.91 is far above 1.75, so return-position `impl Future` in a trait is available.
The signature in the goal at line 162 compiles.

### The lifetime does not break the impl

`ActorRef<'a>` is `{ client: &'a KeyedClient, actor: ActorId }` at
`macros.rs:2572-2575`. `impl<'a> Orders for ActorRef<'a>` is ordinary.

A return-position `impl Trait` in a trait captures every in-scope lifetime
implicitly, because it desugars to an associated type over the impl's parameters.
The returned future can borrow `'a` with no extra bound. **This is not a problem.**

### `Send` is fine, with one caution

The three async surfaces already carry `Send` work. Adding `+ Send` makes
`settle(&impl Orders)` spawnable. The bound forbids a non-`Send` caller. On wasm
the concrete types stay `Send`, so this is a caution and not a blocker.

One consequence must be documented: the trait is **not dyn-compatible**. `&dyn
Orders` will not compile. Nothing in the tree needs it, and the goal's example
uses `impl Trait` only.

### Why blocking cannot implement it

This is the decisive finding.

`blocking::Runtime::run` is `pub fn run<F: Future>(&self, fut: F) ->
Result<F::Output>` in `keylink/src/blocking.rs`. **Its first statement is
`guard()?`.** `guard()` returns `Err(Error::BlockingInAsync)` whenever
`tokio::runtime::Handle::try_current().is_ok()`.

A generated blocking method is synchronous: `pub fn $name(&self, ..) ->
$crate::Result<$ret>` at `macros.rs:3088-3094`, and the `tell` twin at
`macros.rs:3077-3086`.

An impl can be written. It compiles:

```rust
fn place(&self, n: u64) -> impl Future<Output = Result<u64, Self::Error>> + Send {
    async move { self.place_blocking(n) }   // compiles
}
```

That future parks an OS thread when polled. Worse, `settle(&blocking_client)
.await` inside **any** tokio runtime returns `Error::BlockingInAsync` every time,
by design. The only context that succeeds is a non-tokio executor on a thread with
no tokio context. The crate depends on no such executor.

So the blocking client can satisfy the trait's **types** and cannot satisfy its
**meaning**. A trait that says "await me", implemented by a type that says "never
be awaited", is the representable-but-impossible state the goal's own invariants
forbid. The goal makes that argument itself at lines 268-270, against a unified
error enum.

**A `#[cfg]` does not help.** Both APIs exist in the same build under the
`blocking` feature.

### The honest shape

Two traits from one `service!`, sharing the associated error type:

- `orders::Orders` — async, implemented by `Handle`, `Client`, `ActorRef`.
- `orders::blocking::Orders` — same method names, returning `Result<T, E>`
  directly, implemented by `blocking::Client`.

**Success criterion 1 (goal line 182-184) must be rewritten.** One function over
four surfaces is impossible. One function over the three async surfaces is
achievable, and is the real prize.

### Blocking coverage, measured

Blocking is **2 of 5**: `rpc` and `tell` only. The arms at `macros.rs:3095`,
`3096` and `3100` are `=> {};` for `send`, `recv` and `blob`.

Reaching 5 of 5 needs `blob` (cheap) and overturns a written refusal for `send`
and `recv`. `blocking.rs:33-38` refuses them, and calls the alternatives "buffer a
whole file in memory" or "hand back a handle that only works inside a runtime".

**That is a false dichotomy.** A third option exists: a `blocking::BodyReader`
that implements `std::io::Read` and calls `runtime.run(next_chunk)` once per
chunk. It keeps the one-chunk peak. It fails inside async exactly as the rest of
the module does. The refusal is refutable, and refuting it costs new public API.

---

## 3. Reserved `__status()` / `__ended()` doors

**Verdict: buildable, and proven by a compile.**

### Can the macro reserve a name

**Yes. I ran it.** The question was whether `macro_rules!` backtracks to a later
arm after `$kind:ident` already matched. It does.

Control first, on a normal name:

```
--- control: a normal name must compile and run ---
BACKTRACK-OK place=0
```

Then the reserved name, same macro, one identifier changed:

```
error: `__status` is reserved by keylink; rename the method
  --> u.rs:3:9
10 | munch!(@rest [ rpc __status () -> u64 ; ]);
   | ------------------------------------------ in this macro invocation
```

A literal-identifier arm placed **before** the general `@rest [ $kind:ident
$rpc:ident ( .. ) -> $ret:ty ; .. ]` arm at `macros.rs:405` reserves the name and
emits a readable message. The crate already writes long `compile_error!` messages
in that same muncher, at `macros.rs:433-445` and `macros.rs:447-457`.

### What happens today if a user declares `rpc Status()`

Nothing. `Status` is not reserved and generates an ordinary `Request::Status`
variant. No collision exists today.

The existing collision is `Served`. `macros.rs:846-850` documents it: "A method
**named** `Served` would collide with it, rename the method." Today that collision
surfaces as a raw `error[E0428]` pointing into generated code. The muncher's own
catch-all arm calls that failure mode worse than a custom message.

So the reserved-name arm fixes an existing rough edge at the same time. The `__`
prefix keeps the reservation clear of names a user would take.

### `Ended` on the wire — a correction the goal needs

`Ended` **cannot** cross. `Ended::Observer` carries `Arc<dyn Error + Send + Sync>`,
which has no schema. `console.rs:173-176` says so.

`EndReason` already exists for this, at `console.rs:184`. It derives `Serialize`,
`Deserialize` and `Schema`. `console.rs:178-182` states the invariant that keeps
it honest: exactly one `From<&Ended>` conversion, no wildcard arm, `Ended`
exhaustive. A new ending is therefore a compile error on that one line.

**Reuse it. Do not duplicate it.**

The goal contradicts itself here. Line 164 writes `fn ended(&self) -> ..
Result<Ended, Self::Error>`. Line 80 says "`Ended` crosses using `EndReason`'s
existing flattening". Both cannot hold.

**Resolution: the trait method returns `EndReason` on every surface.** A local
`Handle` can keep a separate richer accessor that answers `Ended`. If the trait
method's return type differs by surface, the one-trait goal breaks.

Cost: `EndReason` lives in `console.rs` and is tied to `Inspect`. Every service's
hashed graph will now reach it. That is a hash move, and it is already budgeted.

---

## 4. Keyed `tell` returning the routing acknowledgement

**Verdict: buildable. The refusal's premise is true. Its conclusion does not
follow.**

The refusal sits at `macros.rs:3694-3718`. It names four refusals a keyed message
can meet — a malformed id, an id nothing is mounted under, a registry at capacity,
and an expired queue budget — and states all four "are decided AFTER the bytes
reach the wire, and all four are reported on the reply".

### Is there a point where "routed and enqueued" is known

**Yes, and the code names it exactly.** In `deliver_to_actor`
(`keylink/src/actor/remote.rs:~415-470`) the order is:

1. `actor.validate()` — the malformed id.
2. `system.route(&actor)?` — `NoSuchActor`, and the registry at capacity.
3. `route.mail.send(..)` — the enqueue.
4. the budget match — `TimedOut`.

The rustdoc above that function states the property outright: "Once the permit is
in hand the message will be taken, and nothing above this can stop it ... so
`enqueued()` is marked". And: "It is marked *after* the send and not before, and
that placement is the whole of the precision: a caller refused at step 4 provably
queued nothing."

So the crate **already computes a decisive routed-and-enqueued verdict before the
turn runs**, and already acts on it through the idempotency marker.

All four refusals resolve at or before that point. **None needs the turn.**

### What answering costs

Answering does not make it an `rpc`. It needs a reply that closes at the enqueue
point instead of at the turn's end.

The cost is a wire change. `RpcClient::tell` writes on a **unidirectional** stream
precisely so nothing can be read back (`macros.rs:3701-3703`). An acknowledged
`tell` needs a bidirectional stream whose response is written at enqueue time and
then finished — a new response shape, roughly `WireResponse::Routed`, plus a new
path in `serve_one_tell`.

That moves the hash. The move is already budgeted.

The caller still pays one round trip. It is cheaper than a turn. It is not free.
The refusal's closing advice — write an `rpc` returning `()` — stops being the
only honest option, but the round trip does not disappear.

**The distinction the refusal misses is "after the wire" against "after the
turn".** The one thing that genuinely cannot be answered early is the method's own
return value.

---

## 5. `Withheld` on the wire

**Verdict: every field type is supported. No item forces a second move. The
"once" criterion is ambiguous and must be restated.**

### Field types

Every field in the sketch at goal lines 134-142 is a `String`, beside three unit
variants. Nothing else appears.

`String` is `DataModelType::String` in postcard-schema. The wire already carries
`String` four times: `RemoteError::Unauthorized(String)`, `NoSuchActor { id:
String }`, `Failed(String)` (`reply.rs`), and `EndReason::Panicked { message:
String }` (`console.rs`).

`Schema`, `Serialize`, `Deserialize`, `thiserror::Error`, `Clone`, `PartialEq` and
`Eq` all derive. `RemoteError` already derives the identical set at
`reply.rs:54-56`.

### What the hash actually walks

Read from `postcard-schema-0.2.5/src/key/hash.rs`:

| Element | Hashed? | Line |
|---|---|---|
| Type name | **No** — commented out for "safe type punning" | `~193-201` |
| Enum **variant** name | **Yes** | `203-204` |
| Struct / struct-variant **field** name | **Yes** | `~228-231` |

**Consequence for goal item 11.** Renaming `NodeStatus::lost` **does move the
hash**. It is a wire change, not a cosmetic one. The goal treats it as a rename.
It rides the same commit, so the budget holds — but the plan must know.

### The pins

Two pins, both literal:

- `keylink/tests/protocol.rs:1201` — `"keylink/Probe/d75913b8a4742547"`
- `keylink/tests/protocol.rs:1305` — `"keylink/Inspect/0572763878b7fb9d"`

`keylink/tests/supervision.rs:110-116` states what they cover: "the **shared
hashed graph every service reaches** — `Envelope`, `WireRequest`/`WireResponse`,
`Outcome`, `RemoteError` and everything beneath them — by value, twice".

`RemoteError` gaining `Withheld` therefore moves **both**. Read both new values
off the failing run. Never compute them.

### Does anything force a second move

No single item does. But **five separate items each move the hash**:

| Item | Moves | Why |
|---|---|---|
| 1, 9 — `Withheld`, `RemoteError` reshape | every contract | `RemoteError` is in the shared graph |
| 9, 11 — `FollowRefused`, `NodeStatus.lost` | `Inspect` | variant and field names are hashed |
| 7 — `__status` / `__ended` | every contract | new `Request`/`Response` variants |
| 8 — `shutdown` / `abort` / `evict_idle` | every contract | new variants |
| 4 — acknowledged keyed `tell` | every contract | new `WireResponse` variant |

Items 2, 3 and 6 move nothing. They are client-side or local.

**The ambiguity.** Measured `main` to branch tip, each pin changes exactly once.
Measured commit by commit, on a branch of "atomic commits, one logical change
each" (goal, Process, line 286), each pin changes up to five times.

The goal's Process section and its "moves once" invariant cannot both hold under
the second reading. **State the criterion as `main` to tip.** Otherwise the two
sections contradict.

---

## 6. Peer-reachable `shutdown` / `abort` / `evict_idle`

**Verdict: expressible. The allowlist concern in the prompt is correct, and the
goal does not state it.**

### Id plus verb

Yes, for two of the three. `Addressed<Request>` already carries an `ActorId`
beside a request (`macros.rs:2565`, `Addressed::new(actor.clone(), request)`), and
`deliver_to_actor` splits it. So `shutdown(id)` and `abort(id)` take the shape of
any keyed rpc.

`evict_idle` names no actor. It is a system-level verb, not an id-plus-verb. It
belongs on the keyed system, not on an `ActorRef`. The goal groups all three
together and that asymmetry needs naming.

### What the `Allowlist` gates

**A whole served protocol, decided once per connection. Not a method.**

`Allowlist::admit(ProvenPeer) -> Option<AdmittedPeer>` runs at accept
(`keylink/src/authz.rs`). `rpc.rs:92` states the server "authorizes the peer once
per connection, then answers". An `Allowlist` is supplied per service.

Per-method authorization exists, and it is a **different** mechanism: a caller's
own `Middleware`. See `rpc.rs:1390` (`layer`), the worked example at
`rpc.rs:1444-1457` returning `RemoteError::Unauthorized`, and `rpc.rs:1513`, which
names `tests/actors.rs`'s `PerMethod`.

### Why that is wrong for admin verbs

Put `shutdown` on the same service as ordinary calls, and **one allowlist admits
both**. Any peer allowed to call `place_order` is, by construction, allowed to
call `shutdown`. The `Allowlist` cannot separate them.

The only in-tree separator is a `Middleware`, which is **the caller's own code**.
The safety of a destructive verb would then depend on every downstream user
remembering to mount one. That is fail-open, in a crate whose `Allowlist` has
"deliberately no `Default` ... so it can never be what you get by forgetting"
(`authz.rs`).

### Honest shapes, cheapest first

1. **A separate admin service, with its own ALPN and its own `Allowlist`.** This
   matches the existing grain. It costs one more ALPN, and `macros.rs:2405-2407`
   already accepts one ALPN per service as "a bounded number". It also dissolves
   `evict_idle`'s problem, because an admin service is naturally system-scoped.
   **Recommended.**
2. A distinct `Allowlist` at registration, such as `.with_admin(allow)`. This is a
   new concept in the crate.
3. Reuse `Middleware` and document it. **Refused** — fail-open.

**Note the tension the goal must resolve.** Item 7 removes a second ALPN
(`Inspect`) for read-only facts. Item 8 adds one back for destructive verbs. That
is coherent, because reads and writes differ in blast radius. The goal does not
say so, and as written the two items read as contradictory.

---

## Being bold: what else this tree could close

Mechanism only. Each item names the code that makes it buildable.

### A. `RemoteError` is `#[non_exhaustive]` and the goal forbids that

**This is the highest-value omission, and it is a blocker for execution.**

Goal lines 221-226 state the invariant: "**Public error enums stay exhaustive** —
no `#[non_exhaustive]`, no wildcard arm inside the crate."

The tree disagrees. `RemoteError` carries `#[non_exhaustive]` at `reply.rs:57`.
`FollowRefused` carries it at `console.rs:406`.

The goal's own public-interface sketch, at lines 148-157, writes `RemoteError`
**without** the attribute. So the goal silently proposes removing it. That is a
breaking change for every downstream `match`. It appears in no scope item, no
success criterion and no blast-radius line.

**Either the invariant gives, or the two attributes give. Rule on it before
execution, not during.**

### B. Reserve every identifier the expansion already occupies

The proven arm from item 3 costs three lines each. `Served` is documented as a
collision at `macros.rs:846-850` and today fails as a raw `E0428` inside generated
code. `Actors`, `Mail`, `Request`, `Response`, `Client` and `Handle` are in the
same position. Zero wire cost.

### C. `Body` can be built and cannot be drained

Every `Body` constructor is `pub`. `write_to` is `pub(crate)` at `body.rs:103`.
A user builds a value with no terminal operation.

Item 1 creates an in-process sink anyway. Exposing `Body::copy_to<W: AsyncWrite>`
mirrors `BodyReader::copy_to` and makes the pair symmetric. The loop already
exists at `body.rs:103-113`. Zero wire cost.

### D. Five `wire.rs` tests can stop needing a live QUIC pair

`wire.rs:271`, `319`, `336` and `375` each call `send.finish().unwrap()`, so each
stands up a real stream. Item 1's in-process source turns all five into ordinary
unit tests. That is gate time the goal does not claim.

### E. `schema_hash` is `pub(crate)` for a caller that no longer exists

`node.rs:142-148` says the one caller that needed a bare hash "is gone", and calls
parsing hex back out of an ALPN "worse". Since type names are not hashed and field
names are, a user cannot predict a hash for a type they own. Re-exposing
`schema_hash` (`node.rs:149`) is one line. Low value. Listed for completeness.

### F. A count in scope item 12 that I could not reproduce

Goal line 93 states `ServableAsActor::CHECK` "fires at three of four doors".

I count **four** `ServableAsActor::<A>::CHECK` sites — `macros.rs:1383`, `1468`,
`1574`, `2145` — and **three** `SpawnableUnfed::<A>::CHECK` sites — `1384`,
`1603`, `1634`.

Call sites are not doors, so this is not a contradiction. But the audit's own
definition of "door" must be carried into the plan. I did not re-derive it. I flag
it so the number is read off the audit and not off this file.

---

## Summary table

| # | Mechanism | Verdict | Real cost |
|---|---|---|---|
| 1 | Local `send` / `recv` | **Buildable** | 4-6 days. Use a chunk-channel enum, not `AsyncRead`. 2 types, 0 public signatures. |
| 2 | One trait, four surfaces | **Refuted as written** | Blocking cannot be awaited. Criterion 1 needs rewriting. Two traits is the honest shape. |
| 3 | Reserved doors | **Buildable, proven** | Small. `__ended()` must return `EndReason`, not `Ended`. |
| 4 | Keyed `tell` ack | **Buildable** | A new bidirectional response shape. The verdict already exists before the turn. |
| 5 | `Withheld` on the wire | **Buildable** | All fields are `String`. Both pins move. Restate "once" as `main` to tip. |
| 6 | Peer-reachable admin verbs | **Expressible, gated wrong** | `Allowlist` is per protocol. Use a separate admin service. |

**Three things to decide before any code is written:**

1. Success criterion 1 — blocking cannot join one async trait.
2. `#[non_exhaustive]` on `RemoteError` and `FollowRefused` against the stated
   invariant.
3. Whether "the hash moves once" is measured per commit or `main` to tip.
