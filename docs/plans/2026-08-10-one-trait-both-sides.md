# Wave 2 — one trait, both sides

Plan for `docs/goals/2026-08-09-one-trait-both-sides.md`, **revision 1**, written
against `one-trait` at `2dd3eb4` — the merge of wave 1.

The goal was written on 2026-08-09, before wave 1 landed. **Eight of its factual
claims are false against this tree**, and §11 lists each one with the measurement
that refutes it. Read §11 before §2, because three of the rulings below exist
only because a goal claim did not survive.

**This plan asserts no totals.** Wave 1's plan gate returned 7 Critical and 16
Major, and the merge gate 4 more Critical. None of the failures was arithmetic:
four counts grew on every measurement, each partial enumeration adding up
correctly. So every figure below is either a **measurement at `2dd3eb4` for
sizing**, labelled as such, or a **predicate with the command that proves it**.

**Every test named as a proof below is marked `EXISTS` or `TO WRITE`.** Wave 1
shipped a success criterion naming a test that three documents cited and nobody
wrote. A name with no mark is a plan defect.

---

## 1. What this is

A caller writes one function. It runs against a local actor in a test and a
remote one in production. Today it cannot, and the reason is exact: **the two
surfaces share no error type.**

Measured at `2dd3eb4`, per `rpc` declaration:

| Surface | Declared at | `rpc` return type |
|---|---|---|
| `Handle<A>` | `macros.rs:1972` | `Result<$ret, Unanswered>` (`macros.rs:3858-3859`) |
| `ActorRef<'a>` | `macros.rs:2573` | `Result<$ret, Error>` (`macros.rs:3724`) |
| `Client` | `macros.rs:2610` | `Result<$ret, Error>` (`macros.rs:3975`) |
| `IdempotentClient` | `macros.rs:2720` | `Result<$ret, Error>` (via `macros.rs:2908-2914`) |
| `blocking::Client` | `macros.rs:2784` | `Result<$ret, Error>`, **synchronous** (`macros.rs:3092`) |

The argument list and `$ret` already agree across all five. Only the error
position disagrees, and that one disagreement is the whole of why no trait can be
written today.

Wave 1 supplied the missing half: `Withheld` is public (`observe.rs:870`), it is
`Unanswered::Turn`'s payload (`turns.rs:3213-3231`) and `RemoteError::Withheld`'s
payload (`reply.rs:161`). **One value now reaches both sides.** What is still
missing is a type that a trait signature can name.

So this wave adds four things:

1. **`Failure`** — a shared error with `Withheld` in exactly one arm, reachable
   from both sides.
2. **`Calls`** — one generated trait per service, carrying that service's `rpc`
   methods, implemented for four surfaces.
3. **`Governed`** — a crate-level sealed marker, so `impl Calls + Governed`
   refuses a `Handle` at the signature a reviewer reads.
4. **`keyed_serve`** — the twin of `Handle::serve`, five lines, §2.8.

**And it refuses most of the goal's remaining scope.** §12 lists **ten**
refusals. Six of them quote an argument the tree already carries; the tree
disagrees with the goal more often than the goal knows. §12 is the larger half of
this document on purpose — a refusal nobody wrote down is a refusal that gets
re-derived.

---

## 2. Eight rulings

### 2.1 `Calls` carries `rpc` and nothing else

Measured at `2dd3eb4`: **`rpc` is the only kind present on all four async
surfaces.** The other four kinds are each absent from at least one, and every
absence is a *written refusal with an argument*, not an oversight:

| Kind | `Handle` | `ActorRef` | `Client` | `blocking::Client` |
|---|---|---|---|---|
| `rpc` | yes | yes | yes | yes |
| `tell` | yes | **refused** `macros.rs:3695-3719` | yes | yes |
| `send` | **refused** `macros.rs:3961` | yes | yes | **refused** `macros.rs:3096` |
| `recv` | **refused** `macros.rs:3963` | yes | yes | **refused** `macros.rs:3097` |
| `blob` | yes, `BlobId` | yes, `BlobHandle` | yes, `BlobHandle` | **refused** `macros.rs:3101` |

`blob` is the interesting row: it is present on three async surfaces and
**returns a different type locally**. `Handle` yields `Result<($ret, BlobId),
Unanswered>` (`macros.rs:3929-3930`); `Client` yields `Result<($ret,
BlobHandle)>` (`macros.rs:4030-4031`), built by `self.inner.blob_handle(id)`
(`macros.rs:4037`). A `BlobHandle` is documented at `macros.rs:4026-4029` as *"a
content address plus the node that holds it"*. A local `Handle` names no node, so
it cannot build one — the same argument `macros.rs:3958-3960` gives for `send`
and `recv`: *"Streaming is a transport capability, and this handle names no
transport."*

**Ruling: `Calls` declares one method per `rpc` and emits nothing for the other
four kinds.** The emitter takes the house shape — five arms, four of which expand
to `{}`, exactly as `__keylink_local_method!` already does at `macros.rs:3961-3964`.

The cost, stated so nobody meets it as a surprise: **a service declaring no
`rpc` gets an empty `Calls` trait**, and a service mixing kinds gets a trait
whose methods are a subset of what each surface can do. This must be in the
generated trait's own rustdoc, naming all four excluded kinds and the reason for
each. A caller who cannot find `note` on `Calls` and cannot find out why will add
it, and adding `tell` is how `ActorRef` acquires a method its own refusal
(`macros.rs:3695-3719`) argues against.

### 2.2 `Failure` has three concrete arms and no `dyn Error`

The goal proposes `enum Failure { Withheld(Withheld), Other(Box<dyn Error>) }`.
That shape erases everything that is not a `Withheld`, so a caller can no longer
match `Unauthorized`, `Reentrant`, `SchemaMismatch` or `TimedOut` without
downcasting. The crate's own rule is the opposite one — `middleware.rs:369-371`,
about `Caller`: *"this crate's rule is that a known value set is an enum."*

**Ruling:**

```rust
// keylink/src/error.rs, beside `Error`
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum Failure {
    /// The turn was reached and its value was not handed back.
    #[error("{0}")]
    Turn(Withheld),
    /// The call was made from inside the target actor's own turn.
    /// **No served surface produces this.**
    #[error("this call was made from inside the target actor's own turn: it would deadlock")]
    Reentrant,
    /// Everything a call can meet that the shared vocabulary does not name.
    /// **No local surface produces this.**
    #[error("{0}")]
    Other(#[from] Error),
}
```

Three properties this shape has and the goal's does not:

1. **`Withheld` appears in exactly one arm regardless of which side produced it.**
   `Unanswered::Turn(w)` and `Error::Remote(RemoteError::Withheld(w))` both land
   on `Failure::Turn(w)`. That is the deliverable, and it is the only thing a
   generic caller needs a new type for.
2. **Nothing is erased.** `Other` carries the concrete `Error`, so every remote
   condition stays matchable and the `source()` chain survives.
3. **Two arms are one-sided, and both are checkable.** `Reentrant` has no served
   producer (wave 1 §2.4: `turns.rs:4769-4780` argues the peer path is
   unreachable). `Other` has no local producer. Neither is representable-only
   guesswork — §6 turns both into tests.

`#[non_exhaustive]` **is** on this one, and the discriminator is wave 1 §2.1's:
does every caller handle every variant the same way, or must a caller be *made*
to look at a new one. A generic caller's normal move is `?` — it propagates the
whole value. `Withheld` stays open one level in, which is where a new loss is
actually added. This is the `RemoteError` half of the wave-1 rule, not the
`Withheld` half.

**Two costs, stated:**

- **`Failure` is `Debug + Error` and nothing else.** `Error` is neither `Clone`
  nor `PartialEq` (`error.rs:15-17` derives `Debug` and `thiserror::Error` only),
  so `Failure` cannot be either — while `Unanswered` today is `Clone + PartialEq
  + Eq` (`turns.rs:3212`). A test that writes `assert_eq!(handle.get(1).await,
  Err(Unanswered::Turn(Withheld::Gone)))` today **cannot** write that through
  `Calls`. The remedy is one accessor, not a hand-written `PartialEq`:

  ```rust
  impl Failure {
      /// The shared condition, when this is one.
      #[must_use]
      pub fn withheld(&self) -> Option<&Withheld> {
          match self { Self::Turn(w) => Some(w), _ => None }
      }
  }
  ```

  `Withheld` is `PartialEq + Eq` (`observe.rs:869`), so the comparable part stays
  comparable. Say this in `Failure`'s rustdoc; a test author who discovers it by
  hitting `E0369` will reach for a derive that cannot exist.
- **No `From<NotQueued>`.** `tell` is not in the trait (§2.1), so the conversion
  has no caller. Adding one on speculation puts a third door into `Failure` that
  nothing walks through. `turns.rs:3401` already has `From<NotQueued> for
  Unanswered` if it is ever wanted.

### 2.3 `Failure` lives in `error.rs`, not in `observe.rs`

Wave 1 §2.2 made `observe.rs` the failure vocabulary. This type does not join it.

`observe.rs` names what happened to a **turn** — `Withheld`, `TurnLost`,
`EndReason`. `Failure` names what happened to a **call**, and its `Other(Error)`
arm makes it a strict superset of `Error`'s job. `error.rs` already reaches the
served half of the vocabulary through `Error::Remote(#[from] crate::RemoteError)`
(`error.rs:71`), so the type sits beside the enum it wraps rather than beside the
enum it quotes.

State the direction in the module header: `error.rs` gains a `use crate::Withheld`.
The reverse — `observe.rs` importing `Error` — would make an observation module
depend on the transport error, which is the dependency wave 1 §2.2 was careful to
introduce deliberately and name out loud.

### 2.4 RFC-04 §5.5 is not overturned, and this is the paragraph that says why

**`docs/rfc-04-actors.md:783-800` is a written permanent rejection of exactly the
thing this goal sounds like.** The goal never names it. Its heading is *"This
rejection of location transparency is permanent"*, and it opens:

> Recorded here so that a future proposal to unify `Handle` and `Client` has an
> answer already written, rather than relitigating section 5.2 from scratch.

Its argument, at `rfc-04-actors.md:788-800`:

> A unified handle must either claim failures the local case cannot have, or hide
> failures the remote case does have. […] If a future RFC proposes the
> unification, it must first answer what `Error::SchemaMismatch` means for two
> objects in one process. There is no honest answer, which is the point.

**The answer, and it must be written into the RFC in commit 5:**

§5.5 forbids merging the **types**. This wave merges neither: `Handle` and
`Client` stay distinct types, and their **inherent** methods keep their exact
error types — `Unanswered` locally, `Error` remotely. Every caller that exists
today is unaffected. `Failure` is reachable **only through `Calls`**, which is a
surface a caller opts into by writing a generic bound, and writing that bound is
precisely the act of saying *I have chosen not to know which surface I hold.*

So §5.5's question has an answer now, and it is not a dodge:
`Error::SchemaMismatch` reaches a generic caller as `Failure::Other`, an arm
**no local surface produces**, and §6 makes that a test rather than a claim. The
crate keeps the property §5.5 is defending — a caller can tell from the type what
can go wrong — everywhere except the one signature where the caller asked for the
width.

**Two edits §5.5 needs anyway, which wave 1 scheduled and did not make.** Wave
1's plan §9 listed *"RFC-04 §5.5 re-argued"* among commit 7's appends; measured at
`2dd3eb4`, the section is unchanged. It carries two statements that are now false:

- `rfc-04-actors.md:791` — *"A local mailbox has exactly one failure: the task is
  gone."* `Withheld` has seven variants (`observe.rs:870`), and `Unanswered` two
  arms.
- `rfc-04-actors.md:788` — names `MailboxClosed`, a type that no longer exists.
  The current spelling is `NotQueued::Closed` (`turns.rs:3314`).

Repairing those is in radius for this wave because this wave is the proposal §5.5
was written to answer.

### 2.5 One trait, and it is async. There is no sync twin.

The goal argues for a sync twin from a claim that is false: *"`blocking::Runtime::run`
calls `guard()?` first, returning `Error::BlockingInAsync` whenever a tokio
runtime is present. An impl compiles and always fails when awaited."*

Measured: `guard()` (`blocking.rs:115-120`) returns `Err` **only when
`tokio::runtime::Handle::try_current().is_ok()`**. Under `futures::executor::block_on`
there is no current tokio runtime, so `guard()` returns `Ok` and the call
succeeds. **"Always fails" is false**, and a plan that shipped it would put a
false sentence into a `#[diagnostic::on_unimplemented]` note.

The true argument does not need it, and it is stronger:

**The blocking side has no local surface.** `blocking` generates `Client`
(`macros.rs:2784`) and `IdempotentClient` (`macros.rs:2854`) and nothing else —
there is no `blocking::Handle`, because there is no such thing as a synchronous
local actor call. So a sync trait would range over two surfaces that are **both
remote and both governed**. There is no local-versus-remote genericity for it to
express, which is the entire intent of the goal. A trait that cannot state the
goal's claim is machinery.

Two supporting facts, neither load-bearing on its own:

- `blocking::Client` lacks `send`, `recv` and `blob` (`macros.rs:3096-3101`), so
  it could never carry the same method set even if the set grew past `rpc`.
- The type exists for callers who have no runtime (`blocking.rs:32`,
  `tests/protocol.rs:752-753`). Handing them an `async fn` returns the problem
  the type was built to remove.

**Ruling: one trait, async, `Send` futures.** `blocking::Client` gets **no
impl**, and its absence is the guard the goal asked for — a `compile_fail`
doctest reaches it with no second trait in the tree.

**The method shape follows an existing house precedent**, and this is not a free
choice: `peer.rs:41-45` declares

```rust
fn dial(node: &Node, addr: EndpointAddr)
    -> impl std::future::Future<Output = Result<Self>> + Send;
```

`async fn` in a trait gives no `Send` bound, so a future from a generic call
could not cross `tokio::spawn`. `Dial` already pays for `+ Send` in return-position
`impl Trait`, and `Calls` matches it exactly.

**MEASURED at `b2d046a`, and this was the plan's only inference.** A scratch
integration test declared a one-method service, took a `Handle` from
`probe::actor`, and did two things: passed `h.ping()` to
`fn assert_send<T: Send>(_: T)`, and drove a clone of the handle through
`tokio::spawn(..).await`. **Both compiled and the test passed.** So a generated
`Handle<A>` rpc future is `Send` and survives a spawn, which is the property that
matters — `+ Send` in return position is viable and the fallback ladder below is
not needed. The probe was deleted after measuring; it existed only to answer this.

### 2.6 `Governed` is adopted — and it does **not** mean at-most-once

The goal's hazard framing needs two corrections before the marker can be ruled on.

**Correction 1 — `Middleware` is not the only thing a local call skips, and the
handler is not blind.** `Caller` (`middleware.rs:389-397`) is an argument to the
actor's **own handler methods**: `Origin::local().caller()` yields `Caller::Local`
(`actor/remote.rs:287`), and the in-crate expansion shows every handler taking
`_cx: crate::Caller<'_>` (`macros.rs:4133-4160`). So an actor that cares about who
called **can already tell**, on both paths, and `Caller::Local`'s own rustdoc
(`middleware.rs:390-393`) says there is nothing to attribute a local call to. What
a `Handle` bypasses is what hangs off the `ServiceBuilder`: the `Middleware`
stack, the `Allowlist`, and the idempotency ledger.

**Correction 2 — and this one kills the goal's proposed use of the marker.**
`Governed` is one bit, and it conflates two facts. A plain `Client` goes through
the `Dispatch` (so: `Middleware`, `Allowlist`) but carries **no idempotency key**,
so it has **no ledger**. The goal's correctness face — *"`settle(&keyed_client)`
is at-most-once through the ledger; `settle(&handle)` has no ledger at all"* — is
therefore **not closed by `impl Calls + Governed`**: that bound accepts a plain
`Client`, which is at-least-once under retry exactly like a `Handle`.

**Ruling: adopt `Governed`, defined narrowly and documented as half a fix.**

```rust
// keylink/src/rpc.rs
/// This surface reaches the actor through a [`Dispatch`], so the service's
/// `Middleware` stack and its `Allowlist` ran.
///
/// Write `impl Calls + Governed` where that must be true. A `Handle` is refused,
/// because a local call reaches the mailbox directly.
///
/// # It does NOT mean at-most-once
///
/// At-most-once is a property of a **key**, not of a type: it starts at
/// `Client::with_idempotency(key)` and it holds only while the key is stable
/// across retries. A bound cannot see a key's stability, so no bound can promise
/// this. Name `IdempotentClient` in the signature where the ledger is required.
pub trait Governed: GovernedSeal {}
```

Sealed through a `#[doc(hidden)] pub trait GovernedSeal {}` re-exported at the
crate root, because the impls are written by `service!` expanding in a downstream
crate and must be able to name the supertrait.

**Refuse the second marker.** An `Idempotent` twin would have exactly one
implementor per service and would still promise something a type cannot check —
key stability. The honest instrument is the type's own name in the signature.

**Refuse `const GOVERNED: bool`.** The goal names it as a logging fallback. It
loses on the goal's own criterion: the whole value is **visibility at the
signature a reviewer reads**, and a const asserted in a function body is not
visible there. A trait bound plus `#[diagnostic::on_unimplemented]` is.

**`Governed` is implemented only where `Calls` is** — `ActorRef`, `Client`,
`IdempotentClient`. Three one-line impls per service. `blocking::Client` and
`blocking::IdempotentClient` are governed in fact and get **no impl**, because a
marker on a type that cannot satisfy the other half of the bound is a name nobody
can use.

### 2.7 Four implementors, and `Peer<C>` is not one

`impl Calls` is generated for `Handle<A>`, `ActorRef<'_>`, `Client` and
`IdempotentClient`.

`Peer<C>` (`peer.rs:56`) is refused. Its whole surface is `call(|c| async move {
… })` (`peer.rs:107-116`) — it takes a closure over an `Arc<C>` and reconnects
once. A per-method impl would re-emit every method a fifth time to gain a
reconnect the caller can have for free:

```rust
let client = peer.connect().await?;
shared_generic_fn(&*client).await?;
```

State that in `Peer`'s rustdoc so the next reader does not re-derive it. The one
thing lost is the automatic single reconnect, and `peer.rs:107-110` documents
that `call` retries **once** and returns the second result whatever it is — a
caller who wants that around a generic function writes the loop.

**`Handle`'s impl delegates to the inherent method, and does not reach for
`ask_catching`.** This is a real fork and the wrong branch is tempting: locally a
handler panic **unwinds into the caller** (`turns.rs:3856-3876` — `ask` calls
`panic.resume()`), while remotely it arrives as `Withheld::Panicked`
(`turns.rs:3892-3895`). Routing the trait through `LocalActor::ask_catching`
(`turns.rs:3896`) would make the two agree — and would give `Handle` **two
behaviours under one method name**, selected by whether the receiver is generic,
because an inherent method outranks a trait method. Same call, same type, two
outcomes.

**Ruling: the trait's body is `self.$name(args).await.map_err(From::from)`** for
all four implementors — one emitter, four uses, and the trait can never disagree
with the inherent method because it *is* the inherent method. The divergence is
then real and must be documented on the generated trait, not hidden by it:

> **A handler panic behaves differently by surface.** Against a `Handle` it
> unwinds into the calling task. Against a served surface it arrives as
> `Failure::Turn(Withheld::Panicked { .. })`. `Failure` cannot express an unwind,
> and `observe.rs:779-783` argues why it should not try.

This also removes the goal's item 6 (*"Expose `ask_catching` on `Handle`"*) as a
separate deliverable — see §12.

### 2.8 `keyed_serve` is adopted, and it is the cheapest thing here

Goal item 7. `Handle::serve` (`macros.rs:2234-2251`) is three lines of body:

```rust
$crate::Server::builder(node)
    .service(self.service(allow).build())
    .spawn()
```

The keyed path has no twin, so a caller writes the builder by hand — the shape
`console.rs:26` documents. **Ruling: generate the twin**, beside `keyed_service`:

```rust
/// Serve every actor of this service on `node`, on this service's keyed ALPN.
///
/// The keyed twin of [`Handle::serve`]. Use [`keyed_service`] instead when the
/// node serves more than one thing.
pub fn keyed_serve(
    system: &System,
    node: &$crate::Node,
    allow: $crate::Allowlist,
) -> $crate::Result<$crate::Server> {
    $crate::Server::builder(node)
        .service(keyed_service(system, allow).build())
        .spawn()
}
```

It earns its place on symmetry rather than on saved keystrokes: an author who
finds `serve` on the singleton path and not on the keyed one will ask why, and
there is no answer. **It is also the one droppable commit in this wave** — it
depends on nothing and nothing depends on it, so if the wave runs long it is cut
without re-planning anything.

---

## 3. The shape

```rust
// keylink/src/error.rs
pub enum Failure { Turn(Withheld), Reentrant, Other(Error) }   // §2.2
impl Failure { pub fn withheld(&self) -> Option<&Withheld>; }

impl From<Unanswered> for Failure {
    fn from(e: Unanswered) -> Self {
        match e {
            Unanswered::Turn(w)     => Self::Turn(w),
            Unanswered::Reentrant   => Self::Reentrant,
        }
    }
}

impl From<Error> for Failure {           // the derive on `Other` is NOT enough
    fn from(e: Error) -> Self {
        match e {
            // THE LIFT. Without this arm the shared condition arrives as `Other`
            // and the whole wave buys nothing.
            Error::Remote(RemoteError::Withheld(w)) => Self::Turn(w),
            other => Self::Other(other),
        }
    }
}
```

**`#[from]` on `Other` must be dropped in favour of the hand-written impl**, or
the derive and the match are two `From<Error>` impls and the tree does not
compile. Keep the arm's doc comment naming the lift; a reviewer reading
`Self::Other(other)` alone cannot see that one variant was intercepted above it.

```rust
// generated inside `mod $svc`, one per service
#[diagnostic::on_unimplemented(
    message = "`{Self}` is not a calling surface for this service",
    label = "not a `Calls` surface",
    note = "implemented for `Handle`, `ActorRef`, `Client` and `IdempotentClient`.\n\
            `blocking::Client` is deliberately absent: it is synchronous, and every \
            method here returns a future."
)]
pub trait Calls {
    // from __keylink_calls_decl!, one arm per kind: `rpc` emits this signature,
    // `tell`/`send`/`recv`/`blob` emit `{}`
    fn $name(&self $(, $arg: $ty )*)
        -> impl ::core::future::Future<
               Output = ::core::result::Result<$ret, $crate::Failure>
           > + Send;
}

// Two emitter macros, both `#[doc(hidden)] #[macro_export]`, both five-armed:
//   __keylink_calls_decl!  — the signature above
//   __keylink_calls_impl!  — the one body below
// Four impl blocks, one emitter, so the bodies cannot drift apart.
impl<A: Actor> Calls for Handle<A> { /* __keylink_calls_impl! per rpc */ }
impl Calls for ActorRef<'_>        { /* same emitter */ }
impl Calls for Client              { /* same emitter */ }
impl Calls for IdempotentClient    { /* same emitter */ }

impl $crate::GovernedSeal for ActorRef<'_>     {}
impl $crate::Governed     for ActorRef<'_>     {}
impl $crate::GovernedSeal for Client           {}
impl $crate::Governed     for Client           {}
impl $crate::GovernedSeal for IdempotentClient {}
impl $crate::Governed     for IdempotentClient {}
```

One `rpc` body, four times:

```rust
fn $name(&self $(, $arg: $ty )*)
    -> impl ::core::future::Future<
           Output = ::core::result::Result<$ret, $crate::Failure>
       > + Send
{
    async move { self.$name($( $arg ),*).await.map_err(::core::convert::From::from) }
}
```

**`+ Send` is the one thing in this plan that must be compiled before it is
committed to**, and it is step 1 of commit 3. `LocalActor<A, M>`'s fields
(`turns.rs:3455-3492`) are an `mpsc::Sender`, a `watch::Receiver`, a `Fate` and
two `Arc`s, so `Handle<A>: Sync` should hold wherever `Actor` already requires
`Send` — *should*, not *does*. If it does not:

- **Do not** relax the whole trait to a bare `impl Future`. A future that cannot
  cross `tokio::spawn` is a trait nobody can use in a server.
- Add the narrowest bound that makes it hold and record which one, in the
  generated trait's rustdoc.
- Only if no bound works, drop `+ Send` **and** write the cost into the trait's
  own docs in the same commit.

---

## 4. The work, in commits

`lint-fast` is `cargo clippy -p keylink --lib` (`mise.toml:254`) — **library
target only**, so a green `pre-commit` is not proof a commit compiles. Every
commit below is verified with **all four** of:

```
cargo check --workspace --all-targets
cargo nextest run --workspace
mise run docs
cargo test -p keylink --test protocol contract_hash_has_not_moved
```

The fourth is a **negative** check: no commit here may move it. §6 says why.

| # | Commit | ALPN pin | anchors registry | intra-doc links broken |
|---|---|---|---|---|
| 1 | `Failure` in `error.rs` + `From` impls + re-export | **no** | **rows `C1`, `C2`** | 0 |
| 2 | `Governed` + `GovernedSeal` in `rpc.rs` + re-exports | **no** | no | 0 |
| 3 | `Calls` — generated trait + 4 impls + 3 `Governed` impls | **no** | **row `C3`** | 0 |
| 4 | `tests/surfaces.rs` + the two `compile_fail` doctests | **no** | no | 0 |
| 5 | RFC-04 §5.5, RFC-05, README appends | **no** | no | 0 |
| 6 | `keyed_serve` | **no** | no | 0 |

**No commit moves an ALPN pin, and this is a claim with a mechanism.** What
`postcard_schema` hashes is variant names and struct field names — verified in
the dependency itself, `postcard-schema-0.2.5/src/key/hash.rs:203-204`
(`hash_named_variant`) and `:232-233` (`hash_named_value`), with the type-name
line commented out at `:194-200`. **Not a keylink file**; wave 1 §2.2 cites the
same coordinates without saying where they live. This wave adds a trait, three impl
blocks and one free function. It adds no variant to `Request` or `Response` and
renames no field of either. The pins stay at `keylink/Probe/75cb158fa0f636c8`
(`tests/protocol.rs:1251`) and `keylink/Inspect/f62a21fb09137ace`
(`tests/protocol.rs:1380`). **If either moves, stop** — something changed the
wire that this plan did not predict, and updating the literal would bury it.

**Every commit adds intra-doc links and breaks none.** The rule that matters is
the other one: **`mise run docs` cannot see a link to a `#[doc(hidden)]` item.**
It resolves, emits no warning, and ships a dead raw href. `GovernedSeal` is
`#[doc(hidden)]`, so **`Governed`'s rustdoc names it in backticks and never as a
link**, and neither does anything else. Dead hrefs of this kind already exist in
the tree — reported as ten, unmeasured here, and not a figure this plan carries.
**The predicate is what matters: this wave adds none**, and no command proves
that, because no command can. It is enforced by not writing the link.

**Order matters and 1 → 2 → 3 is not negotiable.** Commit 3's generated impls
name `$crate::Failure` and `$crate::GovernedSeal`, so both must already be
exported. Commits 4, 5 and 6 are independent of each other.

**Commit 2 lands a trait with no implementors.** That is deliberate — it keeps
commit 3 to one concern — and it is inert rather than dead: `Governed` is `pub`,
so `dead_code` does not fire.

**Commit 3 is the big one and is deliberately not split.** It carries the two
emitter macros, four impl blocks, three `Governed` impls, the assertion inside
`mod expansion` (`macros.rs:4059`), and the consumer-position resolution in
`tests/api.rs`. Splitting the trait from its impls leaves the tree with a
generated trait nothing implements, which `pre-commit` cannot see is wrong.

**Commit 3 must also read `macros.rs:4097-4175` before touching anything.** That
module — `no_streaming_on_a_local_handle` — proves `Handle` has no `store` and no
`fetch` by giving it default trait methods and asserting the calls resolve to the
trait. It survives this wave **because `Calls` carries no `send` and no `recv`**
(§2.1); a `Calls` that did would make `handle.store()` ambiguous between two
traits (`E0034`) and break a guard this plan is not otherwise near. The trait's
`rpc` methods do not collide, because an inherent method outranks a trait method
and `Handle::ping` is inherent.

---

## 5. Method parity — what closes, and what is refused with a reason

The goal asks which gaps close. Measured at `2dd3eb4`, every gap already carries a
written argument in the tree. **None of them closes in this wave**, and each
refusal below quotes the argument that already exists rather than inventing one.

| Gap | Refused at | Argument |
|---|---|---|
| `Handle` has no `send`/`recv` | `macros.rs:3941-3964` | A `BodyReader` reads a QUIC `RecvStream`; a `recv` handler drains a `Body` into a `SendStream`. "Neither half exists in this process." |
| `ActorRef` has no `tell` | `macros.rs:3695-3719` | A keyed `tell` is fire-and-forget into a registry that can refuse four ways the caller can never learn about — including "you addressed an actor that does not exist". |
| `blocking::Client` has no `send`/`recv`/`blob` | `macros.rs:3096-3101`, `macros.rs:2776-2779` | Their bodies are async by nature; a synchronous wrapper buffers a whole file or hands back a handle that only works inside a runtime. |
| `blob` returns `BlobId` locally, `BlobHandle` remotely | `macros.rs:4026-4029` | A `BlobHandle` is a content address **plus the node that holds it**. A local handle names no node. |

§12 prices the one the goal argued hardest for.

---

## 6. What is measured, and by what

| Claim | Proof | State |
|---|---|---|
| One function, four surfaces | `one_function_calls_four_surfaces` — a single `async fn` generic over `journal::Calls`, driven against `Handle`, `ActorRef`, `Client` and `IdempotentClient` in one `#[tokio::test]` | **TO WRITE**, `keylink/tests/surfaces.rs` |
| The lift happens | `a_peer_failure_carrying_a_withheld_lands_on_turn` — build `Error::Remote(RemoteError::Withheld(Withheld::Gone))`, convert, assert `Failure::Turn(Withheld::Gone)`, and assert `matches!(.., Failure::Other(_))` is false | **TO WRITE**, `keylink/src/error.rs` `#[cfg(test)]` |
| `Reentrant` survives the conversion | same test, second half: `Unanswered::Reentrant` converts to `Failure::Reentrant` and not to `Other` | **TO WRITE**, same module |
| A local surface never produces `Other` | `a_local_surface_never_reports_other` — drive `<Handle as Calls>` into every loss it can reach (stop the actor for `Gone`; call from inside a turn for `Reentrant`) and assert every error is `Turn` or `Reentrant` | **TO WRITE**, `keylink/tests/surfaces.rs` |
| `blocking::Client` is not a `Calls` surface | a `compile_fail` doctest on the generated trait | **TO WRITE**, `macros.rs` |
| `Handle` is refused by `impl Calls + Governed` | a `compile_fail` doctest on `Governed` | **TO WRITE**, `keylink/src/rpc.rs` |
| The new names resolve from a consumer's position | `tests/api.rs` gains `Calls`, `Governed` and `Failure` — it is "the only place in the repo these names are written from a consumer's position" (`tests/api.rs:36-38`) | **EXISTS** (file), **TO WRITE** (the lines) |
| The generated code compiles for every kind | `mod expansion` (`macros.rs:4059-4060`) already declares all five kinds and two feeds. Add nothing; the trait must expand there or the commit does not build. `mise run features` runs it across eight feature subsets (`macros.rs:4053-4055`) | **EXISTS** |
| The wire did not move | `the_contract_hash_has_not_moved` (`tests/protocol.rs:1250`) and `the_inspect_contract_hash_has_not_moved` (`tests/protocol.rs:1379`) stay **green**, at every commit | **EXISTS** |
| Nothing regressed | `mise run gate`, `cargo nextest run --workspace`, `mise run anchors`, `mise run replay-anchors`, `mise run docs` under `-D warnings` | **EXISTS** |

**Both `compile_fail` doctests use a BARE fence, and name `E0277` in a comment
inside the doctest.** Do not write ```` ```compile_fail,E0277 ````. The crate has
written this rule down twice and it is the opposite of the obvious one —
`turns.rs:416-420` and `observe.rs:189-193`, in near-identical words:

> The code is named HERE and deliberately **NOT** on the fence. rustdoc does not
> check the code after `compile_fail`, so an annotated code asserts a
> verification it does not perform — this project has shipped five guards that
> were satisfiable without proving their claim, and a fence reading
> `compile_fail,E0639` is how the sixth would look.

So the annotation buys nothing and reads as a check. What the guard actually
rests on is the **manual replay** below, which is why every `compile_fail` in
this wave gets one. The residual risk stays real and stated: a bare
`compile_fail` also passes when the snippet fails for an unrelated reason, and
only the replay separates the two. `macros.rs:217` records the other half —
rustdoc reports *"Test compiled successfully, but it's marked `compile_fail`"*
when the guard **stops** failing, which is what the replay looks for.

**Do not name the `Withheld` equality test as a proof of this wave.**
`a_local_caller_and_a_peer_read_the_same_withheld` (`tests/defer.rs:882`,
**EXISTS**) is wave 1's, it is untouched here, and citing it would credit this
wave with a claim it does not make.

---

## 7. Blast radius

**Measured at `2dd3eb4` for sizing.** The predicate, not the list, is the
instruction: *every file that names `Failure`, `Governed`, `GovernedSeal` or
`Calls` as items*, verified by `cargo check --workspace --all-targets`.

| File | What changes |
|---|---|
| `keylink/src/error.rs` | `Failure`, `withheld()`, two `From` impls, **and a first `#[cfg(test)] mod` — the file has none today** |
| `keylink/src/rpc.rs` | `Governed`, `GovernedSeal`, one `compile_fail` doctest |
| `keylink/src/lib.rs` | three re-exports; `Failure` joins the `error` list, `Governed` and `GovernedSeal` the `rpc` list |
| `keylink/src/macros.rs` | two emitter macros; the `Calls` decl and four impls inside `__keylink_service!`; three `Governed` impls; `keyed_serve`; one `compile_fail` doctest |
| `keylink/tests/api.rs` | consumer-position resolution of the three new names |
| `keylink/tests/surfaces.rs` | **new file** — the shared function and the one-sidedness test |
| `docs/rfc-04-actors.md` | §5.5 append (§2.4), plus the two false statements at `:788` and `:791` |
| `docs/rfc-05-idempotency.md` | append: `Governed` does not mean at-most-once (§2.6) |
| `README.md` | the trait, and the four kinds it does not carry |

**`keylink-persist/` and `demo/` are untouched.** Both are additive-only
consumers here: nothing is renamed, nothing changes type, and no existing
signature moves. `demo/src/main.rs` calls handler methods directly with
`Caller::Local` (`demo/src/main.rs:684-740`) and never names a client surface
generically.

**The four-surface setup already exists — copy it, do not invent it.**
`keylink/tests/actors.rs:699-716` stands up exactly what `tests/surfaces.rs`
needs, in one test body: a mounted `system` plus a `journal::actor(..)` handle,
**both** served on one node (`.service(journal::keyed_service(..))` at `:706` and
`.service(singleton.service(..))` at `:707`, with the expectation string *"the
keyed and singleton ALPNs are distinct"*), then a `KeyedClient` at `:711` and a
`Client` at `:714`. That gives `Handle`, `ActorRef` and `Client`;
`IdempotentClient` is one more call, `Client::with_idempotency`
(`macros.rs:2654`).

The two ALPNs are `alpn()` (`macros.rs:567`) and `keyed_alpn()`
(`macros.rs:814`), and `tests/protocol.rs:1055-1066` already asserts they must
not hash the same *"or one node cannot serve both"*.

**A new file is still the right home for the claim**, because *one function
against four surfaces* is a new cross-cutting assertion with no owner —
`tests/actors.rs` is about the actor surface, not about surface-genericity. The
new file borrows that file's fixture and nothing else.

---

## 8. Break records and anchors

**`RECORD_BLOCKS` is `176` at `.mise/tasks/anchors:3318`, measured at `2dd3eb4`
for sizing.** Wave 1's plan recorded `177` at `anchors:3239`; both were true when
written, which is the whole reason this figure is never a target. **Read the new
value off the failing run of `mise run anchors`. Never compute it** —
`anchors:3249-3294` refuses a computed value, because an agreeing subtraction
reads as a derivation and is a coincidence. One comment block can hold two rows
(`anchors:3155`), so a record count is not a block count.

**Three break records, and the reason each one is expressible as a single-line
substitution:**

| Row | Guard | The one line to break |
|---|---|---|
| `C1` | the lift in `From<Error> for Failure` | the `Error::Remote(RemoteError::Withheld(w)) => Self::Turn(w)` arm — reroute it to `Self::Other(other)` |
| `C2` | `From<Unanswered> for Failure` keeps `Reentrant` typed | the `Unanswered::Reentrant => Self::Reentrant` arm |
| `C3` | the trait's bodies are the inherent methods | the single `rpc` arm of `__keylink_calls_impl!` |

`C3` breaks all four impls from one line, which is what makes it worth a record:
a substitution there reddens `one_function_calls_four_surfaces` for every surface
at once, proving the test walks the trait and not the inherent methods.

**Registry rows take the tuple shape at `anchors:1745`** —
`(id, test_file, edited_file, literal)`. `C1` and `C2` edit `keylink/src/error.rs`
from tests in the same file, so **the record must not quote its own search
literal**: `anchors:1753-1757` records that a same-file record quoting itself
aborts the harness at *"found 2"*. Indentation is what keeps each unique.

**Count anchor uniqueness AFTER `cargo fmt`.** The formatter owns the line breaks
a record quotes, and a literal measured before `fmt` has gone dead through a whole
gate before.

**`── selected=1` is the only reading a record supports** (`break-once.sh:169-215`).
`0` means the filter named nothing. **`?` means no totals line at all** — an abort
or a tree that did not compile — and `break-once.sh:204-206` warns it is **not**
a zero. A break that leaves the macro non-compiling produces `?`, which reads
exactly like a caught defect.

**The two `compile_fail` doctests get NO break record, and get a manual replay
instead.** The reason is mechanical, not a preference: each guard asserts an
**absence** — that no `impl Calls for blocking::Client` exists, and that no
`impl Governed for Handle<A>` exists — and an absence has no line to substitute.
`break-once.sh:160` substitutes one quoted literal; writing a missing impl is an
addition. So each doctest's rustdoc carries a replay recipe in prose, and the
implementer runs it once and records the output in the commit message:

1. Add the impl by hand in `macros.rs`.
2. `cargo test -p keylink --doc <name>`.
3. Confirm rustdoc reports *"Test compiled successfully, but it's marked
   `compile_fail`"*.
4. Revert **that one line by hand**. Do not `git checkout` the file — a revert
   destroys co-located edits.

**Nothing in the gate replays break records.** `mise run replay-anchors` self-tests
the harness, and `scripts/replay-fate-breaks.sh` is not a gate step. So the three
records above are re-checked by hand, once, in the commit that registers them.

**No existing record is at risk.** This wave renames nothing, deletes nothing and
changes no watched output. `mise run anchors` fails on the three new blocks and on
nothing else; a failure naming any other row means something unplanned moved.

---

## 9. Docs

**Four appends, and each is append-only.** No ordinary prose line in any RFC is
edited — except the two false statements named in §2.4, which are corrections to
statements this wave makes checkable, not history being rewritten.

- **RFC-04 §5.5** (`rfc-04-actors.md:783-800`) — §2.4's answer, in full: the types
  stay distinct, inherent methods keep their exact error types, `Failure` is
  reachable only through a bound a caller opts into, and `Error::SchemaMismatch`
  reaches a generic caller as an arm no local surface produces. Plus the two
  repairs at `:788` (`MailboxClosed` → `NotQueued::Closed`) and `:791` (*"exactly
  one failure"* → seven `Withheld` variants and two `Unanswered` arms).
- **RFC-04, a second append** — the four written refusals of §5, gathered in one
  place with the marker's limit, so the next author does not rediscover them one
  compile error at a time.
- **RFC-05** — §2.6's correction: `Governed` means the `Dispatch` ran. It does
  **not** mean at-most-once, because a plain `Client` is governed and carries no
  key. Name `IdempotentClient` where the ledger is required.
- **README** — the trait, the four kinds it does not carry, and the panic
  divergence of §2.7.

**No gate reads `README.md`.** Nothing catches a stale line there, which is why
it is scheduled rather than assumed.

**`docs/audits/` is NOT updated, ever.** It is dated evidence, and its citations
are correct at the commit it names. Editing it destroys the thing that makes it
checkable.

**`CONTRIBUTING.md` needs nothing.** This wave edits no break-record search
literal, so wave 1 §2.6's carve-out is not stretched.

---

## 10. wasm and features

**Four of the gate's steps are `wasm`, `wasm-guards`, `wasm-web`, `wasm-browser`,
and the dimension that matters is `--no-default-features` compilation.** Nothing
here is `#[cfg]`-gated: `Failure` sits in `error.rs`, `Governed` in `rpc.rs`, and
the generated `Calls` sits outside `__keylink_if_blocking!`
(`macros.rs:2942-2957`). Stated so a red step is read as a `Send`-bound problem
rather than a feature one.

**The `blocking` feature is the one interaction, and it is a non-interaction.**
`blocking::Client` gets no `Calls` impl and no `Governed` impl, so the generated
output is byte-identical with the feature on and off, apart from the
`blocking` module itself. `mise run features` runs the in-crate expansion
(`macros.rs:4053-4055`) across eight feature subsets, which is what turns that
from a belief into a check.

**`mise run docs` runs `--all-features`** (`mise.toml:513`), so both
`compile_fail` doctests are compiled under a tree where `blocking::Client`
exists. A doctest asserting `blocking::Client` is not a `Calls` surface would pass
vacuously if the type were absent — under `--all-features` it is present, and the
assertion is about the missing impl rather than the missing type.

---

## 11. What the goal got wrong

Eight claims, each measured against `2dd3eb4`. Three of them changed a ruling.

1. **"`keyed_service!` macro"** — there is no such macro. `service!`
   (`macros.rs:252`) is the only public one; every other `macro_rules!` in the
   file is `__keylink_`-prefixed and `#[doc(hidden)]`. `keyed_service` is a
   generated free **function** (`macros.rs:1202`).
2. **"only `Handle::service` builds a `Dispatch`"** — `Dispatch` is a type
   **alias** (`rpc.rs:89`), and it has **three** construction sites:
   `macros.rs:705` (the plain `service`), `macros.rs:1207` (`keyed_service`) and
   `macros.rs:2149` (`Handle::service`). The hazard framing built on "only one"
   does not stand; §2.6 rebuilds it.
3. **"`Handle::ask_catching`"** — it is `LocalActor::ask_catching`
   (`turns.rs:3896`), and `Handle`'s only field is a private `inner:
   LocalActor<A, Mail>` (`macros.rs:1973`). Reaching it needs a generated method
   per `rpc`, not an exposure. §2.7 and §12 rule on it.
4. **"`BodyReader` gains a `Local(Body)` arm"** — `BodyReader` is a **struct**
   (`body.rs:135`), so this is a struct-to-enum conversion of a public type, not
   an arm addition. It is also **cheaper than either the goal or its reviewer
   assumed**: `self.frames` is read at exactly **one** line (`body.rs:176`, inside
   `next_chunk`); `copy_to`, `read_to_bytes` and `drain` all route through it. So
   the split needs one `match`, and the lazier form keeps the public type a
   struct and makes the **private field** the enum. §12 prices the rest.
5. **"an impl compiles and always fails when awaited"** — false. `guard()`
   (`blocking.rs:115-120`) fails only when a **tokio** runtime is current; under
   `futures::executor::block_on` it returns `Ok`. §2.5 re-argues the twin from a
   true premise and reaches the opposite conclusion.
6. **"Client types 6 → 4"** — measured, there are **6 generated**
   (`macros.rs:2413, 2573, 2610, 2720, 2784, 2854`), **2 hand-written**
   (`rpc.rs:1834` `RpcClient`, `rpc.rs:1968` `IdempotentClient`) and `Peer<C>`
   (`peer.rs:56`). The generated `IdempotentClient` **wraps** the hand-written one
   (`macros.rs:2721`), so the proposed fold reaches 6 + 1, not 4, and moves the
   hazard into `rpc.rs` rather than removing it. §12 refuses it.
7. **"`ServableAsActor::CHECK` fires at four sites; `SpawnableUnfed` is 3 of 3"** —
   confirmed, and this plan carries neither as a claim of its own. `CHECK`:
   `macros.rs:1383, 1468, 1576, 2147`. `SpawnableUnfed`: `macros.rs:1384, 1605,
   1636`. Recorded only so nobody re-measures it.
8. **"a shared `Withheld` is what a trait can be generic over"** — true, and it is
   why this wave is unblocked. But it is not sufficient: `Withheld` is a payload,
   not an error type, and `Unanswered` (`turns.rs:3213`) versus `Error`
   (`error.rs:17`) is the disagreement a signature actually meets. §2.2 supplies
   the missing type.

**A ninth thing the goal does not mention, and it is the largest.** RFC-04 §5.5
(`rfc-04-actors.md:783-800`) is a written **permanent rejection** of unifying
`Handle` and `Client`, recorded so a future proposal *"has an answer already
written"*. This is that proposal. §2.4 answers it rather than working around it.

---

## 12. Refused, so nobody re-derives them

- **Local `send`/`recv` (goal item 4).** The goal itself marks it *"UNVERIFIED by
  the reviewer — compile it before committing to it."* Priced, at `2dd3eb4`:
  a private `Frames` enum in `body.rs` with one `match` at `:176`; a chunking
  reader over `Source::Bytes` and `Source::Reader` (`body.rs:37-42`), which is
  new code producing `Bytes` from a `Pin<Box<dyn AsyncRead + Send>>`; two new arms
  in `__keylink_local_method!` at the crate's doc density; the **rewrite of an
  18-line written refusal** (`macros.rs:3941-3960`) whose argument is still true
  about the transport; an RFC append overturning it; **and a rewrite of
  `no_streaming_on_a_local_handle` (`macros.rs:4097-4175`)**, which the goal never
  names — a `Calls` carrying `store`/`fetch` makes `handle.store()` ambiguous
  between two traits (`E0034`) and reddens a guard on the very refusal being
  overturned. Against that: `Calls` works without it, because `rpc` is the kind
  every surface has and the goal's own motivating example (`settle`) is an `rpc`.
  **Revisit when a second kind must be in `Calls`** — the trait grows additively
  and nothing here forecloses it.
- **A sync twin trait.** §2.5. The blocking side has no local surface, so a sync
  trait cannot state the goal's claim. Revisit if a `blocking::Handle` ever
  exists.
- **Client types 6 → 4 (goal item 5).** §11 item 6. The generated
  `IdempotentClient` wraps `rpc::IdempotentClient` (`macros.rs:2721`), so a
  `Client<K = Unkeyed>` typestate leaves the hand-written pair intact one layer
  down and buys a count, not a property. It also costs two impl blocks per
  surface (one `impl Client<Unkeyed>` carrying `tell`, one `impl<K> Client<K>`
  carrying the rest), a phantom, and a `where` clause on every generic function
  that names `Client`. The goal's own success criterion for it is a **count**, and
  this plan asserts none.
- **`ask_catching` on `Handle` as a public per-`rpc` method (goal item 6).**
  §2.7. The trait is where the catching door becomes reachable, and giving
  `Handle` a second door under a second name multiplies the generated method set
  to expose something the trait already exposes. The panic divergence is
  documented instead of papered over.
- **`blob` returning one type on both sides (goal item 6, second half).** §2.1.
  `BlobHandle` is *"a content address plus the node that holds it"*
  (`macros.rs:4026-4029`); a local `Handle` names no node. The two ways out are
  both regressions: hand a remote caller a bare `BlobId` and make it dial, or
  invent a node for the local path.
- **An `Idempotent` marker beside `Governed`.** §2.6. One implementor per
  service, and it would promise key stability, which no type can check.
- **`const GOVERNED: bool`.** §2.6. It loses on the goal's own criterion — the
  value is visibility **at the signature**, and a const asserted in a body is not
  there.
- **`impl Calls for Peer<C>`.** §2.7. Its surface is a closure, not a method set.
  `peer.connect().await?` then the generic call, and the only thing lost is the
  single automatic reconnect `peer.rs:107-110` documents.
- **Routing local calls through a fake `Dispatch`.** Unchanged from wave 1 §12.
  `middleware.rs:389-396` argues against it — through the `Context` argument,
  rather than through `Dispatch` by name.
- **Keyed `tell` returning a routing ack.** Out of scope per the goal, and the
  tree agrees at `macros.rs:3695-3719`. A separate decision under a new name
  (`deliver`), not a silent widening of `tell`.
