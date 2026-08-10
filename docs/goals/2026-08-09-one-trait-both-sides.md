# Goal 2 of 3: One trait, both sides — and a boundary you can see

**Blocked on goal 1** (`2026-08-09-one-failure-vocabulary.md`). Without a shared
`Withheld` there is no error type a trait can be generic over.

## Intent

A caller should write one function that works against a local actor in a test and
a remote one in production. Today the surfaces share no type, no trait and no
error. This goal gives one generated trait per service — **and makes visible the
boundary that trait would otherwise hide.**

## The hazard this goal creates, and must close in the same change

The crate's real seam is **served vs not-served**, not local vs remote.
`Middleware`, authorization and the idempotency ledger attach to a `Dispatch`,
which only `Handle::service` builds. A local `Handle` on a **served** actor
bypasses all three while a peer on the **same mailbox** passes through them.

A `Handle` bypasses authz today and the crate documents it. What a trait adds is
the loss of **visibility**: the surface is chosen by whoever calls the generic
function, possibly in another crate, and neither its author nor its reviewer can
see whether authorization ran.

**And there is a second face, which is correctness rather than security.**
`settle(&keyed_client)` is at-most-once through the ledger. `settle(&handle)` has
no ledger at all. Same function, two delivery guarantees, no signal — a caller
that retries on error silently turns at-most-once into at-least-once by switching
surface.

**The close:** a sealed marker `trait Governed: Calls`, implemented for `Client`,
`ActorRef` and `blocking::Client` only. Code that must be governed writes
`impl Calls + Governed` and the compiler refuses a `Handle`. Two limits stated
rather than hidden: the bare bound remains the permissive default (inverting it
breaks the core use case), and a `const GOVERNED: bool` is for logging, not
enforcement — you cannot bound on a const value on stable. **Do not** route local
calls through a fake `Dispatch`; `middleware.rs:391-394` argues correctly against
it.

## Scope

**In scope**

1. **Two generated traits, sharing the error type** — async for `Handle`,
   `Client`, `ActorRef`; a sync twin for `blocking::Client`. One trait cannot
   cover both: `blocking::Runtime::run` calls `guard()?` first, returning
   `Error::BlockingInAsync` whenever a tokio runtime is present. An impl compiles
   and always fails when awaited — a trait that says *await me* implemented by a
   type that says *never be awaited*. Carry `#[diagnostic::on_unimplemented]` so
   the mistake reads well.
2. **`Governed`**, per the hazard above.
3. **`Failure`** — the type the bound needs, which does not exist today.
   Recommended `enum Failure { Withheld(Withheld), Other(Box<dyn Error>) }`:
   shared conditions typed, everything else opaque, and no impossible arm forced
   on any caller.
4. **Method parity.** Local `send`/`recv` — **not** by generalising `FrameReader`
   over `AsyncRead`, which forces type erasure and puts a virtual call on the
   success path of every remote call. Give `BodyReader` a second arm instead
   (`{ Wire(FrameReader), Local(Body) }`); `Body` already holds the right source.
   *Marked UNVERIFIED by the reviewer — compile it before committing to it.*
   Blocking to 5/5 needs a `blocking::BodyReader: std::io::Read`, which overturns
   a written refusal — price it.
5. **Client types 6 → 4.** `ActorRef` is a client type and must survive. Fold the
   two `IdempotentClient`s — but **not into a field**: that type exists to remove
   every `tell`, because `with_idempotency(key).some_tell(..)` used to compile and
   send no key. Reach four via `Client<K = Unkeyed>` typestate instead.
6. **Expose `ask_catching` on `Handle`**; `blob` returns one type on both sides.
7. **`keyed_service` gains a `serve` twin.**

**Out of scope**

- Keyed `tell` returning a routing ack. `RpcClient::tell` opens a
  **unidirectional** stream — answering needs bidirectional, which is an `rpc`.
  Close the capability under a new name (`deliver`), or `tell` means a fourth
  thing. **A separate decision, not a silent widening.**
- `with_state` remotely — genuinely closed. Every near-miss collapses into
  "declare an `rpc`", which the method's own doc already says for the mutating
  case; extend that sentence to the projection case.
- Peer-reachable administration — goal 3.

## Success criteria (the shape, not the full list)

- [ ] **One generic function** compiles and passes against `Handle`, `Client` and
      `ActorRef` — in **two** tests sharing that function, because the async
      surfaces need a runtime and the blocking one needs its **absence**
      (`tests/protocol.rs:753` already records that rule). The shared function is
      the proof; a single test never was.
- [ ] A `compile_fail` doctest shows `blocking::Client` rejected by the async
      trait's bound, and another shows a `Handle` rejected by `impl Calls +
      Governed`. **These two are the guards** — without them the traits get
      quietly merged and the boundary re-hidden.
- [ ] Four client types remain — proven by a count of `pub struct` client types,
      **not** by `rg 'pub struct IdempotentClient'`, which passes while four
      remain and does not measure the claim above it.
- [ ] Local `send`/`recv` exist and the **remote** path gains no virtual call —
      proven by reading the generated code, not by a benchmark.

## Note on a claim inherited from the audit

`ServableAsActor::CHECK` fires at **four** sites, not three, and the exempt door
is **deliberately** exempt — a concurrent handler has no serial turn, so `send`
is safe there. `SpawnableUnfed` coverage is 3 of 3. The audit's `REPORT.md:134`
is a false finding; do not carry it.

Invariants, process, interaction policy and definition of done: **as goal 1**.
