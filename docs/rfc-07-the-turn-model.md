# RFC-07 — The turn model: one dispatch path, a typed mailbox, two inputs

Status: in progress. This RFC records the model that issue #3 builds, and supersedes
the parts of RFC-04 and RFC-06 that it makes wrong. Sections are added as each wave
lands; the head of this file is meant to stay true, which is why the superseded
sections are removed rather than appended to.

---

## 1. Wave 0 — the gate. Does the shape compile?

### 1.1 The question

The mailbox stops being a boxed closure (`type Turn<A> = Box<dyn FnOnce..>`,
`local.rs:128`) and becomes a per-service generated enum whose variants each carry their
own typed reply channel. The loop holds `&mut A` across the handler's await, then runs
the observer chain across a second await, then answers.

The risk is the one that produced the `Incarnate` Critical: a future that captures `&A`
and thereby forces `A: Sync` — the bound `observe.rs` exists to keep out, because it is
what lets a state holding a `RefCell`, a `Cell` or a `Box<dyn Any + Send>` be an actor
at all.

Two revisions of RFC-06 believed the wrong answer because their probe used a
unit-struct `Panic`, which is `Sync` where the real one is not. Hence the house rule:
**no stand-in types in a probe.**

### 1.2 The verdict — GREEN

A loop over a generated `Mail` enum, holding `&mut A` across the turn's await and across
the observer chain, with nothing boxed, is `Send + 'static` for a `Send + !Sync` state.

Verified on rustc **1.97.1** and on rustc **1.91.1** (the crate's declared floor), with
tokio **1.53.1** — the same version the workspace resolves, checked by diffing the
derived lock against the repo's across all 388 shared packages: zero drift.

The probe uses the **real** `keylink::Panic`, `Observer`, `TurnInfo`, `TurnOutcome`,
`Caller`, `Context`, `guarded` and `catching` through a path dependency, and the real
`tokio::sync::oneshot`. Only `Answered<T>` is copied, because it is private to
`local.rs`.

### 1.3 What the probe holds, and why each part is load-bearing

- A `Send + !Sync` state carrying **both** a `RefCell<u32>` and a real `Panic`. Two
  independent `!Sync` members, so no single member's removal can silently make the probe
  vacuous.
- The real RPITIT shape from `macros.rs:443/479/1913` — `trait Actor: Send + 'static`
  with `fn m(&mut self, cx: Caller<'_>, ..) -> impl Future<Output = R> + Send`, an
  `async fn` impl, and a generic `async fn __actor_turn<A: Actor>` dispatcher. A plain
  free `async fn` does not reproduce the desugaring the `Incarnate` Critical blames.
- Per-variant `oneshot::Sender<Answered<T>>` for **two distinct** payload types, so a
  monomorphisation coincidence cannot mask a fault.
- The observer chain, with the `catch_unwind` → build-the-future-synchronously → `await`
  sequence copied verbatim from `local.rs:213-231`.
- Nothing boxed. Deleting the boxed turn is the point.

### 1.4 The reply channel is a red herring. Say so, so nobody re-reasons it.

`oneshot::Sender<Answered<T>>` is `Send` — and in fact `Sync` — whenever `T: Send`, and
`Answered<T>: Send` because `Panic: Send`. **No arrangement of per-variant reply channels
can make the loop non-`Send`.**

The risk is entirely in what the loop holds **by reference** across an await. A reader of
the probe must not conclude that it validated the channel choice; it did not, because
there was nothing there to validate.

### 1.5 The red tests. Each was executed; a guard nobody watched fail is not a guard.

The criterion issue #3 stated — "a variant that holds `&A` across the await fails to
compile" — is **vacuous**: it yields byte-identical diagnostics with `Mail`, `oneshot`,
`Panic` and `Answered` all deleted. It re-proves `&T: Send ⇒ T: Sync`, which
`observe.rs:37-44` already documents. It was replaced by four that are not vacuous.

| probe | result | what it proves |
|---|---|---|
| `r0` — `State: Sync` | RED | the state under test is genuinely `!Sync`; without this the whole probe is vacuous |
| `r1` — observer future built inside an `async` block | RED | **the load-bearing one.** `local.rs:206-210` predicts this in prose; `r1` executes it |
| `r2` — `cx: Caller<'static>` | RED, `E0597` | the spec's own `Mail` sketch does not compile. See §1.6 |
| `r3a` — `RefCell` → `Mutex`, `Panic` kept | RED, **different member** | the harness discriminates on `Sync`, not on a fixed diagnostic |
| `r3b` — both `!Sync` members removed | GREEN | identical code, so the only variable was `Sync` |
| `rc_loop`, `rc_actor` | RED | see §1.7 |

`r1`'s decisive line:

```text
error[E0277]: `RefCell<u32>` cannot be shared between threads safely
    = note: required for `&State` to implement `Send`
note: required because it's used within this `async` block
 65 |  let ran = guarded(async move { watching.after_turn(shared, info).await }).await;
```

The `r3a`/`r3b` pair is stronger than the mutation originally specified. Swapping
`RefCell` for `Mutex` does **not** turn the probe green, because `Panic` is still
`!Sync` — instead the `RefCell` diagnostic disappears and only
`(dyn Any + Send + 'static)` remains. Red, red-about-a-different-member, then green:
that brackets the claim in a way a single red/green pair cannot.

### 1.6 Amendment to Wave 1: `cx` is an owned `Origin`, not `Caller<'static>`

Issue #3's Wave 1b sketch gives every `Mail` variant `cx: Caller<'static>`. **That cannot
be built for a peer-originated call**, proved three ways: `E0597` (the borrow does not
live long enough), `E0624` (`Context::new` is `pub(crate)`), `E0515` (cannot return a
value referencing a parameter).

`Caller<'a>::Peer(&'a Context)` holds a borrow, and `Caller` is covariant in `'a`, so a
`Caller` that generated code was *handed* never widens to `'static`. Only the data-free
`Caller::Local` reaches it.

This is not a new constraint — today's keyed mailbox message already carries an **owned**
`pub cx: Context` (`actor/remote.rs:291`) and borrows from it in the loop
(`macros.rs:615`). That is the only shape that works, and the sketch simply lost it.

The generated enum therefore carries an **owned** discriminant:

```rust
#[doc(hidden)]
pub enum Origin { Local, Peer(Context) }        // deliberately NOT Clone — see below

impl Origin {
    /// `pub`, because the loop that calls it is generated into the USER's crate.
    #[doc(hidden)]
    pub fn caller(&self) -> Caller<'_> {
        match self { Self::Local => Caller::Local, Self::Peer(cx) => Caller::Peer(cx) }
    }
}
```

The loop borrows a `Caller<'_>` out of the owned `Origin` for the length of one turn.

**Revision, 2026-08-07.** This section was first written with `fn caller` private and
justified by "`Context` is `Clone`". Both were wrong, and an adversarial review of Wave 1
caught them. Recorded rather than quietly corrected, because the second one is a live
hazard and the first is this RFC reproducing, inside the amendment, the defect class the
amendment exists to fix.

1. **`caller` must be `#[doc(hidden)] pub`.** The mailbox loop is generated into the
   *caller's* crate (`macros.rs:538-540`), so a private method is `E0624: method 'caller'
   is private` at the one site that must call it. This is the same error Wave 0 hit on
   `Context::new`, one layer up.

2. **`Clone` was a non-sequitur, and inviting it is dangerous.** The fix works because
   `deliver` *owns* the `Context` — `remote.rs:407` takes `cx: Context` by value and
   `remote.rs:435` moves it into the message, with the comment *"Read before `cx` moves
   into the `Mail`"*. Ownership, not cloning.

   It matters because `Context` does derive `Clone`, and `Context.progress` is
   `Progress(Arc<AtomicU8>)` (`middleware.rs:112`) whose `Clone` **shares the `Arc`,
   deliberately** (`middleware.rs:235`). `Progress::reached()` is what `rpc.rs:398` reads
   to decide **record versus release** on an idempotency key. Two messages carrying clones
   of one `Context` would share one `AtomicU8` whose marks are `fetch_or` and never
   cleared — so message A reaching `COMPLETED` makes message B read `Reached::Completed`
   and **record a reply B never produced.** A silently wrong at-most-once answer, which is
   the guarantee `idempotency.rs` exists to hold.

   `Origin` is therefore **not `Clone`**, so the mistake is a compile error rather than a
   wrong answer on the wire.

**Second revision, 2026-08-07 — the correction above overstated the guard, and this is the
third pass on the same claim.** An adversarial review of Wave 1a produced three compiled
counter-examples against that last sentence. It is recorded here rather than edited away
because the pattern is the finding: this section has now been wrong about *what withholding
`Clone` buys* in every version of it, each time in the safe-sounding direction.

The sentence is false, and the reason is one word: **nobody clones the `Origin`.** What a
caller clones is the `Context`, before the origin exists.

1. `Origin::Peer(Context)` was a **public tuple variant** over a publicly-`Clone` `Context`,
   so `(Origin::Peer(cx.clone()), Origin::Peer(cx))` type-checked from any crate — two
   origins, one `Arc<AtomicU8>`.
2. Worse, and untouched by taking that variant away: `deliver_to_actor` is
   `#[doc(hidden)] pub` and takes `cx: Context` **by value**, so two calls with
   `cx.clone()` and `cx` share one marker without naming `Origin` at all.
3. The mark is writable from outside the crate too — `Caller::Peer(cx)` reaches
   `cx.progress().completed()`, because `Progress`'s marks are `pub` for generated
   dispatch to call.

Wave 1a is what made any of this reachable downstream: the predecessor was a **private
inherent method**, `System::deliver`.

What Wave 1a's follow-up actually changed, and what it deliberately did not:

- `Origin` is now `pub struct Origin(Provenance)` over a **private** enum, with
  `pub(crate) local()` / `pub(crate) peer()` and `#[doc(hidden)] pub fn caller()`. That
  closes (1) — generated code never constructs an origin, it destructures a `Mail` and
  borrows out of it — and it is held by a `compile_fail` doctest on `Origin` in
  `keylink/src/actor/remote.rs`, which fails with `E0624: associated function 'local' is
  private` and, on `Origin::Peer(cx)`, with `E0599`, there being no public variant left
  to name. **Third revision, 2026-08-07 — that bullet read `compile_fail,E0624` until
  now, and there is no code on that fence.** See §1.6.1.
- (2) and (3) are **not closed**, and the documentation now says so in those words. While
  `Context: Clone` and `deliver_to_actor` is `pub`, two deliveries can share one marker.
  What makes it unreachable today is the call graph — `serve_one` builds one `Context` per
  request and generated dispatch delivers once with it — which is a property of two call
  sites, not of a type. `representability.rs` cannot state it: a compile-time probe refuses
  shapes that do not type-check, and this one does. It is pinned instead by a unit test
  that asserts the sharing is real, so the claim cannot quietly grow back.

**A guard whose documentation overstates it is worse than no guard**, because the next
author reads the guarantee and stops checking — which is exactly how this section got two
revisions.

#### 1.6.1 Third revision, 2026-08-07: the fence had no code, and this is the same wave's own record going stale

The bullet above described the guard on `Origin`'s constructors as *"a `compile_fail,E0624`
doctest"*. **No fence in this workspace carries an error code.**
`grep -rEn '```compile_fail[,[:space:]]*[A-Za-z0-9]' .` returns nothing over the whole
tree; nine bare `compile_fail` fences remain, three of them in `keylink-persist/src/lib.rs`
and one of them the `Origin` fence this bullet is about.

What makes this worth a subsection rather than a silent correction is **whose record it
was**. The bullet was written by `24b7c1b` at 02:48:54, and `3504670` took the code off
that exact fence at 02:54:46 — **six minutes and one commit later**, under the title *"the
error code belongs in the prose, not on the fence"*. So the document recording a guard
outlived the guard's own wording by six minutes, in the same wave, on the same day, and
survived a merge and two further waves before an adversarial review of Wave 1b found it.
Two sibling claims went the same way in `keylink-persist` and are corrected in
`keylink-persist/src/representability.rs` and `keylink-persist/tests/versioned.rs`.

The reason no fence carries a code: **rustdoc does not check the code after
`compile_fail`.** Demonstrated rather than argued, twice in this workspace — three
doctests annotated `compile_fail,E0308` that in fact fail with `E0425` (a typo in the
probe body) or `E0282` all passed green and silently, and swapping the pinned `E0624` on
this very fence for `E0599` left the test green, which was red-tested. An annotated code
therefore asserts a verification that does not happen, which is the shape of every guard
this project has caught being satisfiable without proving its claim.

So the fence asserts exactly one thing — **this does not compile** — and the codes are
prose, both in the bullet above and inside the fence's own comments. What stops the fence
passing for the *wrong* reason, a rename making `local` vanish rather than the privacy
holding, is the in-crate unit test that calls `Origin::local()` and would go red if the
name were gone: one pins that the name exists, the fence pins that it is out of reach.

`representability.rs` objects to `trybuild` for matching full rustc stderr, and that
objection stands. The answer is not a code on a fence.

### 1.7 `Rc` is the wrong exemplar, and it would have failed the gate for the wrong reason

Issue #3's Wave 0 bullet says to make the state `!Sync` by holding an `Rc` in it. `Rc<T>`
is `!Send` **as well as** `!Sync`, and `local.rs:311-314` requires `A: Send` because a
turn's future holds `&mut A` across an await.

An `Rc`-based probe therefore fails for a reason unrelated to the `Mail` enum. Because
Wave 0's failure branch is "stop and re-plan the whole issue", following that bullet
literally would have abandoned nine waves on a false negative.

Recorded because it is counter-intuitive: under the **real** `Actor: Send + 'static`
RPITIT shape the `Rc` failure surfaces three times *earlier* than expected — at each
trait method (`required by a bound in Actor::reserve::{anon_assoc#0}`) and at the `impl`
itself — not at the loop's `recv().await` as a naive reading predicts. A probe written
against the simpler shape reports RED about a construct that is not the one under test.

The state that is actually wanted is `Send + !Sync`: `RefCell`, `Cell`, `Mutex` — the
family `observe.rs:37-44` names by hand.

### 1.8 A separate defect this surfaced: `TurnInfo` is unconstructible downstream

`TurnInfo` is `#[non_exhaustive]` (`observe.rs:96`), so a crate outside keylink cannot
build one — `E0639`. `TurnOutcome` is not sealed; only the wrapper is.

**Consequence: a downstream author who implements `Observer` cannot unit-test their own
`after_turn`, because they cannot construct its argument.** Nothing in keylink is
affected, because everything that builds a `TurnInfo` is inside the crate.

`#[non_exhaustive]` is right — Wave 2b adds a `method` field to this struct, and that
must stay additive. The fix is a `#[doc(hidden)] pub` constructor, or a small
`keylink::testing` helper. Deferred to Wave 2b, which is where the field lands, and
recorded here so it is not lost.

**Resolved in Wave 2b — see §3.2.** The constructor won over the helper for one reason
that is not taste: `keylink::testing` is already taken. `keylink/src/testing.rs` is a
`pub(crate)` two-endpoint QUIC fixture, so a public module of that name would either
collide with it or force it to be renamed for the sake of a two-line function. The door
is `TurnInfo::for_test`, `#[doc(hidden)]`, and the prediction that `#[non_exhaustive]`
was right is now tested rather than asserted: `method` landed under it and no call site
outside `keylink` moved.

### 1.9 What Wave 0 did NOT verify

- **`Reply<T>` and `Deferred<T>` (Wave 2c).** `reply.rs` today holds only the wire types
  (`RemoteError`, `Outcome`, `WireRequest`, `WireResponse`) — there is no channel
  wrapper, and the name collides with the module. More importantly, a `Deferred<T>`
  parked *past* the end of its turn changes what the loop holds across an await. Wave 0
  probed `oneshot::Sender<Answered<T>>`, which is what its own criterion states, and says
  nothing about the deferred shape. Wave 2c must probe its own.
- **The runtime behaviour of the observer chain.** It is type-checked, not executed —
  which is where its property is decided, since it is a compile-time one. The probe's two
  runtime tests do cover the real RPITIT handler, under the real generic dispatcher,
  under the real `guarded`, producing a real `Panic` into `Answered::Ran(Err(..))` across
  a real `oneshot`.

### 1.10 Reproducing this

The probe lives at `.tmp/probe-mail/`, which is gitignored — it is scaffolding, not a
committed guard. The permanent guard is Wave 1's code compiling, plus the surviving
assertions landed as an in-crate `#[cfg(test)]` module in Wave 1.

```sh
cd .tmp/probe-mail
cargo check --locked --all-targets          # exit 0
cargo +1.91 check --locked                  # exit 0
cargo test  --locked                        # 2 passed
for f in r0 r1 r2 r3a rc_loop rc_actor; do  # each exit 101
    cargo check --locked --features $f
done
cargo check --locked --features r3b         # exit 0
```

---

## 2. Wave 1b — the mailbox is a generated enum

Wave 0 proved the shape compiles. Wave 1a made the wire typed. This is the other
half of Wave 1: `type Turn<A> = Box<dyn FnOnce(&mut A, &mut [Box<dyn Observer<A>>])
-> Pin<Box<dyn Future<Output = Option<Ended>> + Send + 'a>> + Send>` and
`Message::Turn` are gone, and a `service!` expansion now generates the mailbox.

### 2.1 The shape that landed, and the three places it differs from the sketch

```rust
// generated, local only, never serialized, derives NOTHING
#[doc(hidden)]
pub enum Mail {
    Reserve { origin: Origin, sku: String, qty: u32, body: (), reply: Reply<Outcome> },
    // ... one per declared method ...
    Served { origin: Origin, request: Request, body: BodyReader,
             reply: Reply<(Response, Body)> },
}

impl<A: Actor> keylink::Turn<A> for Mail {
    async fn run(self, state: &mut A, hooks: &mut keylink::Hooks<A>) -> Option<Ended> { .. }
}
```

1. **`cx: Caller<'static>` is an owned `Origin`** — §1.6, already settled in 1a.
   The origin is now handed to the closure **by value** and borrowed inside it,
   because a `Caller<'o>` borrowed in the loop and captured by the closure would
   need `'o: 'b` for every `'b` the higher-ranked bound quantifies over.
2. **There is no `Stop` variant.** `Stop` stays in keylink's own
   `Message<A, M>`, beside a `Read` variant, so a generated enum cannot forget
   it and a wave that adds a third keylink-level message does not regenerate
   anybody's crate.
3. **`send` and `recv` get variants nothing builds.** The sketch says "a kind the
   local handle does not serve simply gets no variant". `macro_rules!` cannot do
   that: a macro invocation is not legal where a variant goes, so emitting a
   variant for four kinds out of five needs a token-muncher over the whole
   declaration list. Both are written to be *correct* if something built one,
   which is the honest alternative to an arm that panics. Same mechanism as S-07.

### 2.2 `read` is still a boxed closure, and that is the right answer

`with_state` takes an arbitrary caller closure rather than a declared method, so
there is no enum a macro could generate for it. It lives in keylink's
`Message::Read(Box<dyn FnOnce(&A) + Send>)` — **synchronous**: no future to
build, no `&mut A`, no observer chain. Making it a declared method instead would
put a cross-cutting concern into the service's own wire contract, which is the
opposite of why it exists.

### 2.3 The reply channel is a value the loop holds, and the loop is one loop

The property Wave 2 needs. `Reply<T>` is an opaque newtype over
`oneshot::Sender<Answered<T>>`, so `Answered` went from private to `pub(crate)`
rather than to `pub`, and Wave 2's fourth state is additive to a type nothing
outside keylink can match on. It also retires `Answer<Resp>`, which was a
*transparent* alias and therefore had `tokio::sync::oneshot::Sender` in keylink's
public API behind a `#[doc(hidden)]` name.

`keylink::turn(state, hooks, origin, reply, f)` is what every generated arm calls,
and it is where the queue deadline, the panic guard, the observer chain, the
idempotency marks and the reply all are. What that replaced:

| decision | before | after |
|---|---|---|
| queue deadline, turn side | 3 hand-written comparisons, two spelling `>` and one `>=` | 1, `Queueing::expired` |
| `dispatched` / `completed` / `unrun` | 2 dispatchers, each with its own arms | 1 |
| a panic, as a peer reads it | keyed `{panic}`, singleton `panicked: {panic}` | 1 rendering |
| `Answered` → `Outcome` | 2 copies, 5 arms each | 1 `rendered` |

The panic rendering is a **behaviour change on the keyed wire**: a keyed actor's
panic now reaches a peer as `Failed("panicked: the actor panicked: ..")` where it
used to be `Failed("the actor panicked: ..")`. The singleton's rendering is
unchanged, which is why `both_halves_of_a_lost_turn_carry_their_own_prefix` stayed
green — and that test's own documentation had already named the gap: *"There is no
keyed twin, which is how a stutter on one path and a bare rendering on the other
both stayed green."* There is one now,
`one_panic_reads_the_same_whichever_way_the_service_is_served`, and it asserts
byte equality between the two paths rather than that both contain "panicked".

The stutter survives and is recorded rather than fixed: `Panic::caught` builds its
message as `the actor panicked: {payload}`, and that same string is what
`Ended::Panicked` and `TurnOutcome::Panicked` carry, so removing one of the two
prefixes is a decision about `panic.rs`'s vocabulary and not about a dispatch.

### 2.4 `LocalActor<A, M>`, and what the second parameter cost

The alternative was one parameter and a bound tying the mailbox to the state, and
it does not work: the enum is generated into the caller's crate, so keylink cannot
name it, and an associated type on a trait keylink *could* name would put the
mailbox inside the state's definition — where a state used by two services could
only have one.

The cost is paid by generated code. A caller writes `inventory::Handle<Warehouse>`
and the generated `Handle` applies `M` itself. Three hand-written sites moved:

- `representability.rs`'s read probe is now generic in `M`, which is itself the
  claim: `read` needs to know nothing about a turn, so it works on a hand-written
  non-`service!` state with no mailbox enum to name. Bound it to `M: Turn<A>` and
  the probe stops compiling.
- `blocking.rs`'s runtime test names an uninhabited `enum Nothing {}`.
- `local.rs`'s own tests carry a hand-written `Mail`, deliberately: this is where
  the loop lives, so a probe only the macro could build would prove the macro and
  the loop agree rather than that the loop holds.

The methods are split across three impl blocks by what each genuinely needs.
`spawn`/`spawn_observed` need `M: Turn<A>`; `ask`/`ask_catching`/`notify` need
`M: Send + 'static`; `read`/`stop`/`abort`/`ended` need neither parameter bounded.

`ask` and `notify` take a **builder** — `impl FnOnce(Origin, Reply<T>) -> M` —
rather than a message. keylink makes both the origin and the reply channel and
hands them in, so generated code never constructs an `Origin` on the local path
either, and both of its constructors stay `pub(crate)`. That is what made
`Origin::local()`'s `#[allow(dead_code)]` removable: Wave 1b is its consumer, as
predicted.

`Queued<T>` and `ask_catching_before` are deleted. Their one caller was
`Handle::service`'s dispatch, which is now `deliver_to_local` — the twin of
`deliver_to_actor`, in keylink, sharing one `rendered`.

### 2.5 The measured result: an idle actor stopped paying for the service's types

The wave that replaces a fat pointer with an enum sounds like it should cost
memory. It saved some, and the reason is a second box nobody had asked for.

`Mailbox<M> = mpsc::Receiver<Box<M>>` boxed the slot at the channel and carried
the argument for it. Nothing boxed the **task's future**, which the keyed loop had
always sized to the service's own handler by inlining `guarded(__actor_turn(..))`
into it. Measured, at ten thousand idle actors of a one-`u64`-argument service:

| | keyed | local |
|---|---|---|
| before (`c9056e0`) | 1,671 B | 1,725 B |
| enum, turn future unboxed | 2,567 B | 3,005 B |
| enum, turn future boxed | **1,159 B** | **1,469 B** |

And the number that matters, because it is what makes `BUDGET_PER_ACTOR` a
statement about keylink rather than about whoever wrote the service — the same
measurement against a service whose declared arguments are a kilobyte larger:

| `size_of::<Mail>()` | before | after |
|---|---|---|
| 232 | 1,686 B | 1,159 B |
| 1,248 | 4,662 B | 1,146 B |

About 2.9 bytes of permanent per-actor cost per byte of message before, and a flat
line after. `tests/actor_memory.rs` asserts the flatness — `BUDGET_PER_DECLARED_BYTE`,
128 bytes of slack over a kilobyte of arguments — rather than describing it, and
that assertion was watched to fail at 5,067 bytes with the box removed.

The bill is one heap allocation per message for the turn's state machine, on a
path that already allocates the mailbox slot and, for a served call, a QUIC stream
and a postcard decode.

### 2.6 What Wave 1b did NOT do

- **Observers on a keyed actor.** The keyed loop runs the same `Mail` dispatch
  now and hands it an **empty** hook slice, so this is one line rather than a
  second loop — but it is still Wave 3's decision, because feeds ask the same
  question about the same loop.
- **Remove the per-turn `Box::pin` inside `turn`'s closure.** `turn` takes
  `for<'b> FnOnce(Origin, &'b mut A) -> Pin<Box<dyn Future + Send + 'b>>`, and
  expressing it over a generic future needs a helper trait with a lifetime-generic
  associated type plus a `Send` bound on it — implemented per method by generated
  code. Possible, not free, and not measured to matter: the allocation is on the
  message path, not the idle one.
- **Forward `#[$meta]` to the `Mail` variants.** Deliberately not, following
  `Response`'s precedent: a `#[serde(..)]` written for the wire enum and
  re-emitted onto an enum with no `Serialize` derive is `cannot find attribute`
  pointing at `macros.rs`. The cost is that a `#[cfg]`-gated method would gate its
  `Request` variant and not its `Mail` one — which was already true of `Response`,
  so this wave neither fixes nor worsens it.
- **Move the ALPN.** `Mail` is local-only and derives no `Schema`, so nothing it
  contains is hashed. `keylink/tests/protocol.rs`'s `keylink/Probe/d3fe34471921164e`
  is unchanged and green.

---

## 3. Wave 2 — what the enum unlocked: `Faulted`, the method name, and `finish`

Three of Wave 2's four items. `#[defer]` (2c) is a separate lane and is not recorded
here.

Wave 1b's claim was that holding the reply channel in the loop rather than sealed inside
a closure would make three things cheap. This is the section that says which of the three
were actually cheap, and which of the issue's premises about them were wrong.

### 3.1 2a — the fault was already answered; what was missing was a *name* for it

**The premise in issue #3 is false against this tree, and the correction matters more
than the feature.** The issue says a panicking handler's reply sender dies and the caller
gets a `RecvError` that a blanket arm maps to `TurnLost::Gone`. It does not. `guarded`
caught the panic before Wave 1b and catches it now; the panic crossed the reply channel
as `Answered::Ran(Err(Panic))` and `ask` resumed it. A panicking turn's caller never got
`Gone`.

What was true is subtler and was the real defect: **a fault was a shape of a successful
answer rather than an answer of its own.** `Ran(Result<T, Panic>)` put the distinction
between "the state was mutated and here is the value" and "the state may be torn" one
level in, where every consumer had to look for it, and the two consumers that matter
looked differently — the local path unwrapped and resumed, the served path unwrapped and
formatted. And the public error type that exists to answer *why there is no value* could
not say "it faulted" at all.

So the flattening, not the addition, is the change:

```rust
pub(crate) enum Answered<T> { Ran(T), Faulted(Panic), Expired, Refused { name } }
pub enum TurnLost { Gone, Refused { name }, Faulted(Panic) }
```

**`Faulted` carries the payload, against the issue's sketch, which shows a payload-free
variant.** A payload-free `Faulted` destroys the `Box<dyn Any + Send>` that
`Panic::resume` needs, so `ask` could no longer resume a handler's panic into its caller
— a documented, tested, load-bearing behaviour and half of what keeps `MailboxClosed`
honest — and `rendered`'s `panicked: {panic}` string, which `tests/` pins, would have
nothing to format. The sketch was taken as intent, not as a signature.

**The price is `Copy`, `Clone`, `PartialEq` and `Eq` on `TurnLost`.** A `Panic` is none
of them. Nothing in the repo compared a `TurnLost` — it had no consumers at all — so
this cost nothing today, and it buys the one-layer signature:

```rust
// was: Result<Result<T, Panic>, TurnLost>
pub async fn ask_catching<T>(..) -> Result<T, TurnLost>
```

Two error positions for one question is the two-layer unwrap `Answered`'s own
documentation says a caller will get wrong. `ask` still delegates to `ask_catching` and
resumes `TurnLost::Faulted`, so there is one send-and-wait, in a private `answered`.

#### The blanket arms: two were named, one of them had already been deleted

The issue names arms at `local.rs:551` and `:638`. Verified against `83f92b5`, the second
was inside `ask_before`, which Wave 1b deleted with its method. What exists now is one
`Err(_) => Err(TurnLost::Gone)` in `ask_catching` and two structurally identical
`RecvError` collapses on the served paths — `deliver_to_local` and `deliver_to_actor`,
both producing `NoSuchActor`. All three are `Result<_, oneshot::error::RecvError>`, and
`RecvError` is a unit type, so each is **total over a one-case error** rather than blanket
over an enum. The blanket-ness was never in the `Err` arm; it was in the `Answered` enum
being unable to distinguish a fault, and every one of the three now matches `Answered`
exhaustively with a `Faulted` arm.

**What stays collapsed, and why that is not inference.** A reply sender is dropped without
sending only when the loop is not there to send: `abort`, a runtime shutdown, or a
panicking `Drop` — in the state, or in a reply value dropped because its caller went away.
The comment that justified folding those three was doing two jobs, and only one of them
was right. Folding three ways of being gone is right — a dead loop cannot send a fourth
answer, so there is nothing to infer. Folding a *fault* in with them was wrong, and that
half is finished: the loop that survives a panic says so.

#### `read` stays out of `Faulted`, and `notify` cannot observe it

**`read` stays out, deliberately.** Its channel is `oneshot::Sender<Result<T, Panic>>`
and not `Answered<T>`: a read runs no observers (`tests/observe.rs::a_read_runs_no_observers`
pins it), so it can never be `Refused`, and it carries no `Queueing`, so it can never be
`Expired`. Giving it an `Answered` would add two arms that cannot occur to a path whose
whole argument is that it is not a turn. It catches its panic synchronously and resumes
it in the caller, which is the same policy under a different type.

**`notify` — a local `tell` — has no way to learn of a fault**, and `Faulted` does not
change that. It builds a reply channel like every other message and drops the receiving
half *before* the send, so the loop answers `Answered::Faulted` exactly as it would
answer an `ask` and the answer lands nowhere. The actor still survives; nobody is told.
That is unchanged by this wave and is the reason Wave 8's limit list says there is no
reliable `tell`.

#### The incarnation counter the issue says survives does not exist

2a says `LocalActor::incarnations()` "survives only as observability" and asks Wave 6 to
ship it, and gives `rg 'incarnation' keylink/src` finding nothing in Wave 2's diff as a
criterion. `rg -n incarnation` over the whole repository returns **no hit in any source
file** — the only hits are the two paragraphs of this section — so there is no counter,
there never was one on this branch, and the criterion is trivially true and proves
nothing. **No counter was added.** Whether Wave 6 wants one is
Wave 6's decision, made against a tree where a fault is answered rather than reconstructed
by comparison — which is exactly the argument that made the counter unnecessary.

### 3.2 2b — `TurnInfo.method`, and the door that lets somebody test their observer

The field is the wire spelling — `"Post"`, not `"post"` — because `Rpc::method` is the
other half of an agreement an operator reads in one metrics label. The generated arms pass
`stringify!($rpc)`; the `Served` arm reads `Rpc::method(&request)` before the request moves
into the closure, because that one arm serves every method.

**A feed will report the feed's name in this same field.** Documented where the field is
declared, so Wave 3 neither widens `TurnInfo` nor adds a second field: what the field
names is *whichever declaration produced the turn*, and a feed is a declaration.

§1.8's defect is fixed here because this is the wave that proves the attribute was right.
`TurnInfo::for_test(outcome, method)` is `#[doc(hidden)] pub`, and a `compile_fail`
doctest on `TurnInfo` holds that the struct literal is still shut from outside. That
doctest names `E0639` in prose and **never on the fence** — rustdoc does not check the
code after `compile_fail`, twice demonstrated in this workspace — and it is protected
against passing for the wrong reason by reading both field names off a `for_test` value
before writing the literal, so a rename fails the *first* two lines rather than silently
satisfying the fence. Red-tested: with `#[non_exhaustive]` removed the doctest fails with
`Test compiled successfully, but it's marked compile_fail`.

### 3.3 2d — `Observer::finish`, and the two ways to reintroduce `A: Sync`

```rust
fn finish<'a>(&'a mut self, _state: &'a A, _ended: &'a Ended) -> FinishFut<'a> {
    Box::pin(async {})
}
```

Defaulted, so no existing implementor moved. A plain `fn` returning a boxed future, never
an `async fn` — an `async fn` (and an RPITIT, which desugars to one) stores every argument
in the future it returns, so `Send` on that future would demand `A: Sync` for every actor
in the crate. That is Wave 0's trap, arrived at from a second direction.

**Ordering: after the last turn, before the three drops** (`drop(hooks)` → `drop(inbox)`
→ `ended.send(reason)`), whose comment says the order *is* the fix. So `stop().await`
returning still means every claim an observer held is free, and a flush that needed the
state still had it. Red-tested by publishing the end before the loop instead: the log is
empty when `stop()` returns.

**The open decision, made explicitly: `guarded` does NOT wrap the finish loop.** Each
observer gets the two catches `after_turn` has — a synchronous `catching` around the call,
then `guarded` on the built future — and there is no third guard around the iteration.
Both reasons matter and they are independent:

1. A single `guarded` needs a *built* future, hence an `async` block, hence a block
   holding `&state` across an await — `Send` only when `A: Sync`. This is not a matter of
   care: the block is spawned under `A: Send + 'static` and nothing more, so an `A: Sync`
   requirement introduced here is a compile error in `keylink` itself.
2. It would stop the chain at the first panic. `finish` is a flush; an adapter that fell
   over must not cost the next adapter its write, and there is no refusal to short-circuit
   for, because `finish` is infallible.

A panic in either half is caught and **dropped**, and the `Ended` is not rewritten. There
is no caller to resume into, and `ended` has already been handed to whichever observers
ran earlier — rewriting it would make two observers of one ending disagree about it. The
panic hook already ran at the fault, which is the trade `notify` documents for a `tell`.

**`finish` does not run on `abort`, by construction rather than by policy.** `abort()` is
`task.abort()` and nothing else, so the loop's future is dropped at its next await and
never reaches its exit; anything after the loop is unreachable. **One documented hole,
and it is `abort`'s own:** a turn that never reaches an await is not interrupted either,
so an actor aborted during such a turn can exit its loop normally afterwards and `finish`
then runs. Nothing claims more than that.

`representability.rs::Recorder` now implements both methods, so its claim reads "every
method of this port lends the state through a shared reference" instead of "the one method
does". **That is a different and weaker hole than the one its doc named.** It said the
probe "is silent about a *second* method", naming `after_turn_mut(&mut self, state: &mut A)`
as the hazard — a *new* port added beside these two, which no probe here can forbid and
which this one would stay green next to. What closed is the second method that actually
landed; what is still open is a third. Both are now written there.

### 3.4 Corrections applied after Wave 1b/2's review, and the one number they found

Seven findings from the review of Wave 1b were applied before 2c started. Five are
documentation — a comment asserting in the present tense a thing that had been
deleted, a fourth site claiming rustdoc checks an error code, a stale method name on
a test, a measurably false size argument, and a constant whose name asserted a bound
about 1,000× looser than the check underneath it. Two changed code, and both belong
on the record here rather than only in a commit message.

**`guarded` and `catching` are `pub(crate)`.** Wave 1b removed their last consumer
outside the crate: `rg '\$crate::guarded' keylink/src/macros.rs` returns 0 where
`c9056e0` returned 1, and nothing in `tests/`, `demo/` or the README calls either —
the remaining hits are prose. Every generated arm calls `turn`, and the guard is
inside that. §1.2 records the Wave 0 probe reaching them "through a path
dependency"; that is history, and a probe rebuilt today would have to live in-crate
or re-export them deliberately.

**`TurnLost` has a fourth variant, `Expired`.** `ask_catching` mapped
`Answered::Expired` to `TurnLost::Gone` behind a comment reading *"a local origin
carries no `Queueing`, so nothing can expire on this path. Typed rather than
asserted."* It was asserted, by that comment, and the mapping under it said a live
actor was no longer running. Unreachable today, and reachable the moment a local
origin gets a queue budget — which is what a deferred reply and a feed both are. So
the variant exists now and the arm is correct if something reaches it, the same
choice §2.1 made for the `send` and `recv` `Mail` variants nothing builds.

`TurnLost` is **not** `#[non_exhaustive]`, against the review's suggested package,
and the reason is `Ended`'s own: *"Exhaustive, like `Departure`: a caller matches it,
and a wildcard arm is the collapse it exists to prevent."* Sealing it would let a
downstream `_` swallow a fifth answer exactly the way `Gone` swallowed this fourth
one. Nothing outside keylink constructs a `TurnLost`, so the additive argument for
sealing is about matching, and matching is the thing that should break.

#### The number: Wave 2 cost 128 bytes per idle **local** actor, and nothing measured it

`tests/actor_memory.rs` asserted `BUDGET_PER_LOCAL_ACTOR` at exactly one `Mail`
size, because phase 3 mounted `bulky::System` and never built a single
`bulky::actor(..)`. Adding that phase found the drift immediately. Bisected by
running the one test at two commits:

| path | `c9056e0` | Wave 1b `c8712b0` | Wave 2 `836179c` |
|---|---|---|---|
| keyed, mounted in a `System` | 1,671 | 1,159 | 1,159 |
| local, `ledger::actor(..)` | 1,725 | 1,469 | **1,597** |

§2.5's table and three comments said 1,469. The keyed column did not move, which
names the cause: **2d put the `finish` loop inside `LocalActor::spawn_observed`'s own
async block**, so it sits in the task's future while the actor is idle, and
`macros.rs::spawn`'s keyed loop has no `finish` loop at all. One allocator step.

Nothing in 2d was wrong. What was wrong is that a wave which grows the loop's own
frame reported no number, on the path with **less** headroom — the local margin over
keyed is 438 bytes now, not 310 — and the whole argument for the `Box::pin` at that
await is an absolute ceiling that sits between 1,597 and 2,048.

The local slope is asserted now, separately from the keyed one, because the box that
holds it is a different line of code. Red-tested by removing that line: a local idle
actor goes to 3,005 bytes and a bulky one to 9,114, a slope of 6.0 B/B, and the
assertion fires naming both. The old constant was called
`BUDGET_PER_DECLARED_BYTE` — 128 bytes of slack over a 1,016 byte delta is 0.126
B/B, so a name promising 128 B/B would not have caught even that. It is
`SLOPE_SLACK_OVER_A_KILOBYTE`.

#### The size argument that was false

`__keylink_mail_body!` justified a unit field over `Option<BodyReader>` by "paying
`size_of::<BodyReader>()` on every variant … would be a tax on the mailbox — which
is sized to its largest variant". Measured: `size_of::<ledger::Mail>()` is 232, and
232 is exactly `Served` — `Origin` 120 + `Request` 8 + `BodyReader` 96 + `Reply` 8 —
against 136 for the per-method variant. `Option<BodyReader>` is also 96, the niche
being free. So the enum would stay 232 bytes and the tax is zero. The decision is
right for the reason the same comment's second half already gave; the size clause is
gone.

### 3.5 What Wave 2 (2a, 2b, 2d) did NOT do

- **`#[defer]`.** A separate lane, and it needs its own `Send` probe: Wave 0 probed
  `oneshot::Sender<Answered<T>>` held *inside* a turn, and a `Deferred<T>` parked past
  the end of its turn changes what the loop holds across an await. §1.9 already says so.
- **Observers on a keyed actor.** Still an empty hook slice, still Wave 3's line to
  change. `finish` reaches keyed actors the moment that slice is not empty, with no
  further work.
- **Move the ALPN.** `Answered` and `TurnLost` are local types that derive no `Schema`;
  `TurnInfo` is local; `rendered`'s two wire strings are byte-identical.
  `keylink/tests/protocol.rs`'s `keylink/Probe/d3fe34471921164e` is unchanged and green.
- **An incarnation counter.** See §3.1. It does not exist and was not added.

---

## 4. Wave 2c — `#[defer]`: an attribute the macro reads, and a reply that outlives its turn

```rust
keylink::service! {
    service Orders;
    rpc Fill(price: u64) -> u64;
    #[defer]
    rpc Place(size: u64) -> u64;
}

// generated
async fn place(&mut self, cx: Caller<'_>, reply: Reply<u64>, size: u64) -> Replied<u64>;
```

`Replied` makes "forgot to answer" a compile error. `Reply` is not `Clone`, and its
only two doors — `send(v)` and `later()` — each consume it and produce exactly one
token, so a handler declared to return one cannot return without having answered or
explicitly parked. Pekko's enforced-replies shape, and the reason deferral is safe as
an attribute where a sixth method kind was refused.

### 4.1 The muncher, and what it cost

`macro_rules!` cannot match on `#[defer]`. `service!` had one matcher capturing
`$( #[$meta:meta] )*`, and a `meta` fragment is an opaque AST node: nothing
downstream can ask whether it is `defer`. Those metas are forwarded verbatim to nine
sites, so `#[defer]` parsed and was re-emitted where it is not an attribute at all —
`cannot find attribute` from inside a `paste!`, pointing at `macros.rs`.

So `service!` is now three macros. `service!` itself is one arm that starts the walk;
`__keylink_service_munch!` walks the raw tokens **before the fragment forms**, four
arms and no state beyond two lists — one for the attribute keylink reads, one for the
attributes it forwards — and hands
`__keylink_service!` one record per method: `{ [attrs] [defer?] kind Name(args) ->
Ret }`. `__keylink_service!` is the 1,300 lines that were `service!`'s body,
unchanged except at three sites.

**Three of thirteen helper macros take the flag**, and which three is itself the
statement that the wire does not move: the `Actor` method, the mailbox arm and the
served arm. `Request`, `Response`, `Contract`, the `Handler` method, both client
methods, the blocking method and the local `Handle` method do not know the attribute
exists, because a deferred method is byte-for-byte an ordinary `rpc` at every one of
them.

Each of the three has an arm for `[defer], rpc`, an arm for `[defer], <anything
else>` that falls through to the ordinary shape, and the ordinary arms. The
fall-through matters: `__keylink_defer_check!` is the single place a bad combination
is refused, and if the three sites generated *deferring* shapes for a kind that
cannot defer, that one clear error would arrive buried under a trait that no longer
matches its mailbox.

`#[defer]` is idempotent when doubled rather than an error, because a matcher that
refused the second one reports "no rule expected this token", which is worse than
nothing.

Recursion is one frame per attribute plus one per declaration. The deepest service in
this repository munches about a dozen frames against a default `recursion_limit` of
128.

### 4.2 The four refusals, and why they are messages rather than a missing arm

`#[defer]` on a `tell`, `send`, `recv` or `blob` is a `compile_error!` naming what a
deferred reply would have to be for that kind. Leaving the arm out would produce "no
rule expected `tell`" pointing into a macro the author did not write — and worse, it
would make the `compile_fail` doctest that pins the refusal **satisfiable without
proving its claim**: renaming the `tell` arm away leaves the fence red for the wrong
reason. Red-tested the other way instead, by making the arm a no-op: the fence goes
green with *"Test compiled successfully, but it's marked `compile_fail`"*.

### 4.3 `send` does not send, and that is the decision this wave got right

The obvious `Reply::send` writes to the channel. That would have cost a `#[defer]`
service its durability, silently: an answer given inside the turn would reach the
caller **before** the observer chain ran, so a durability adapter refusing the write
would leave the caller holding a value the store never accepted — the one property
RFC-06 exists for, deleted by a convenience.

So `send` hands the channel and the value *back* to the turn, inside the `Replied`
token, and `turn_deferred` answers after the observers. An answer given inside a
deferred turn is committed exactly as an ordinary method's return value is, and a
refusal withholds it. Red-tested by making `send` answer immediately:
`the observer refused this turn, so its answer must not reach the caller: got Ok(42)`.

That is why `Replied<T>` is generic against the issue's bare `Replied`. It has to
carry the channel and the value; a payload-free token would have forced the
immediate write.

`later()` is the other case and it genuinely escapes. The answer is not this turn's
output, so this turn's commit says nothing about it — which is what deferral *means*,
and is stated at the type rather than left to be discovered.

### 4.4 The served path was not optional, and the criterion is what forced it

The tempting scope cut was to refuse `#[defer]` on the actor-served paths the way
`send` is refused, keeping `Reply<T>` a bare `oneshot::Sender` and leaving
`__actor_turn` alone. **It does not work, and the criterion "the contract hash does
not move for adding `#[defer]`" is what rules it out.** If the hash cannot move, the
`Request` variant of a deferred method must exist. A macro cannot omit a match arm
for it, so `Mail::Served`'s dispatch must have one, so the served path must be
expressible. Any refusal late enough to be a `ServableAsActor::CHECK` is also too
late: `Turn<A> for Mail` is instantiated by `actor()`, so a check inside the served
arm would refuse a local-only actor too.

Two things followed.

**`Mail::Served` decodes the request in the loop's own match.** It used to hand the
whole undecoded `Request` to one `turn` and let a generated `__actor_turn` match on it
*inside* the turn's closure — by which point which of the two turns to run has
already been chosen. The arms are written out by `service!` now and the per-method
body is an expression macro, exactly as the local arms already were.
`__actor_turn` and `__keylink_actor_invoke_fn!` are deleted, and the last dispatcher
that was not the mailbox's own is gone with them.

**`Reply<T>` grew a second shape.** A served reply is `Reply<(Response, Body)>` for
every method; a deferred handler needs `Reply<$ret>`; and the parked answer arrives
when no generated code is running, so nothing can apply the conversion at a call
site. It travels *with* the channel instead:

```rust
enum Answerer<T> {
    Direct(oneshot::Sender<Answered<T>>),
    Wrapped(Box<dyn FnOnce(Answered<T>) + Send>),
}
```

installed by `Reply::wrapping`, called only by a generated served arm for a
`#[defer]` method. Measured cost: `Reply<T>` 8 → 24 bytes, and
`size_of::<ledger::Mail>()` 232 → 248. Nothing per idle actor, because both mailboxes
box their slot; one heap allocation per deferred **served** call, and none otherwise.

`Answered::map` carries the three payload-free states through unchanged, which is the
whole point of it: a fault, a refusal or an expired budget must not be rewritten as a
`Response`.

### 4.5 The `completed` mark moved into `Reply::answer`

A deferred handler returns before it answers. Marking `completed` where the handler
returns would settle a served key as recorded for a reply that may never exist —
exactly the at-most-once hazard `idempotency.rs` holds — so the mark travels with the
channel: `Reply` carries the `Progress`, and `Reply::answer` marks on `Ran` and
nothing else.

That makes it **one** marking site rather than two, which is stricter than what Wave
1b left. `turn` and `turn_deferred` share `began`, which reads the deadline, answers
`Expired`, marks `unrun` or `dispatched`, and installs the progress — so the two
turns cannot disagree about when a turn begins, which is the disagreement Wave 1b
found three copies of.

### 4.6 §1.9's open question: the `Deferred` shape stayed `Send`

Wave 0 probed `oneshot::Sender<Answered<T>>` held *inside* a turn and said nothing
about a channel the state keeps. §1.9 flagged it. **Green, and green for a reason
rather than a coincidence.**

A `Deferred<T>` is `Send` when `T: Send` and deliberately **not** `Sync` — its wrapped
arm is a `Box<dyn FnOnce + Send>`. A state that parks one is therefore `Send + !Sync`,
which is not a problem but the design: it is the family `observe.rs` names by hand,
and the reason `A: Sync` is kept out at all. The loop never lends the state across an
await in a way that would need `Sync`, because an observer's future is the observer's
own choice — which is why `after_turn` is a plain `fn`.

The probe is `representability.rs`, positive code, with **no stand-in types**: a state
holding a real `Deferred<u64>` beside a real `RefCell<u32>`, so no single member's
removal makes it vacuous. Non-vacuity red-tested by bounding it `Sync`: `E0277`,
attributed to `Deferred<u64>` itself and not only to the `RefCell`.

### 4.7 What a `Deferred` parked past a fault resolves to: nothing. It stays parked.

Wave 2a's `Faulted` is *that turn's* answer to *that turn's* caller. The actor
survives a panic, so the state survives, so every promise the state was holding
survives and a later turn can still answer it. Answering a parked reply `Faulted`
would tell one caller about another caller's fault — with a `Panic` that cannot be
cloned to tell two.

Three ways a parked reply does end, all of them the state's own doing: somebody
`send`s it; somebody drops it, including a turn that takes it out of the state and
then panics; or the actor ends, which drops the state and every reply in it, as
`Gone`. An observer refusing the faulted turn reaches parked replies the same way
`stop()` does. Pinned by
`tests/defer.rs::a_reply_parked_before_a_fault_is_still_answered_after_it`.

### 4.8 The one thing `#[defer]` costs, said where a reader meets it

A handler that **panics before consuming its `Reply`** drops it inside the unwinding
future, so there is nothing left to answer with and the caller is told `Gone` —
`MailboxClosed` through a generated `Handle` — rather than being handed the panic the
way `ask` hands one on every other kind. The actor survives, the observers still read
`TurnOutcome::Panicked`, and the panic hook already ran at the fault.

Preserving the resumption would need the reply's destination handed back to the turn
on drop, i.e. a return channel and a `Drop` impl on the reply of **every** method, to
keep panic-resumption for the one kind whose premise is that its answer escapes the
turn. Not built. Documented on `Replied`, and tested:
`a_deferred_handler_that_panics_costs_its_caller_the_answer_and_not_the_actor`.

### 4.9 One fence in this wave proves less than it looks like it does, and says so

The `compile_fail` doctest showing a handler with an empty body is **not** the guard.
An empty body not being a `Replied<u64>` is rustc's `E0308` and holds whatever keylink
does. Red-tested by changing the generated arm's `Output` to `()`: the fence stayed
**green** — the impl then mismatched the trait's associated type instead, failing for
an unrelated reason while the obligation it claims to pin was gone.

The guard is the **positive** fence beside it, which compiles a handler written the
way `#[defer]` means it to be written and therefore goes red (`E0271`) the moment the
generated signature stops demanding a token. Both are kept, and which one is the guard
is now written down — because two fences and a belief in both is how this project
keeps shipping guards that are satisfiable without proving their claim.

The *at most once* half is `representability.rs`'s coherence probe: `Reply`,
`Deferred` and `Replied` are not `Clone`. Red-tested with a hand-written `Clone` for
`Deferred`: `E0119`.

### 4.10 What Wave 2c did NOT do

- **Move the ALPN.** Pinned, not asserted:
  `adding_defer_to_a_method_does_not_move_the_contract_hash` compares two declarations
  of the *same* service — the name is folded into the hash, so two differently named
  ones would differ for a reason unrelated to the attribute — differing only in the
  attribute, on both the singleton and the keyed ALPN. Red-tested by changing one
  method's argument type. `keylink/Probe/d3fe34471921164e` is unchanged and green.
- **Reach a keyed actor's observers.** `spawn` still hands an empty hook slice, so a
  `#[defer]` method mounted in a `System` answers correctly and is observed by nobody
  — exactly as every other kind is there. Still Wave 3's line.
- **Give `notify` a deferred shape.** A local `tell` drops the receiving half before
  the send, so a parked reply on that path answers into a channel nobody holds. There
  is no `#[defer] tell`, and §4.2 has the refusal.
- **Bound how long a reply may stay parked.** A `Deferred` held forever is a caller
  waiting forever, and nothing here watches for it. `QueueTimeout` bounds the wait for
  a *permit* and `turn` bounds a queued message, neither of which is this. A caller's
  own `n0_future::time::timeout` is the answer today; a per-method budget on a
  deferred reply would be a new mechanism and belongs with whichever wave wants one.
