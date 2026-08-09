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

`later()` is the other case and it escapes *this* turn. **REVISED, 2026-08-07 (§6.10):
it does not escape every turn.** The sentence here used to read "the answer is not this
turn's output, so this turn's commit says nothing about it", and that was the whole of
the durability window: the parked answer escaped the turn that received it and then
escaped the turn that produced it as well. `Deferred::send` now parks the *value* for
whichever turn is running when it is called, so the answer is committed by the turn that
computed it. What deferral means is unchanged — the reply outlives the turn that
*received* it — and what it never meant is that an answer computed from the state may
cross before that state is stored.

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
`send`s it **in a turn that is then committed** (§6.10); somebody drops it, including a
turn that takes it out of the state and then panics; or the actor ends, which drops the
state and every reply in it, as `Gone`. An observer refusing the faulted turn reaches
parked replies the same way `stop()` does. Pinned by
`tests/defer.rs::a_reply_parked_before_a_fault_is_still_answered_after_it`.

A turn that *sent* a parked reply and then panicked collapses into the second of those
rather than being a fourth case, and §6.10 argues why: the value waited for a verdict, the
verdict is a fault, and the channel is dropped unanswered — which is `Gone`, the same
answer this section already gave for a turn that took one out and panicked.

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

### 4.11 REVISED, 2026-08-09: the caller is told which, and only the panic is still lost

§4.8 priced `#[defer]` at one cost and the price had two halves. One is paid off.
The other is unchanged, and was re-checked rather than assumed.

**The name first, once, for the whole document.** `MailboxClosed` no longer
exists. It is `Undelivered`, renamed while the mailbox depth became a knob, and
this document names the old type eight times outside this section — in §3.1,
§4.8, §6.7, §6.10, §8.7, §10.6, §12.10 and §14.3, one of them inside §4.8's own
sentence. Six are prose naming the type and are read as `Undelivered` from here
on. The other two, in §6.7 and §6.10, are **quoted output from break records**:
they are transcripts of runs that happened, the runs printed that word, and they
stay as they are. Sections rather than line numbers, because appending to this
document moves every line below the append — the eight were first written down as
line numbers and six of them rotted before this paragraph was finished. Nothing
above this section is edited.

**What is paid off: the caller can now tell this from a dead actor.** §4.8's cost
was that a caller is told `Gone`. It now reads `Undelivered::Dropped`, whose text
is *the actor is serving and this reply was never answered* — the same sentence a
peer reads behind the `"dropped: "` prefix, from a rendering that reads the same
fact about the same actor. The distinction §4.8 gave up is closed, and it was
closed for a reason larger than this section: `Gone` was answering four
conditions, and three of the four left the actor alive.

`defer::a_local_caller_and_a_peer_agree_about_an_abandoned_deferred` holds the two
doors together, and what it can hold is narrower than "both sides see it" sounds:
**a reply has exactly one destination**, so a peer's abandoned reply and a local
caller's are two replies and never one seen twice. The claim is *one actor, one
cause, two doors, two answers that agree*.

**What is still refused: the panic itself does not reach the caller.** §4.8
priced that at *"a return channel and a `Drop` impl on the reply of every
method"*, and the price stands. Neither of the two mechanisms built since is that
channel, and both are close enough to be worth ruling out by name:

- **The chain** carries actor identities from caller into turn, cloned into each
  message for a membership test. It says who is calling. It cannot say where an
  answer goes, and it flows the wrong way to carry one back.
- **`Fate`** is a cell about the *actor*, read after the channel has died. It
  carries no payload and names no reply, which is why it can make the caller's
  sentence true and cannot hand it a `Panic`.

`Reply` and `Deferred` still have no `Drop` impl, which is the whole of what §4.8
asked for and did not build. The cost is the same cost.

**The guard §4.8 cites now asserts the variant.**
`a_deferred_handler_that_panics_costs_its_caller_the_answer_and_not_the_actor`
asserted `is_err()` and nothing more, so it stayed green whichever variant
arrived and this section's citation proved only that *something* went wrong. It
now asserts `Dropped`. The second assertion beside it — that the actor answers
the next call — is what makes "and not the actor" a measured half rather than a
restatement.

**What this does not close**, so it is not rediscovered as a defect: the read can
still be late. A restart whose next incarnation has already published `Running`
reads as *dropped* rather than *restarting*, and the generation counter that
would close it is refused in the other direction — queued turns survive a
restart, so a bumped generation answers *restarting* about an actor that is
running. §12.4 carries both directions and the design that would close them. The
local path inherits that window exactly; it adds none of its own.

**And that is the answer to the one residual this wave's review filed.** A parked
local caller woken by a dropped channel reads the cell some scheduler-dependent
time later, and a supervisor walking `Restarting` → `factory().await` →
`settle(Running)` can get there first, so the caller reads `Running` and is told
*dropped* where *restarting* was settled for it. It is §12.4's window and not a
second one — the same cell, read after the same death, under the same
happens-before — and the ordinary restart path is not exempt from it, because the
ordinary restart path is the one §12.4 describes. The local door renders that
window; it does not widen it.

**A row in §12.8 was edited in place, and half of that edit is put back.** The
edit did two different things and only one of them is the kind CONTRIBUTING.md's
RFC rule forbids.

- **The rename stays in the row.** The test §12.8 named is
  `defer::a_local_caller_and_a_peer_agree_about_an_abandoned_deferred` now, and the
  row carries that name. The belief §12 recorded is *this guard is covered by this
  break*, and that belief is unchanged — only the name moved. A row pointing at a
  test that no longer exists preserves nothing and rots, and the cost of that is
  measured rather than argued: `scripts/replay-fate-breaks.sh`'s stanza 16 named a
  test that existed nowhere, so `cargo test` printed `0 passed; N filtered out` and
  **exited 0**, and the script read that as *the break was not reproduced* — the
  same report a genuinely rotted anchor gives, from the opposite cause. That is why
  it now discriminates on `0 passed; 0 failed` and calls it a broken stanza. Stanza
  11 replays this row's break and filters on this test's name; `e3047da` moved it
  with the rename, and had it not, the stanza would have sat in exactly that state.
  Keeping a table runnable is not editing it to match new code.

  The old name was `defer::a_live_actor_does_not_tell_a_peer_no_such_actor`. It is
  half of what the test now holds, and it named a setup that cannot be built — see
  the destination argument above, which is why the extension took a new claim
  rather than a wider version of the old one.
- **The second break comes out of the row and lands here.** The edit also extended
  that cell to name a break that did not exist when §12 was written, which makes
  the table read as though two breaks were known then. §12 built one: `lost`'s
  `Standing::Running` arm, because the local door read no `Standing` at all. It
  does now, so the test carries a second — `answered`'s
  `Standing::Running => TurnLost::Dropped,` → `… TurnLost::Gone,` — and **either
  break alone leaves the other half green**, which is the measurement that makes
  the pairing worth asserting rather than a restatement. The peer half runs first,
  so its watched sentence came back unchanged when the local half was added, and
  that was recorded rather than assumed.

**The four guards `e3047da` added, which §12.8 does not list.** They are the local
half of what §12 built for a peer, so they belong to this section rather than to
that table. Each was watched red then green, and each names the wrong-reason pass
it is exposed to in its own doc; the breaks and sentences below are read off those
records.

- `defer::a_live_actor_that_drops_a_deferred_is_told_dropped_and_not_gone` — the
  sanctioned loss through the local door. Break: `answered`'s
  `Standing::Running => TurnLost::Dropped,` → `… TurnLost::Gone,`. Watched:
  ``a dropped `Deferred` costs the caller its answer and not the actor``,
  `left: Err(Gone)`, `right: Err(Dropped)`. It asserts the variant **and** the next
  call, because a `Fate` stuck at `Standing::Running` for an actor that is in fact
  gone answers `Dropped` with its meaning inverted, and only "the next call is
  answered" refuses that.
- `supervision::a_local_caller_can_tell_restarting_from_gone` — the served twin
  without the network. Break: `answered`'s
  `Standing::Restarting(reason) => TurnLost::Restarting(reason),` →
  `Standing::Restarting(_) => TurnLost::Gone,`. Watched: `round 1: a lost reply on
  a restarting actor must say so`, `left: Gone`, `right: Restarting`. Round 2
  asserts the **parked** reply rather than a fresh call, because a fresh call after
  the give-up answers `Gone` off `NotQueued::Closed` and never reaches the
  `Standing` read this is about. Round 2 stays green under that break by design —
  the fold lands on the answer it already expects, and the served twin carries the
  break that moves it.
- `observe::a_local_caller_is_told_refused_when_a_non_fatal_observer_refuses` —
  §2.6's third row under a supervisor. Break: `ask`'s
  `Err(TurnLost::Refused { name }) => Err(Undelivered::Refused { name }),` →
  `Err(TurnLost::Refused { .. }) => Err(Undelivered::Gone),`. Watched: `a non-fatal
  refusal ends one incarnation, and the caller must not be told the actor is gone`,
  `left: Err(Gone)`, `right: Err(Refused { name: "saver" })`. `Refused` rides the
  turn axis and arrives with or without a supervisor, so the second `post` is what
  makes this the supervised case rather than a rename of the test above it.
- `turns::every_turn_lost_except_faulted_has_one_undelivered_twin_with_the_same_words`
  — one fact reads as one sentence whichever door a caller came through. Two
  exhaustive matches, so a seventh variant in either enum is `E0004`. Broken twice,
  because one break exercises the equality and the other the coverage: changing one
  character of `Undelivered::Dropped`'s `#[error]` text watches `one fact must read
  as one sentence: Dropped and Dropped`; pointing `slot`'s `Undelivered::Expired`
  arm at `0` watches `every variant is written exactly once, and this one was
  written 2 times: [2, 1, 1, 1, 0, 1] at slot 0`. **That second record predicted
  slot 4 and "written 0 times" and the run said slot 0 and 2** — the loop walks the
  slots in order, so the doubled slot is reached before the emptied one.

  Its residual is named where it lives and is narrower than it first reads. `EVERY`
  and `lost` are two hand-written sample arrays, and `E0004` forces an author into
  the file without forcing either array to grow — so a variant added to both enums,
  given its two arms, and left out of the `TurnLost` sample is exercised by nothing.
  What that author does **not** get past is pointing `slot` at an existing index
  while the variant *is* in the sample: the shared slot is then written twice, and
  the exactly-once count catches it.

---

## 5. Wave 3 — feeds: an actor's second input, and one loop at last

```rust
keylink::service! {
    service Orders;
    rpc  Cancel(id: OrderId) -> Result<(), Rejected>;
    feed Fills(fill: Fill);            // local only, never on the wire
}

// generated on the Actor trait — no Caller (nobody called), no return (no reply)
async fn on_fills(&mut self, fill: Fill);
```

Two things landed together because neither could land alone. A feed needs a `select`,
and a `select` written twice is a `select` that will disagree with itself — so the
generated keyed loop and `LocalActor`'s became **one** loop, which is where the
observer chain, the panic guard and `finish` reach keyed actors.

### 5.1 THE DECISION THIS WAVE OWNS — `Ended` stays exhaustive, and the usual argument for it was hollow

`Ended::Feed { name }` is the first ending added since the enum had consumers, so the
cost of exhaustiveness could finally be measured rather than predicted.

**Measured, and it was zero.** Adding the variant produced **no `error[E0004]`
anywhere in this workspace**, against a brief that expected eleven. Every match on an
`Ended` in `keylink/tests` is one arm plus `other => panic!("got {other:?}")` — a test
asserting *which* ending happened, which is not the collapse the exhaustiveness guards
against and would be unaffected by sealing either way. keylink's own code builds
`Ended` values and reads one only through `Debug`.

So the "accept the churn" half of the choice decides nothing, and saying so matters
more than the conclusion: the record must not claim a guard fired when it did not.
What is left is the asymmetry between the two failures.

- **Sealed**, a downstream matcher is *forced* to write `_ =>`, and that arm silently
  swallows every ending added later. That is exactly how `TurnLost::Gone` swallowed a
  fault for three waves (§3.4), and it is the ground on which Wave 2's review refused
  to seal `TurnLost`.
- **Exhaustive**, a downstream matcher gets a compile error the moment a new reason to
  end exists, which is the moment it has a decision to make. The bill is a major
  version per ending once this crate is published; today it is unpublished with no
  downstream consumers at all.

**Chosen: exhaustive.** "The actor stopped" and "the actor's feed died" ask an operator
for different actions, and a type whose whole job is to say *which* must not be able to
answer "one of the ones you have heard of".

Because the churn argument turned out to be empty, the decision needed enforcement it
did not have. `representability.rs::every_ending_is_named` is a `#[cfg(test)]` match
with no wildcard — the only exhaustive match on `Ended` in the tree — so a new variant
is one `E0004` **inside keylink**, at a site whose doc comment carries this reasoning.

**What this means for Wave 9.** `Ended::GaveUp` — a supervisor that stopped restarting
— lands the same way and should expect the same near-zero in-tree churn: one arm in
`every_ending_is_named`, and whatever new tests it writes. It must **not** reach for
`#[non_exhaustive]` to silence that one error, because a downstream `_` reporting a
supervisor's give-up as an ordinary `Stopped` is precisely the failure this buys. If
Wave 9 wants a *keyed* actor's ending to be observable at all it needs something else
first — see §5.7.

### 5.2 The five decisions the spec forces, and how each is held

| decision | how it is held, not merely done |
|---|---|
| a feed takes no `Caller` and returns nothing | the generated trait method's signature; either one is `E0053` against the impl in `tests/feeds.rs` |
| feeds are not on the wire | the muncher partitions declarations into two lists and no wire generator sees the second — pinned by `adding_a_feed_to_a_service_does_not_move_the_contract_hash` |
| fair selection, not biased | `run_loop` alternates which input polls first; three tests assert exact sequences, all red-tested |
| a feed does not consume a mailbox permit | structural: a feed item is a `Feed` and there is no `Mail` variant for one. Witnessed by `a_full_mailbox_still_admits_feed_items` |
| a feed that ends, ends the actor | `Ended::Feed { name }`, red-tested |

Two of those need the argument written out rather than tabulated.

**A panic in a feed handler ends the actor, and that is deliberately unlike an `rpc`.**
The issue's own text says a fault "faults the incarnation exactly as a panic in
`cancel` does", and against this tree those are different behaviours: a panicking `rpc`
costs its message and **not** the actor. The reason it can is that somebody is *told* —
the panic crosses the reply channel as `TurnLost::Faulted` and `ask` resumes it in the
caller (§3.1). A feed item has no channel and no caller, so the same policy would drop
the fault on the floor and carry on with a state the handler may have left half
applied. The criterion's wording is the correct one and the sketch's is not; the actor
ends as `Ended::Panicked`, every observer's `finish` runs, and `ended()` names it.

This is the same argument `Ended::Feed` makes about a feed that *stops*, and it is why
a `feed` is a declaration rather than a `tell` fed from a stream. A `tell` is somebody
in this process choosing fire-and-forget, and `notify` documents that nobody is told.
Nobody chose anything here.

**Fairness is two rotations and the numbers are stated.** `run_loop` alternates between
the mailbox and the feeds; a generated `Feeds::poll_next` rotates among its own feeds
through a table of function pointers, so a saturated feed cannot starve its siblings
either. With everything ready at once the mailbox gets half the turns and each of `n`
feeds gets `1/2n`. Said out loud, because "fair" without a number is how a priority
knob gets argued for two waves later.

### 5.3 A `feed` is not a sixth kind, and that is what made it two arms instead of twelve

`$kind:ident` is matched positionally and discriminated by thirteen helper macros, each
with a per-kind arm. A sixth kind would need an arm in every one of them — thirteen
different spellings of "not on the wire", any one of which could be forgotten into a
`Request` enum.

So `__keylink_service_munch!` **partitions**. A `feed Name(item: Type);` goes into a
second list and `__keylink_service!` receives `@wire [ .. ] @feeds [ .. ]`. The wire
list expands into `Request`, `Response`, `Contract`, `WireKinds`, `Handler`, `Client`,
`KeyedClient`, `Mail` and the rest exactly as before; the feed list reaches four sites
and none of them is a wire site. **"Feeds are not on the wire" is a fact about which
list a declaration is in**, checked by the compiler at every wire generator, rather
than a claim thirteen macros have to keep agreeing about.

Cost: two muncher arms for the declaration, plus one refusal each for `feed X() -> T;`
and for a feed of any arity but one.

**A feed declares exactly one argument**, and refusing the rest is not fussiness. The
item type is `Stream::Item`, and a stream yields one value at a time; two arguments
would make the item an anonymous tuple that nobody named. Several fields is a struct,
which is a domain type. The refusal names the declaration and says what to write.

`#[defer]` on a `feed` is `__keylink_defer_check!`'s sixth arm — "no reply channel to
defer, and no caller to defer one to". Red-tested the way §4.2 requires: renaming the
arm away leaves the fence red for the *wrong* reason, so it was broken by making the
arm a no-op, which turns the fence green with *"Test compiled successfully, but it's
marked `compile_fail`"*.

### 5.4 The wiring API departs from the sketch, because the sketch cannot be written

The issue shows `orders::actor(Book::new()).feed_fills(exchange.subscribe())`. `actor`
returns a `Handle` to an actor that is **already running**, so there is no loop left to
wire a feed into; and a builder can always be stopped early, so a declared feed left
unwired would be a stream that never yields — silence a caller cannot tell from a quiet
exchange.

What landed instead:

```rust
let orders = orders::actor_wired(
    Book::new(),
    vec![Box::new(saver)],
    orders::Feeds::new(exchange.subscribe()),
);
```

`Feeds::new` takes **one argument per declared feed**, in declaration order, so
forgetting one is `error[E0061]` at the call site. `actor`, `actor_with` and the keyed
`spawn` wire none, and a service that declares a feed cannot reach them: `SpawnableUnfed`
is a `const` assertion on a generic type — `ServableAsActor`'s trick — so it fires at
the instantiation and nowhere else, and its message names the door to use. Red-tested by
folding `DECLARES_FEEDS` to `false`, which turns the `compile_fail` fence green.

Each stream is boxed: one allocation per feed per actor, paid once at spawn, against
making `Handle`, `Mounted` and `System` generic in one parameter per declared feed.

### 5.5 One loop, and what it took

`local.rs::run_loop` is the loop. Both paths enter it with a state, a `Vec` of
observers and a `Feeds`; `LocalActor::spawn_fed` does on the local side what the
generated `__mount` does through `run_keyed` on the keyed one. `rg 'Turn::run|\.run\('
keylink/src` returns exactly one turn-dispatch site, `local.rs::dispatch`.

Three things had to be solved.

**Two channel shapes, one loop.** A `LocalActor` queues `Message<A, M>` — it has a
`read` and a `stop` — and a keyed actor's mailbox is `Mailbox<M>`, because the registry
is generic in the service's message and knows nothing of `A`. Making those one channel
would put the state's type on `System`, `Mounted` and `KeyedService`, cascading into
`registry.rs` and `remote.rs`. Abstracting the *receive* instead costs a private
`Inbox<A, M>` trait and two impls, and reaches nothing outside `local.rs`. **No line of
`registry.rs` changed in this wave**, which was a constraint and is also the evidence
that the seam is in the right place.

The keyed impl is on a newtype rather than on `mpsc::Receiver<Box<M>>` directly, and the
reason is inference rather than taste: while the local channel carried
`Box<Message<A, M>>`, that type *was* a `Box<M2>`, so two bare impls left
`Receiver<Box<Message<..>>>: Inbox<A, ?M>` with two solutions and every call site
needing a turbofish — `E0283`, observed. Coherence allows such a pair, the trait
parameters differing, which is what makes it a trap: it compiles as a definition and
fails at the use.

**A poll, not a future.** The obvious `async fn next(&mut self)` on `Feeds` has a
cancellation hazard a synchronous poll does not: the loop would build that future, race
it against the mailbox, and drop it every time the mailbox won — losing an item it had
already taken, silently, under load. Polling the streams directly means there is no
future to drop. It also means the loop needs no `select!` macro, which is not a detail:
`tokio`'s is behind the `macros` feature, which keylink does **not** enable for
`wasm32-unknown-unknown`, so a `tokio::select!` here would not compile for a browser at
all. The spec's "plain `tokio::select!` is random-fair" is right about the property and
unavailable as a mechanism.

**The frame.** This wave cost 256 bytes per idle actor and it took three attempts to
stop it costing 1,987.

| path | Wave 2 (`836179c`) | first attempt | second | landed |
|---|---|---|---|---|
| keyed | 1,159 | 1,799 | 1,543 | **1,415** |
| local | 1,597 | 2,109 | 1,853 | **1,853** |
| slope over a kilobyte of arguments | 0 | 1,987 | 1,011 | **13** |

Both regressions were the *same* mistake, and it is the mistake `tests/actor_memory.rs`
exists for: a value sized by the caller's service materialised inside the loop's own
future, which is what an idle actor holds.

1. `async fn dispatch(turn: T, ..)` — an `async fn` stores **every argument** in the
   future it returns, so the service's message lived in that future, which `run_loop`
   awaits inline. The `Box::pin` inside was boxing a future that had already been paid
   for. Slope 1,987, the assertion firing by a factor of fifteen.
2. A plain `fn dispatch(turn: T, ..) -> Pin<Box<dyn Future>>` — better, and still
   materialising the argument as a temporary in `run_loop`'s generator on the way in.
   Slope 1,011, almost exactly `size_of::<bulky::Mail>() - size_of::<ledger::Mail>()`.

The fix is a rule rather than a patch: **nothing sized by a service's declarations is
ever spelled out inside the loop's future.** The box moved from the channel slot to
`Message::Mail(Box<M>)` and `Fed::Item(Box<T>)`, and `dispatch` takes a `Box<T>` and
unwraps it inside its own plain stack frame. That is allocation-neutral on the message
path — the old slot box and the new mail box are one allocation either way — and it made
the local channel slot a three-word enum, costing 512 bytes of eagerly allocated block
until `Read` was thinned to a single-word box. `with_state` pays one extra allocation
for that; it is a cross-cutting read, not a turn.

Neither regression was visible in any other test, which is the argument for that file.

### 5.6 The keyed-observer gap, closed

`spawn_wired(state, observers, feeds)` is the general keyed door and the answer to the
question `observe.rs` left open — who owns the `Vec` for a lazily spawned actor. The
factory does:

```rust
.spawn_with(|id| Some(orders::spawn_wired(state, vec![Box::new(saver)], feeds(id))))
```

which is the state-observers-feeds triple re-incarnation needs. `spawn(state)` still
means "no observers" and is still the door most keyed actors use; it is a shorthand now
rather than a limit. RFC-06 §5.3's "no observers on the keyed path" is false and the
paragraphs that said so are in the past tense, in `observe.rs` and on
`Handle::service`.

Pinned by `an_observer_attached_to_a_keyed_actor_runs`, which needs no node and no
`KeyedClient` — a keyed actor with a feed is driven entirely in-process, so what is
tested is the loop with no transport in the way to explain a pass. Red-tested by having
`spawn_wired` pass an empty `Vec`.

### 5.7 What Wave 3 did NOT do

- **Give a keyed actor an `ended()` channel.** It is the one thing `spawn_wired` does
  not deliver. An observer's `finish` is told why the actor stopped, so a durability
  adapter still commits — but a keyed actor whose observer refused, whose handler
  panicked or whose feed ended reports that to nothing a caller can await, and the
  registry's eviction clock simply finds a task that already finished. `System::stop`
  and `System::abort` await the task, not a reason. **This is Wave 9's first
  requirement**: supervision cannot decide to restart something whose ending it cannot
  read. It needs a field on `Mounted`, which means `registry.rs`, which is why it was
  not taken here.
- **Move the ALPN.** Pinned, not asserted:
  `adding_a_feed_to_a_service_does_not_move_the_contract_hash` compares two
  declarations of the same service differing only by two feeds, one of whose item types
  is `[u8; 32]` — a type the wire graph has never seen, so a leak into `Request` or
  `Response` would move the hash where a `u64` would not. Both ALPNs, keyed and
  singleton. Red-tested by re-declaring the same `feed` as an `rpc`: the ALPN moves
  from `keylink/Fed/15ef2886d11e6a37` to `keylink/Fed/15f62d61d7ba0efe`.
  `keylink/Probe/d3fe34471921164e` is unchanged and green.
- **Allow a service of nothing but feeds.** A `service!` generates a wire contract, and
  a feeds-only service would be an empty protocol with a state attached. Refused by the
  muncher with a message that says so. Whether a purely feed-driven actor deserves a
  declaration of its own is an open question and not this wave's.
- **Bound a feed.** Nothing rate-limits a feed, buffers it, or drops from it under
  load: the stream is the caller's and so is its backpressure — a feed that yields
  faster than the actor consumes simply fills whatever the caller wired. `QueueTimeout`
  is about a mailbox permit and has no analogue here, because there is no permit.
- **Reach a feed from a peer.** A feed is local in the way a `Handle` is local. A
  remote event source is a `tell` on a wire contract, and the two are not the same
  thing wearing different names.
- **Use `tokio::time::pause`.** The wave was briefed to, and `tests/feeds.rs` reads no
  clock at all instead — it gates the loop inside one turn, loads both inputs, and then
  releases, so every ordering assertion is an exact sequence rather than a statistical
  one. That is strictly stronger than a paused clock, and it is written at the top of
  that file so the departure is not mistaken for an omission. The one claim it cannot
  make is about a *continuously* saturated mailbox: with `MAILBOX = 1` on a
  single-threaded test runtime the loop drains the slot and polls again before the
  parked sender is rescheduled, so that state is unconstructible and the reverse
  direction is asserted as "a queued message does not preempt a queued feed item".

---

## 6. Wave 5 — the durability adapter the port is owed, and what a turn costs

`Observer` had **zero non-test implementors**. By the standard `keylink-persist`'s own
module header sets — a port is justified by one adapter written on purpose — the port
was unjustified, and the shape of `Observer::after_turn` was a prediction about an
implementation nobody had written. This wave writes it.

```rust
// keylink/src/persist.rs, behind `persist` / `persist-file`
pub struct Persist<D, S: Snapshots> { durable: Versioned<S::Claim>, dirty: bool, .. }

let (restored, saver) = Persist::<Ledger, _>::open(&store, &id).await?;
let ledger = ledger::actor_with(restored.unwrap_or_default(), vec![Box::new(saver)]);
ledger.post(1).await?;   // stored, and fsynced, before this returns
```

### 6.1 THE NUMBER THIS WAVE OWNS — a stored turn costs 7.6 ms, which is ~130 turns/second

RFC-06 §7 asked for a measurement and never got one. Every estimate in this repository's
history was low by more than an order of magnitude.

| profile | no observer | with `Persist` | runs |
|---|---|---|---|
| debug (`cargo test`) | 16.5 – 20.2 µs | 7,520 / 7,652 / 7,927 µs | 3 |
| release | 12.8 µs | 7,083 µs | 1 |

Apple M5 Max (`Mac17,6`), macOS 27.0 build 26A5378n, APFS on the internal SSD,
2026-08-07. 1,000 turns of a fixed-size 8-byte state, so the payload is 20 framed bytes
on every turn and the number is not a function of how far the run got.

**Debug and release agree to within 8%, which is the evidence that this is a syscall
cost and not a code-generation one.** It is two `fsync`s and a `rename`: the file
adapter syncs the temporary file and then the directory, and the second sync is what
makes "an answered call is a stored call" survive a power cut rather than merely a
process restart. An adapter that skipped it would report a much better number and
quietly mean something weaker.

**SUPERSEDED by §6.10 — the paragraph below was true when it was written and is not
true now. It is left because deleting it would hide what the number was scoped against.**

> **"An answered call is a stored call" is true of the reply the *loop* sends, and this
> document shipped it unqualified.** An ordinary `rpc`, a `blob`, a `recv` and a feed item
> all go handler → observer chain → store → answer. A `#[defer]` method answering through
> `Deferred::send` does not: that call hands the value to the caller from inside the
> running handler, where *"this turn's observers neither see it nor gate it"*, so the
> caller holds its answer for the whole ~7.6 ms below. A crash there loses state somebody
> was already told about. Scoped in `persist.rs`'s module header, on the type, in
> `examples/durable_ledger.rs`, and demonstrated by
> `tests/persist.rs::a_deferred_reply_is_answered_before_its_turn_is_stored` — a caller
> holding a receipt for a debit that is not on the disk. A `#[defer]` method answering with
> `Reply::send` inside its own turn is gated normally and keeps the property.

The claim now, in one sentence: **an answered call is a stored call, for every answer a
*turn* produced.** A `#[defer]` method's parked answer waits for the same observer chain
as any other, because `Deferred::send` parks the value instead of writing it. The one
send that is not covered is one made from outside every turn — a `Deferred` handed to a
spawned task, or reached from a `with_state` closure — and that is outside the claim
rather than a hole in it. §6.10 has the argument and the four decisions it forced.

So the number is not a defect to fix in the adapter — it is the price of the promise.
It is recorded in `keylink/src/persist.rs`'s module documentation, in
`keylink/examples/durable_ledger.rs`, and reproducible with
`cargo test -p keylink --test persist -- --ignored --nocapture`, which prints it and
asserts nothing about it. **There is no wall-clock assertion anywhere in this wave.**

What the number decides: RFC-06 §7's debounce stops being a nice-to-have and becomes the
obvious next knob. `Observer::finish` is the port hook it will need — but `Persist` does
not implement one, and §6.4 says why the version that did was worse than nothing.

### 6.2 Placement: a module in `keylink`, not a third crate

Three arguments, and the third is the one that had to be built rather than argued.

- A `keylink-durable` crate whose entire content is one adapter is a hypothetical seam.
- The dependency-direction objection does not survive. The hexagon's rule is that the
  **port** must not know the framework, and `keylink → keylink-persist` is the
  application depending on the port. `keylink-persist` still compiles, tests and
  documents alone and names `keylink` nowhere.
- The browser objection was a **feature** problem. `keylink-persist` arrives
  `optional = true, default-features = false`, and the feature is split:

  | feature | what it adds | browser |
  |---|---|---|
  | `persist` | the adapter, over the port | **builds** |
  | `persist-file` | `persist` plus the file adapter | refused |

  `.mise/tasks/wasm` now runs a second `cargo build` for
  `wasm32-unknown-unknown --features keylink/persist`, so "the objection was a feature
  problem" is a build rather than a paragraph.

`persist-file` is a **default** feature, beside `blocking`, `fs` and `env`, and for the
same reason all three are: a native build gets the adapter that exists, and a browser
build already passes `--no-default-features`. It also keeps a bare `cargo test
--workspace` running this wave's tests, which a default-off feature would have made
invisible to the gate — `blobs-fs` is the in-tree example of a feature whose code no
test ever runs.

`keylink/Cargo.toml`'s dev-dependency comment — *"Test-only, and acyclic… rather than
making the hexagon point the wrong way"* — was rewritten in the same commit rather than
left as a rule the change broke. The dev-dependency is **gone**: the dependency is real
now, so tests name it as a normal one.

### 6.3 The type parameter is the STORE, and that is what keeps the framing gate honest

`Versioned<C>` demands `C: Claim`, and `keylink_persist::Claim` is the trait whose one
method writes **unframed** bytes. `.mise/tasks/framing` reads a property of Rust rather
than a naming convention: `store` is a trait method, so a file that never brings the
trait into scope cannot call it, whatever it calls its variables.

So `Persist` is generic over `S: Snapshots` and holds a `Versioned<S::Claim>`, which
needs no bound of its own because `Snapshots` already promises one. `keylink/src/persist.rs`
therefore never names `Claim`, and the strongest available statement about the one file
in keylink's library that writes a snapshot is that **it has no route to an unframed
write at all**. It is on the framing task's second allowlist (it calls
`Versioned::store`) and deliberately not on the first.

Two files came **off** that allowlist in this wave, which is the part worth recording:
`keylink/examples/durable_ledger.rs` lost its commit loop and `keylink/tests/feeds.rs`
lost the hand-rolled `Saver` that prototyped this adapter. Neither stores anything now.
A stale allowlist entry is a hole nobody can see.

**REVISED, 2026-08-07: the first check refused what the port is for.** It matched any
file outside `keylink-persist` bringing `keylink_persist::Claim` into scope — and
`impl Claim for MyStore` matches, because there is no spelling of "implement this trait"
that does not name it. So the grep achieved in-tree exactly what sealing the trait would
have, three lines below a paragraph in the same file rejecting sealing for costing *"the
hexagon its whole point … the port exists to be implemented from outside, for a bucket
or a vault"*. Both could not be true.

It was not theoretical: the test-crate adapter that fails a `store` on demand — the one
that turns §6.4's rule from an argument into a red test — is an in-tree implementor, and
this gate is what stopped a reviewer compiling the counter-example. The check now exempts
any file containing `impl … Claim … for`, and still catches a bare importer (watched:
one file with `use keylink_persist::Claim;` and no impl, and the gate names it). What is
forbidden is *calling* `Claim::store` on somebody else's claim, which is the second
check's job and has a named allowlist.

### 6.4 REVISED, 2026-08-07: the `dirty` flag is gone, and its retry was a double-apply

This section used to describe a `bool` on `Persist` meaning "there is a committed change
that is not on the disk yet", set before the write and cleared after it, and an
`Observer::finish` that wrote the state one last time whenever the flag was standing.
The paragraph read: *"the state is complete and consistent and only the write is
missing, and one more attempt before the state is dropped is the difference between
losing the last committed turn and not."*

**That was wrong, and a review caught it. It is money-adjacent, so the correction is
recorded here in full rather than edited over.**

The only thing that could leave the flag standing was a `store` that had already
returned `Err`. That `Err` refuses the turn, which ends the actor, which hands the
caller `TurnLost::Refused` — *"it ran and was not committed"* — and hands a peer
`observer refused: persist` under a comment in `local.rs` saying the same thing. So the
turn `finish` retried was **exactly** the turn somebody had been told was not committed.
A caller acting on that refusal by replaying applied the entry twice; on a `Post(entry)`
against a ledger that is money.

The fix is a decision, and this is the direction taken: **the code was made to match the
promise, not the promise weakened.** The refusal's text is what crosses the wire and the
only thing the caller has, so it stands, and the retry goes.

The rule the adapter now holds, in both directions:

> **What is stored is exactly the set of turns that were answered.**

The first half is the wave's headline. The second half — a turn that was not answered is
not stored either — is the one that was broken, and it is now
`tests/persist.rs::a_turn_whose_store_failed_is_not_written_on_the_way_out`, over two
real nodes so the assertion is on the sentence a **peer** reads. Made red by putting the
retry back: `left: [1, 2]`, `right: [1]`.

Three consequences worth stating rather than leaving to be re-derived:

- **A feed turn has no caller, so retrying its failed store really would be harmless.**
  The adapter still does not, because it cannot tell the two apart: `TurnInfo` carries
  the outcome and the method name and nothing about who is owed a reply. Fail closed.
- **`finish` and the flag are both deleted, not repaired.** With the retry gone the flag
  had no reader, and a `finish` that cannot legitimately write anything is not a
  guaranteed final flush — it is speculative code for a debounce that does not exist. A
  debounce returns `Ok` from `after_turn` **without** writing, which gives up "an
  answered call is a stored call" on its own terms; its `finish` therefore answers a
  different question than this one would have, and pre-building the answer got it wrong
  once already.
- **RFC-06 §4.2's deleted `Durable::revision` is still deleted for its own reason** — an
  implementor-supplied counter with nothing connecting it to a mutation. That argument
  did not depend on this flag existing.

### 6.5 The `A: Sync` trap, from the other side of the port

`after_turn` is a plain `fn` returning a boxed future, and not an `async fn`, so an
implementation can do synchronous work with `state` **before** the future exists. Wave 0
probed the trap; Wave 2d hit it again on `finish`. This is the first time an
implementation had to obey it.

`Persist::after_turn` calls `Snapshot::of(state)` synchronously and moves the bytes into
the future. Two breaks, failing in two different places, are what make that a checked
claim rather than a remembered one:

- Write the naive version — `store(&Snapshot::of(state))` *inside* the `async move` —
  and **keylink itself** stops compiling before any test is reached:
  `error: future cannot be sent between threads safely`, at `persist.rs`'s `Box::pin`,
  with `help: consider further restricting type parameter D with trait Sync`. The bound
  is demanded of the generic `D`, so the library refuses it for everybody.
- Add `Sync` to the impl's bound and
  `tests/persist.rs::a_state_that_is_not_sync_is_still_persisted` stops compiling,
  naming the state: `error[E0277]: Cell<u64> cannot be shared between threads safely`,
  `required because it appears within the type Counter`. Every `Ledger` test in the file
  still passes, which is what makes the break discriminating rather than merely red.

The state in that test holds a real `Cell<u64>` **and** a real `RefCell<u64>`, so
neither member's removal can make the claim vacuous — the house rule a unit-struct
`Panic` earned through two revisions of RFC-06.

### 6.6 A keyed actor's state outlives its process, which Wave 3 is what made possible

`tests/persist.rs::a_keyed_actors_state_survives_its_own_process` mounts a `Persist` on
a keyed actor, drives it over **two real nodes** through the generated `KeyedClient`,
stops it, and re-opens the state in a second "process".

Say plainly why it could not be written before. A keyed actor used to run a second loop,
generated into the user's crate, which handed the dispatch an **empty hook slice** — so
a keyed actor had no observers, no `finish`, and no route to durability at all. Wave 3
merged the two loops into `run_loop` and `spawn_wired` is the door that carries the
triple. Red-tested by putting `Vec::new()` back where `spawn_wired` passes its observers
on: `left: [20], right: [10, 20]`, the second run having restored nothing.

**One limit this wave found and did not fix.** A `spawn_with` factory is a plain `Fn`, so
it cannot `await`, and a `Persist` has to claim its state before it can observe anything.
A durable keyed actor is therefore mounted **eagerly** — `Persist::open(..).await` and
then `System::mount` — rather than built on first message. `spawn_wired`'s documentation
says so now; the doc example that used to imply otherwise named a `store.open_blocking`
that never existed.

### 6.7 Every criterion, and the break that proves it

| criterion | test | the break, and what it printed |
|---|---|---|
| a state survives its own process | `a_state_survives_the_process_that_built_it` | `after_turn` does not store → `run one stored something` |
| an answered call is a stored call | `an_answered_call_is_a_stored_call_and_its_bytes_are_framed` | same break → `NotFound` reading the snapshot |
| bytes are framed by 4c's envelope | (same test) | magic, layout and `Ledger::VERSION` read byte by byte off the file |
| a panicked turn is not stored, and ends the actor | `a_panicked_turn_is_not_stored_and_ends_the_actor` | the panic arm never fires → `left: [1, 99], right: [1]` |
| a refusal before `Persist` prevents the store | `a_refusal_before_persist_prevents_the_store` | swap the two observers → a refused turn is on the disk |
| the claim is free the instant `stop().await` returns | `the_claim_is_free_the_instant_stop_returns` | `std::mem::forget(hooks)` in `run_loop` → `Busy { id: StateId("ledgers/sp-01") }` |
| two `open`s on one `StateId`, the second `Error::Busy` | `a_second_open_on_one_state_is_busy` | `WouldBlock` hands out a `FileClaim` instead of `Error::Busy` → `a second open on a claimed state must be refused` |
| a feed turn is stored on the same rule | `feeds.rs::a_feed_turn_is_stored_on_the_same_rule_as_a_mailbox_turn` | `after_turn` does not store → `something was stored` |
| a keyed actor's state survives its own process | `a_keyed_actors_state_survives_its_own_process` | empty hook slice → `left: [20], right: [10, 20]` |
| a non-probe `Observer` implementor exists | `rg 'impl.*Observer<' keylink/src` | `keylink/src/persist.rs` is the only hit outside `#[cfg(test)]` — ~~only~~ **true when Wave 5 wrote it; see the note under this table** |
| `A: Sync` stays out of the design | `a_state_that_is_not_sync_is_still_persisted` | §6.5's two breaks |
| the per-turn cost is measured, not estimated | `the_per_turn_store_cost` (`#[ignore]`) | §6.1 |
| a refused turn is not stored on the way out | `a_turn_whose_store_failed_is_not_written_on_the_way_out` | put the `finish` retry back → `left: [1, 2], right: [1]` |
| ~~a `#[defer]` reply is answered before its turn is stored~~ **INVERTED by §6.10** | `a_deferred_reply_whose_turn_was_not_stored_is_refused` | `park` never finds a lot → `a refused turn's receipt reached its caller: Ok(60)`; or stop storing → the refusal never happens |
| a `#[defer]` reply that IS answered is a stored one | `a_deferred_reply_that_is_answered_is_a_stored_reply` | `Verdict::Committed => drop(value)` → `the receipt reaches its caller when the turn commits: MailboxClosed` |
| the restored state cannot be dropped on the way to the observer | `Opened::start_from` is the only route | write the old `let (_restored, saver) = ..` → `E0308`, `expected Opened<_, Files>, found (_, _)` |
| both shipped crates still package | `mise run package` | drop `version` from keylink's `keylink-persist` dependency → `all dependencies must have a version requirement specified when packaging` |

**The `Observer` row's uniqueness clause rotted inside this same PR, and the row is kept
rather than rewritten so the rot is visible.** Wave 6 landed `impl<A> Observer<A> for Turns`
at `keylink/src/console.rs:602`, which is production code — `rg '#\[cfg\(test\)\]'
keylink/src/console.rs` returns nothing across 1,035 lines. The command now yields four
hits: `console.rs:602` and `persist.rs:403` outside `#[cfg(test)]`, `local.rs` inside its
test module, and `representability.rs`, whose whole file is `#![cfg(test)]`.

**The criterion still passes and the evidence no longer does.** What Wave 5 had to show
was that a non-probe implementor *exists*; that is now true twice over, which is a
stronger result than the one recorded. Only the word "only" is false, and it was false
because a later wave in the same merge did the right thing. Worth keeping as an example
of the failure mode: an existence claim evidenced by a uniqueness command ages badly, and
the fix is to write the command that matches the claim.

`keylink/tests/durable.rs` is unchanged in behaviour and keeps committing once per run
through `Handle::with_state`: **`with_state` survives and is not deprecated.** It is a
*read*, and `representability.rs::a_read_cannot_mutate_either` still holds the claim.
What Wave 5 subsumed is using it as a **commit** mechanism, which is what the example
did and no longer does.

### 6.8 What Wave 5 did NOT do

- **Debounce.** The number in §6.1 is the argument for it, but nothing here batches, and
  §6.4 explains why no part of it was pre-built either. A caller who wants the other
  trade has it by choosing a different adapter, not by configuring this one.
- **Retry a failed store, at all.** The version that retried once at `finish` retried the
  turn whose caller had been told it was not committed — §6.4. It is deleted, and the
  path is no longer untested: it needs a store that fails **transiently**, which a real
  `Files` cannot be asked to do (`limit()` fails permanently, and removing the directory
  breaks the lock the claim holds), so `tests/persist.rs` implements the port a second
  time. `Flaky` fails exactly one `store` and then works, which is what makes the break
  discriminating.
- **Close the `#[defer]` window.** `Deferred::send` answers from inside the running
  handler, before the observer chain — §6.1. It is scoped in five places and
  demonstrated by a test rather than argued about. **The honest fix, not built here:**
  park the *send* as well as the reply, so the value is released after the observer
  chain of the turn that produced it. That keeps `#[defer]`'s whole point — the reply
  still outlives the turn that *received* it — and costs one queue in the loop. It
  touches `local.rs`, so it is a decision for whoever owns the loop.

  **DONE, 2026-08-07 — §6.10.** Built exactly as described, and the queue turned out to
  belong to the *turn* rather than to the loop, which is what makes "nothing is parked at
  `finish`" structural. This bullet is left as written because it is a true statement
  about what Wave 5 did.
- **Make the idempotency ledger survive a restart, or say the guarantee has changed.**
  Said, in `keylink/src/idempotency.rs`'s "What is NOT guaranteed". Durable state plus a
  volatile ledger is a **new cross-restart double-apply**, and this wave created it:
  turn *n* is fsynced, `Guard::record` writes the replayable reply into memory, the
  process dies before the frame reaches the peer, the peer retries, the restarted
  process restores a state that already contains turn *n*, the ledger is empty,
  `claim_key` answers `Claimed::Run`, and the handler runs turn *n* again. Neither half
  is wrong alone; the pair is new. Effectively-once is a property of one process's
  lifetime, not of a key.

  **`README.md`'s limit 2 says "the ledger is memory … `keylink-persist` makes an
  actor's *state* durable and does nothing for the ledger", which is true and stops one
  sentence short of the consequence.** That file is owned by a parallel session, so this
  wave did not stage it; the sentence it wants is the interleaving above.
- **Move the ALPN.** `keylink/Probe/d3fe34471921164e` is unchanged, and all five hash
  tests in `tests/protocol.rs` are green. An observer is local: it is a constructor
  argument, not a declared type, so there is nothing for the wire graph to hash.
- **Reach `registry.rs`.** Zero lines. A keyed actor's observers arrive through
  `spawn_wired`, which Wave 3 already built.
- **Answer what a dead keyed actor should do.** This wave makes an existing gap
  consequential rather than theoretical, so it is recorded here rather than
  discovered later. `Persist` refuses a panicked turn, which ends the actor — and a
  keyed actor whose task has ended stays in the registry's map with a sender whose
  receiver is gone, so `Registry::route` hands that sender out and the send fails.
  The peer is told something, the actor is never rebuilt, and §5.7's missing
  `ended()` channel means nothing can be told **why**. On a `LocalActor` the same
  fault is fully observable through `ended()`, which is what
  `a_panicked_turn_is_not_stored_and_ends_the_actor` asserts. This is Wave 9's
  problem and it needs the field on `Mounted` that §5.7 already names.
- **Make the `persist-file` wasm guard fire.** It was watched and it did not: cargo
  compiles dependencies first, so the build stops at tokio's own `compile_error!` naming
  `fs`. `lib.rs` says so at the guard rather than leaving a reader to assume otherwise.
- **Fix `Persist::open`'s inference.** `D` appears only in the return type, so it usually
  has to be named — `Persist::<Ledger, _>::open(..)`. Handing the restored value to a
  generic `actor_with<A: Actor>` leaves `D` ambiguous even where exactly one type
  implements that service's `Actor`, because rustc will not pick an impl to resolve a
  type parameter. Documented on `open` rather than worked around.

### 6.9 REVISED, 2026-08-07: two claims the manifest and the type system did not hold

**`open` returned a tuple, and a tuple element is not `#[must_use]`.** The doc said
*"handing the restored state back with the adapter that will keep writing it makes the
read impossible to leave out"*. It did not: `let (_restored, saver) = ..` compiled clean,
and it was the shortest form — which this wave's own tests wrote at five sites, with a
sixth in `tests/feeds.rs` and one that passed `Ledger::default()` beside a restored state
it had just read. An author copying the short form into a service with live stored state
loses it on turn 1, silently, once per deploy.

`open` returns `Opened<D, S>` now: one value, `#[must_use]`, with private fields.
`was_stored()` asks the first-run question without taking anything, and `start_from`
is the only method that yields a `Persist` — and it yields the state in the same
expression. Every call site moved; the old spelling is `E0308`, watched.

It is not a claim that the state cannot be *discarded* — `let (_state, saver) = ..
.start_from(..)` still compiles, and no type can stop that. It is that the discard has to
be written on purpose, instead of being the shortest thing to copy.

**`keylink` could not be packaged, from the commit that made this dependency real.**
`cargo package -p keylink` failed with *"all dependencies must have a version requirement
specified when packaging"*: cargo strips unversioned dev-dependencies and the
`keylink-persist` line was fine while it was one. `keylink-persist` was also
`publish = false`, which would have refused the publish a second way, and `keylink`
carries `description`/`license`/`repository` and a manifest comment about "a published
build".

Fixed both ways — `version = "0.1.0"` beside `path`, and `keylink-persist` is publishable
— and **priced**: that is a second crate name on crates.io, released before `keylink` and
version-bumped in lockstep whenever the port moves. It is what a port in its own crate
costs once anything ships, and the alternative (folding the port back in) costs the
browser build and the "the port does not know the framework" claim.

`mise run package` is in the gate now, because a manifest can stop being publishable
without one line of code changing and nothing else in the gate looks. Watched by removing
the version again.

### 6.10 REVISED, 2026-08-07: the `#[defer]` window is closed, and what the closing decided

§6.8 recorded the fix and did not build it: *"park the send as well as the reply, so the
value is released after the observer chain of the turn that produced it."* Built, in
`keylink/src/parking.rs`. A value handed to `Deferred::send` **during a turn** is held in
that turn's lot and released after its observer chain, with the same verdict the turn's
own caller is answered by. `#[defer]`'s point is untouched — the reply still outlives the
turn that *received* it — and what it no longer does is outrun the turn that *answered*
it.

**The lot belongs to the turn, not to the loop**, which is the one place the built thing
differs from the sketch and the difference is load bearing. `observed_turn` wraps the
handler in a `Parking` future that owns the lot; the lot is reached through a thread-local
that is exchanged in on poll-entry and restored on poll-exit, including on an unwinding
poll. Three questions answer themselves from that:

- **Nothing is parked at `finish`.** A lot cannot outlive the turn it is a local of, so
  `run_loop` reaches the `finish` chain with nothing held. Structural rather than a drain
  nobody would have watched fail — and it matters here because §6.4 already took
  `Persist::finish` out for writing a turn its caller had been told was not committed.
- **A cancelled turn answers `Gone`.** `abort()` drops the loop's future, which drops the
  lot, which drops the reply channels in it. The turn never committed.
- **A send from off the turn's task is answered immediately**, because there is no lot in
  scope. That is the residual limit, and it is outside the claim rather than a hole in it:
  nothing ran, no observer saw anything, and there is no commit for the answer to wait
  for. Withholding it would hang a caller on a turn that may never come.

**Four decisions, and the arguments rather than the outcomes.**

1. **A refused turn refuses its parked answers, naming the same observer.** The turn's own
   caller gets `Answered::Refused { name }`; so does every answer that turn parked, read
   from the same two facts six lines apart in `observed_turn`, so the two cannot disagree.
   The alternative — dropping them, so `Gone` — was rejected because `Gone` means *the
   actor is not there* and a caller told that stops calling, while `Refused`'s own text is
   *"it ran and was not committed"*, which is exactly what happened to the work behind the
   answer. The Critical-1 mirror does not apply: a refusal ends the actor, so the state
   this value was computed from is dropped rather than stored, and nobody is told "not
   committed" about something that was.
2. **A panicked turn drops them, so `Gone`.** Not `Faulted`, and not a near miss: there is
   one `Panic`, it is the original payload rather than a message, `resume_unwind` on the
   original payload is the difference between forwarding a panic and starting a second one,
   and it belongs to the turn's own caller. A payload cannot be cloned to tell two, and a
   second one manufactured from a string is what `panic.rs` refuses. `Gone` is also the
   answer `Deferred`'s own documentation already gave for a fault that reaches a parked
   reply, so this is one rule rather than a new one.
3. **Shutdown needs no rule**, per the lot's ownership above.
4. **Ordering: parked answers first, in send order; the turn's own reply last.** The lot is
   a `Vec` and the release is a `for` loop, so send order is the release order. It runs
   inside `observed_turn`, which returns *before* `turn` answers the turn's own reply — so
   a handler that answers other callers and then returns a value of its own is read in
   that order by everybody. Asserted at `parking.rs`'s own unit test, where the release is
   observable directly; **deliberately not asserted across two spawned callers**, because
   that would be a statement about tokio's scheduler rather than about the release.

**`Reply::send` was left alone**, and that is a decision too. It already held its value in
the `Replied` token until after the chain, so the property was never broken there. Routing
it through the lot as well would have bought exact program order between a `#[defer]`
handler's own reply and the answers it sent to other callers — an ordering no caller can
observe — and cost a type erasure and one allocation per deferred reply that costs nothing
today. Two mechanisms, because they hold two different things: the token holds *this
turn's own* answer, which the turn function has in hand; the lot holds answers the turn
function never sees.

**The ALPN did not move.** `keylink/Probe/d3fe34471921164e` and
`keylink/Inspect/7bbd34fa83c9060c` are unchanged and all five hash tests in
`tests/protocol.rs` are green. A `Deferred` is local, never serialized, and not part of
any declared type, so there is nothing here for the wire graph to hash.

**`tests/persist.rs::a_deferred_reply_is_answered_before_its_turn_is_stored` was inverted,
not deleted.** Its own doc said that if `Deferred::send` were ever made to wait for the
chain the test would go red and that the redness was the notice. It went red — *"the
parked caller was answered from inside the handler: MailboxClosed"* — and it is now
`a_deferred_reply_whose_turn_was_not_stored_is_refused`, same vault, same failing store,
opposite assertion, with the inversion recorded in its doc.

---

## 7. Wave 4a and 4d — mailbox depth becomes a knob, and a registry can be asked what it is doing

Two items, in this order because the second one's field list depends on the first one's
answer: whether `ActorStatus` carries a buffered count at all depends on whether depth
can exceed one.

### 7.1 The knob is on the system, and that is what made it possible at all

`MAILBOX = 1` was a constant in two places. The registry's is now
`DEFAULT_MAILBOX_DEPTH`, the default behind
`SystemBuilder::mailbox_depth(NonZeroUsize)`.

**A builder method with a documented default, not a mandatory argument.** The
`Eviction`/`Retention`/`Allowlist` family has no `Default` because a wrong choice there
loses state or admits a stranger. Depth is latency against memory: a wrong value is
slower or fatter, both visible and both recoverable, so forcing every call site to state
one is friction with no matching hazard. `representability.rs`'s `HasNoDefault` probe is
deliberately **not** extended to depth.

**`NonZeroUsize` and not `usize`.** `mpsc::channel(0)` panics, and after this wave the
channel is created per actor inside `Registry::entry` — so a zero would panic at the
first *mount*, or inside a lazy factory on a serving node, one message after the builder
that caused it returned. The type moves that from an untestable runtime panic to an
argument nobody can write. Saturating a zero up to one would have been the silent
fallback this crate refuses everywhere else.

**The structural change is that `Mounted<M>` no longer holds a mailbox.** It held the
sender and an already-built future, so the channel — and therefore the depth — was fixed
by `mount()` before any registry saw the actor. A knob on `SystemBuilder` would have
applied to nothing. `Mounted` now carries `FnOnce(Mailbox<M>) -> impl Future` and
`Registry::entry` makes the channel, which is:

- the **only** place a mailbox is created, so the depth reaches a lazily spawned actor as
  well as a hand-mounted one — and a factory is a `Fn(&ActorId) -> Option<Mounted<M>>`
  with no argument for a depth and no caller present to supply one, so a per-`spawn`
  depth could not have covered the case a serving node builds by itself;
- source-compatible: every existing call site compiles unchanged, generated code
  included. `mount(|inbox: Mailbox| ..)` is the same line it was.

The cost is a second transient `Box` per mount, on a path that already allocates a
channel and a task. Nothing the registry *keeps* is boxed by it, and
`tests/actor_memory.rs` shows the per-idle-actor figure unmoved.

### 7.2 `unbounded` is refused permanently, and a full mailbox blocks

An unbounded mailbox converts backpressure into memory growth: a latency problem becomes
an OOM, at a moment nobody chose, in a process that had been reporting healthy. That is
the inversion of failing loudly, so it is the one kameo feature rejected outright rather
than deferred.

The refusal is a compile-time probe, not a sentence.
`representability.rs::no_mailbox_can_be_asked_to_be_unbounded` holds two, because the two
ways it would arrive cannot see each other: a shadowing probe on the *names*
(`unbounded`, `unbounded_mailbox`, `without_backpressure`), and a coerced function item
pinning `mailbox_depth`'s parameter to `NonZeroUsize` — which is the shape the refusal
would really arrive in, since nobody adds a method called `unbounded`, they widen the
argument of the one that exists. Both were watched to fail:

- an inherent `unbounded(&self) -> usize` on `SystemBuilder` →
  `error[E0308]: expected NoSuchMethod, found usize`;
- `mailbox_depth(mut self, depth: usize)` → `expected fn pointer ... found fn item`.

What neither reaches is `tokio::sync::mpsc::unbounded_channel` called directly inside the
crate. That is a claim about a foreign function and no probe on a local type can make it;
`rg 'unbounded_channel' keylink/src keylink-persist/src` is the check, and its only matches
are the two sentences of the probe's own documentation. The tests use unbounded channels
for their own logs, which are not mailboxes.

**A full mailbox blocks the sender. It never drops and never errors.**
`registry.rs::a_full_mailbox_blocks_the_sender_and_never_loses_the_message` watches all
three outcomes an overflow policy chooses between: the send has not returned while the
mailbox is full, it returns `Ok` once a slot frees, and the actor counts both messages.
Red-tested by moving the default from one to two, which makes the second send return at
once.

The one caller-visible exception is opt-in and is not the mailbox dropping anything: a
`QueueTimeout` layer turns the wait into `RemoteError::TimedOut`, and a message refused
that way provably never entered the mailbox, because the send future is dropped before a
permit arrives.

### 7.3 The 32-slot fact, measured — and the claim it corrects

`tokio::sync::mpsc` allocates its first block of 32 slots eagerly, so the "latency versus
memory" trade does not exist below 32. `tests/actor_memory.rs`'s depth phase measures it
rather than reading tokio's source, because the table on `Mailbox` carried three figures
about this allocator that were wrong for months under a sentence claiming they were
measured. At two thousand idle keyed actors, `aarch64-apple-darwin`, 2026-08-07:

| mailbox depth | bytes per idle actor |
|---|---|
| 1 (the default) | 1,424 |
| 32 | 1,424 |
| 1,024 | 1,424 |

**Identical, not merely close — and the flatness goes further than the claim needed.**
Issue #3 says the trade "only becomes true above 32". Measured, it does not become true
at 1,024 either: later blocks are allocated when messages *arrive*, not when capacity is
declared, so declaring a deep mailbox costs nothing at all until something fills it. The
trade is about a mailbox that is **full**, not one that is deep. That correction is in
the knob's own rustdoc.

Two things this measurement needed to be honest:

- The ids must not mention the depth. `depth/1024/1999` is two characters longer than
  `depth/32/1999`, and that `String` size class showed up as 7 bytes per actor of
  "mailbox cost" in the first run.
- The phase was red-tested, because a measurement nobody has watched move is not a
  measurement: leaking a `Vec::with_capacity(depth)` per mailbox inside `Registry::entry`
  — the shape of the cost this phase exists to refuse — gives 1,425 / 1,456 / 2,448 and
  fires the flatness assertion at depth 32.

What is **not** measured: a mailbox actually holding more than 32 messages allocates a
second block. Stated in the test's header as reasoning rather than as a measurement.

### 7.4 The shutdown budget's meaning is depth-dependent, and that is a defect surface

`shutdown(id, budget)` drains everything an actor has: at depth 1 that is at most one
queued turn plus the one in flight, at depth `N` up to `N + 1`. A budget sized against
the default therefore silently under-drains at any deeper setting — and an expired budget
does not wait, it destroys the actor where it stands, which is 4b's detach defect
arriving through the other door.

`a_budget_sized_for_the_default_depth_under_drains_at_a_deeper_one` proves it with the
same budget twice: at depth 1 the actor drains and reports `Ended::Stopped`; at depth 3
the same budget covers two of four turns and it reports `Ended::Vanished` with two turns'
work destroyed. Exact rather than sampled — the clock is paused, a turn is one `TURN`,
and the budget is two and a half of them, so no assertion rests on which of two
simultaneous timers fires first. Red-tested by making `Registry::entry` build every
channel at the default depth: the deep system then reports `Stopped`.

The relationship is documented at the builder method and not only here, which is what the
item asked for.

### 7.5 The local path keeps its constant, and issue #3's premise about it was wrong

Item 4a's framing says the constant's argument "reasons from `max_concurrent_bidi_streams`
— a bound that does not exist off the wire". That is true of `registry.rs`. It is **not**
true of `local.rs`, which as of this tree already made the in-process argument in full:
*"In-process there is no transport and no such bound — a caller can spawn ten thousand
tasks that each send"*, and then refused to raise the number for a reason that is a
hazard rather than a preference:

> Raising it would let callers run ahead without feeling it, and would make a self-`tell`
> succeed more often — which is worse, not better, because `Handle` documents self-calls
> as a deadlock and the difference between "usually" and "always" is what makes a deadlock
> a production incident instead of a test failure.

So the local path is exactly the case with the matching hazard 4a says depth lacks: a
`LocalActor` is addressed from whatever task holds the handle, *including its own turn*,
and a deeper mailbox turns a documented always-deadlock into an intermittent one. An
intermittent deadlock is not a slower program; it is a different program in production
than in the tests. A keyed actor is addressed from another task — a QUIC stream's — so
there the trade really is only latency against memory.

The decision: **the knob is on the keyed path only.** `local.rs`'s constant keeps its
value, is renamed `LOCAL_MAILBOX_DEPTH` so that `rg MAILBOX keylink/src` shows the pair
and which of the two is a default, and gains the cross-reference saying why the asymmetry
exists — so the next reader finds a decision rather than an oversight. If a case for a local knob arrives, it has to
answer the self-call hazard first, and the answer is not a bigger number.

### 7.6 4d — `System::status()`, and the signal `busy` needed

```rust
#[non_exhaustive]
pub struct ActorStatus {
    pub id: ActorId,
    pub idle_for: Duration,
    pub busy: bool,
    pub buffered: usize,
}
impl<M> System<M> { pub fn status(&self) -> Vec<ActorStatus>; }
```

Shipped as sketched, including the two fields that are **not** there: no `incarnation`
(nothing in the crate counts one — Wave 2 looked and deliberately added no counter), and
no `queued`. `representability.rs::an_actor_status_claims_only_what_it_measures`
destructures all four fields with no `..`, so a fifth is `E0027` at a doc comment
recording those two refusals. Watched: adding a `queued: usize` **together with a value
for it in `status()`** makes that probe the only error in the build, which is the one
shape the field list alone can catch.

`idle_for` is `now - last_seen` on the registry's own `n0_future::time` epoch — the same
reading `Eviction::Idle` compares against, never `std::time`, which panics on
`wasm32-unknown-unknown`. The test makes the two numbers agree at the boundary rather
than merely exist: at 59 s of a 60 s policy `status` says 59 and the sweep leaves the
actor alone; one second later the sweep takes it. It is *last routed*, not *last handled*
— a message still in the mailbox has already reset it, exactly as it has for eviction.

**`buffered`, and the name is the design.** `max_capacity() - capacity()` is exact for
what the channel holds; the parked-sender count is not observable at all, because
`Semaphore` does not expose its waiters. So the field is a documented **floor**, and the
test asserts both halves: three messages in three slots read 3, and a fourth sender
parked on a permit leaves it at 3.

That test is also this wave's own vacuity story. Its first version asserted only "3 with
three messages queued" — and deleting the subtraction in `status()`, so that `buffered`
reported the channel's *capacity*, left it **green**, because a field reporting the depth
says 3 too. The readings are now taken from zero and one message at a time, and the same
break fails three tests. A guard that cannot tell the measurement from the constant is
not a guard.

### 7.7 Where the `busy` flag lives, and why not where the issue put it

`busy` needed a signal that did not exist. `last_seen` is stamped on *route*, and
`capacity()` is restored the moment the loop *receives* — before the handler is entered.
Both say "busy" about an actor that has not started and neither says anything about a turn
still going.

Issue #3's answer: make `Mailbox<M>` a `#[doc(hidden)]` newtype with one `recv` that
clears the flag on the way in and sets it on the way out. **Half of that is what shipped
and half of it is a defect**, and the defect is the criterion two lines below it in the
same item:

1. **A feed item is a turn and it never touches the mailbox.** A flag maintained inside
   the receive reads `false` through every feed turn, so "`busy` is true during a feed
   turn as well as a mailbox turn" cannot hold.
2. After Wave 3 the receive is `Inbox::poll_next`, a synchronous poll that the loop
   **skips entirely** on any iteration where the feed was ready first. So the flag would
   not merely be unset during a feed turn; it would hold whatever the last mailbox turn
   left in it. Stale, not wrong.

What shipped instead. `Mailbox<M>` **does** become a newtype, because something has to
carry the flag from the registry (which makes the channel) to the loop (which knows when a
turn starts) — and doing that through the mailbox costs the generated code nothing, while
widening `mount`'s closure would have put the pairing in every expansion of `service!` in
the world. That is the item's own rejected alternative, and rejecting it was right.

But the flag is set and cleared in **`local.rs`'s `run_loop`**, around the one dispatch
both inputs go through, reached by one defaulted method on Wave 3's private `Inbox` trait:

```rust
inbox.mark_busy(true);
let ended = dispatch(mail, &mut state, &mut hooks).await;
inbox.mark_busy(false);
```

`Inbox::mark_busy` is defaulted, and `Keyed<M>` forwards to the mailbox's flag. This
keeps every property the item wanted: one place, in keylink's own code, not in generated
code where two sites could drift. It adds the one the item's spelling could not have:
both inputs, by construction, because there is one dispatch.

**This paragraph used to say a `LocalActor` "has no registry entry to report to" and so
costs a no-op. That stopped being true when `Watched` shipped**, and the sentence is
corrected here rather than deleted, because the reasoning it carried is the reasoning the
seam was designed on. A `LocalActor` still has no registry entry; what changed is that it
carries the cell on its handle instead, which is where its `Fate` already lived. The
refusal that stood on `mark_busy` itself weighed an idle actor, whose holder can ask the
handle — and a wedged one cannot be asked, because the question queues behind the turn.
A bare `mpsc::Receiver` still takes the default, and the supervision tests that hand
`supervise` one are why it stays.

Red-tested per arm, separately, because they are separately load-bearing. Both arms were
broken and both counts below were watched, one arm at a time.

Breaking the **mail** arm reddens seven tests across **two cargo targets** and leaves
`busy_is_true_during_a_feed_turn` green. Breaking the **feed** arm reddens exactly that
one — `--lib` stays at 170 green, `--test actors` at `47 passed; 1 failed`.

`cargo test -p keylink --lib` — three, all added with `LocalActor::is_busy`:

- `a_turn_reports_busy_and_a_read_does_not`
- `a_supervised_actor_reports_busy_through_its_handle`
- `an_aborted_turn_does_not_leave_the_handle_reporting_busy`

`cargo test -p keylink --test actors` — four:

- `busy_is_true_during_a_turn_and_false_between_turns`
- `busy_stays_true_across_an_await_inside_a_turn`
- `a_saturated_mailbox_at_depth_does_not_starve_a_feed`
- `different_actors_run_concurrently_while_one_actor_stays_serial`

**This list stood at three names for one wave and it was measured wrong.** It said "the two
mailbox tests" plus one of the three local ones, which is what happens when the author who
watched a break adds the test *they* watched to a list somebody else wrote. The two targets
are the reason it survived: a reader running `--lib` sees three reds and not one of them is
a mailbox test.

**One honest limit, stated on `Mailbox` rather than left to be discovered.** `mount` is
`#[doc(hidden)] pub`, so a loop can be written by hand — the tests in `registry.rs` and
`tests/actors.rs` do exactly that, to build actors that will not drain — and such a loop
reports no turns, so `busy` stays `false` for it. Every actor a `service!` produces runs
the one loop. A flag set inside `recv` would have been *worse* here, not better: `recv`
cannot see when the turn it handed out ended, so it would read `true` for an idle actor
from its first message until its last.

### 7.8 The tests, and one that got stronger by losing its stopwatch

`tests/actors.rs::different_actors_run_concurrently_while_one_actor_stays_serial` asserted
`across < hold * 3 / 2` on the wall clock and flaked three times in one run under
concurrent load. CONTRIBUTING.md forbids exactly this: *"Do not assert on wall-clock
time… a load spike is not a bug in your code."*

Both halves are counted overlap now. `Inside` records how many handlers were inside a
turn at the same moment and the most that ever were, and the two halves assert **opposite
peaks on the same counter**: two actors reach a peak of 2, and a third actor handling two
messages — the second provably in the mailbox, because `status` reports `busy` with
`buffered == 1` — has a peak of 1. A peak of 1 on a counter just watched to reach 2 is a
fact about the loop rather than about the instrument. Both halves get stronger under load,
and a serialised dispatch fails the timeout guard instead of hanging.

That pairing exists because the first rewrite was vacuous. It asserted the serial half
with `entered.try_recv().is_err()`, and under `break-once.sh` — releasing the held turn
before the check — it stayed **green**, because a scheduler hop is not a turn. Watched,
retracted, replaced.

The saturated-feed test is the other place depth bought a stronger claim than the tree
could previously make. `tests/feeds.rs` says of the same rule at depth 1: *"the loop takes
the slot's message, runs the turn and polls again without yielding, so the parked sender
has not been rescheduled and the mailbox is deterministically empty at the next poll."* At
depth 3, three messages are genuinely in the mailbox and `buffered` says so before
anything is released, so `a_saturated_mailbox_at_depth_does_not_starve_a_feed` asserts an
exact position: the feed's item is taken **before** the three messages queued ahead of it.
Red-tested with the loop's alternation deleted, which puts the tick fourth.

### 7.9 What Waves 4a and 4d did NOT do

- **Give the local path a depth knob.** §7.5 is the argument, not an omission.
- **Move the ALPN.** `keylink/Probe/d3fe34471921164e` is unchanged. Depth and status are
  local: no request variant, no schema, nothing a peer can see. A `feed` was added to the
  `gate` service inside `tests/actors.rs` and a feed is local too — it produces no wire
  type — so no hash in `tests/protocol.rs` moved.
- **Report the parked-sender count.** It is not observable. `buffered` says so in its own
  documentation and the probe keeps a field from claiming otherwise.
- **Measure a mailbox holding more than 32 messages.** §7.3.
- **Sort `status()`.** The map is a `HashMap` and the order is arbitrary. Sorting a
  diagnostic nobody asked to have sorted is a cost every caller pays for one caller.
- **Sample all rows at one instant.** Each row is read as the iterator reaches it, so ten
  rows are ten readings. `ActorStatus`'s documentation says so rather than pretending to a
  snapshot; a consistent one would mean holding the write lock across every read.
- **Give a keyed actor an `ended()` channel.** Still §5.7's gap, and still Wave 9's.
- **Watch the `docs`, `features` or `wasm` gates fail on this diff.** They were run green;
  no red was manufactured for them.
## 8. Wave 7 — composition, and the argument every one of them had to answer

Five patterns, none of which touches the transport. `keylink::compose` is one
`pub mod` with four types and two functions in it, and the reason it is a module
rather than five root re-exports is that they share exactly one thing worth
grouping: **a statement about what a slow consumer costs.**

### 8.1 THE DECISION THIS WAVE OWNS — one item in flight, and two ways to be full

kameo implements all five in ~2,400 lines, and the difference between its
versions and these is not size. Its versions predate a `MAILBOX = 1` argument, so
they answer a slow consumer by buffering, and the buffer has no bound. Issue #3's
refused table lists an unbounded mailbox as the one kameo feature *actively
rejected* rather than deferred, on the ground that it converts backpressure into
memory growth and a latency problem into an OOM. A fan-out is where that
conversion hides best: "publish to everybody" with a queue per subscriber is an
unbounded queue **per subscriber**, invisible until the day one subscriber is
slow.

So every pattern here names its bound, and the table is in `compose/mod.rs` where
a reader meets it rather than only here:

| pattern | a slow consumer costs | bounded by |
|---|---|---|
| `Topic` + `Overflow::BlockPublisher` | the publisher waits | one item in flight per subscriber; `publish` returns when the slowest has taken it |
| `Topic` + `Overflow::DropSubscriber` | the subscriber is unsubscribed, loudly | one item in flight per subscriber; a second ends that subscriber's stream, so its actor ends as `Ended::Feed` |
| `Pool` | the caller waits on that member's own mailbox | the pool holds **no** queue at all |
| `every` | the next run is late | a run is never re-entered and a missed tick is never queued |
| `Names` | nothing waits | a map and a lock; no channel exists |

**There is no third policy on a `Topic`, and the missing one is the one every
fan-out reaches for**: keep the item for later. It is refused by name, in the
module doc, so that the next author meets the argument rather than the gap.

`IN_FLIGHT` is a `const` and not an argument. That is the load-bearing half: a
per-subscriber depth that a caller supplies is an unbounded queue with an extra
parameter, and it arrives disguised as symmetry with Wave 4a's mailbox depth. If
a depth is ever wanted here it must be bounded by the same constant the mailbox
is, and a subscriber at its bound must still hit one of the two policies rather
than a third that grows. Nothing in `compose` reads `MAILBOX` or depends on its
value, so Wave 4a lands underneath this without touching it.

Both policies are proven, and by the same break: folding `IN_FLIGHT` from 1 to 8
turns **both** the `DropSubscriber` and the `BlockPublisher` guard red, which is
the evidence that the bound is what they rest on rather than an accident of
scheduling.

### 8.2 A subscriber is a `feed`, and that is why this waited for Wave 3

`Topic::subscribe` hands back a `Subscription<T>`, which implements
`n0_future::Stream` and is therefore exactly what a generated `Feeds::new` takes.
Nothing about fan-out reaches a `service!` declaration: the actor declares
`feed Fills(fill: Fill)` as it would for any stream, and the topic is wired in at
`actor_wired` like any other.

The alternative was a fan-out task holding `Handle`s and calling `tell`. It is
worse for the reason §5.2 gives about feeds in general: a `tell` is somebody in
this process **choosing** fire-and-forget, and `notify` documents that nobody is
told. A pushed event is not a choice anybody made, and a `tell` would also
consume a mailbox permit, so a fan-out would compete with the actor's own callers
for the one slot.

`Subscription` is deliberately not `Clone`: two holders of one subscription would
each get *some* of the items, depending on who polled first. Two subscribers are
two `subscribe` calls.

### 8.3 The scheduler, and the two guards that were vacuous first

`after` and `every` are `n0_future::time::sleep` in a spawned task. There is no
`Instant`, no `Interval` and no clock read at all — a sleep needs a duration, not
a `now` — which is what keeps `std::time::Instant::now()`'s wasm panic
structurally unreachable rather than merely avoided. `rg 'std::time'` over the new
modules matches only prose.

`every` sleeps, runs, and sleeps again, so the period is a floor on the **gap**
between runs rather than a rate. `n0_future::time::interval`'s default
`MissedTickBehavior::Burst` is precisely the queue this refuses: it fires
repeatedly to catch up, and pointed at a mailbox of one that is a pile of tasks
all waiting for the same permit. The cost is stated where a reader meets it: a
slow work function **loses ticks silently**, because there is nobody to tell. It
is the one place in `compose` where something is dropped without a loud signal,
and the answer is that a tick which matters individually is a message and wants a
`Topic`.

**Two of this wave's guards were written with `tokio::time::advance` and both were
vacuous.** `advance` jumps the clock and yields once; a `yield_now` leaves the
test task immediately runnable, so the runtime never *parks*, the timer wheel is
never turned, and a spawned schedule's `sleep` does not fire at all. An `after`
with its delay replaced by `Duration::ZERO` passed, and an `every` that spawned a
run per period instead of awaiting it passed — the exact defect the second test
exists to catch. Both are now `tokio::time::sleep` in the test, which parks it, so
the paused clock auto-advances to the *earliest* pending deadline and a schedule
that should not have fired demonstrably has not. Both breaks are now red.

This is the twelfth guard in this project caught satisfiable without proving its
claim, and it is written down because the instrument was the fault rather than the
assertion: `advance` is the tool the brief named, and it is the wrong one for a
timer that belongs to another task.

**A dropped `Scheduled` cancels**, which is the opposite of `LocalActor` and of a
bare `JoinHandle`, both of which detach. A repeating task whose handle was lost
would run forever, hold whatever it captured — a `Handle` clone keeps an actor
alive — and be unreachable. `detach` is the one way to say "run without me" out
loud, and both constructors are `#[must_use]`.

### 8.4 `Pool` adds no queue, and says what round-robin does not do

`pick` returns `(Member, &H)`. Every wait after that is the ordinary mailbox
permit on one member, so a pool of N actors is N mailboxes and nothing else. A
pool that queued would be an admission point in front of an admission point: work
accepted by something that cannot run it.

`from_fn(NonZeroUsize, impl FnMut(Member) -> H)` is the only constructor, and both
halves are decisions. `NonZeroUsize` makes an empty pool unrepresentable rather
than a `Result` nobody can act on. The factory rather than a `Vec` is the sharper
one: a pool's members are interchangeable *by definition*, so a constructor taking
a heterogeneous `Vec` would accept members that are not — which is a name registry
wearing a pool's clothes, and `Names` is that.

The limitation is named rather than hidden behind a claim of fairness: **this is
fair in dispatch and not in service time.** `pick` can hand back a member that is
mid-turn while a sibling is idle. Least-loaded needs to read how many permits each
mailbox has free and a `Handle` exposes none, so a `try_pick` would need a
`try_reserve` that does not exist. With a mailbox of one the two coincide more
often than they look like they would — a member is either running a turn or has a
free permit — and a deeper mailbox is exactly what would separate them.

"Reports which handled each" is checked rather than asserted: the factory is
handed the `Member`, so the actor knows its own number, and the guard compares the
pool's claim against the number the actor that answered reported. A rotation that
reported one member and dispatched to another fails there and would pass a cursor
assertion.

### 8.5 `Names`, and the lifecycle rule it changes

The key is `ActorId` and not a new newtype: a local actor's name and a keyed
actor's id are the same kind of thing, bounded the same way, and a name that works
here works when the same actor is later mounted.

`register(name, handle, ended)` takes the ending future as an **argument** rather
than asking the handle for one. That is `Subscribers::insert`'s shape and it is
kept for a second reason here: §5.7's missing keyed `ended()` channel means `H`
cannot be required to have one, and the day `Mounted` grows that field this takes
it with no change.

**The rule this type changes, and it is the thing to know:** the registry holds a
handle, so a registered actor no longer ends when the caller's last handle drops.
`stop`, `abort`, a panicking turn, an observer refusing and a feed ending all
still end it and all still unregister it; "went out of scope" does not. That is
not a defect to be fixed by holding something weaker — a registry of dead names is
not one — and it is pinned by a guard rather than left in prose.

Re-registering a live name **replaces** and hands back what it displaced. Refusing
was the alternative and it is worse: a name is freed by the eviction task
asynchronously, so a registry that refused a taken name would refuse a restart
nondeterministically, the same restart succeeding or failing depending on whether
a task had been scheduled. The generation stamp is what makes replacement safe,
and it is `Subscribers`' argument exactly: without it, the first incarnation's
eviction removes the second one's entry, because both are filed under one
`ActorId`.

**Debt, recorded rather than left to be noticed.** `Names` and `Subscribers` are
one generic registry — a keyed map with a generation stamp and a future-driven
eviction — differing only in the key (`ActorId` against `EndpointId`) and the
trigger (`Ended` against `Departure`). The merge was not taken in this wave
because `subscribers.rs` is a file a parallel wave owns, and a duplicated
*correctness* argument is the worst kind of duplication. It is the first thing to
do to this module.

### 8.6 Handle ergonomics: already here, and no alias was added

The brief asked for `is_alive`, `stop_gracefully` and `wait_for_shutdown`. All
three exist under keylink's names, and adding a second spelling for each would be
"one way to build a thing" broken for nothing:

| asked for | what it is here |
|---|---|
| `is_alive` | `Handle::is_running`, documented as advisory |
| `stop_gracefully` | `Handle::stop` — ordered, drains what was queued, waits on the `ended` channel |
| `wait_for_shutdown` | `Handle::ended` — and it answers *why*, which a `wait_for_shutdown` returning `()` cannot |

What is genuinely absent is on the keyed path and is still §5.7's: a keyed actor
has no per-actor `ended()`. This wave does not add one; it is arranged so that the
day it exists, `Names` takes it unchanged.

### 8.7 What Wave 7 did NOT do

- **Move the ALPN, or touch the wire at all.** `keylink/Probe/d3fe34471921164e` is
  unchanged and all 25 tests in `tests/protocol.rs` are green. Stronger than the
  test: `rg 'iroh|ALPN|Envelope|WireKinds|postcard'` over `keylink/src/compose/`
  matches **no code line**, only two sentences of prose, and `macros.rs` is not in
  this wave's diff — so there is no wire generator that could have seen any of
  this.
- **Touch `macros.rs`, `local.rs`, `registry.rs` or `remote.rs`.** Zero lines. The
  only existing files changed are `lib.rs` (one `pub mod`, one crate-doc bullet)
  and `representability.rs` (two probes). A pool of generated handles is generic
  over the handle for exactly this reason: generating pool methods would have
  meant a thirteenth per-kind arm.
- **Give `Topic` a history, a replay or a durable queue.** An item published before
  a subscriber joined is gone, and a subscriber that joins during a `publish` is
  not delivered to. Pinned, because "a topic is not a queue" is the sentence a
  reader most wants to disbelieve.
- **Make `publish` atomic.** Under `BlockPublisher` it delivers sequentially and
  dropping its future leaves the item delivered to the subscribers already reached
  and to no others. Sequential rather than a `join_all` is what makes that subset a
  *prefix* rather than arbitrary. There is no rollback, because a delivered item
  cannot be recalled.
- **Detect an actor publishing to a topic it subscribes to.** Under
  `BlockPublisher` that deadlocks, and it is `Handle`'s self-call deadlock in a
  second costume: the actor is inside a turn, so it is not polling its own feed, so
  the permit it waits for can only be freed by the loop it is blocking. No type can
  tell one subscriber's channel from another's. Documented on `publish`.
- **Supervise anything.** `Pool` does not restart a member, notice one ending or
  take one out of the rotation; `pick` keeps handing back a stopped actor's handle
  and the call answers `MailboxClosed`. `Names` unregisters on ending and does not
  re-spawn. Both were Wave 9's. **Wave 9 has landed, and the answer turned out to
  be composition rather than a change here:** a member built with the generated
  `supervised()` restarts itself, and because a supervised actor's mailbox outlives
  its incarnations, `pick` keeps working across a restart with nothing in `compose`
  knowing. `compose` still holds no supervision mechanism of its own, and that is
  the design rather than an omission.
- **Red-test the shared-rotation claim on its own.**
  `two_clones_of_a_pool_share_one_rotation` was watched to fail, but only under the
  break that stops the cursor advancing at all — which also fails its sibling. A
  break isolating *sharing* would have to move `at` out of `Inner`, which is a
  restructure rather than a literal, so `break-once.sh` cannot express it. Said
  here rather than counted as a proven guard.
- **Red-test "a topic keeps no history".** It is an assertion about behaviour with
  no mechanism to break — the buffer it denies does not exist — so there is no
  one-literal edit that turns it red. It is a regression pin, not a guard.
- **Reach a topic from a peer.** A `Subscription` is local in the way a `Handle`
  is. A remote fan-out is `Subscribers` plus a reverse contract, which
  `subscribers.rs` already documents and which is a different thing wearing a
  similar name.

### 8.8 C-14 and C-15 — two corrections to issue #3 that were found and never written down

`.tmp/spec-corrections.md` runs C-1 to C-13 and stops. The invariant pass raised two more,
both graded Critical, and neither reached any tracked file — so the tree's departures from
the spec were recorded everywhere except where a reader looks. They are numbered here,
continuing that list, and this file is their durable home because `.tmp/` is not tracked.

C-15's subject belongs to §4 (Wave 2c) rather than to this section. It is written here
anyway, so that the numbering stays in one place and a reader finds both at once.

#### C-14 — the scheduler is two free functions and a handle, against a refused row that says "as an actor"

**Issue #3's refused-designs table** carries a row for `Timer` whose resolution clause
reads *"Scheduling arrives in Wave 7 as an actor, not as a core type."* What landed in
`keylink/src/compose/schedule.rs` is `after` and `every` — two free functions returning
`Scheduled`, a newtype over a `JoinHandle` with a `detached` flag. Not an actor.

**The "as an actor" departure was recorded nowhere until now.**
`rg -in 'as an actor|core type' keylink/src/compose/schedule.rs` → exit 1. The command names
the clause rather than the row, because the module doc *does* engage the row — its other
clause, the one about `cancel`, below — and never this one. §8.3 above discusses the vacuous
guards and "a dropped `Scheduled` cancels" without engaging either.

**And the departure is right, which is why this is a correction and not a defect.** The row
has two clauses and they are not the same claim. "Not a core type" is satisfied: nothing in
`schedule.rs` is reachable from `keylink`'s root, it reads nothing of the runtime, and
`local.rs`, `registry.rs` and `macros.rs` are untouched by it. "As an actor" is the clause
that does not survive contact:

- **An actor scheduler is an admission point in front of the work.** This crate's mailbox
  is one message deep, deliberately. A single scheduler actor holding N schedules would
  serialise every fire through that one slot, so a slow job would delay an unrelated one —
  the exact failure `Pool` is documented as refusing to build.
- **Its message could not be typed.** A scheduler actor has to name the work it fires. The
  module doc already argues this: `(handle, message)` needs one signature per method kind —
  a `tell` has no reply, an `rpc` has one nobody awaits, a `blob` has a body — so it would
  belong in the generated `Handle` rather than in a composition. A closure returning a
  future is what a caller already has.
- **Cancellation would get worse, not better.** Dropping a `Scheduled` cancels. Through an
  actor, cancelling means addressing a schedule by an id the actor minted, which is a second
  identity model beside `ActorId` for something that is not addressable in the first place.

**What the shape costs, stated rather than hidden:** a schedule has no identity, so it
cannot be listed by `System::status`, reached by a peer, or supervised. A process that wants
those has an actor with a `feed`, not a schedule.

**One thing from the same row WAS re-derived and is now deleted.** `Scheduled::cancel`
shipped — *"The same thing `drop` does"*, by its own doc, with zero call sites repo-wide —
which is the row's other clause, *"`Timer::cancel` beside 'dropping cancels it' is two ways
to do one thing"*, rebuilt verbatim. It is gone, and `schedule.rs`'s module doc now carries
the argument so it is not inherited by omission a third time.

#### C-15 — Wave 2c's central guard is a positive doctest, not the `compile_fail` the criterion named

**Issue #3's Wave 2c criterion** puts the safety claim in a `compile_fail` probe in
`representability.rs`.

**What landed** is a **positive** doctest — the first fence under *"The guard, and exactly
which half of it each piece holds"* in `Replied`'s rustdoc, `keylink/src/local.rs`: a handler
written the way `#[defer]` means it to be written, which fails if the *generated signature*
stops demanding a token. Red-tested — with the arm's `Output` changed to `()` it goes red
with `E0271`. Cited by item and heading rather than by line, because a line range is wrong
the first time anything is inserted above it — and this one already was.

**The `compile_fail` twin, the second fence in that same section, is kept and demoted in
place.** Under the same break it stayed **green**: the impl mismatched the trait's `Output`
for an unrelated reason, so the fence went on passing while the obligation it claims to pin
was gone. The doc says so, in those words, rather than keeping two fences and believing both.

**This is C-4 applied a second time, not a new decision.** C-4 established that rustdoc
ignores the error code on a `compile_fail` fence, so such a fence proves only "this does
not compile" — which is why the repo-wide rule is no error code on any fence, and why the
load-bearing probe is positive. A criterion that asks for a `compile_fail` is asking for
the weaker of the two instruments.

**Why it needed writing down:** the wave satisfied its criterion's *intent* more strongly
than its *letter*, and a later reader grading the tree against issue #3 finds a
`representability.rs` probe missing and no explanation. That reader would either re-add the
weak fence or record a false FAIL.

---

## 9. Wave 9 — supervision, phases 1 and 2

**The supervisor is an outer loop on the actor's own task.** Not a type, not a
tree, not a second task spawning children: `LocalActor::spawn_supervised` wraps
the loop that already exists, owns the mailbox for the actor's whole life, and
lends it to one incarnation at a time. The whole of the new public surface is
`Intensity`, `Incarnation<A, F>`, `Stop::fatal`, `Stop::is_fatal`,
`Ended::GaveUp`, one `fatal` field on `Ended::Observer`, `Display for Ended`, and
two generated doors — `supervised` and `spawn_supervised`.

`registry.rs` is untouched — **zero lines**, verified by `git diff --numstat`.
`mount` takes a closure over a `Mailbox<M>`, and the
supervisor is that closure's whole body, so `System::mount`, eviction, `shutdown`
and `System::status` see one task exactly as they do for an unsupervised keyed
actor.

### 9.1 THE DECISION THIS WAVE OWNS — the sliding window the design asked for cannot terminate, and the replacement terminates only *conditionally* (§9.1.1)

Issue #3 specifies two things that do not compose: an OTP-shaped `Intensity`
(*`max` restarts within `within`*) and a backoff of **`within / max`**. Spending
the budget therefore takes exactly `within` — and against a sliding window of the
same length, the oldest restart falls out of the window at the instant the newest
one is granted. Worked through with `max = 4`, `within = 8s`, `backoff = 2s`:

| restart | at | restarts in the last 8s | granted? |
|---|---|---|---|
| 1 | 0s | 0 | yes |
| 2 | 2s | 1 | yes |
| 3 | 4s | 2 | yes |
| 4 | 6s | 3 | yes |
| 5 | 8s | **3** — the one at 0s has just aged out | yes |
| … | … | 3, for ever | yes |

The steady state is exactly `max` restarts per `within`, for ever, and
`Ended::GaveUp` is unreachable. An actor faulting on every turn would restart
until the process died, which is the hot loop the backoff exists to prevent,
arrived at by the mechanism that was supposed to prevent it.

So the second number means something else here, and it is **named** for what it
means rather than documented into it: `Intensity::new(max, recovered_after)` is
*`max` restarts in a row, and the count resets when one incarnation runs for
`recovered_after` without ending*. `Intensity::new(3, 10s)` reads *three restarts
in a row, and an actor that stayed up ten seconds has recovered*. The backoff is
unchanged, `a_restart_storm_ends_the_actor` terminates at `after: max`, and
`the_backoff_spreads_the_budget_across_its_window` still measures a full
`recovered_after` of virtual time.

The alternative — keeping the sliding window and pruning with `>` instead of
`>=` — was considered and refused. It terminates only when the elapsed time is
*exactly* the window, which is true of a paused clock and false of a real one, so
it would have been a guard that passes in the test suite and fails in production.

#### 9.1.1 CORRECTION — the replacement terminates *conditionally*, and this section said it terminated

The heading above and the paragraph before this one were written as an
unconditional claim, and they are not one. **Review found the defect §9.1 exists
to have killed, reached from the replacement, with a compiled counter-example:**
an `Intensity::new(2, 10ms)` whose every incarnation stays alive 50 ms and *then*
fails built **nine** incarnations on a budget of two and was still running.

The mechanism is `Budget::spend`'s reset, read against a *failing* incarnation:

```rust
if ran_for >= self.limit.recovered_after { self.used = 0; }
```

`ran_for` is how long the incarnation lasted, not how healthy it was, so an
incarnation that reaches `recovered_after` and then faults resets the count on
its way out. An actor whose failures are reliably slower than its
`recovered_after` therefore never exhausts a budget and `Ended::GaveUp` is
unreachable **for that actor** — which is §9.1's own defect, arrived at from the
other side.

**The policy is kept and the claim is narrowed, because the policy is right.**
OTP's window has exactly this property, and an actor that stays up for the
declared span between failures is a slow leak rather than a restart storm:
giving up on it discards live state to fix nothing. So *recovered* means what it
says, `Intensity` carries the precondition in prose —
**`recovered_after` must outlast a failing incarnation, or `GaveUp` is
unreachable** — and `a_failure_slower_than_its_recovery_span_never_gives_up`
asserts the behaviour rather than leaving a later reader to find it. What the
caller must take from it is that the number is chosen against an actor's
**failure period**, not against its latency.

**One case is not a precondition at all, and it is rejected rather than
documented.** `Intensity::new(max, Duration::ZERO)` was constructible from safe
public API, and `ran_for >= Duration::ZERO` is true of *every* incarnation that
ever ran — so the reset fires on every failure for every actor, with nothing a
caller could do to make the precondition hold, and `backoff()` is `ZERO` too: an
unbounded restart loop with no sleep in it. `new` now returns `Option<Self>`, and
the guard is on the **backoff** rather than on the span, because
`recovered_after / max` truncates to nanoseconds and
`Intensity::new(4, Duration::from_nanos(3))` is the same busy loop spelled
differently. The property that buys is worth stating plainly: **for any
`Intensity` that exists, `backoff()` is non-zero, so a supervisor always sleeps
between incarnations.** `an_intensity_whose_backoff_would_be_zero_is_refused`
is the guard; breaking it to `if false` was watched red.

**And the rejection must not be read as closing more than it does — asked
directly, answered directly.** It closes two cases exactly and narrows a third
without closing it:

| case | closed? | why |
|---|---|---|
| `recovered_after == 0` | **yes, exactly** | `ran_for >= ZERO` holds for every incarnation that ever ran, so the reset is unconditional for **every** actor. It is the only value of which that is formally true. |
| backoff truncates to zero (`recovered_after < max` ns) | **yes** | integer division on nanoseconds; a restart loop with no `sleep` in it. This is why the guard is on the backoff and not on the span. |
| `recovered_after` shorter than a *failing* incarnation's life | **no, and not closable here** | `Intensity::new(4, Duration::from_nanos(4))` is accepted — backoff 1 ns — and no real incarnation ends in four nanoseconds, so its budget resets on every failure exactly as `ZERO`'s would. |

The third differs from the first two **in kind and not in degree**: where that
line falls is a fact about the caller's actor, not about the value, so no
constructor can see it. So the honest summary is that **the guard bounds the
sleep, not the hazard** — it removes the spin and the one unconditional value,
and the documented precondition carries everything else. That is stated on
`Intensity` itself rather than left to be inferred from the fact that `new`
returns an `Option`, because a constructor that refuses *something* invites the
reading that everything it accepts is safe.

`Option` and not a panic: the same shape as `NonZeroU32::new`, one argument
along, so both of `Intensity`'s numbers are checked where the value is built.
That the wave reasoned the `max` half through — *"a budget of zero is `spawn_fed`
spelled through a supervisor that does nothing"* — and did not apply the same
reasoning to the duration is the honest summary of what this correction is.

### 9.2 The four properties that fall out with no mechanism

They are worth naming because each is a mechanism this design did **not** have to
build:

- **`task: Arc<JoinHandle<()>>` and `abort()` are unchanged.** The task is the
  supervisor's, so `abort` destroys the actor rather than one incarnation.
  `abort_destroys_a_supervised_actor_rather_than_one_incarnation`.
- **The mailbox outlives the incarnation for free.** No re-queue, no
  `push_front`. `run_loop` drops a `Lent` — a borrow — and closes nothing;
  messages queued during a restart are handled by the next incarnation.
- **`ended()` *cannot* fire on a restart.** Only the task's exit publishes
  `Life::Ended`, and a restart is not an exit. `a_restart_is_not_an_ending`.
- **`is_running()` is true during one**, which is the truth.

And the reason this is not OTP: `one_for_all` exists because a crashed Erlang
process leaves its peers holding a **dead pid**. Here identity does not die with
the incarnation — the outer loop keeps the `mpsc::Sender` — so **there is no
stale reference to repair**. `a_handle_survives_its_actors_restart` is that
sentence as a test. What remains is *state* coupling, and a group restart is both
too broad (it discards a healthy actor's live state) and too narrow (it cannot
say *what* went stale) to fix it. That is a message, not a strategy.

### 9.3 `Stop::fatal` is an inherent function because of coherence — and the flag is a `bool` for a different reason

`Stop` has a blanket `impl<E: Error + Send + Sync + 'static> From<E>`, which makes
**every** other `From` impl an `error[E0119]`. So a `Fatal<E>` wrapper a caller
could convert from cannot exist, and `fatal` is an inherent associated function
with `new`'s exact `impl Into<Box<dyn Error + Send + Sync>>` shape. The
discriminator therefore travels beside the error rather than inside its type,
which is what `into_reason` becoming `into_parts` is: a name that went on
promising one value while handing back two is how a caller drops the second.

**Corrected after review — the heading of this section used to end at
"because of coherence", and the sentence under it read "the discriminator then
has nowhere to live but a second field on the tuple struct".** That is one step
further than coherence reaches. What `E0119` forbids is the *convertible
wrapper*; it says nothing about how the discriminator is spelled once it is a
field. A `pub(crate) enum Severity { Retryable, Fatal }` as `Stop`'s second field
has no coherence problem at all — nothing converts *from* it — and compiles.

It stays a `bool` for a weaker, ordinary reason, and the weak reason is worth
writing down precisely because it is weak: **the two-state fact is already on the
public surface as a `bool`, twice** — `Stop::is_fatal(&self) -> bool` and
`Ended::Observer.fatal`, the latter argued separately and soundly. `into_parts`
is `pub(crate)` with exactly two callers, both in `local.rs`, so an enum would
buy a self-describing return at two sites and be a private synonym for a shape
the public API spells as a `bool` regardless. **That is a judgement, not a
constraint, and the type is unchanged — only the claim is.**

*(The first draft of this correction said "`into_parts` is public, so a private
enum could not appear in its return type". It is `pub(crate)`. The docs gate
caught it as a private-intra-doc-link error, which is the second time in this
one section that an argument was stated more strongly than it holds. Recorded,
because that is the pattern worth noticing rather than the fact.)*

**A fatal refusal of a turn that *panicked* is not honoured, and that is
deliberate.** The loop reports a refused fault as `Ended::Panicked` naming the
handler rather than the observer — a durability adapter refuses every panicked
turn, and reporting the adapter points an operator at the wrong code. That ending
carries no observer and therefore no flag. The rule is on `Stop::fatal`'s own
documentation: the reason the actor is ending is the fault, and a fault is the
kind of thing another incarnation might not repeat.

**That rule now has a test, which is the second thing review found here.** It was
a deliberate, documented contract held by prose in three places and by nothing
executable, so a later change to `retryable` or to `local.rs`'s `(refused,
panicked)` match would have broken it silently.
`a_fatal_refusal_of_a_panicked_turn_is_not_honoured` drives an observer that
returns `Stop::fatal` from `after_turn` on a turn that panicked and asserts the
actor is **rebuilt anyway**. The residual risk the review graded *defensible*
stands and is unchanged: the primary `fatal` case — a claim another machine
holds, a snapshot from a newer build — is detected at `Persist::open` in the
**factory**, and `supervise`'s `if !fatal && budget.spend(..)` does honour it
there.

### 9.4 `Ended::GaveUp` was predicted to cost one `E0004`, and it cost exactly one

§5.1 decided `Ended` stays exhaustive, measured the churn of adding `Feed` at
**zero**, and predicted that Wave 9's `GaveUp` would land the same way with
`representability.rs::every_ending_is_named` as the one `E0004`. Measured, by
deleting that arm and building `--all-features --all-targets`:

```
$ cargo check -p keylink --all-features --all-targets 2>&1 | grep -c 'error\[E0004\]'
1
error[E0004]: non-exhaustive patterns: `&observe::Ended::GaveUp { .. }` not covered
   --> keylink/src/representability.rs:814:11
```

`#[non_exhaustive]` was not reached for. The `fatal` field on `Ended::Observer`
did cost churn — five `error[E0027]`s in `keylink/tests/observe.rs`, all fixed
with `..` — which is the honest price of a struct variant gaining a field and is
recorded rather than buried.

### 9.5 The re-acquire window, bounded three ways

A durable `Claim` **cannot** outlive an incarnation: `observe.rs` states that it
dies with the actor deliberately, and reaching a shared one needs an async mutex
the port rejects by name, because `Claim` is `!Sync` and `store` takes
`&mut self`. So supervision has to release and re-acquire, and the window is
bounded three ways:

1. **The old claim is dropped before the next incarnation is built.**
   `run_loop`'s teardown drops `hooks` before it returns, and the factory is
   called after it returns, so the process never races itself for its own claim.
   `an_observer_is_rebuilt_for_each_incarnation` asserts the *order* —
   `built 0, dropped 0, built 1, …` — not a count.
2. **A failed re-acquire counts against the budget.** The factory is `async` and
   fallible for exactly this reason, and a build that produced no actor spends
   one restart with `Duration::ZERO` on the clock. A state another machine has
   taken therefore ends `Ended::GaveUp` rather than looping.
3. **The window is `backoff + claim latency`**, and the backoff is `within / max`.

That the factory is `async` has a second consequence worth stating: the keyed
`spawn_supervised` is the first keyed door a durable actor can use **lazily**.
`SystemBuilder::spawn_with`'s factory is a plain `Fn` and cannot `await`, which is
why §6.6's keyed durable actor is mounted eagerly after `Persist::open(..).await`.

### 9.6 The served path: `"restarting: "` is a defect fix, not decoration

The one message a restart genuinely loses is a `Deferred` parked in the state,
because the state is dropped when the incarnation ends. Every other message
survives: a turn in flight is always answered before the loop breaks, and queued
turns stay in the mailbox the supervisor owns.

Before this wave that lost reply was rendered `RemoteError::NoSuchActor` —
*there is nothing here, stop asking* — which is true of an unsupervised actor and
a **lie** about one that is mid-restart and about to answer the same peer again.
So `deliver_to_local` reads the supervisor's decision and renders
`Failed("restarting: {reason}")`. A prefixed string and not a new variant, so the
ALPN does not move; `keylink/tests/protocol.rs` still pins
`keylink/Probe/d3fe34471921164e`.

**The ordering is the whole of it.** The decision is made and published in
`Inbox::ending`, which `run_loop` calls between the loop's exit and `finish` —
*before* anything is dropped. Deciding after `run_loop` returned would be a race
against the very answer channel it is about, and the peer would read
`NoSuchActor` for a restarting actor about half the time. That is why the hook is
on `Inbox` — the trait through which something outside the loop already owns the
mailbox and already learns what the loop is doing, via `mark_busy` — rather than
a parameter two of three callers would pass `|_| {}` to.

Three prefixes now, and none is a prefix of another: `"panicked: "` (your message
exploded, the actor kept its state), `"observer refused: "` (the turn ran and was
not committed), `"restarting: "` (the incarnation that owed you an answer is
gone). `a_remote_caller_can_tell_restarting_from_gone` asserts all three
distinctions on one QUIC connection between two real nodes.

### 9.7 Phase 3 — REFUSED, and its trigger rewritten

Under supervision a faulted turn's idempotency key still settles `Interrupted`, a
permanent refusal. Releasing it — via a fourth `Reached` state and a per-mount
`AfterFault { Retryable, Unknown }` declaration — is **refused, and nothing of it
is built.**

The reason stands on its own merits and is not documentation: *the premise "the
restart rolled it back, so we know it did not land" is true of keylink's state
and not of the world.* A turn that charges a card over HTTP and then panics
leaves the card charged; the restart rolls back the in-memory struct and nothing
else. Releasing that key lets a retry charge it twice, which is the exact
double-effect the ledger exists to prevent. Phases 1–2 need none of it, so
supervision ships with **no new failure mode**.

**Its trigger is rewritten, and this is the one thing this wave changes about the
refusal.** It used to read *a service exists that (a) uses idempotency keys, (b)
faults in practice, and (c) whose author can state its turns have no effects
outside the actor's state* — a declaration, which is the shape that killed
`Restartable`. It now reads:

> **Journaled side effects land.**

Durable *execution*, as against the durable *state* keylink now has: an effect
recorded as completed before the fault is not re-issued on replay, so the
premise above stops being false at the root rather than being asserted around.
That converts a permanent refusal into a sequenced one. It is still per-method
and not per-mount when it arrives — Wave 2c's `#[defer]` established the
attribute grammar — and the arm easiest to miss is still that a *panicking* turn
is answered **and** faults the incarnation, so a generated panic arm would have to
release its key too.

### 9.8 Refused in this wave, with the reason, and one caveat recorded

`Supervisor`, `Strategy`, `OneForAll`/`RestForOne`, parent/child trees,
`Watching { PerIncarnation, Outliving }`, a fault-injection seam, and an
incarnation counter. §9.2 carries the argument for the first four;
`Watching`'s only candidate consumer was the durable `Claim`, and it cannot
outlive an incarnation; a fault-injection seam is unnecessary because the factory
is a closure, an `Observer` is a user impl, and a panicking turn is one line of
test. `LocalActor::incarnations()` does not exist, was confirmed absent
repo-wide, and none was added.

**One caveat the owner recorded, and it is the next question rather than this
wave's.** The dead-pid argument defeats sibling-*restart* strategies and does not
touch **ordered shutdown, escalation, or lifecycle grouping**. Those are judged
wanted. They are not built here, and nothing in this wave forecloses them: a
group that shuts down in an order is a statement about several actors, and
supervision as built is a statement about one.

### 9.9 Every criterion, and the break that proves it

Fifteen tests in `keylink/tests/supervision.rs`, every one of them watched to
fail under a break. The twelve below are the wave's own; §9.1.1's two guards and
§9.3's `a_fatal_refusal_of_a_panicked_turn_is_not_honoured` are the three review
added. **Fourteen of the fifteen breaks are one literal; the fifteenth is two
adjacent lines**, because honouring a flag the loop drops means rebinding the
match arm's pattern and its body together, and splitting that into one line does
not compile.

**Corrected after review, because the first version of this sentence and the
table under it disagreed.** It read *"Eleven were watched to fail under a
one-literal break; the twelfth is recorded below as **not** proven by a break"*,
and then gave the twelfth a break anyway. The table is the accurate half and the
sentence was stale: `.tmp/wave-9/red-tests.sh` runs twelve breaks, all twelve
exited 101, and the commit message's *"all twelve exited 101"* is right. Verified
by re-running break 3 on this branch — it exits 101.

Two facts the first version left out, and both are about how much the twelve
prove:

- **The twelve breaks touch eight distinct source literals.** `retryable`'s
  `Panicked | Feed => true` line carries three of them, its
  `Observer { fatal, .. } => !fatal` line two (broken to `true` and to `false`),
  and `Budget::spend`'s `max.get()` two. A criterion whose break shares a literal
  with another's is evidenced against the same line of code, not against its own.
- **Break 3 does not isolate the claim its test is named for.**
  `a_supervised_actor_that_gave_up_answers_stop` opens by asserting the actor
  gave up at all, so a budget that never refuses fails it at that *first*
  assertion — `supervision.rs:349`, the bounded `ended()` helper — and never
  reaches the `stop()` timeout the test exists for. Re-run and read: the red is
  real, and it is a red about giving up rather than about `stop()`. No break in
  this file isolates the `stop()` claim, and §9.9's own note explains why the
  obvious one cannot: once the supervisor's task exits the `watch` sender drops
  and `wait_for` returns `Err` whatever the predicate says.

| criterion | break |
|---|---|
| `an_actor_restarts_from_its_last_committed_state` | `retryable`: `Panicked \| Feed => false` |
| `a_restart_storm_ends_the_actor` | `Budget::spend`: `max.get()` → `max.get() + 1` |
| `a_restart_is_not_an_ending` | `Lent::ending` publishes `Life::Ended` |
| `the_backoff_spreads_the_budget_across_its_window` | `Intensity::backoff` → `Duration::ZERO` |
| `a_fatal_stop_skips_the_budget` | `retryable`: `Observer { fatal } => true` |
| `an_observer_is_rebuilt_for_each_incarnation` | `run_loop`: `drop(hooks)` → `mem::forget(hooks)` |
| `a_feed_is_resubscribed_on_each_incarnation` | `retryable`: `Panicked \| Feed => false` |
| `abort_destroys_a_supervised_actor…` | `abort`: `self.task.abort()` → a no-op |
| `a_handle_survives_its_actors_restart` | `retryable`: `Observer { fatal } => false` |
| `a_remote_caller_can_tell_restarting_from_gone` | the `"restarting: "` prefix → `"panicked: "` |
| `a_keyed_actor_restarts_from_its_last_committed_state` | `retryable`: `Panicked \| Feed => false` |
| `a_supervised_actor_that_gave_up_answers_stop` | `Budget::spend` never refuses, so the supervisor never exits |
| `an_intensity_whose_backoff_would_be_zero_is_refused` | `Intensity::new`: `if limit.backoff().is_zero()` → `if false` |
| `a_failure_slower_than_its_recovery_span_never_gives_up` | `Budget::spend`: `if ran_for >= self.limit.recovered_after` → `if false` |
| `a_fatal_refusal_of_a_panicked_turn_is_not_honoured` | `run_loop`'s `(refused, panicked)` match: the `Some(..), Some(message)` arm rebound to `Ended::Observer { name, error, fatal }` — i.e. the flag honoured |

Plus one compile-time probe: `Intensity` in `representability.rs`'s
`HasNoDefault`, which a `#[derive(Default)]` or a hand-written impl trips.

**Two of those breaks were themselves found to be defective, and the fix is a
rule.** The first attempt at `a_supervised_actor_that_gave_up_answers_stop`
broke `stop()`'s wait predicate and came back **green**: once the supervisor's
task exits, the `watch` sender drops and `wait_for` returns `Err` whatever the
predicate says, so `stop()` cannot hang for an actor that ended. The only way it
can go wrong is a supervisor that never exits — which is the break above. And
the first attempt at `a_fatal_stop_skips_the_budget` *hung* rather than failed,
because an unbounded `ended().await` on an actor that no longer ends is a hang,
and a hang is not a red a harness can read. So **every `ended()` in
`tests/supervision.rs` goes through one bounded helper**, and the two-line
version of the lesson is: a guard's break must produce a *failure*, and an
unbounded wait converts every interesting break into silence.

### 9.10 What Wave 9 did NOT do

- **Touch `registry.rs`.** Zero lines, and `rg -i 'supervis' keylink/src/actor/registry.rs`
  returns nothing, so there was no stale claim to correct either. Every
  registry-visible fact about a supervised keyed actor — one task, one mailbox,
  one entry — is unchanged, because `mount` already took a closure.
- **Give a keyed actor an `ended()` channel.** Still §5.7's gap. A supervised
  keyed actor's `Ended::GaveUp` reaches its own observers' `finish` and nothing a
  caller can await, and `run_keyed_supervised` allocates a `Life` channel with no
  reader to keep one code path rather than two.
- **Render `"restarting: "` on the *keyed* served path.** `deliver_to_actor` has
  no supervisor-visible signal to read, because a keyed actor has no `Life`
  channel anyone reads. A `Deferred` lost to a keyed restart is still reported
  `NoSuchActor`. This is the one asymmetry the wave leaves, and it is a
  consequence of the previous bullet.

  **Citation corrected: `deliver_to_actor` is `keylink/src/actor/remote.rs:419`,
  and it has never been in `registry.rs`.** `rg -n 'deliver_to_actor'
  keylink/src/actor/registry.rs` returns nothing. The wrong location was written
  here and in `.tmp/wave-9/commit.txt`, and from there it was copied into the
  wave's review and into the brief that commissioned this fix — four places, each
  taking it from the last without checking. **The commit message is immutable and
  still says `registry.rs`; this line is the correction, and it is the one to
  believe.** The zero-lines claim about `registry.rs` above is unaffected and
  still verified — `remote.rs` is zero lines this wave too.

  **Corrected after review: it is a *new* wrong answer, not a pre-existing gap,
  and stating it here was not enough.** Before this wave keyed actors did not
  restart, so the case could not arise; `NoSuchActor` on this path now says
  *there is nothing here, stop asking* about an actor that is mid-restart and
  about to answer the same peer again, and a client that treats the name as
  terminal — which Wave 8's reliable-delivery recipe encourages — stops retrying
  an actor that is coming back. The sentence now sits on the public
  documentation of the door a user calls: the generated keyed
  `spawn_supervised` and `run_keyed_supervised`, not only in this RFC.

  **The proper fix is declined out loud, and it is not one paragraph.** Traced
  rather than estimated: `run_keyed_supervised` builds the `Life` channel
  *inside* the closure `mount` was already handed, in the user's crate, so
  nothing outside that closure has ever seen it. Reaching it from
  `deliver_to_actor` means `Life` stops being private to `local.rs`; `Mounted<M>`
  carries a receiver beside its `Loop<M>`; `mount` grows a second door to build
  that variant, or every keyed actor pays a `watch` channel it has no reader for;
  `Registry`'s `Entry` stores it; `System::route` hands back a pair instead of a
  mailbox; and only then does the served path have something to match on. Six
  touch points across three files, one of them `Mounted` — a public type — and
  the `Option`-shaped version of it is the *"second construction path that
  quietly does less"* the house rules ban by name.

  It is also the same seam §5.7's keyed-`ended()` gap wants, and doing it here
  would re-shape that seam twice for the smaller half of the problem. So: the
  trade is `registry.rs`'s *zero lines* against a wrong answer to a peer, the
  wrong answer is documented on the door instead, and this is recorded as the
  next question rather than left reading like an oversight.
- **Build any of §9.8's refused designs**, or phase 3 in any part.
- **Add a knob for the backoff**, an incarnation counter, or a way to ask an actor
  how many times it has restarted. `Ended::GaveUp::after` is a number the
  supervisor counted on its way out, not a counter kept beside the actor.
- **Restart on `Ended::Stopped` or `Ended::Vanished`.** Somebody asked, or the
  last handle went; restarting would be overruling them.
- **Bound the *first* factory call.** A supervised actor whose factory fails
  immediately and fatally ends `GaveUp { after: 0 }` before any incarnation ran,
  which is honest but means `spawn_supervised` can hand back a handle to an actor
  that never existed. There is no `Result` on the call, because the failure is
  asynchronous and `ended()` is where it is reported.
- **Measure the per-actor cost of the second `watch` state.** `Life` replaced
  `Option<Ended>` in the channel that already existed, so no allocation was added
  on the local path; `tests/actor_memory.rs` was run and not re-baselined.

  **Corrected after review — "no allocation" was true and "nothing moved" was
  not.** It is still one `watch` channel, but `Life` is **48 bytes** against
  `Option<Ended>`'s **40**: `Ended` is 40 before and after this wave, so `GaveUp`
  and the `fatal` field cost *zero*, and the eight bytes are `Option` niche-packing
  into `Ended`'s spare discriminants where `Life`'s two payload-carrying variants
  need a tag of their own. Measured and not reasoned — a throwaway `size_of`
  probe in `local.rs`'s own test module, because `Life` is private:
  `Ended = 40`, `Option<Ended> = 40`, `Life = 48`.

  Re-baselined at ten thousand idle actors:

  | | Wave 3 recorded | measured now |
  |---|---|---|
  | keyed, mounted in a `System` | 1,415 | **1,412** |
  | local, `ledger::actor(..)` | 1,853 | **1,861** |

  **Local is +8, exactly the `Life`.** Keyed is untouched by this wave — an
  unsupervised keyed actor gets no `Life` at all — and its −3 is **not** Wave 9's:
  it is Waves 4 through 8 measured against a figure last written down at Wave 3,
  and it is recorded here rather than quietly overwritten, because a table nobody
  re-measures is exactly where a number like that hides. Nothing failed and
  nothing would have — `BUDGET_PER_LOCAL_ACTOR` is 2,048 — which is why *"the
  suite ran green"* was not evidence and re-measuring was.
- **Watch the `docs`, `features` or `wasm` gates fail on this diff.** They were
  run green; no red was manufactured for them.
## 10. Wave 6 — observability, and the layer that is a parked reply rather than a held handler

Three layers were briefed. Two shipped, and the third shipped as its client half
and no further; §10.8 says exactly what is missing and why that was the right cut.

### 10.1 THE DECISION THIS WAVE OWNS — `snapshot()` was refused, and the reason is in `status()`'s own docs

Issue #3 asks for `System::snapshot()`, "named `snapshot`, not `actors`, because
eviction runs on a timer and the list is stale the instant it returns". Wave 4d had
already shipped `System::status() -> Vec<ActorStatus>` with exactly the four fields
6a asks for. So the wave's first question was whether `snapshot` is a rename, an
addition, or nothing.

**It is nothing, and `snapshot` would have been the worse of the two names.**
`status()` samples each row as the iterator reaches it — a list of ten is ten
readings, not one instant — and 4d's own documentation says so, in the words *"why
`ActorStatus` says so out loud rather than pretending to a snapshot"*. A consistent
list would mean holding the write lock across every read. So a method called
`snapshot` would promise the one property this cannot have, while doing exactly
what `status` does. Adding it beside `status` would break "one way to build a
thing" twice over: a second name **and** a second, false claim.

What the issue actually asked for and 4d had not delivered is the **documentation**:
staleness stated at the method with eviction named as the cause. That is the change,
and it is a paragraph rather than a method. It says the part a caller can act on —
this list is evidence, never a precondition; never `status().find(..)` and then
address the result as if it were there; `idle_for` is the same reading
`Eviction::Idle::after` is compared against, so a row near the policy is a row whose
actor is about to go. It also says, in one sentence, that there is no `snapshot()`
beside it and why, so the next reader finds a decision rather than an omission.

`incarnations` stays absent, as the brief directed and for Wave 2's reason: nothing
in the crate counts one, and `representability.rs::an_actor_status_claims_only_what_it_measures`
destructures all four fields with no `..`, so adding one is `E0027` at the doc
comment that refuses it.

### 10.2 A subscriber is a parked reply. That is the whole of 6b's design

Issue #3's sketch has `feed Events(event: LifecycleEvent)` inside the `Inspect`
service and calls the feed arm "the point", on the grounds that before Wave 3 a
live console would have been *"a hand-rolled `recv` body held open inside a handler
— which on an actor blocks every other turn"*. The diagnosis is exactly right and
the prescription does not follow, because **a `feed` is not on the wire**. It is an
actor's second *input*, wired at construction; it produces no `Request` variant, no
client method and no ALPN movement. `feed Events` in a service block gives the
`Inspect` actor a stream to consume. It gives a console nothing.

So the wave had to answer the question the sketch skipped: how does a console
*receive*? Three shapes were considered.

- **Reverse dial** — `Subscribers` plus a `Console` service the console serves, which
  is RFC-04 §4.4's Design B and is already supported. Refused: it makes the console a
  **server**, and a browser tab cannot accept connections. 6c's stated point is that
  keylink compiles for wasm32 so the client can be a tab, and this shape takes that
  off the table.
- **A `recv` body** — the reply streams events down beside itself. Workable, and it
  needs a hand-written `AsyncRead` over a stream plus a second framing inside
  keylink's framing. Two layers of length prefix to carry a type the wire already
  knows how to encode.
- **`#[defer]`** — Wave 2c. What shipped.

`rpc Follow(after: u64) -> EventPage` is `#[defer]`. A caller with nothing new to
read has its `Reply` parked and **the turn ends**; the actor goes straight back to
its loop. A `LifecycleEvent` arriving on the `Events` feed is a turn like any other,
and that turn answers everybody parked. The sketch's own diagnosis is satisfied by
the two features the sketch was written to motivate, in the other order: `#[defer]`
is what lets the reply outlive the turn, and the `feed` is what wakes it with no
caller present.

Nothing about it is on the wire beyond an ordinary `rpc`: a deferred method is
indistinguishable from a plain one to a peer, which §4 established and
`tests/protocol.rs::adding_defer_to_a_method_does_not_move_the_contract_hash`
pins.

### 10.3 The property that makes it checkable rather than believable

"The console does not hold a handler open on an actor" is a claim a weak test passes
without touching. A test that only checks *the console received an event* passes
just as well against a `follow` sitting inside a turn holding `&mut Inspector`.

`tests/inspect.rs::a_parked_subscriber_does_not_stop_the_actor_serving_turns` is
ordered so that it cannot: a subscriber attaches and parks; the **inspect** actor
then serves twenty more round trips; the **watched** actor serves one; and only then
does the subscriber wake, **with the event that turn produced**. The last step is
what stops the twenty being vacuous — twenty answered calls beside a subscriber that
had already returned would prove nothing.

Both halves are watched to fail, separately, because they are separately
load-bearing:

- a `sleep` inserted before `reply.later()`, so `follow` holds its turn → the twenty
  calls never answer and the timeout fires: *"a handler held open by the subscriber
  would never answer these: Elapsed(())"*;
- `on_events` re-parking instead of answering → *"the feed turn must answer the
  parked reply: Elapsed(())"*.

### 10.4 Where events come from: two producers, one sequence, and a port on the registry

A console needs three facts and no single existing seam has them. `Observer` sees
turns and nothing else — it is owned by the actor, and `TurnInfo` carries no id
because nothing inside a turn knows which registry entry it belongs to. The registry
sees mounts and endings and never sees a turn.

So there are two producers and one sink, `Events`, which is what makes them one
sequence instead of two unrelated logs:

| fact | producer | why not the other one |
|---|---|---|
| `Mounted` | the registry | a lazy `spawn_with` factory mounts with **no caller present** |
| `Turn` / `Faulted` | `Events::observer(id)`, one per actor | only the actor sees what it ran |
| `Ended` | the registry | an **evicted** actor has no caller either |

The registry half is a new `pub trait Lifecycle` with two methods, on
`SystemBuilder::lifecycle`. It names `ActorId` and `Ended` and nothing else, so
`registry.rs`'s standing claim — that it names no wire type — is intact, and
§16.8's bill does not grow. `LifecycleEvent` and its wire derives live in
`console.rs`, on the other side of the port.

**`Ended` is reported by the registry and not by `Observer::finish`, deliberately.**
`finish` does not run for an aborted actor — its own documentation says so — which
is precisely the case that needed a signal. Reporting from both would be two answers
to one question.

### 10.5 The gap this closes, which the tree had already written down

`Registry::sweep` carried a header reading *"A known gap: an eviction that destroys
a busy actor is silent"*, and ended: *"**Wave 6 owns this.** Its `System::snapshot`
and the console are where a destroyed-on-eviction count belongs."* `Eviction::Idle`'s
`shutdown_budget` carried the same sentence from the other side.

It was literal. `sweep` computed an `Ended` for every actor it destroyed and dropped
it on the floor — `.map(|(_, entry)| entry)`, the id discarded with it — so a budget
below the longest turn destroyed that turn with no log, no counter, no observer and
no caller, because the caller of an evicted actor is a peer whose stream is severed
rather than answered.

The id travels with the entry now and the `Ended` goes to the sink. A system with no
`Lifecycle` computes the same value on the same path and drops it exactly as before,
so nothing changes for anybody who did not ask.

`an_eviction_that_destroys_a_busy_actor_is_no_longer_silent` proves the destructive
case rather than a clean drain: the actor is *inside* a turn, wedged on a channel, and
`finished == 0` is what says the work was thrown away. **Its first version was the
instrument's fault, which is worth recording.** The actor slept 100 ms per turn and
the test advanced the clock 60 seconds to trigger the eviction — which also completed
the turn it was meant to destroy, so `finished` read 1. An actor parked on a channel
cannot be resumed by the eviction clock. Red-tested by deleting the `report_ended`
call from `sweep`: `["mounted one"]` against `["mounted one", "ended one Vanished"]`.

### 10.6 Two things the compiler decided, and one it decided against

**`usize` cannot cross keylink's wire, and that is right.** `ActorStatus::buffered`
was a `usize` and `postcard_schema` implements `Schema` for every fixed-width integer
and neither pointer-sized one. That is a deliberate refusal, not an omission: a type
whose width is the compiler's choice cannot be half of a contract between two
machines, and keylink builds for `wasm32-unknown-unknown` **and** for 64-bit natives
— exactly the pair that would disagree. So `buffered` is now a `u64`, widened at the
one construction site with the `u64::try_from(..).unwrap_or(u64::MAX)` the registry's
own clock already uses. `NodeStatus::actors` is a `u64` for the same reason. This is
a public signature change, reported rather than buried; it moved three call sites,
all in tests.

**`extern crate self as keylink;`, one line in `lib.rs`, and it is load-bearing.**
`service!`'s generated rustdoc links back to the crate *by name* — `keylink::mount`,
`keylink::MailboxClosed`, `keylink::RpcClient::key_state` and thirty more — because
every other expansion of it is rendered in somebody else's crate, where that is the
only spelling that resolves. Inside keylink there was no such path, so the moment the
crate expanded the macro on a non-`#[cfg(test)]` item, `mise run docs` produced **69
broken intra-doc links**, measured by commenting the line out. `representability.rs`
has expanded `service!` for waves without hitting this only because rustdoc does not
render test-only code. The alternative was an `allow(rustdoc::broken_intra_doc_links)`
inside the macro, which would have turned those links off in every downstream crate
to fix one crate's problem.

**And the one refused: no new Cargo feature.** `console.rs` adds no dependency and no
`cfg`, so gating it would double the feature lattice `mise run features` walks in
exchange for removing four hundred lines from a build. It compiles for wasm32 like
the rest of the crate.

### 10.7 A new ALPN, pinned, and the old one proved not to have moved

`keylink/Inspect/879d78ee456a20b7` is keylink's first wire contract of its own, and
`tests/protocol.rs::the_inspect_contract_hash_has_not_moved` pins it beside the
probe's.

`keylink/Probe/d3fe34471921164e` **did not move**, which is the claim that mattered:
`ActorStatus` gained `Serialize`, `Deserialize` and `Schema` in this wave, and a
service's hash walks *its own* request and response types, so a struct no existing
service carries cannot reach one. Both pins were watched together under a break that
adds a variant to `EndReason`: `1 passed; 1 failed` — the inspect hash moved to
`71d62d9cd8e8eb9c` and the probe's stayed put. One break, two facts: the new pin is
sensitive, and the two contracts are independent.

The cost is stated where a reader meets it, on `ActorStatus`: a fifth field is still
additive in Rust and is **not** additive on the wire. It moves `keylink/Inspect`'s
hash and an old console is refused at the handshake rather than handed four fields
where it expected five. That is the same trade `derive_alpn` makes everywhere, and
it is another reason the two refused fields are refused rather than reserved.

### 10.8 What Wave 6 did NOT do

- **Ship a rendered console.** 6c was briefed as the largest and least load-bearing
  item, with explicit permission to descope it. What shipped is its **client half**:
  `Console`, which owns the cursor — the one part of the protocol that is easy to
  get wrong, since a console that forgets to advance it re-reads one page forever —
  and is a client of nothing but `inspect`, so it compiles for a tab. What is not
  here is a rendering: no TUI, no wasm demo page, no `demo/` binary. That is a
  presentation layer over `Console::events()`, and it would have been the rushed
  part.
- **Report the parked-sender count.** Still not observable, still `buffered`'s
  documented floor, and `Inspect` carries `ActorStatus` **itself** rather than a
  wire twin, so `representability.rs`'s four-field destructure governs what a console
  can be told. `the_inspect_service_reports_the_status_type_itself` pins that with a
  coerced constructor: a parallel row struct is `E0308` there.
- **Add an `incarnation` count.** §10.1. If Wave 9 adds a restart count it is that
  wave's field to add, and it will move `keylink/Inspect`'s hash when it does.
- **Guarantee delivery of an event.** `Events::record` is a `try_send` and never
  blocks, because a diagnostic that can stall a mount or an eviction is worse than
  one that skips an event — the same argument `Subscribers` makes about
  `Connection::closed`, one layer up. This inverts the mailbox's rule on purpose:
  there the sender is a caller and blocking is backpressure; here the sender is a
  teardown. What is dropped is counted and published, at `NodeStatus::lost` for the
  channel and `EventPage::lost` for the ring.
- **Order events against anything but themselves.** A turn on one actor and a mount
  of another are two tasks, and their order in the log is the order they reached the
  channel.
- **Bound the parked followers with a number.** Every parked `Deferred` is one open
  QUIC stream, and iroh caps those at `max_concurrent_bidi_streams` per connection
  times the peers the allowlist admits. A second cap would be a second bound on one
  thing.
- **Cover the lazy-spawn race arm.** `Registry::route`'s `Occupied` arm is reached
  only when two messages to one new id race between the read lock and the write lock.
  A break that announces from it left `every_way_an_actor_arrives_and_leaves_is_reported`
  **green**, because the second `route` finds the actor on `touch`'s fast path and
  never reaches the match. Watched, retracted, and recorded here: the "a second route
  is not a second mount" claim is carried by the fast path, and the race arm is
  covered by argument.
- **Give `Inspector` a way to reach the system beyond `status()`.** It holds
  `Arc<dyn Fn() -> Vec<ActorStatus>>` and not a `System<M>`, which is what lets one
  console answer for a system of any service — a `service!` block generates concrete
  types and cannot be generic in a mailbox. The erasure is narrow on purpose: there
  is no mount, no shutdown, no abort and no downcast that gets them back.

### 10.9 kameo's console, read — what was taken, and the four fields refused with reasons

`~/repos/ext/kameo/src/console/{wire,server,registry}.rs` was read. It is a
pull-based TCP server: the client opens a socket, sends one byte, and gets back a
length-prefixed MessagePack `Snapshot` of the whole system. The transport half is
the one keylink does not need — no listener, no `SocketAddr`, no auth to invent,
because a console here is an ordinary peer on an ALPN behind an `Allowlist`. What
was worth reading is its **field list**, which is far richer than `ActorStatus`, and
going through it item by item is what produced the honest gaps below rather than a
shorter list that looked complete.

**Taken.** Its `MailboxStats::len` is `max_capacity - capacity`, the same derivation
4d arrived at independently — which is some comfort that the reading is the natural
one. Its `Snapshot::seq`, a monotonically increasing sequence the client uses to
detect a source restart, is the shape of `EventPage::next`; keylink's is per-event
rather than per-snapshot because the log is a stream and not a poll, and it carries
the same property: a `next` that went backwards is a process that restarted.

**Refused, each with a reason that is keylink's rather than kameo's.**

- **`captured_at: SystemTime` and `uptime`.** kameo's client diffs `captured_at`
  between two snapshots to get an accurate `dt` rather than trusting its own receive
  time, which is a genuinely better instrument. keylink does not have the clock to
  do it: `SystemTime::now()` panics on `wasm32-unknown-unknown` exactly as
  `Instant::now()` does, and this crate's whole timekeeping is `n0_future::time`
  precisely so a browser is not a special case. A field readable on one target and
  fatal on another is worse than no field. `idle_for` is a `Duration` off the
  registry's own epoch, which crosses fine and means the same thing on both.
- **`grave_window` and `ActorStatus::Stopped { at, reason }`.** kameo keeps a dead
  actor in the snapshot for five seconds so it fades rather than blinks away, and so
  a very short-lived actor is visible for at least one poll. That is a real problem
  with polling a list, and keylink does not have it: an ending is an **event** in an
  ordered log, so a short-lived actor's whole life is three entries a console cannot
  miss by polling at the wrong moment. Keeping a dead row in `status()` would put a
  second meaning into a list documented as "what is running".
- **`Links { parent, children, siblings }`, `SupervisionInfo`,
  `SupervisorStrategy`, `ActorCounters::restarts`.** Nothing in keylink has a parent,
  a child, a restart policy or a restart count. These are Wave 9's, and inventing
  fields for them now is how an observational number becomes a fact people route on
  before the thing it describes exists — Wave 2's argument about `incarnation`,
  applied to five more fields at once.
- **`RefCounts { strong, weak }` and `MailboxKind::Unbounded`.** Unrepresentable
  here, and in the second case *deliberately*: `SystemBuilder` has no `unbounded`
  spelling and `representability.rs::no_mailbox_can_be_asked_to_be_unbounded` holds
  the probe that says so. A field with one reachable variant is a field claiming a
  choice nobody has.

**And the two that are honest gaps rather than refusals**, named here so the next
wave finds a decision instead of an omission:

- **`HandlerActivity { message, elapsed }`.** kameo reports *which* message a
  handler is running and for how long, and calls it the primary slow-or-stuck
  signal. `ActorStatus::busy` is the one-bit version of exactly that field.
  keylink **can** measure it — `local.rs`'s one dispatch already knows the method
  name and the moment the turn started, which is where `Busy` is set — so this is
  a gap and not an impossibility. It was not taken here because it changes a field
  4d shipped, at the end of a wave, unreviewed, and because the cost is on the turn
  hot path: `Busy` is one `AtomicBool` written twice per turn, and carrying a name
  and an instant is a different structure. It is the highest-value follow-up in this
  area.
- **`WaitEdge` and the wait-for graph.** kameo's client walks those edges to detect
  deadlocks, which is the most valuable thing in its console. keylink cannot build
  the graph today: a turn blocked on another actor is blocked inside a caller's
  future, and nothing records who it is waiting for. `Handle` documents a self-call
  as a deadlock rather than detecting one. Recorded as wanted and unbuilt.

---

## 11. Wave 6, reviewed — three refusals in §10.8 that did not survive contact

Appended rather than edited, per CONTRIBUTING.md: what §10.8 says is what was
believed when Wave 6 landed, and this section is what a review found afterwards.
Read together, the pair is the record. All three moved `keylink/Inspect`'s ALPN
from `080c9e481c937d67` to `7bbd34fa83c9060c`; `keylink/Probe` did not move.

### 11.1 "Bound the parked followers with a number" — the stated bound was false

§10.8 refuses a cap because *"every parked `Deferred` is one open QUIC stream,
and iroh caps those at `max_concurrent_bidi_streams` per connection times the
peers the allowlist admits. A second cap would be a second bound on one thing."*

Both halves of that are wrong, and in opposite directions.

**The entry outlives the stream.** Nothing removed a `(after, Deferred)` from
`Inspector::waiting` except `on_events` finding a page for it. A peer that opened
a stream, called `Follow` and went away left its entry behind until the node
recorded *something*, which on a quiet node is never. So the list was not bounded
by open streams; it was bounded by event traffic, which is the thing a console
watches precisely because it may be zero.

**And a cursor at or above `seq` was never released at all.** `page(after)`
answers `None` for it forever, so it was re-pushed on every event — and
`on_events` walked the whole vector calling `page()` per waiter, so the cost was
`O(waiting × history)` per event as well. `follow(u64::MAX - 1)` is the limit
case: it needs 2^64 events. Compiled by the reviewer: ten peers holding one each,
then fifty mounts, and all ten `Deferred`s retained across all fifty.

That is the refused-designs table's own argument — *an unbounded mailbox converts
backpressure into memory growth and a latency problem into an OOM* — turned
inward, on the first service this crate puts on the wire, reachable by one call
from an admitted-but-buggy console.

**Both remedies landed, because they close different halves.** `Follow` answers
`Result<EventPage, FollowRefused>`; a cursor **ahead** of the log is refused as a
client error rather than waited on, and a call arriving past `MAX_FOLLOWERS`
(128) is refused as `Crowded`. The refusal is what makes the cap's bound honest:
every parked cursor is now `<= seq` as of its park, so the next recorded event is
past all of them and the list is emptied by every event. Nothing accumulates.

The general lesson is the one this project keeps relearning: **a bound stated in
terms of something the code does not actually track is not a bound.** The stream
was never consulted; only the event was.

### 11.2 `EventPage::next`'s restart property was unreachable — and the same refusal implements it

§10.9 took kameo's `Snapshot::seq` property, *"a `next` that went backwards is a
process that restarted"*. Through the shipped client it could not happen: the
sequence counter starts at zero in every process, so `follow(at = 50)` against a
restarted node found nothing `> 50` and **parked** — until fifty fresh events
existed, and then answered with a `lost` computed against the new ring, the new
incarnation's first fifty events gone and unreported. The console never saw a
`next` go backwards because it never got a `next` at all.

`FollowRefused::Ahead { after, seq }` is the property, delivered immediately and
with both numbers. A node whose log is *behind* a cursor that was correct a
moment ago is, for a counter that only goes up, not the same log.

`Console::events` deliberately does not catch and retry it. Resuming silently
would hide the restart, which is the whole signal.

### 11.3 "the erasure is narrow on purpose" — it was narrow in capability and not in lifetime

§10.8 defends `Arc<dyn Fn() -> Vec<ActorStatus>>` as a narrow erasure: no mount,
no shutdown, no abort, no downcast. That much held. What it did not say is that
the closure captured a `System` **clone**, so an inspector *owned* the registry:
a program that dropped its own system went on running every actor in it, for as
long as the inspect actor lived. `spawn_sweeper` was already `Weak` for exactly
this reason, which is what made the strong reference here the odd one out rather
than a considered choice.

`System::weak_status` is the version §10.8 describes. The signature becomes
`Fn() -> Option<Vec<ActorStatus>>`, and the `None` is the system being gone —
which a caller must be able to tell from a system with nothing in it.

### 11.4 Three smaller ones, recorded because each was a claim rather than an omission

- **`Ended::Feed { name: "Events" }` was unreachable.** The `Inspector` held an
  `Events` clone for the drop counter, which is a `Sender` for the very channel
  it reads, held inside the actor reading it. "Every clone dropped" could not
  happen while the actor existed. It keeps only the `Arc<AtomicU64>` now.
- **§10.8's "what is dropped is counted and published, at `NodeStatus::lost` for
  the channel and `EventPage::lost` for the ring" is true and was not enough.**
  The two counters live on two different RPCs, and the loop `Console`'s own
  documentation tells a reader to write called only one of them — so a burst
  deeper than `Events::new(depth)`, which is the *likelier* loss, was invisible
  to the recipe. `EventPage::dropped` carries the sink's count now.
- **§10.3's "the property that makes it checkable" claimed a test that cannot
  exist.** It said an actor with N subscribers reads `busy == false`. `busy` is a
  registry fact and the `Inspector` is a `LocalActor` with no registry entry, so
  there is nothing to read it from. The property that *is* proven — twenty calls
  answered while a subscriber is parked — is the sharper one anyway: a handler
  parked inside a turn fails it, while a flag can be wrong without anybody being
  kept waiting.

### 11.5 And one that was not §10.8's at all: the fourth way out of a registry

Recorded here because three reviewers found it from three directions and it is
the same defect §10.5 says this wave closed *half* of.

An actor that ends **on its own** — a feed that runs out, a handler that panics,
an observer that refuses, a supervisor that gives up — was never removed from the
registry and never reported. Entries left the map at exactly three sites, and
every one of them is *somebody asking*. At the same moment on the same node:
`System::status()` and `Actors()` listed the actor with `busy: false`, a real
call answered `NoSuchActor`, and the event log held no `Ended`. Under
`Eviction::Never` that was permanent, and the id was unmountable for the life of
the process.

`registry.rs`'s own rustdoc had already named the repair — *"the keyed loop
already computes the true `Ended` and throws it away. Whoever wants it should
give the loop somewhere to put it"* — and that is what landed: `Loop<M>` resolves
to `Ended` rather than `()`, so a loop cannot return without naming its ending,
and `Registry::entry` wraps the spawn in the one place an actor's task is
started. One mechanism serves both keyed paths, supervised and not, because a
**restart is not an ending**: `supervise` runs many incarnations behind one
`Output` and returns only when there will be no more.

`route` checking `Sender::is_closed` was the other remedy offered and is refused
on its merits: it repairs *routing* and leaves `System::status` lying and the
`Lifecycle` untold, and it cannot carry a reason — `is_closed` knows that an
actor is gone and never why.

## 12. The `Fate` wave — one verdict, both served paths, and where certainty runs out

§9.10 recorded a wrong answer and declined to fix it. This is the fix, and the
part worth reading is not the mechanism — it is which claims about it are proved
and which are only true *at the moment they are made*.

### 12.1 THE DECISION THIS WAVE OWNS — the answer is about the actor NOW, not about what killed the reply

A served reply channel that dies without sending has three causes, and the peer's
next move differs for each: an incarnation that ended with another coming (retry),
a live actor that dropped one reply (do not retry this call, the actor is fine),
and an actor that is finished (stop asking). `deliver_to_local` split them in
a3171df. `deliver_to_actor` rendered all three as `NoSuchActor`.

The obvious design is a cause tag travelling with the death. It is not
buildable, and saying why fixes the shape of everything below. The three causes
are distinguished by **who drops the `Reply`**: a handler, or the state's own
`Drop`. Both are `Deferred::drop`, and the parked one lives in *user state* — the
loop does not know which `Deferred`s the state holds, which is the whole point of
`#[defer]`. So nothing at the point of death can label it, and the answer has to
come from a cell read afterwards.

That reframes the question, and the reframing is the decision:

> The cell says **what is true of this actor now**, not **what killed your reply**.

Every answer is then a statement that is true when it is written, and the three
map onto the three next moves without ever claiming to know a cause. A live actor
that dropped a reply and then ended reads `Gone`, and *stop asking* is right for
it. A restart whose next incarnation is already up reads `Running`, and *the actor
is there* is right for it. §12.4 is where that stops being free.

### 12.2 The mechanism, and why it is not the six touch points §9.10 priced

`Standing` — `Running | Restarting(Arc<Ended>) | Gone` — behind a
`Fate(Arc<Mutex<Standing>>)`. One cell per actor, made where the actor is made,
written by the actor's own loop, read by whoever serves it.

`Inbox::ending` **returns** it rather than publishing it. `run_loop` publishes at
one line:

```rust
fate.settle(inbox.ending(&reason));
```

§9.10 priced the keyed half at *"`Life` stops being private to `local.rs`;
`Mounted<M>` carries a receiver beside its `Loop<M>`; `mount` grows a second door
…; `Registry`'s `Entry` stores it; `System::route` hands back a pair"* — six touch
points, one of them a public type, and an `Option`-shaped `mount` the house rules
ban by name. Only the last two happened. The cell rides on `Mailbox<M>`, which
`registry.rs`'s own documentation had already argued is the right vehicle for
exactly this, for `Busy`: *"widening `mount`'s closure would have carried it just
as well and put the pairing in **generated** code, one expansion per `service!` in
the world."* `mount(|inbox: Mailbox| ..)` is the same line it was; `run_keyed` and
`run_keyed_supervised` take the cell off the mailbox they were already handed;
`macros.rs` is untouched but for prose. Neither ALPN moves, no `RemoteError`
variant is added, and there is no fifth prefix.

**A `watch` channel was refused.** Nobody awaits this: it is read once, on a dead
reply, by a task that is already awake. A `watch` per keyed actor buys a wait
list, a version counter and a `Notify` for a value nothing waits on, and §12.5 is
what a per-idle-actor byte costs here.

**Folding it into `Busy` was refused**, though both are one word about one actor
and it would save an allocation on the keyed path. A `LocalActor` needs the fate
and deliberately has no `Busy` — its documentation refuses one — so folding would
force a registry fact onto the path whose reason for existing is that it has no
registry.

### 12.3 The happens-before for each answer

The reader is `deliver_to_local` or `deliver_to_actor`, at the instant
`answer.await` resolves `Err` — the reply channel's sender was dropped. tokio's
`oneshot` closes with an AcqRel RMW and the receiver observes it with Acquire, so
**everything the dropping task did before that drop happens-before the read.**
Every claim below is that edge plus a program order on one task.

| answer | what makes it true |
|---|---|
| `Restarting` | `Lent::ending` grants the restart and hands the verdict back; `run_loop` settles it at the line above — before `finish`, before `drop(inbox)`, before `drop(state)`. Every reply this teardown kills dies after the store. |
| `Gone`, ordinary ending | The default `Inbox::ending` for an inbox with no supervisor above it, settled at the same line. A loop that reached it and is nobody's incarnation will not run again. |
| `Gone`, supervised give-up | The same line, the arm `Lent::ending` refuses. This is the Major regression: the old hook wrote a channel itself on the restart arm and returned nothing, so the give-up published *nothing*, and `is_running()` — `!mail.is_closed()` — was `true` because `supervise` owns the receiver for the actor's whole life. A peer was told `dropped: `, *the actor is still there*, about an actor that would never answer again. A hook that must hand a value back has no arm that quietly says nothing. |
| `Gone`, factory give-up | `supervise` settles before it returns. No incarnation ever reached a loop, and the receiver — this function's own parameter — drops on the way out, killing everything queued behind an actor that will never exist. |
| `Gone`, destroyed | `LocalActor::abort`, `System::abort` and `drain`'s expired budget settle **before** they ask the runtime. `JoinHandle::abort` is an RMW on the task state; the runtime drops the future after observing it and drops the state after that, so the settle happens-before every reply the destruction kills. An aborted loop never reaches the hook, so nothing inside it could have said this. |
| `Gone`, hand-written loop | `Mailbox`'s `Drop`, which is a **floor and says so**: a `Drop` body runs before its fields, so it lands before the receiver closes and before what is queued behind it is dropped — exact. Where it sits relative to a hand-written loop's own state is not knowable from here, because that state is whatever the closure captured. The loops keylink generates never rely on it. |

The two registry settles look redundant against that floor and are not. On an
**unsupervised** keyed actor the mailbox is `run_loop`'s own parameter and drops
before the state, so the floor answers and breaking either settle leaves the
end-to-end tests green — recorded in those tests, because that is exactly the sort
of belt that reads like a guard. They earn their place on the **supervised** keyed
path, where the mailbox belongs to the supervisor's frame and its `Drop` is too
late, and on the local path, which has no `Mailbox` at all.

### 12.4 Where certainty runs out: the read can be LATE, and no cheap fix is honest

One direction is open and it is stated rather than closed. The reader is woken by
the death and reads afterwards, so **the actor may have moved on in between.** A
restart whose next incarnation has already published `Running` reads as *dropped*.
The window is a supervisor's backoff wide — `run_loop` returns, `supervise` sleeps
the backoff, calls the factory, then publishes — so it takes a starved worker to
hit, but it is reachable.

What it costs is exactly one thing: such a caller is not told to retry. The
sentence it does get is true when it is written — the actor *is* there — which is
the §12.1 frame doing its job.

**A generation counter was considered and refused, and this is the reasoning to
keep.** Stamp an incarnation number in the cell, capture it at `route`, compare it
at the read: a bump proves the incarnation we sent to is gone. It is wrong in the
other direction, and worse. Queued turns survive a restart — the supervisor owns
the mailbox — so a message sent to incarnation *N* may **run** in *N+1* and have
its reply dropped live there. The generation bumped, so this would answer
`restarting: `, which claims the actor is *between* incarnations while it is
running. Today's imprecision produces a sentence that is true and less useful;
the counter produces one that is false. Trading the first for the second is what
"differently wrong" means.

Making it exact needs the *turn* to stamp which incarnation ran it, which puts an
incarnation number inside `Reply<T>` and therefore inside every generated mailbox
variant — a cost to every service for a window bounded by a backoff. Not built,
and named here as the only design that would close it.

**No "cannot be certain" class is added, and that is a finding rather than an
omission.** The brief asked for a distinct class where certainty is unattainable,
and it would have to be a fifth `Failed` prefix. There is no reachable case that
needs one: every answer above is provable at the moment it is written, and the
late read does not produce *uncertainty* — it produces a different true statement
about the same actor. A fifth prefix would have to be reached by some arm of
`lost`, and there is no condition to put in that arm. The two residuals are named
instead: this section, and the hand-written-loop floor in §12.3.

The one case genuinely outside the argument is a **runtime shutdown**, which
destroys tasks without going through any of the three doors. It gets no class
either: the task that would read the cell is being destroyed too.

**A second residual, and the floor bounds it rather than closing it: a caller's
`Drop` that panics on the restart path.** §12.11's floor answers it, and for this
one case the floor is *necessarily late* — provably, not by a race:

1. `run_loop`'s teardown reaches `drop(state)`, having already settled `Restarting`
   and disarmed its own floor.
2. The state's `Drop` — caller's code, outside any `catching` — panics.
3. Drop glue continues **during that unwind**, which is where a `Deferred` parked
   in that state dies and where its peer is woken.
4. The unwind leaves `run_loop`, whose floor is spent.
5. Only then does it reach `supervise`'s frame and fire that floor.

Step 5 is in a strictly outer frame than step 3, so the floor **cannot** precede
the death. It makes the cell right for every later reader; it does not make the
answer right for the peer whose read that death woke, and that peer can still see
`Failed("restarting: ..")` for an actor that will never come back. An end-to-end
probe of it returned `NoSuchActor` — a race won by a wide margin, because the whole
unwind completes before the peer task is polled, and not an order kept.

**It is unfixable with this mechanism**, which is the part that makes it a residual
rather than a defect: settling `Gone` before `drop(state)` would overwrite
`Restarting` on the *normal* restart path, which is N2 exactly. What would close it
is catching a caller's `Drop` — and that is the third instance of the decision
§12.11 refuses for the factory and the feed, on the same grounds: it changes what a
panicking `Drop` *means*, from an actor destroyed to an actor that restarts, which
is a supervision-semantics choice with its own guards to write. Not attempted, and
named here rather than left to be discovered.

`a_state_that_panics_on_drop_mid_restart_is_gone_not_restarting` therefore asserts
the cell's **final** value after joining the unwound task, not the value at the
instant of death. That is the part the third disarm destroys and the part a
regression would show; the cell-to-wire step is break 20's. The gap between them is
this residual, and it is disclosed rather than papered over — which is the same
distinction §12.3's table draws for `Mailbox::drop`, a floor whose ordering against
a hand-written loop's own state is likewise unstated because it is unstateable.

### 12.5 What it costs, measured

`tests/actor_memory.rs`, ten thousand idle actors, same machine:

| path | before (`810650a`) | with `Fate` | with the `Floor` (§12.11) |
|---|---|---|---|
| keyed, mounted in a `System` | 1,549 | 1,690 | **1,722** |
| local, `ledger::actor(..)` | 1,861 | **2,029** | **2,029** |

**+141 keyed, +168 local** for the cell — one `Arc<Mutex<Standing>>` per actor plus
the handles that reach it — and the local path gave eight bytes back (below). The
floor added **32 more on the keyed path and nothing measurable on the local one**:
it is a `&Fate` and a `bool` in two frames, and only the keyed one crossed an
allocator step with it. Re-measured because the middle column was written before
the floor existed, and a stale figure in that table is the defect it warns about.

**Run-to-run variance on this machine is zero** — five consecutive runs, identical
totals to the byte on both paths. That retires the argument the budget raise was
made on ("nineteen bytes is inside the noise"): there is no noise here, so
`LOCAL_SLACK` is an allowance for **an allocator this has never run against**,
which is real, unmeasured, and now says so. One Linux CI run replaces it with a
measurement.

It measured **+165 and +192** first, with `Standing::Restarting(Ended)` inline:
`Ended` is 40 bytes, so `Standing` was 40 and `Mutex<Standing>` 56, and every idle
actor paid for a value only a restart ever writes. Behind an `Arc` they are 16 and
32 — all four figures from a `size_of` probe rather than from arithmetic — worth a
measured 24 bytes per idle actor on both paths, with the allocation moved to the
restart that needs it.

`BUDGET_PER_LOCAL_ACTOR` goes **2,048 → 2,304**, and a budget raised by the change
that would have broken it is the weakest thing here, so: it went red at 2,053, the
guard working; the first response was to make the change cheaper, which landed
2,029, which *passes*; it is raised anyway because nineteen bytes under a ceiling
is inside the difference between one allocator and another, so a green run on this
machine would say nothing about a Linux CI. The keyed budget stays at 2,048 with
**326** bytes under it. `SLOPE_SLACK_OVER_A_KILOBYTE` — the guard that catches
*unbounded* growth rather than a constant — is untouched, and the slope is 13
bytes over a kilobyte of declared message, flat as before.

**That paragraph is superseded by the variance measurement above, and two things in
it were wrong.** Its reason — *"nineteen bytes … is inside the difference between
one allocator and another"* — is the argument the zero-variance measurement retired:
there is no run-to-run difference on this machine to be inside of, and the slack is
an allowance for an allocator that has never run this, which is a different and
unmeasured claim. And **358 was `2048 − 1690`**, computed from the pre-floor keyed
figure in the very commit that measured 1,722. It is 326.

The house habit is to leave superseded reasoning visible and correct it in place,
but here the correction landed *earlier* in the same section than the thing it
corrects, which inverts the convention and reads as two live arguments. Hence this
marker rather than a rewrite. It is also the **second** stale subtraction in this
one paragraph's neighbourhood — the "339 bytes of headroom" sentence a few lines up
was the same defect, found in round one — so the pattern is worth naming: a figure
derived from another figure rots silently when the source is re-measured, and
`2048 − measured` should not have been written out as a literal at all.

Per call: nothing on the answered path. One uncontended `Mutex` lock on the arm
where a reply already died.

Code paths touched: `local.rs` (the type, `run_loop`, `Inbox`, `Lent`,
`supervise`, `LocalActor`, `lost`, `deliver_to_local`), `actor/registry.rs`
(`Mailbox`, `Entry`, `Route`, `route`, `touch`, `entry`, `drain`, `System::abort`,
`System::route`), `actor/remote.rs` (`deliver_to_actor`). `macros.rs` and the
whole public surface: prose only.

### 12.6 `Life` is deleted, and the reader-less channel with it

Wave 9 gave the local `ended` channel a third state, `Life::Restarting(Ended)`, so
that `deliver_to_local` and `supervise` could both read a restart off it. Neither
does now — both read the verdict — so the arm has no writer and the channel is
back to `Option<Ended>`. Three consequences, all in the same direction:

- the eight bytes §9.10 measured onto every local actor come back;
- `run_keyed_supervised`'s `watch::channel(Life::Running)` with **no reader at
  all** — §9.10's *"one small channel per supervised keyed actor is the cost of
  that"* — is deleted rather than explained;
- one fact has one home. `Lent::ending` decides, `run_loop` publishes, `supervise`
  and both served paths read the same cell. The blunt break — deleting the publish
  — now reddens the *supervisor* too, which is recorded in that guard as the more
  informative half.

### 12.7 Corrections to §9.10

Three of its bullets described a permanent state of affairs and are now false.
They are corrected here rather than edited there, per this file's habit:

- *"a keyed actor has no `Life` channel anyone reads"* and *"`run_keyed_supervised`
  allocates a `Life` channel with no reader"* — there is no `Life` at all.
- *"The proper fix is declined out loud, and it is not one paragraph … Six touch
  points across three files, one of them `Mounted` — a public type"* — the trace
  was accurate for the design it traced, and that design was not the only one. Two
  touch points, neither public, because the vehicle was already in the tree
  carrying `Busy`. The lesson is narrow and worth keeping: a cost estimate is
  about a *route*, and naming the route it prices is what lets a later reader find
  a cheaper one.
- *"the other two want a mounted-vs-gone answer from the registry and are a design
  rather than a line"* — true, and the design is §12.3. The risk named with it —
  that ordering a mounted-vs-gone answer against a restart "has a real risk of
  being differently wrong" — is real and is §12.4, where it is refused rather than
  guessed at.

### 12.8 Every guard, and the break that proves it

Each was watched red; each records the break and the exact output in its own doc
comment. **Every one of them replays**, which is not a claim this section could
make at first: six recorded a literal the harness cannot find exactly once —
usually because the record itself quotes it, so the count is two — and an
unreplayable record is the one thing a record must not be. They are anchored on
two lines now, and `scripts/replay-fate-breaks.sh` runs the whole set.

| guard | break |
|---|---|
| `defer::a_live_keyed_actor_does_not_tell_a_peer_no_such_actor` | `deliver_to_actor`'s `lost(..)` → `no_such_actor(&actor)`, the pre-fix line verbatim |
| `supervision::a_remote_caller_can_tell_restarting_from_gone_on_a_keyed_actor` | `Lent::ending`'s `Standing::Restarting(..)` → `Standing::Gone` |
| `supervision::a_remote_caller_can_tell_restarting_from_gone` | `Lent::ending`'s `Standing::Gone` → `Standing::Running` — the regression verbatim, under four workers |
| `local::an_ending_incarnation_settles_its_verdict_and_its_mailbox_before_its_state` | three: the settle moved after `drop(state)` via a `Drop` guard holding the same verdict; `Lent::ending`'s `self.close()` deleted; the settle deleted outright |
| `local::a_supervisor_that_refuses_a_restart_answers_gone_rather_than_running` | the refusal arm answers `Standing::Running` |
| `registry::abort_settles_the_served_answer_before_it_destroys_the_actor` | `System::abort`'s settle → `Standing::Running` |
| `registry::an_expired_shutdown_budget_settles_the_served_answer_before_the_abort` | `drain`'s settle → `Standing::Running` |
| `remote::no_other_served_failure_begins_with_one_of_the_five` | `RouteError::AtCapacity`'s `Display` gains a `"dropped: "` prefix |
| `defer::a_local_caller_and_a_peer_agree_about_an_abandoned_deferred` | `lost`'s `Standing::Running` arm → `NoSuchActor` |
| `defer::a_peer_whose_deferred_handler_panicked_reads_the_same_dropped_answer` | the same arm → `Failed("panicked: bad")` |
| `defer::a_stopped_actor_still_tells_a_peer_no_such_actor` | the default `Inbox::ending` → `Standing::Running` |
| `defer::an_aborted_actor_tells_a_peer_no_such_actor` | `LocalActor::abort`'s settle → `Standing::Running` |
| `defer::an_aborted_keyed_actor_tells_a_peer_no_such_actor` | `Mailbox`'s `Drop` → `Standing::Running` |
| `defer::a_keyed_actor_destroyed_by_an_expired_budget_tells_a_peer_no_such_actor` | the same |
| `supervision::a_peer_whose_factory_panicked_is_told_the_actor_is_gone` | `supervise`'s per-iteration floor disarmed before the factory call |
| `supervision::a_peer_whose_feed_panicked_is_told_the_actor_is_gone` | `run_loop`'s floor disarmed before the loop |
| `local::the_gone_answer_is_not_a_failed_and_carries_no_prefix` | `lost`'s `Gone` arm becomes a `Failed` |
| `reply::the_five_prefixes_are_the_text_a_peer_matches_on` | `DROPPED` → `"lost: "` |
| `local::a_supervisor_between_incarnations_is_gone_when_it_is_destroyed` | `supervise`'s floor disarmed at its declaration |
| `local::a_transient_factory_failure_does_not_overwrite_a_pending_restart` | the floor moved back inside the loop body |
| `local::a_state_that_panics_on_drop_mid_restart_is_gone_not_restarting` | the third disarm around `run_loop`, applied verbatim |

`scripts/replay-fate-breaks.sh` runs all twenty-three and asserts, for each, that
**the exact sentence its guard's doc comment records as watched appears in the
run**. It exits non-zero with a count if any is missing, and it refuses to start
against a dirty tree, because `break-once.sh` restores a backup on EXIT and a run
racing an unsaved edit restores over it.

**It counted exit codes first, and that design cannot see the failure this harness
exists for.** A break that stops reddening because a *second writer* masks it has
happened three times in this tree — `Mailbox::drop` masking `System::abort`'s
break, the same masking `drain`'s, and a whole-function `Floor` masking
`Lent::ending`'s give-up arm, which is why §12.11's third attempt exists. A fourth
time a break stayed red at a **different** assertion: the record for
`an_ending_incarnation_..`'s blunt break said it reddens as `an incarnation is
there to take it: SendError` *"because `supervise` reads the same cell"*, and three
commits later the same wave deleted that read (§12.9's m1). `exit=101` is identical
in all four. The recorded sentence is not.

Three of the twenty-three carried a promissory note instead of an output at some
point — two supervision guards and `the_gone_answer_..` — which made the premise
above false for each until it was filled. Writing the record before running the
break leaves a sentence that reads like evidence, and a reviewer grepping for the
placeholder is what caught all three.

Proved by execution rather than argued, twice: a deliberately no-op break reports
its recorded output missing and the script exits 1; and break 6 run against the
*stale* expectation reports `exit=101` — genuinely red — **and still fails**, which
is the historical case reproduced.

Two smaller things it did wrong, both fixed: the exit code was the status of the
**last** `run` only, so red-then-green exited 0 — worse than always zero, because a
non-zero looked like it meant something — and `set -e` let one rotten record hide
every record after it.

**The ordering guard needs an in-drop observer, and that is a finding.** From
outside the actor there is nothing to see: the whole teardown runs before any
other task is polled on a current-thread runtime, so a served call always reads
the final value and every ordering — including the broken ones — looks correct.
That is why the empirical probe of the supervised give-up window hung and proved
nothing, and why the guard is a state whose own `Drop` reads the cell.

**Three guards are recorded as leaving a line uncovered**, rather than being
credited with it:

- breaking `System::abort`'s or `drain`'s settle leaves the two keyed end-to-end
  teardown tests green, because the `Mailbox` floor answers first on an
  *unsupervised* keyed actor. Those lines are held by the registry unit tests
  above, which observe inside the drop.
- breaking `Lent::ending`'s give-up arm leaves
  `a_remote_caller_can_tell_restarting_from_gone_on_a_keyed_actor` green, for the
  same shape of reason: `Mailbox`'s `Drop` settles `Gone` a few instructions
  later, when `supervise` lets go of the receiver, and on the run that was watched
  the peer read after that. Its **singleton twin is reliably red** on the same
  break, because a `LocalActor` has no floor under it, and the deterministic
  guard for the ordering is the in-drop test.

### 12.9 `is_running()` was lying too, and it is a second consequence rather than a second decision

A reviewer's independent analysis of the same window recommended a different
remedy: give `Lent::ending` the `else` branch it was missing, calling
`tokio::sync::mpsc::Receiver::close()` so that `Sender::is_closed()` becomes true
at once while the buffer still drains — after which `deliver_to_local`'s
`is_running()` read would have been *true* rather than removed.

**Not adopted as the remedy, and adopted anyway for what it independently
repairs.** The two halves:

- It does not reach the keyed path. `deliver_to_actor` never read `is_running()`
  and has no `LocalActor` to read it from, so a remedy that repairs the invariant
  behind that read leaves every keyed answer where it was. This wave's brief is
  precisely to remove that asymmetry, and the verdict is what removes it —
  `deliver_to_local` reads no mailbox at all now, so the invariant has no reader
  left to be true for.
- **`LocalActor::is_running()` is public, and it lied on its own account.**
  `supervise` takes the receiver *by value* and holds it across every
  incarnation, so between `run_loop` returning on a permanent give-up and
  `supervise` returning, `is_running()` answered `true` for an actor that would
  never take another message, and `ask` acquired a permit into a queue nobody
  would drain. Nothing to do with the wire.

So `Inbox::close` exists, `Lent::ending` calls it on the non-restart arm, and
`supervise` calls it on the factory give-up. One decision — made in `ending`,
once — with two consequences published where each is needed: the `Standing` a
dispatch reads, and the mailbox a caller meets. The restart arm closes nothing,
which is the structural promise that a restart needs no re-queue, and the in-drop
guard asserts both halves as `[(Restarting, false), (Gone, true)]`.

Two alternatives the reviewer refuted are recorded so they are not re-derived:
reordering `supervise` to drop `inbox` first is impossible, because the state dies
inside `run_loop`, which holds the mailbox as a `&mut` through `Lent`; and closing
unconditionally in `Lent`'s `Drop` is the right instant but destroys the design,
since the mailbox must survive a restart. A third — publishing
`Life::Ended(reason.clone())` early — would have let `ended()` resolve with the
un-wrapped reason and lose `GaveUp { after, .. }`, because `supervise` rewrites it
through `gave_up`. `Life` is deleted, so that one is now unreachable rather than
refused.

### 12.10 The prefixes are public, and the claim about them was false

Three findings, one shape: the four `Failed` prefixes were a contract nobody could
name and a claim nothing checked.

- **They are `RemoteError::PANICKED`, `REFUSED`, `RESTARTING` and `DROPPED` now.**
  Private `const`s in `local.rs` for four waves, documented as a peer's
  discriminator in four rustdocs, while every caller — including this repository's
  own tests — hard-coded `starts_with("dropped: ")`. Exporting them moves no ALPN:
  `postcard_schema` hashes an enum's variants and fields, and an associated
  constant is neither. `keylink/tests/protocol.rs`'s two pins are unmoved,
  checked rather than assumed.
- **"Every prefix a `RemoteError::Failed` carries on the served path" was false.**
  keylink builds a `Failed` from at least three other places whose text is a whole
  *sentence*: the registry's capacity refusal, an `ActorId` a peer sent that fails
  validation, and a console follow refusal. None collides, and nothing would have
  noticed if one did —
  `no_failure_prefix_is_a_prefix_of_another` compares the four against each other
  and would stay green while a `RouteError` variant beginning `"dropped: "` made
  every peer's `starts_with` ambiguous. The claim is narrowed to *every prefix
  `rendered` and `lost` build*, and `no_other_served_failure_begins_with_one_of_the_five`
  renders the other three and checks them. What no guard can cover is a `Failed`
  whose text came from a caller's own `Middleware`, which is exactly why naming
  the constants is the other half of the fix.
- **The `dropped: ` sentence asserted a cause the arm cannot know — twice.** It
  read "the reply was taken out of the turn and never answered", the sanctioned
  `reply.later()` + `drop`. The first correction changed it to "the reply left its
  turn and was never answered" and this section claimed the sentence had stopped
  asserting a cause. **It had not.** That is the same claim in different words,
  and it is false of a *third* producer this arm has: a handler that panics
  **before** consuming the channel drops the `Reply` itself, inside the turn —
  never a `Deferred`, and the reply never left. `turn_deferred` had named that
  case since `#[defer]` landed, and
  `a_peer_whose_deferred_handler_panicked_reads_the_same_dropped_answer` is
  precisely the guard the reword shipped alongside, exercising the case the reword
  was false about.

  It reads **"the actor is serving and this reply was never answered"** now, which
  says nothing about where the reply died, because nothing at the point of death
  can. The three producers are named in the code rather than summarised. The
  served case had never been asserted at all before that guard: the existing test
  is a *local* call that checks `is_err()` on a `MailboxClosed` and never reaches
  `deliver_to_local`.

### 12.11 The path with no settle, which was on the side this wave called correct

Two panics reach no settle, because they reach no loop: a caller's `factory`,
awaited by `supervise` with no guard, and a caller's `Feeds::poll_next`, polled by
`next` inside a `poll_fn` with no guard — the mailbox turn has `catching` around
it and the second input does not. Either unwinds the actor's whole task. Nothing
publishes an ending and nothing settled the verdict, so the cell kept `Running`
and a peer whose reply died in that unwind was told `dropped: `, *the actor is
serving*, about an actor that would never answer again. The feed half reaches
**unsupervised local** actors, which have no supervisor anywhere near them.

**It was a regression, and that is why it is fixed here rather than filed.**
`deliver_to_local` used to read `is_running()` beside the verdict; an unwind drops
the `mpsc::Receiver`, and `Receiver::drop` closes the channel *before* it drains,
so `is_closed()` was already true when the dying reply woke its peer and the old
code fell through to `gone()`. Deleting the cross-check — correct on its own
terms, since the converse of that implication was the defect this wave exists to
fix — deleted the only thing covering an unwind. Reproduced by execution before
anything was changed: both new guards fail on the pre-fix tree with
`Remote(Failed("dropped: ..."))`.

The remedy is a `Floor`: a guard that settles `Gone` unless the code it guards
reached a verdict of its own. `run_loop` carries one, disarmed at the settle;
`supervise` carries one **per iteration**, armed across the factory call and
disarmed the moment the incarnation's own floor takes over. It rests on the
language's drop order — reverse declaration, and a body's locals before the
function's parameters — so a floor declared first in a body drops before `state`,
`inbox` and `feeds`. That is a floor under an explicit settle, not the mechanism:
on every path the loop reaches, the verdict is settled at the top of the teardown
and the floor is disarmed.

**Catching the two panics instead was considered and refused for this wave**, not
omitted: `guarded` around `factory().await` and `catching` around
`feeds.poll_next` would remove the unwind rather than floor it, and would turn
both into a named `Ended` a caller could await — but that changes what a panicking
factory and a panicking feed *mean* (`GaveUp`, and an ending, where today they
vanish the task), which is a supervision-semantics decision with its own guards to
write. The floor is correct under either and stays correct if they are caught
later.

**Where the floor goes took three attempts, and the two failures are the whole
lesson.** The shape is: **one floor per scope, disarmed immediately before each
way that scope returns, and nowhere else.**

1. *Armed for the whole of `supervise`, never disarmed.* It fires on the normal
   give-up return too, writing the right answer a moment after the wrong one — so
   breaking `Lent::ending`'s give-up arm left two end-to-end guards **green**. A
   floor that outlives what it guards launders a broken verdict.
2. *Armed per loop iteration.* This fixed attributability and opened a hole at
   each end of the iteration, both of them the **false** answer rather than the
   merely less useful one §12.4 accepts:
   - `continue` drops a loop-body local, so the **transient factory failure** — a
     documented path, non-fatal `Stop` with budget left — dropped it armed and
     wrote `Gone` over a live `Restarting`: *stop asking*, about an actor being
     rebuilt.
   - it was disarmed before `run_loop` and re-armed nowhere, so the **backoff
     between incarnations** had no floor at all: an `abort` there left
     `Restarting` standing for an actor that had been destroyed — *retry, another
     is coming*, about nothing.
3. *One floor for the function, disarmed only before each `return`.* Both windows
   are covered because the floor is never dropped except on the way out, and
   attributability holds because a disarm immediately precedes each return.

**A fourth shape was instructed and declined, and the decline turned out to be
load-bearing rather than merely economical.** The instruction was to add a third
disarm — around the `run_loop` call, since `run_loop` carries its own floor. It was
declined on the grounds that it buys nothing: the floor is not dropped there, so
leaving it armed costs nothing. That understated it. Disarming across `run_loop`
**reintroduces a defect**, and the case is one nobody had enumerated:

> a caller's state whose `Drop` **panics** on the restart path. `run_loop` settles
> `Restarting`, disarms its own floor, and then `drop(state)` — caller's code,
> outside any `catching` — panics. Drop glue continues during the unwind, so a
> `Deferred` parked in that state still dies, with `Restarting` in the cell and
> only `supervise`'s floor above it. Disarmed, nothing catches it and the peer
> reads *retry, another incarnation is coming* about an actor that will never come
> back.

Measured both ways: shipped gives `NoSuchActor`, with the third disarm
`Failed("restarting: a turn panicked: ..")`.

So the rule on `Floor::disarm` is *immediately before each way the scope returns*,
and **a nested call is not a way the scope returns**. That is the reading the rule
invites and it is wrong. `a_state_that_panics_on_drop_mid_restart_is_gone_not_restarting`
enforces it — break 23 is the third disarm, applied verbatim — because until it
existed this was the only line in the design whose correctness rested on a comment,
and the comment said *harmless to leave* where the truth is *required to leave*.
Nothing in the replay set had covered it: break 21 disarms at the declaration,
which is a different edit.

**And `LocalActor::abort`'s settle was deleted, which was a defect of mine.** I
judged it unreachable because no break could redden
`an_aborted_actor_tells_a_peer_no_such_actor` — a test that aborts an actor
sitting *inside* `run_loop`, where the floor genuinely is earlier. The line also
covered the window where `run_loop` does not exist, and **that window had no
guard**: twenty recorded breaks and not one touched it. The line was not
unguardable, it was unguarded. `a_supervisor_between_incarnations_is_gone_when_it_is_destroyed`
is the guard it never had, and the remedy is attempt 3 above rather than the line
coming back — because with the floor armed across that window the line is
genuinely unreddenable again, and a second writer nobody can hold to account is
what got deleted in the first place.

The general rule this pair yields, which is worth more than either fix: **a line
is only "unreachable" for the windows the guard you checked actually enters.**
"No break of this reddens any test" and "no break of this can redden any test" are
different claims, and the first was mistaken for the second.

`System::abort`'s and `drain`'s settles survive on a narrower claim than they were
first documented with — they answer for a **hand-written** `mount` loop, which has
no `run_loop` and therefore no floor. Their guards drive one for that reason, and
both inline comments say so now.

### 12.12 Why this suite could not have caught it: one `multi_thread` in the whole tree

`rg -n 'multi_thread|flavor' keylink/tests` returned exactly one hit before this
wave, in `singleton.rs`. Every other test runs on a current-thread runtime, where
an actor's entire teardown — hook, observers, every drop, the return, the task's
exit — completes before any other task is polled. A peer therefore always reads
the *last* value written, and every ordering defect in that teardown reads
correct.

That is the mechanical reason the regression shipped, and it is also why a
previous record in `defer.rs` drew a conclusion its harness could not support: it
recorded that breaking the old arm-2 guard to `if true` left
`a_stopped_actor_still_tells_a_peer_no_such_actor` green and concluded "the `Life`
read is late enough here". `Ended` won by *scheduling*, not by ordering. On more
than one worker the same code reads `Life::Running` and is answered correctly
through a different arm. Writing that second sentence is what would have exposed
Finding 1 a wave earlier.

`a_remote_caller_can_tell_restarting_from_gone` and its keyed twin run on four
workers now. Under the fixed code both are deterministic, because the answer is
settled before the window opens rather than raced against it — which is the
difference between a guard and a coin toss, and is the argument for fixing the
*ordering* rather than narrowing the *window*.

---

## 13. Revision, 2026-08-07: the ALPN literal this document names throughout has moved

A forward pointer, and **not one claim above is edited**. Sections throughout this
document say `keylink/Probe/d3fe34471921164e` is *"unchanged and green"*, and §7 says
it beside `keylink/Inspect/7bbd34fa83c9060c`. Deliberately not counted here: how many
sites there are is a value this section does not control, which is the mistake §9.1 of
`docs/rfc-08-serving-side-keys.md` exists to record. `rg` gives the current number. Every one of those claims is still true:
the wave it describes did not move the pin.

**The literals are now `keylink/Probe/d75913b8a4742547` and
`keylink/Inspect/0572763878b7fb9d`.** Substituting them above would be worse than
leaving them, because each sentence asserts that *its own* wave moved nothing, and
writing today's value into it would make it assert something about a value that wave
never saw. So this section exists purely so that a reader grepping the old literal
finds out here instead of concluding the tree has drifted.

The cause is in neither this document nor the waves it records: `IdempotencyRefusal`
gained a `NotKeyable` variant, and it sits inside every service's hashed contract. See
`docs/rfc-08-serving-side-keys.md` §4, and RFC-05 §13.

The general form is worth keeping, because this will recur: **appending to a drift
narrative does not find the citations elsewhere in the file.** After any pin move,
`rg <old-literal>` across the repo, and every surviving hit must be either inside a
history entry describing that transition or a deliberately-signposted forward pointer
like this one. Better still, prose should cite the **guard** — here
`tests/protocol.rs::the_contract_hash_has_not_moved` — rather than its value, and then
there is nothing to sweep. Where the literal *is* the record, as in RFC-05 §12 and
RFC-04 §16.13, it cannot be removed and a pointer is the only honest treatment.

---

## 14. `compose::Group` — §9.8's caveat, built as fate sharing rather than as a strategy

Appended rather than edited, per `CONTRIBUTING.md:322`. Not one claim in §8 or §9
is touched: what they say is what was believed when those waves landed, and this
section is what building the caveat found afterwards. Read together, the pair is
the record.

**Numbered 14 rather than 9.11, deliberately.** The next unused §9 subsection
number *is* 9.11 — §9.10 is the last one — but this file's habit for later work
that overturns an earlier section is a new top-level section that answers it by
name, not a subsection slotted back into the middle. §11 does exactly that to
§10.8, and §12.7 corrects §9.10 from inside §12 rather than from inside §9.
Appending means appending.

### 14.1 What shipped

`keylink::compose::Group`, in `keylink/src/compose/group.rs`. It is a **plain
struct and not an actor**: an `Arc` of shared state and one spawned task. No
mailbox, no `service!`, no ALPN, no turn, and nothing on the wire.

- **The policy is caller-written and synchronous.** `Group::new` takes
  `impl FnMut(&ActorId, &Ended) -> Verdict + Send + 'static`. It returns a
  verdict rather than a future, and it runs on the group's own task.
- **Exactly two verdicts**, and there is no third. `Verdict::Forget` carries on
  without that member; `Verdict::StopAll` retires every other member and ends the
  group. §14.5 is the argument for the ones that are not there.
- **One conductor task decides everything.** `conduct` waits on two inputs with
  no `select!` — `tokio`'s `macros` feature is off for `wasm32-unknown-unknown` —
  so it is the hand-written `poll_fn` the actor loop already uses.
- **A member is two values, not a handle.** `Membership::new` takes the actor's
  own ending and a closure that ends it. The motivating pair, `Reserve` and
  `Charge`, are different services with different handle types, so there is no
  one `H` to be generic over. That is why this is not `Pool<H>`'s shape.
- **Teardown is reverse insertion order, issued concurrently.** `closing` drains
  the member `Vec` `.rev()`; `retire_all` builds every retire future before
  polling any.

Cited by item name rather than by line, on purpose: these are the durable
anchors, and a line number in a file that is still growing is a citation that
rots. `local.rs`, `blocking.rs` and `names.rs` are unchanged by this work, so
those keep their lines.

**The policy is asked once per member, and no filter here decides which endings
are final.** A supervised actor's incarnations run inside one task and only that
task's exit publishes an ending at all — §9.2's third property, and
`a_restart_is_not_an_ending` is that sentence as a test. So every ending a group
is handed already is a final one, and a member's own restarts are structurally
unable to reach the policy.

### 14.2 §9.8's caveat, answered — it named three wanted things, and this is all three

§9.8's caveat (lines 2690-2695 of this file) is the authority this feature exists
under. It says the dead-pid argument *"defeats sibling-restart strategies and
does not touch **ordered shutdown, escalation, or lifecycle grouping**. Those are
judged wanted."*

| the caveat's word | what delivers it |
|---|---|
| **lifecycle grouping** | the type. A `Group` is a set of actors held as one thing, with `add`, `remove` and `names` over it |
| **ordered shutdown** | `closing` drains the member `Vec` in `.rev()`, so retirement is *issued* newest-first. Members live in a `Vec` and not a map precisely so that there is an order to reverse |
| **escalation** | a group held as a member of another group. `StopAll` ends the child; the parent watches the child's ending and decides in turn |

**Escalation is a composition rather than a variant, and its mapping is lossy
rather than free.** `Group::ended` yields a `GroupEnded` and `Membership::new`
wants an `Ended`. There is no total mapping and `Ended` has no variant meaning
"a group ended", so the caller writes the lossy one it wants. The module doc
carries that example rather than claiming nesting is free.

The caveat's own closing sentence is the line the whole design divides on: *"a
group that shuts down in an order is a statement about several actors, and
supervision as built is a statement about one."* It divides on **what the thing
does** — restart versus grouping — and not on which module it lives in. That is
why this half was buildable while the restart half was not.

### 14.3 §8.7's "Supervise anything" row is now false, and this is what replaced it

Lines 2270-2278 of this file say, of Wave 7:

> **Supervise anything.** … `compose` still holds no supervision mechanism of its
> own, and that is the design rather than an omission.

**The last clause is now false.** `compose` holds one. The row's two halves were
always different claims and only one of them fell:

- **Still true, untouched.** `Pool` restarts nothing, `pick` keeps handing back a
  stopped actor's handle and the call answers `MailboxClosed`, `Names`
  unregisters on ending and does not re-spawn, and a member built with the
  generated `supervised()` restarts *itself* with nothing in `compose` knowing.
- **Now false.** That `compose` holds no supervision mechanism of its own.
  `Group` is one. It restarts nothing — that is still the member's own job — but
  noticing that a member ended and acting on it across a set is a supervision
  mechanism, and calling it composition would be a word doing work the code is
  not doing.

The same sentence sat in `keylink/src/compose/pool.rs`'s "Not a supervisor"
bullet and is corrected **in place** there, in the commit that lands the module.
The asymmetry is the house rule and not an inconsistency: an RFC records what was
believed and is appended to; a doc comment is what a user reads as currently true
and is corrected.

### 14.4 §8's count, against the unit it names

§8's opening (lines 2060-2062) says `keylink::compose` is *"one `pub mod` with
four types and two functions in it"*, under a first line calling them *"five
patterns"*.

**Name the unit or the correction is worthless.** That sentence counts **headline
types, one per pattern**, and by that unit it is now **five types and two
functions** over **six patterns**. By the unit of public exports it is a
different number and neither corrects the other: `compose` re-exported eight
types and two functions, and now re-exports thirteen and two, because `Group`
brought five public names — `Group`, `GroupEnded`, `Membership`, `Retire` and
`Verdict`.

`Retire` is the one a reader would not have predicted from the design, and §14.7
is where it came from.

**Appended 2026-08-09 — both counts above have moved, and only one of the moves
is this note's own change.** Nothing above is edited; this is what is true now.

`Group` brought **six** public names and not five. `Added` joined `Group`,
`GroupEnded`, `Membership`, `Retire` and `Verdict` in `47decc9`, when
`Group::add` stopped answering `Option<Retire>` — where a displacement and a
refusal were both `Some` — and started answering a three-variant enum. §14.7's
reason for singling `Retire` out is untouched by it: `Added` is a name a reader
*would* predict from a method that can do three things.

The export count moved **twice**, and the earlier move is not this branch's.
"Thirteen and two" was measured true at `7a48207`, the commit that wrote this
section. `e801cca` then added `compose::Endings`, taking it to fourteen, and
`Added` takes it to **fifteen types and two functions** — counted from
`keylink/src/compose/mod.rs` on 2026-08-09, not derived from the sentence above.

The headline-type count in the same paragraph — "five types and two functions"
over six patterns — has **not** moved. `Added` is not a pattern and not a
headline type; it is part of `Group`'s door. Two counts of one module moving
independently is exactly what naming the unit was for, so this note names its
unit too.

### 14.5 Every restart verdict refused, and the sharpest reason is a visibility one

Three were proposed. All three are gone.

**`RestartAll` (`one_for_all`) and `RestartAfter` (`rest_for_one`)** fall to
§9.8's own line — the dead-pid argument defeats sibling-*restart* strategies —
and §9.2 (lines 2513-2519) is that argument in full. Two further reasons, neither
of which is that line: rebuilding every member from its own commit point is
**not** a consistent cut, because members commit independently and nothing
coordinates them; and `RestartAfter`'s ordering is a *startup dependency*, which
a group would be inferring from insertion order — a different fact wearing the
same shape.

**A single-member `Verdict::Restart`** is the one that had to be argued rather
than cited. It survived the first draft. Three independent facts killed it.

1. **It could not be bounded, and the reason is module visibility rather than
   design.** The draft bounded group restarts with an `Intensity`. `Intensity`'s
   entire public surface is `pub fn new` — `keylink/src/local.rs:2009`, and it is
   the only `pub` item in that `impl` block. `backoff` is private
   (`local.rs:2028`) and `Budget` is a private struct (`local.rs:2274`).
   `compose` is a sibling module, so it cannot spend an `Intensity` at all. The
   promise *"nothing in `local.rs` changes"* and the promise *"group restarts are
   bounded by an `Intensity`"* could not both hold, and the draft made both.
2. **It re-creates the stale reference this crate does not have.**
   `spawn_supervised` builds a fresh `mpsc::channel` (`local.rs:2918` and
   `local.rs:2924`), so a group-driven rebuild produces a new mailbox and every
   handle a caller holds is dead. Issue #3's dead-pid argument is about a problem
   keylink *avoids*; a group restart would import it.
3. **It bought one shape and cost seven.** Given §14.6's limit, the only safe
   shape was a local member republished through `Names` — a call site that writes
   the same `ActorId` twice and calls `ended()` twice on one actor. It paid for an
   `Intensity` argument, a re-entrant `FnMut` factory that can panic on the
   group's own task, a precondition no type catches, an `OutOfRestarts` ending,
   and a policy blind to the budget it spends.

**Dropping it cost nothing that §9.8 asked for.** Escalation is delivered without
it (§14.2), and dropping it makes **adoption the only mode**: `add` takes a
`Membership` built from an actor that is already running, so there is no factory
anywhere in the surface.

### 14.6 The limit: a keyed actor cannot be a member, and it is stated rather than discovered

`Membership` needs the member's ending, and **a keyed actor has no `ended()` to
ask for**. `keylink/src/compose/names.rs:129` says so one type over, and §5.7
(line 1235) has listed it as missing since Wave 3: *"Give a keyed actor an
`ended()` channel. It is the one thing `spawn_wired` does not deliver."* §9.10
records it as still Wave 9's.

So `Group` watches **local actors only**, today. What closes it costs nothing
here: `Membership::new` takes the ending **as an argument** rather than asking a
handle for one, so it needs no change the day §5.7's gap lands. A constructor
that took a handle would have needed one, and an earlier draft had exactly that
constructor.

### 14.7 Two claims narrowed under review, which is the part of this record worth keeping

Both were written as impossibilities. Both are weaker than that. Recorded because
a false impossibility is the kind of claim somebody later builds on.

**"`A → B → A` is unwritable" was false.** A synchronous policy cannot *await*,
which is what the draft meant, but nothing stops it from **blocking**: a
`std::sync::Mutex` an actor holds across an await needs no dependency at all, and
a blocking executor entry point is already in this tree's dependency graph —
`block_on` reaches `n0_future::future` through its glob re-export of
`futures_lite::future` (`n0-future-0.3.2/src/lib.rs:79`, and
`futures-lite-2.6.1/src/future.rs:52-53`, where it is `pub` under
`#[cfg(feature = "std")]` rather than unconditional, which the draft also had
wrong).

What survives a hostile reader is narrower, and it is what the module claims:

> **No cycle can be built out of keylink calls.** The policy has no await point,
> and a group is never *told* anything — it only observes endings. There is no
> call from a member into a group for a cycle to close through …

The sentence continues in the module, and it is the rest of the answer: keylink's
own synchronous API cannot open one either. `blocking::Runtime::run`
calls `guard()?` and answers `Error::BlockingInAsync`
(`keylink/src/blocking.rs:77-80`), and a *local* handle has no blocking method at
all. The residual is stated rather than denied: caller code that blocks inside
the policy stalls the group, because the group takes one decision at a time in
one task. **A caller error with a symptom, not an impossibility.**

**"`abort()` + `ended()` solves the teardown race" proves less than it sounds
like.** It proves the member's **task is gone**, via the dropped-sender path
(`local.rs:3265`). It does **not** prove that the member's observers released
what they held, and the tree already says so on `stop`'s own documentation
(`local.rs:3282-3284`): *"Waits for the receiver to be dropped, which is the loop
ending — not for the state's own `Drop` to have run. A state that flushes in
`Drop` needs its own signal."* So a durable adapter's file claim can still be
open at the moment a group reports that it ended. `Retire::retire` carries that
limit at the door a caller actually calls, rather than leaving it one type away.

The recommendation to build a `Membership` with `abort` rather than `stop` has
evidence and not taste: `stop` puts a message in the mailbox, and `supervise`
polls the inbox only inside the run loop and never across a backoff sleep — so a
`stop`-based teardown of a member sitting in backoff cannot return until that
backoff elapses.

### 14.8 No criteria-and-breaks table here, and the absence is deliberate

§9.9 and §12.8 are this file's habit: a criterion is recorded beside the break
that was **watched** to redden it. At the time this section was written
`keylink/tests/compose.rs` contains no `Group` test and no break has been run.
A table here would be a claim about work that has not happened, which is the one
thing §9.9 exists to prevent. It is owed, and it belongs to the commit that lands
the tests.

One thing is already known about it and is cheaper to say now than to discover
twice: `scripts/break-once.sh` substitutes one literal, so a claim whose defect
is a *restructure* cannot be reddened by it at all. §8.7's own note about
`two_clones_of_a_pool_share_one_rotation` (lines 2279-2284) is both the precedent
and the treatment — say so here, rather than count it as a proven guard.

---

## 15. Revision, 2026-08-09: §4.11's twin-guard bullet — one count corrected, and the name kept

Appended, and **§4.11 is not edited**. Both halves below are about the same
bullet, the one for
`turns::every_turn_lost_except_faulted_has_one_undelivered_twin_with_the_same_words`.

`CONTRIBUTING.md`'s carve-out was checked first and reaches neither half. It
permits renaming *an identifier a break record names*, "because that changes a
pointer and not a claim". §15.1 corrects a **count**, which is a claim and not a
pointer. §15.2 renames nothing. So an append is the only treatment available for
either, and that is what this is.

### 15.1 "a seventh variant in either enum is `E0004`" holds for one enum, not two

§4.11's bullet says:

> Two exhaustive matches, so a seventh variant in either enum is `E0004`.

Measured on `d2f5e6e`:

```
rg -n -A100 '^pub enum TurnLost'    keylink/src/observe.rs | rg '^\s*[0-9]+[-:]\s{4}[A-Z]'
rg -n -A70  '^pub enum Undelivered' keylink/src/turns.rs   | rg '^\s*[0-9]+[-:]\s{4}[A-Z]'
```

`TurnLost` has **seven** variants: `Gone`, `Dropped`, `Restarting`, `Refused`,
`Faulted`, `Expired`, `Reentrant`. `Undelivered` has **six**, that list without
`Faulted`. So a seventh in `Undelivered` is `E0004`, and a seventh in `TurnLost`
is what already shipped — Wave 9's `Restarting`, which the two matches absorbed
without the sentence being revisited.

**The guard did not rot; the number describing it did.** The mechanism is
unchanged and this file is the only site carrying the figure. `keylink/src/turns.rs`
states the same fact numberlessly, in the record above the test — *"two exhaustive
matches, and both are `error[E0004]` the moment either enum grows"* — and that
sentence is still exactly true. Read §4.11's bullet that way. The neighbouring
claim about `EVERY[6]` is a different one, about a six-slot array, and the record
beside the test already withdraws the compile-error part of it.

**The general form, which is §13's lesson arriving a second time.** A count of a
thing this document does not own is a citation that rots on somebody else's
commit. §13 recorded it for a moved pin; this records it for a variant count. Cite
the guard rather than the value it currently produces, and where the value *is*
the record — as it is in a break record's watched output — it cannot be removed
and an appended correction is the only honest treatment.

### 15.2 The guard's name says two enums, its body holds three, and the name is kept

The test is the twin guard, and the decision recorded here is **not to rename
it**. `keylink/src/turns.rs` already carries the one-line version of this beside
the test — *"the name still says undelivered twin, which now undersells the body
by one enum"* — and this is the fuller statement, in the file the name is a
pointer in.

The three enums are `TurnLost`, `Undelivered` and `NotQueued`, and the name says
two: `NotQueued` reaches this guard through `From<NotQueued> for Undelivered`
rather than through `twin`, so it is held as a widening and not as a twin. The
name is a filter and a pointer in §4.11, and under-claiming is the safe direction
for both.

**What the rename would have cost, measured rather than estimated.** This section
aside, five sites name it: the `fn`, two `NotQueued` doc paragraphs, §4.11's
bullet, and one row in
`docs/plans/2026-08-09-items-ef-a-lost-reply-is-not-a-gone-actor.md`. It is in no
operational registry — zero hits in `.mise/tasks/anchors` and zero in
`scripts/replay-fate-breaks.sh`. No `fn` name in `keylink/src`, `keylink/tests`,
`keylink-persist` or `demo` stands in a prefix relation with it, in either
direction; the count of those names is deliberately not written here, because it
is a value this section does not control. So no `cargo test` filter would widen.
§4.11's own record states its filter *by relationship* — "the filter for all three
is this test's own name" — so it survives a rename untouched, which is the one
site a rename normally breaks.

**So the rename was cheap and was still refused.** A name that asserts less than
its body costs a reader one sentence, and that sentence is now written twice. The
failure this crate keeps having is the opposite one: names and numbers that assert
more than the tree can support — §15.1 immediately above is an instance. Buying a
rename here would spend an edit on the safe direction of an error whose expensive
direction is what the file is about.

---

## 16. Revision, 2026-08-09: §9.9's "fifteen tests" was exact, and nothing in the sentence says when

§9.9 opens *"Fifteen tests in `keylink/tests/supervision.rs`, every one of them
watched to fail under a break."* Measured, both ends:

```
git show 75155e3:keylink/tests/supervision.rs | rg -c '^#\[(tokio::)?test'   # 15
rg -c '^#\[(tokio::)?test' keylink/tests/supervision.rs                      # 20
```

`75155e3` (2026-08-07) is the commit that wrote the sentence. **It was exact when
written.** The file holds twenty today, and §9.9 is not edited, because fifteen is
what Wave 9 recorded of its own work and the five since are not claims Wave 9 made.

**The defect is that a reader cannot tell those two apart.** A count that was true
when written and is never re-derived reads exactly like a count that is current,
and this sentence gives a reader nothing to distinguish them with. Somebody who
counts today and finds twenty has three readings available — the record rotted,
the record was always wrong, or the tree moved — and the sentence does not say
which. That is the whole cost, and it is paid by the reader rather than by the
author.

`.mise/tasks/anchors:357` solves the same problem in one clause: *"EVERY FIGURE
BELOW WAS RE-RUN AND RE-TRANSCRIBED AT `82b8366`"*. A reader who disagrees with a
figure under that umbrella knows at once which side is the measurement and which
is the record. No section of this file carries such a marker.

**So: §9.9's fifteen is pinned, here, to `75155e3`.** Anyone counting
`keylink/tests/supervision.rs` and finding a different number has found later work
and not rot.

### 16.1 The general form, stated once, because this is not the first

Three sections of this file now correct the same shape. §13: a pin moved, and the
sentences naming its value went stale without one of them being wrong about its
own wave. §15.1: `Undelivered` and `TurnLost` stopped having the same number of
variants, and a sentence saying "either enum" outlived it. §16: a test file grew
past a count taken of it. Different subjects, one failure — **a figure about the
tree, written into a record, is a measurement of the moment it was taken, and
nothing in the sentence says so.**

It has two halves and both are cheap:

- **Writing one.** Say what it was measured at. *"Fifteen tests, at `75155e3`"*
  costs two words and cannot rot afterwards, because it stops claiming anything
  about today. Better still, cite the guard rather than the count — that is §13's
  own advice, and it needs no maintenance at all.
- **Finding one.** Append, as this section does. The figure stays where it is,
  because it is what the wave believed and that is the value of the record. The
  appended section says what is true now, and why both are right.

`CONTRIBUTING.md`'s `## RFCs` section now carries the first half as a rule, so the
next author meets it while writing the figure rather than after a reader trips on
it. It carries no licence for the second half beyond what the append-only rule
already gives, which is deliberate: correcting a figure is still an append, and
this section is one.
## 17. Revision, 2026-08-09: §4.11's twin guard gains a seventh slot, and its watched array with it

Appended, and **§4.11 is not edited** — the same discipline §15 states, for the
same bullet, and §15.1 is the section half of this one continues.

`CONTRIBUTING.md`'s carve-out reaches the two table rows in §"`scripts/replay-fate-breaks.sh`
runs all twenty-three" and one prose pointer, and nothing else here. Those three
name tests that were **renamed** —
`remote::no_other_served_failure_begins_with_one_of_the_four` and
`reply::the_four_prefixes_are_the_text_a_peer_matches_on`, both `_five` now,
because both embedded a count in the name and the fifth served prefix landed. A
replay row whose filter names a test that no longer exists selects **zero tests**
and reports success, so those are pointers that must stay runnable. Everything
below is a claim and is appended.

### 17.1 Each enum gained one variant, so §15.1's one-variant gap is preserved

§15.1 measured, on `d2f5e6e`, that `TurnLost` had seven variants and
`Undelivered` six — and concluded that §4.11's *"a seventh variant in either enum
is `E0004`"* held for one enum and not two.

`ObserverPanicked` landed on **each** of them, so that a caller whose turn met an
observer that panicked is no longer told the observer refused. Counted at the
tip: `TurnLost` has **eight** — `Gone`, `Dropped`, `Restarting`, `Refused`,
`ObserverPanicked`, `Faulted`, `Expired`, `Reentrant` — and `Undelivered` has
**seven**, that list without `Faulted`.

**So the asymmetry §15.1 found is not closed; it is preserved exactly.** One
variant on each side leaves the gap where it was, and the gap has a name:
`Faulted` is the one `TurnLost` state `ask` resumes rather than maps, which is
the whole reason `twin` returns `Option`. The guard below says so in its own
arithmetic — `lost` holds eight entries against `EVERY: [Undelivered; 7]`, and
the body asserts `without_a_twin == 1`. A reader who took the two enums for the
same width would not expect `twin` to answer `None` at all.

§15.1's figures were exact at the commit it names and are not edited. What that
section says numberlessly is the part that survives every such move: *"two
exhaustive matches, and both are `error[E0004]` the moment either enum grows"*.
Read §4.11's bullet that way; the number in it was never the mechanism.

**What the compiler actually reported, which is not what a first draft of this
section claimed.** It said `slot` and `twin` were the only two `E0004`s inside
`keylink/src`. **No `E0004` fired in `keylink/src` at all**, because every arm
there was written before the first compile. The only two the compiler reported
were in `keylink/tests/cycles.rs`, at its two `SURPRISE` folds. That draft
asserted a measurement nobody took, in a section whose subject is a figure
carried instead of re-derived — so it is corrected here rather than quietly.

The exhaustive sites that had to gain an arm, listed because *that* is checkable
where a count of compiler errors is not: `LocalActor::ask` over `TurnLost` — the
only one in production code, and the stronger form of the argument — plus
`Answered::map` and `ask_catching`'s and `rendered`'s matches over `Answered`,
`Deferred::send` over `Verdict`, and `slot` and `twin` in `mod tests`. `EVERY`
and the `lost` sample grew by hand, which §4.11's own residual paragraph predicts
and which no compiler forces.

### 17.2 The watched array is a break record's output and cannot be corrected in place

§4.11's second break watches:

> `every variant is written exactly once, and this one was written 2 times:
> [2, 1, 1, 1, 0, 1] at slot 0`

That is what the run printed, and it stays. §15.1 states the rule it falls under:
*"where the value **is** the record — as it is in a break record's watched output
— it cannot be removed and an appended correction is the only honest treatment"*.

Re-run at `fdcf412`, with the same break re-expressed against the moved arm —
`slot`'s `Undelivered::Expired` points at `5` now, not `4` — the run prints:

> `every variant is written exactly once, and this one was written 2 times:
> [2, 1, 1, 1, 1, 0, 1] at slot 0`

Read off the failing run and not computed. Two things moved and only one of them
is the seventh slot: the **emptied** slot went 4 → 5 with `Expired`, while the
**reported** slot stayed 0, because the loop still reaches the doubled slot
first. A correction that had only lengthened the array would have been wrong in
the half that record already exists to warn about — §4.11 records that this break
was mispredicted once before, in exactly that field.

`keylink/src/turns.rs`'s own copy of the record was re-run and rewritten there,
because a break record beside its test is a live instruction rather than a
historical claim. This file keeps both readings, which is the difference between
the two homes.

### 17.3 §"the four prefixes" is a count and is not edited

Two sentences in the §9 bullet beginning *"keylink builds a `Failed` from at
least three other places"* say `no_failure_prefix_is_a_prefix_of_another`
*"compares the four against each other"* and describe the guard set as four. The
identifier is unchanged, so nothing is renamed and the carve-out does not apply.

**The fifth is `RemoteError::OBSERVER_PANICKED`**, and the set is whatever
`no_failure_prefix_is_a_prefix_of_another` spells out — cite the guard, not the
total. It is the first prefix in that set to share a word with another one, which is the whole
reason the by-hand list is the guard rather than the eye.

### 17.4 One sentence in that bullet now reads "five renders three", and that is the honest state

**Read this before trying to parse §9's line 3875.** After the rename above it
reads:

> The claim is narrowed to *every prefix `rendered` and `lost` build*, and
> `no_other_served_failure_begins_with_one_of_the_five` renders the other three
> and checks them.

Five renders three, and **both halves are correct in their own register.** The
identifier was carried forward under the carve-out, because a pointer that names
a test which no longer exists is a dead pointer — this branch measured what that
costs, and the answer is a replay stanza selecting **zero tests** while reporting
success. The figure was not carried forward, because a count is what a row
asserts. So the sentence is deliberately half-current and half-historical, and
this section is what tells a reader which half is which.

**What a reader who counts finds, and it depends on the unit.** Measured on
`keylink/src/actor/remote.rs`'s `others` array at `fdcf412`: **five rendered
values, from four places.** The values are the registry's capacity refusal, an
oversized `ActorId`'s validation failure, `FollowRefused::Ahead`,
`FollowRefused::Crowded`, and `Undelivered::Reentrant`. The two `FollowRefused`
arms are one place, which is the whole of the gap between four and five — and a
correction that had written one number without naming its unit would have been
wrong against the other.

**The "three" is not a slip and predates this branch.** The same bullet opens
*"at least three other places"* and then lists exactly three. Both figures were
written when three were known. The fourth — `deliver_to_local`'s re-entrancy arm
— was found later, and `remote.rs`'s own doc records the finding in the register
this file cannot: *"The fourth is built in `turns.rs`, and being the exception is
why this list missed it"*, beside the line that renders it. So the bullet is
internally coherent as a record of its own wave, and editing one number inside it
would leave the sentence above it claiming three as well.

Cite `no_other_served_failure_begins_with_one_of_the_five` and read the array,
which is the form §15.1 prefers and the only one that cannot rot: the guard
renders every case it knows about, and the assertion is against the bytes rather
than against a total.
