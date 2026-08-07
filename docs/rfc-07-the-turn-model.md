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

So the number is not a defect to fix in the adapter — it is the price of the promise.
It is recorded in `keylink/src/persist.rs`'s module documentation, in
`keylink/examples/durable_ledger.rs`, and reproducible with
`cargo test -p keylink --test persist -- --ignored --nocapture`, which prints it and
asserts nothing about it. **There is no wall-clock assertion anywhere in this wave.**

What the number decides: RFC-06 §7's debounce stops being a nice-to-have and becomes the
obvious next knob, and `Observer::finish` — which Wave 2d added for exactly that reason
— is already the guaranteed final flush it needs.

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

### 6.4 `dirty`, and why it is not RFC-06 §4.2's deleted `revision`

`Persist` carries a `bool` meaning **there is a committed change that is not on the disk
yet**. Set before the write, cleared after it, so a write that failed leaves it standing.

RFC-06 §4.2's `Durable::revision` was an implementor-supplied counter with nothing
connecting it to a mutation, and it was deleted for that reason. This flag is set by the
one place that knows a turn happened, and it is read in one place.

Today it costs nothing on every path but one. `after_turn` stores on every turn it
accepts and clears the flag, so an actor that stopped normally reaches `finish` clean.
The single path that arrives dirty is a `store` that returned `Err` — which ends the
actor immediately, so the state is complete and consistent and only the write is
missing, and one more attempt before the state is dropped is the difference between
losing the last committed turn and not.

**`finish`'s `Ended::Panicked` arm looks redundant and is not.** With a store on every
turn it cannot fire: a panicked turn is refused without setting the flag, and a failed
store ends the actor before another turn can panic. It is the rule a debounce needs. The
moment `after_turn` stops writing on every turn, "turn n completed and is unwritten,
turn n+1 panicked" is reachable, the flag stands over a half-applied state, and
`Ended::Panicked` is the only thing there that says so. Losing one committed turn is the
right trade against writing a state a restart would reproduce.

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
| two `open`s on one `StateId`, the second `Error::Busy` | `a_second_open_on_one_state_is_busy` | — the store's own `try_lock`, already red-tested in `file.rs` |
| a feed turn is stored on the same rule | `feeds.rs::a_feed_turn_is_stored_on_the_same_rule_as_a_mailbox_turn` | `after_turn` does not store → `something was stored` |
| a keyed actor's state survives its own process | `a_keyed_actors_state_survives_its_own_process` | empty hook slice → `left: [20], right: [10, 20]` |
| a non-probe `Observer` implementor exists | `rg 'impl.*Observer<' keylink/src` | `keylink/src/persist.rs` is the only hit outside `#[cfg(test)]` |
| `A: Sync` stays out of the design | `a_state_that_is_not_sync_is_still_persisted` | §6.5's two breaks |
| the per-turn cost is measured, not estimated | `the_per_turn_store_cost` (`#[ignore]`) | §6.1 |

`keylink/tests/durable.rs` is unchanged in behaviour and keeps committing once per run
through `Handle::with_state`: **`with_state` survives and is not deprecated.** It is a
*read*, and `representability.rs::a_read_cannot_mutate_either` still holds the claim.
What Wave 5 subsumed is using it as a **commit** mechanism, which is what the example
did and no longer does.

### 6.8 What Wave 5 did NOT do

- **Debounce.** The number in §6.1 is the argument for it and `finish` is the flush it
  needs, but nothing here batches. A caller who wants the other trade has it by choosing
  a different adapter, not by configuring this one.
- **Retry a failed store, except once at `finish`.** The `dirty` flag's one live path is
  untested: making a real `Files` store fail *transiently* is not something this suite can
  arrange — `limit()` fails permanently, and removing the directory breaks the lock the
  claim holds. Listed among the guards that were **not** watched to fail.
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

`Inbox::mark_busy` defaults to nothing, so a `LocalActor` — which has no registry entry
to report to — costs a no-op, and `Keyed<M>` forwards to the mailbox's flag. This keeps
every property the item wanted: one place, in keylink's own code, not in generated code
where two sites could drift. It adds the one the item's spelling could not have: both
inputs, by construction, because there is one dispatch.

Red-tested per arm, separately, because they are separately load-bearing: clearing the
flag on the mail arm fails the two mailbox tests and leaves the feed test green, and
clearing it on the feed arm fails only `busy_is_true_during_a_feed_turn`.

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
