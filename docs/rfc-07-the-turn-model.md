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

The generated enum therefore carries an owned discriminant, since `Context` is `Clone`:

```rust
#[doc(hidden)]
pub enum Origin { Local, Peer(Context) }

impl Origin {
    fn caller(&self) -> Caller<'_> {
        match self { Self::Local => Caller::Local, Self::Peer(cx) => Caller::Peer(cx) }
    }
}
```

The loop borrows a `Caller<'_>` out of the owned `Origin` for the length of one turn.

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
