# RFC-06 — Observers, and durable state as an adapter

**Status:** proposal, revision 4. Nothing here is implemented.
**Target:** `keylink` 0.1.0, tokio 1.52, edition 2021, rustc 1.97.
**Numbering:** RFC-05 was a design for durable actor state on a shared iroh-blobs
node. It was set aside before it reached `docs/`. The sketch is in `.tmp/rfc-05/`,
which is not tracked. Section 8 records what was kept and what was dropped.
**Recommendation in one line:** give keylink **one** generic turn-level extension
point with two methods, and ship durability outside keylink with **one** policy —
store after every turn, before the reply.

**Revision 2** answered an adversarial plan-gate review that found 7 Critical and 11
Major faults. **Revision 3** answers a targeted re-verification of the four fixes that
changed the design. Three held. One did not, and it did not fail on a detail: the
turn future stopped being `Send`.

The failure mode is the same one both revisions were warned about, so it is written
at the top. **Revision 1 proved its hook with a probe that used a stand-in `Panic`.
Revision 2 kept the stand-in and proved more with it.** The real
`Panic` (`actor.rs:378`) holds `Box<dyn Any + Send>` and is therefore not `Sync`, so
`TurnOutcome::Panicked(&Panic)` made `TurnInfo<'a>` non-`Send`, and the loop holds
that value across the observer await. Reproduced and then fixed in
`.tmp/probe-observer/realpanic.rs`. Section 9 makes "no stand-in types in a probe" a
rule rather than a lesson.

**Revision 4** answers a final pass built entirely from real types — the real `Panic`,
a `Send + !Sync` claim with `store(&mut self)`, and a running `tokio::sync::watch`.
Two questions confirmed: the turn future is `Send`, and `A: Sync` appears nowhere in
the design. Two refuted, both in §2.4 and §2.7, and both cheap. The §2.4 fault is the
serious one: revision 3 listed two edits where three are needed, and the missing one
turns an existing test red in one of two teardown orders that nothing in the codebase
pins.

---

## 0. Summary

`keylink-persist` (commit `dab3b58`) is a port and a file adapter. `Handle::with_state`
(commit `e2cf3bc`) is the seam that drives it. What is missing is the part that makes
durability automatic: something inside the turn loop that knows a turn happened.

This RFC adds that as a **generic port**, `Observer`, and makes durability the first
adapter that implements it. keylink learns nothing about storage.

Revision 1 also proposed a background writer with a quiet window. That is cut. Section
7 records the cut and the three conditions that bring it back.

**One honest note on the port's justification.** Revision 1 claimed the README's
idempotency gap was a second consumer. It is not: the ledger lives on `ServiceBuilder`
(`rpc.rs:86`, `rpc.rs:206`), keyed by `(peer, key)`, and is not part of the state `A`
an observer sees. Closing that gap needs a different port. So this port has **one**
consumer today, and the argument for making it generic is section 2.1's boundary rule,
not a queue of waiting adapters.

---

## 1. What is being asked

The core must be closed to modification and open to extension. Durability must attach
from outside. Other concerns must attach later by the same route.

Durability has exactly one behavior: **store after every turn, before the reply**.
There is no policy, no window and no lag.

**Scoped precisely**, because revision 1 overclaimed this in three directions:

> On the **local handle**, a call answered `Ok` is a call whose state was stored.

Three exceptions, all of which exist today and none of which this RFC introduces:

- A `tell` is answered `Ok` when it is **queued**. `local.rs:243-256` says so already.
- A served call refused by a layer such as `Timeout` is answered `TimedOut`, and the
  turn runs and is stored anyway. `macros.rs:626-649` documents that already.
- A served call whose turn was refused by an observer is answered `Failed`. §2.6.

---

## 2. The port: `Observer`

### 2.1 The boundary rule

keylink will have two extension points. One sentence tells a maintainer which to use:

> `Middleware` intercepts a **message on the wire**. `Observer` watches a **turn
> against the state**.

An `Observer` never sees a request or a reply payload. A `Middleware` never sees the
state. The two do not overlap, and neither one grows toward the other.

### 2.2 The trait

```rust
// keylink/src/observe.rs

/// The future an observer returns. Concrete, because `middleware.rs`'s
/// `BoxFut` is `pub(crate)`, takes no lifetime, and is `'static` — it cannot
/// hold `&'a A`, and widening it would move the public `Next` type.
pub type TurnFut<'a> = Pin<Box<dyn Future<Output = Result<(), Stop>> + Send + 'a>>;

/// Something the actor owns that watches its turns.
///
/// `Send` and not `Sync`: the actor owns the observer, so nothing is shared.
pub trait Observer<A>: Send + 'static {
    /// Names this observer in [`Ended`] and in diagnostics.
    fn name(&self) -> &'static str;

    /// Called inside the turn, after the handler, before the reply.
    ///
    /// Returning `Err` ends the actor. Observers run in the order they were
    /// given, one after another, and the first `Err` stops the rest.
    ///
    /// This is a plain `fn` and not an `async fn`, so an implementation can do
    /// synchronous work with `state` **before** it builds the future. An
    /// implementation that holds `state` across an await needs `A: Sync`. One
    /// that does not, does not.
    fn after_turn<'a>(&'a mut self, state: &'a A, turn: TurnInfo<'a>) -> TurnFut<'a>;
}

/// What the turn did. `#[non_exhaustive]`, so a later field is additive.
#[non_exhaustive]
#[derive(Clone, Copy)]
pub struct TurnInfo<'a> {
    pub outcome: TurnOutcome<'a>,
}

#[derive(Clone, Copy)]
pub enum TurnOutcome<'a> {
    Completed,
    /// The message, and **not** the payload.
    Panicked { message: &'a str },
}
```

`TurnOutcome::Panicked` carries `&'a str` and this is load-bearing, not cosmetic.
`Panic` holds `Box<dyn Any + Send>` (`actor.rs:378-385`), so `Panic` is `Send` and
not `Sync`, and `&Panic` is therefore not `Send`. The loop holds `TurnInfo` across the
observer await, and the private turn alias requires that future to be `Send`. Carrying
`&'a Panic` does not compile. Reproduced and fixed in `.tmp/probe-observer/realpanic.rs`:

```
error: future cannot be sent between threads safely
 = help: the trait `Sync` is not implemented for `(dyn Any + Send + 'static)`
 77 |  let info = TurnInfo {
    |      ---- has type `TurnInfo<'_>` which is not `Send`
 88 |  match guarded(observer.after_turn(&*state, info)).await {
    |                                                    ^^^^^ await occurs here
```

Withholding the payload is also correct on its own terms. `ask` resumes it in the
caller, and `Panic::caught`'s doc (`actor.rs:387-397`) exists to stop a payload being
resumed twice.

`Panic` gains one accessor, because `Display` cannot lend a borrow:

```rust
impl Panic {
    /// What the handler panicked with, for an observer that must not take the
    /// payload.
    pub fn message(&self) -> &str;
}
```

`TurnInfo` carries **no** sequence number and **no** method name. Revision 1 had a
`seq` field described as "counts from zero, per actor", which is wrong wherever it
could be built: the closure is created on the **caller's** task, on any of N cloned
handles. Both fields are additive later, which is what `#[non_exhaustive]` is for.

```rust
/// An observer gives up, and says why.
pub struct Stop(Box<dyn Error + Send + Sync>);

impl Stop {
    /// The only route for an observer that fails with a message rather than
    /// with an error type.
    pub fn new(because: impl Into<Box<dyn Error + Send + Sync>>) -> Self;
}

impl<E: Error + Send + Sync + 'static> From<E> for Stop { /* ... */ }
```

`Stop::new` is not a convenience. The blanket `From<E>` above makes **every** other
`From` impl a coherence error, so `From<String>` and `From<&str>` can never be added.
Without a constructor, an external observer that fails with a plain message must build
the `Box` by hand.

The generated module gains one constructor beside `actor`:

```rust
let ledger = ledger::actor_with(state, vec![Box::new(saver)]);
```

`actor(state)` stays, and calls `actor_with(state, vec![])`.

The `LocalActor` constructor behind it is `spawn_observed`, **not** `spawn_with`.
`SystemBuilder::spawn_with` already exists (`actor.rs:846`) and means something else —
a factory for keyed actors. Two meanings on one name in one crate is a reading hazard
that costs nothing to avoid now and cannot be fixed after 1.0.

### 2.3 The actor owns its observers

`after_turn` takes `&mut self`, and the loop owns `Vec<Box<dyn Observer<A>>>`.

Revision 1 had `&self` and `Observer: Sync`, which does not work.
`keylink_persist::Claim` is `Send + 'static` and **not** `Sync`, and
`Claim::store(&mut self, ..)` takes `&mut self` on purpose — its doc says a claim is
exclusive and the type should say so. A shared observer would need a
`tokio::sync::Mutex` around the claim to reach it.

Owning it instead buys two things and costs one:

- No mutex, and no `Sync` bound anywhere.
- The claim dies with the actor, which §2.4 makes true rather than merely likely.
- The observer cannot be shared across two actors. That does not matter here: a claim
  is one state, and a metrics observer holds an `Arc<Registry>` internally.

### 2.4 `stop()` releases the claim before it returns

`local.rs:286-289` states the hazard in its own words: `stop()` "Waits for the receiver
to be dropped, which is the loop ending — **not** for the state's own `Drop` to have
run. A state that flushes in `Drop` needs its own signal."

A claim is exactly such a value. Under §2.3 it lives inside the observer, which the
task drops during teardown — after `mail.closed()` resolves. So `stop().await` could
return while the file lock is still held.

That is not theoretical. `durable.rs:117-129` calls `run()` three times against one
`StateId`, and each `run` ends with `stop().await` and the next one claims again. The
second claim would intermittently get `Busy`, which names the wrong cause.

**Three** edits fix it, all inside `local.rs`, and the order between them is the fix:

```rust
drop(hooks);                     // releases the claim
drop(inbox);                     // closes the mailbox BEFORE anyone is told
let _ = ended.send(Some(reason));// and only now does `stop()` return
```

`stop()` then awaits the end channel instead of `mail.closed()`.

Revision 3 listed only the first and the third, and that is a red test rather than an
inelegance. `is_running()` is `!mail.is_closed()` (`local.rs:323`), which is false only
once the **receiver** drops. Without the explicit `drop(inbox)` the receiver lives
until the async block ends, so `stop()` can return while `is_running()` is still true,
and `local.rs:445` asserts the opposite immediately after `stop().await`. Which of the
two teardown orders you get depends on the async block's capture order, and neither
this RFC nor `local.rs` pins it. Verified: one order passed 25 of 25, the other failed
25 of 25, and the explicit drop passed 25 of 25 in both.

Two more properties `stop()` must have, neither of them free:

- It awaits with `wait_for(Option::is_some)` and **not** `changed()`. A fresh receiver
  marks the current value as seen, so `changed()` would wait forever on a second call
  and `stop()` is documented as idempotent.
- It treats a dropped sender as ended. Otherwise `stop()` hangs after `abort()`.

The channel exists for §2.5 already, so this costs no new machinery. `stop()` still
does **not** promise that the state's own `Drop` has run — only that the observers and
the mailbox are gone. That keeps `local.rs`'s existing honesty rather than replacing it
with a wider promise.

### 2.5 `Handle::ended`

`MailboxClosed` says "gone". It must not be stretched to say "gave up on durability".

```rust
/// Exhaustive, like `Departure`. A caller matches it.
#[derive(Clone, Debug)]
pub enum Ended {
    /// `stop` was called, or the last handle was dropped.
    Stopped,
    /// The loop never reached its exit: `abort`, or a runtime shutdown.
    Vanished,
    /// A turn panicked and an observer refused to continue.
    Panicked { message: String },
    /// An observer returned `Err`, or panicked, after a turn that completed.
    Observer { name: &'static str, error: Arc<dyn Error + Send + Sync> },
}

impl<A> Handle<A> {
    /// Resolves when the actor ends. Holds no mailbox sender.
    pub fn ended(&self) -> impl Future<Output = Ended> + Send + 'static;
}
```

Revision 1 built this from `mail.closed()` plus an `Arc<OnceLock<Ended>>` and claimed
it needed no channel. That deadlocks. A handle holds an `mpsc::Sender`
(`local.rs:142`), and `local.rs:164` ends the loop only when every sender drops. So
awaiting it on a handle **keeps the actor alive**, and the documented "last handle was
dropped" case can never resolve. Both paths were run: `stop()` gave `Stopped`, and the
dropped-handle path never returned.

So it is a `tokio::sync::watch` after all, and it carries **`Option<Ended>` starting at
`None`**. A channel initialized with a reason reports that reason for a live actor.
The loop fills it before exiting; the future holds a receiver and no sender, so
dropping every handle both ends the actor and resolves the future. An aborted task
drops the sender with no value, which is `Vanished`.

Three corrections revision 2 needed here:

- **`Panicked` is a separate variant.** `Saver` returns `Err` on a panicked turn
  (§4.2), so revision 2 reported `Ended::Observer { name: "durable" }` for a fault in a
  `service!` method. That points an operator at the durability adapter for somebody
  else's bug. The branch already knows which case it is in.
- **A panicking observer is reported through `Display`.** `Panic` implements `Display`
  and `Debug` (`actor.rs:421`, `actor.rs:428`) and **not** `std::error::Error`, and it
  is not `Sync`, so it cannot fill an `Arc<dyn Error + Send + Sync>`. Build the error
  from `panic.to_string()`, which is what `RemoteError::Failed` already does
  (`actor.rs:370`).
- **A panicking `Drop` on the state is not `Vanished`.** The loop fills the channel
  before the block ends, and `watch` retains the value, so `ended()` still reports
  `Stopped`. Revision 2 listed it under `Vanished` and was wrong.

Unchanged, and correctly so: `abort()` on a turn that never reaches an await point
does not interrupt it (`local.rs:308-312`), so `ended()` waits. That is today's
behavior and not a regression.

`Stop` holds a `Box`, and `Ended::Observer` holds an `Arc`, because `Ended` is `Clone`
and many callers can await it. The conversion happens once, in the loop.

### 2.6 Where it runs, and who is answered

The hook runs inside the turn closure, after the handler and **before the reply**.
That position is forced. `local.rs::ask_catching` sends the reply from inside the
closure, so anything placed after the closure is always too late.

```rust
Box::new(move |state, hooks| {
    Box::pin(async move {
        // The handler, inside the guard, closure call included.
        let answer = guarded(async { turn(&mut *state).await }).await;
        let info = TurnInfo { outcome: /* Completed, or Panicked { message } */ };

        // The observers, also inside the guard. Revision 1 left them outside,
        // which broke `local.rs:59-67`: every path library code controls is
        // inside `guarded`. An observer that panicked would have unwound out
        // of a detached task and been swallowed.
        let mut refused = None;
        for observer in hooks.observers.iter_mut() {
            let name = observer.name();
            match guarded(observer.after_turn(&*state, info)).await {
                Ok(Ok(())) => {}
                Ok(Err(stop)) => { refused = Some((name, stop.into())); break; }
                Err(panic)   => { refused = Some((name, panic.to_string().into())); break; }
            }
        }
        // the table below decides who is answered
    })
})
```

Three cases, and revision 1 got one of them backwards.

| The turn | An observer | The caller of `ask` gets |
|---|---|---|
| completed | accepted | the value |
| **panicked** | refused | **the panic, resumed** — then the actor ends |
| completed | refused | `MailboxClosed`, and the turn is not committed |

Row 2 is the correction. Revision 1 dropped the reply whenever an observer returned
`Err`, and `Saver` returns `Err` on a panicked turn. So a panicking `ask` against a
durable actor would have returned `MailboxClosed`, which reverses the property
`local.rs:41-48` states in its own module header. `Err(Panic)` is not `Ok`, so the
argument for withholding never applied to it.

There is no race in row 2. The reply is sent, the reason is set, and the loop breaks —
in that order, on one task. A `oneshot` retains a sent value, which is what
`local.rs:232` already depends on, so the caller cannot see `MailboxClosed` instead of
the payload.

**One ordering constraint inside that.** `reply.send(answer)` **moves** the `Panic`,
so the message for `Ended::Panicked` must be copied out first:

```rust
let panicked: Option<String> = match &answer {
    Err(p) => Some(p.message().to_owned()),
    Ok(_) => None,
};
// ... the table decides who is answered, and `answer` moves here ...
// ... then the reason is built from `panicked`, which is owned.
```

Building the reason after the send is `E0382`, not a style choice.

Row 3's cost is real and is not solved here. The turn **ran**, and the caller cannot
tell whether it ran. That is the same ambiguity `client.with_idempotency` exists for.

The private `Turn<A>` alias grows a second parameter, so the loop can lend the
observers per turn. It is private. `deliver` (`local.rs:327`) only wraps the closure,
and `Clone` and `abort` never name the alias, so all three are undisturbed.

With no observers there is no allocation and one branch. An actor without observers
behaves as it does today, which the 226 existing tests cover.

`macros.rs` changes only to add `actor_with`, `Handle::ended`, and to route
`with_state` at a read.

### 2.7 The served path

`ask_catching` needs a third outcome:

```rust
pub enum TurnLost {
    /// The actor was gone, or ended while this turn waited.
    Gone,
    /// An observer refused this turn. It ran and was not committed.
    Refused { name: &'static str },
}
```

`macros.rs:709-716` maps `Err(MailboxClosed)` to `RemoteError::NoSuchActor`, and
`macros.rs:700-708` explains that this means "there is nothing here, stop asking". A
refused turn is not that. It maps to `RemoteError::Failed`, which is `Failed(String)`
(`reply.rs:72`) and has room for the observer name.

**Not wire-visible, decided on purpose.** `actor.rs:60-66` hashes
`Contract { response: Envelope<WireResponse<ActorReply>>, .. }`, `reply.rs:94` sets
`WireResponse<Resp> = Outcome<Resp>`, `reply.rs:77-82` gives `Outcome::Err(RemoteError)`,
and `derive_alpn`'s doc (`node.rs:156`) says the hash walks the whole transitive type
graph. So a new `RemoteError` variant moves the ALPN and the pinned literal at
`protocol.rs:772`. `reply.rs:84-92` records that this exact bug already shipped once:
the contract hashed the bare response while the wire carried `Outcome<Response>`, so
adding a variant did not move the ALPN, and inserting one decoded a refusal as a
different refusal.

`RemoteError` is `#[non_exhaustive]` (`reply.rs:24`), so `CONTRIBUTING.md`'s "a new
variant is not a breaking change" applies to the **Rust API only**. The wire does not
care about `#[non_exhaustive]`.

**The cost, stated rather than hidden.** `macros.rs:695` already maps a panic to
`Failed(panic.to_string())`. Sending a refusal there too gives a peer one variant for
two situations that want opposite behavior: a panic leaves the actor **alive** and a
retry is reasonable, and a refusal leaves it **dead** and every retry gets
`NoSuchActor`. The peer must match on the string. This is a trade against moving the
wire contract, not a free choice.

**Both sides carry a prefix, not just one.** A refusal sends
`observer refused: {name}`, and `macros.rs:695` changes to send `panicked: {msg}`.
Prefixing only the refusal is a guard that cannot fail: `Display for Panic`
(`actor.rs:421`) writes the bare message, panic text is often caller-derived
(`panic!("{}", input)`), and a handler that panics with the exact refusal string
produces a byte-identical `Failed`. Verified — the two were identical. Both prefixes
are pinned in one test. This is a `String` payload and not a variant, so it does not
touch the ALPN or the pinned literal at `protocol.rs:772`.

`ask` keeps `MailboxClosed` and maps both, because a local caller learns why from
`ended()`.

### 2.8 Read-only, stated narrowly

`after_turn` takes `&A`. It never takes `&mut A`, so no observer can call a `service!`
method or assign to a field.

That is the whole guarantee, and revision 1 claimed more. `&A` does not stop interior
mutability: a state with an `AtomicU64`, a `Mutex` or a `RefCell` field is mutable
through `&A`. `Handle::with_state` makes the same claim and carries the same hole.

Per peak-rust rule 4, a "cannot be written" claim with no compile-fail test is
unverified. §9 adds a shadowing probe in the style of `no_streaming_on_a_local_handle`
(`macros.rs:1751`).

---

## 3. Crate layout

```
keylink            defines Observer            core; knows no adapter
  ^  implements
keylink-durable    the glue                    depends on both
  |  drives
  v
keylink-persist    defines Snapshots / Claim   port; still knows no framework
```

Three crates and no dependency cycle, checked against the actual manifests.
`keylink-persist` keeps the property commit `dab3b58` claimed: the port does not know
the framework exists.

`keylink/tests/durable.rs` and `keylink/examples/durable_ledger.rs` move to
`keylink-durable`. That removes `keylink-persist` from keylink's dev-dependencies, so
the two crates stop referring to each other in any direction.

Three things the new member inherits:

- `mise.toml:87` runs `cargo test --workspace`, and `mise.toml:108` runs
  `cargo +1.91 check --workspace --all-targets --all-features`. It must pass the MSRV
  check on day one.
- `keylink-persist` is `publish = false`, so `keylink-durable` must be too.
- `keylink-persist`'s default `file` feature pulls `tokio/fs`, which the wasm build
  refuses. `keylink-durable` refuses `wasm32` by `compile_error!`, as `blocking`, `fs`
  and `env` do.

---

## 4. The adapter: `keylink-durable`

### 4.1 The call site

```rust
let store = Files::open(&root).await?.limit(8 * 1024 * 1024);
let id    = StateId::new("ledgers/sp-01")?;

// Claims the state, loads the bytes, and restores or seeds. `Busy` means
// another process owns it. The caller keeps no `Saver`: the actor owns it,
// and §2.4 releases the claim before `stop()` returns.
let (state, saver) = keylink_durable::open(&store, id, Ledger::default).await?;
let ledger = ledger::actor_with(state, vec![Box::new(saver)]);

// From here nothing mentions persistence again.
ledger.post(1).await?;
```

One `await`, because `Claim::load` is async and the restore happens inside it.
Revision 1 wrote `saver.restore_or_default()?` with no `.await`, which only works if
the bytes are cached, and never said so.

`open` takes a seed closure rather than requiring `A: Default`, matching the
load-or-default shape `durable_ledger.rs` already has.

There is no policy argument, because there is one behavior. When a second behavior
exists it arrives as a second argument and every call site moves — the policy this
repo already states for a pre-1.0 crate.

### 4.2 What `Saver::after_turn` does

1. On `TurnOutcome::Panicked`, return `Err(Stop)` without storing. §5.1.
2. `state.snapshot()`, **synchronously**, before the future is built. That is what
   keeps `A: Sync` off the whole design.
3. `self.claim.store(&bytes).await`. A store error becomes `Err(Stop)`.

**`Durable::revision` is cut.** Revision 1 had it as a defaulted
`fn revision(&self) -> Option<u64> { None }` that skipped the store when the counter
had not moved. It is a guard that moved rather than one that was deleted: nothing
connects a mutation to a bump, so an impl that forgets one disables durability
completely and every test stays green. §1 promises that an answered call is a stored
call, and a defaulted optional method cannot carry that promise.

The cost is a store per read-only turn, and it breaks no existing test — `durable.rs`
asserts on values, and the extra stores write the bytes those assertions already
expect. §7 commits to measuring the cost. If the number forces a skip, the answer is a
type that makes the link provable — a `Revision` newtype only `&mut self` methods can
advance — and not an assertion.

---

## 5. Rules this design forces

### 5.1 A panicking turn ends a durable actor

RFC-04 §8 refuses supervision: a panicking message costs that message, and the actor
survives. That rule stays, and it stays **in keylink**. The stricter rule belongs to
the adapter.

Revision 1 argued this from the torn state alone, which proves the smaller claim. Not
storing a torn state already keeps the disk clean. The reason to end the actor is the
next sentence: after a torn turn the memory and the disk have **diverged**, and every
later turn builds on the torn value. A durable actor that keeps serving from a state
it can never store again is the best-effort mode §6 refuses.

The alternative revision 1 dismissed in silence: snapshot **before** each turn. The
disk then always holds a state no panic passed through. It loses because it makes §1
false — the disk is permanently one turn behind — and because it pays the store on
every turn anyway. It is written here so the reader sees it lose.

### 5.2 A read runs no observers

`Handle::with_state` takes `&A` and cannot mutate, so storing after it is waste.

`local.rs` builds the closure, so a read closure omits the observer step.
`LocalActor::read` is that closure, and the message type does not change.

The rule also closes a loop a future background writer would otherwise create: a
writer that reads the state through a turn would mark the actor dirty, which would
schedule another read, forever.

### 5.3 The keyed `System` path is out of scope

`macros.rs:417`'s `spawn()` runs its own loop over `__actor_turn` and never touches
`LocalActor`. It gets no observers.

That is a scope limit and not an oversight, and it must be **written down** in the
trait doc and beside `spawn`. A user who mounts with `spawn()` and expects durability
would otherwise get no durability, no compile error, and no explanation. RFC-04 §14.8
already says the keyed path costs more and is worth it only when there are many.

---

## 6. Refused

- No retry of a failed store.
- No best-effort mode that keeps serving without durability.
- No store on `Drop`. `Drop` cannot await.
- No observer that can mutate the state through its reference.
- No observer that sees a request or a reply payload. That is `Middleware`.
- No supervision. An actor that cannot honor its contract ends.
- No journal, and no replay. §8.
- No new `RemoteError` variant, because that moves the wire contract.

---

## 7. Cut from this RFC, and what brings it back

Revision 1 proposed a second policy: snapshot inside the turn, store on a background
task after a quiet window, with a mandatory ceiling. It is cut.

What the cut costs, stated plainly: **every turn pays the store latency.** For the
file adapter that is two `fsync` calls, one for the file and one for the directory.
The actor is serial, so its ceiling is one turn per store.

That number must be measured and published rather than guessed, the way RFC-04 §1.4's
"nine lines" was measured and came back as thirteen. Measure it **before** step 2
lands: `durable.rs` already has the adapter and a `Ledger`, so it is a loop around
`ledger.post(..)` with and without the observer.

Three things must be true before a background policy returns:

1. A measured throughput ceiling that a real caller actually hits.
2. A `Durability` enum with no `Default`, matching `Eviction` and `Retention`, whose
   variant names carry the guarantee.
3. A mandatory ceiling on the quiet window. A debounce without one starves on a busy
   actor, and the loss window becomes unbounded.

"Additive later" covers three different kinds of change, and revision 1 blurred them.
A `TurnInfo` field is additive. A `Durability` argument on `open` moves every call
site. Changing what an answered call means is neither, which is why item 2 is a type
and not a parameter.

---

## 8. What this supersedes, and what it does not

RFC-05 proposed one shared iroh-blobs node as a vault for actor state. It was set
aside for three reasons, and this RFC keeps only its port:

- A local file answers the literal requirement, which was "restarts do not erase the
  state".
- S3 with a conditional write beats the vault on every axis except "no cloud".
- Five of the seven Critical findings against the vault dissolve once the design stops
  using iroh-blobs.

Two findings survive and belong to any future vault adapter: fencing protects the cell
and not the executor, and a restore from a stale backup fails toward the stale writer.
`.tmp/rfc-05/index.html` §02 and §08 hold the detail.

**RFC-04 §8's objection, answered on the right axis.** Its sentence is: Restate's
Virtual Object gives keyed serialized access **and a K/V store that survives the
process**, which is strictly more than an in-memory supervisor can offer.

Revision 2 answered a different objection. It argued that Restate resumes a
computation with a journal and replay while this stores a snapshot — true, and beside
the point, because a Virtual Object's K/V store is a snapshot store too. It does what
`Saver` does.

The real difference is deployment. keylink is a library inside your process, and
Restate is a server you run, operate, upgrade and back up. `Saver` adds durable state
to a program that has one process and one file, with no service to stand up. That is
the whole of the advantage, and it disappears the moment a service exists.

So the recommendation is explicit: **a caller who already runs Restate should use its
K/V store and not this.** RFC-06 is for a program that does not, and does not want to
run one to keep a counter across a restart.

The tripwire stands. §6 refuses retry and supervision, and now refuses a journal and
replay as well. If a later revision adds any of the four, it has crossed into
Restate's territory and RFC-04 §8 applies again.

---

## 9. How it is proved

**No stand-in types in a probe.** Both earlier revisions were broken by one: a unit
`Panic` is `Sync`, and the real one is not. A probe copies the real field list, or it
proves nothing. This is a rule, not a lesson.

**The port.** One observer in keylink's own tests that counts turns and nothing else,
with no storage in sight. Made red by removing the call site in `local.rs`.

**Position.** The observer ran before the caller was answered. Made red by moving the
hook after the reply.

**A panicking observer.** `after_turn` panics, and `ended()` names the observer. Made
red by moving the observer chain outside `guarded`, which is where revision 1 had it.

**The three rows of §2.6.** A panicking turn resumes in the caller **and** ends the
actor with `Ended::Panicked`. A refused turn answers `MailboxClosed` and ends with
`Ended::Observer`. Made red by dropping the reply on a panic, which is revision 1's
behavior, and by merging the two variants, which is revision 2's.

**The served path.** A refused turn reaches a remote caller as `Failed` with the
pinned prefix, and not as `NoSuchActor`. Made red by mapping `Refused` to `Gone`.

**`ended()` does not wedge the actor.** Await `ended()`, drop every handle, and assert
it resolves. Made red by holding an `mpsc::Sender` in the future, which is revision
1's design.

**`ended()` on a live actor.** Assert it has not resolved. Made red by initializing the
watch with a reason instead of `None`.

**The claim is released before `stop()` returns.** End a durable actor, then claim the
same `StateId` again in the same process, and assert it succeeds **with no sleep**.
Made red by dropping the observers at task teardown instead of explicitly.

**`stop()` closes the mailbox before it returns.** `local.rs:441`
`stop_drains_what_was_sent_before_it` already asserts this and is the guard. Made red
by removing the explicit `drop(inbox)` of §2.4 — which fails in one teardown order and
passes in the other, so run it 25 times rather than once.

**Both wire prefixes.** One test pins `observer refused: {name}` **and**
`panicked: {msg}`. Made red by removing either prefix: a handler that panics with the
refusal's exact text then produces an identical `Failed`.

**A read runs no observers.** `with_state` increments no counter. Made red by routing
`with_state` through `ask`.

**No `&mut A` route.** A shadowing probe in the style of
`no_streaming_on_a_local_handle`, per peak-rust rule 4.

**Restart.** `durable.rs` moves to `keylink-durable` and loses its explicit
`claim.store` loop, because the observer does it. The assertion does not change, which
is the point: the same proof, less caller code.

**The number.** Turns per second with and without the observer, published in the
commit message. Not an assertion in a test — the house rule forbids wall-clock
assertions in an integration test.

**Still unproven, and named so.** The probes cover the borrow shape, the owned-observer
design, the reply rules, the no-`Sync` claim and the `Send` question. They do **not**
cover `notify`, the `read` path, the served path or the keyed system. Those are proved
by the tests above or not at all.

---

## 10. Public surface, counted

Revision 1 claimed six, and revision 3 claimed twelve. The real count in `keylink` is
**fourteen** added names — thirteen once `LocalActor::ended` is added, and fourteen
because `Stop::new` is new in revision 4:

`Observer`, `TurnFut`, `TurnInfo`, `TurnOutcome`, `Stop`, `Stop::new`, `TurnLost`,
`Ended`, `LocalActor::spawn_observed`, `LocalActor::read`, `LocalActor::ended`,
`Handle::ended`, `actor_with`, `Panic::message`.

`LocalActor::ended` must be **public**, and revision 3 missed it. Generated code
reaches `LocalActor` only through public methods — `macros.rs:588`, `:804`, `:819` and
`:826` are all that pattern — so `Handle::ended` cannot delegate to a private one.

Plus the `Debug`, `Display` and `Error` impls `Stop` needs, `Debug` and `Clone` on
`Ended`, and `Stop`'s blanket `From<E>`.

Plus **one changed signature**: `LocalActor::ask_catching` moves from
`Result<Result<T, Panic>, MailboxClosed>` to a `TurnLost` error. It adds no name and
it is still a compatibility promise. `ask_catching` has exactly two callers in the
repo — `local.rs:202` and `macros.rs:672` — and none in `tests/`, `demo/` or the
README, so the cost is a release note.

`keylink-durable` adds `Saver`, `open` and an error type. `keylink-persist` adds
nothing, because `Durable::revision` is cut.

Four names avoid collisions found in review: `Outcome` is already exported from
`reply.rs`, `Turn` is already a private alias at `local.rs:116`, `Lost` would sit in
one rustdoc index beside `Departure::Lost` meaning something else, and `spawn_with` is
already `SystemBuilder`'s (`actor.rs:846`). Hence `TurnOutcome`, `TurnInfo`, `Ended`
and `spawn_observed`.

Apply `CONTRIBUTING.md`'s own test to each item before it becomes `pub`.

---

## 11. What review changed

**Revision 2**, from the plan gate. Four design changes: owned observers, forced by
`Claim::store(&mut self)` on a non-`Sync` claim; `ended()` on a watch channel, forced
by a deadlock; a panicked turn is still answered, forced by `local.rs`'s own module
header; and `Durable::revision` cut, forced by peak-rust rule 1. Two claims deleted
rather than fixed: the idempotency consumer, and `TurnInfo::seq`.

**Revision 3**, from the targeted re-verification. Three of those four held. One did
not:

1. `TurnOutcome::Panicked` carries `&'a str` and not `&'a Panic`, because the real
   `Panic` is not `Sync` and the turn future must be `Send`. Reproduced, then fixed,
   in `.tmp/probe-observer/realpanic.rs`.
2. `stop()` awaits the end channel and the loop drops the observers explicitly, so the
   claim is released before `stop()` returns. Without it, `durable.rs`'s three
   sequential runs would intermittently fail with `Busy`.
3. `Ended::Panicked` is split from `Ended::Observer`, so a handler's fault does not
   name the durability adapter.
4. The end channel carries `Option<Ended>` starting at `None`.
5. `spawn_observed`, not `spawn_with`.
6. §2.7 states the cost of sharing `RemoteError::Failed` with a panic, and pins a
   prefix.
7. §8 answers RFC-04 §8 on deployment rather than on journals, and recommends
   Restate's K/V store to a caller who already runs Restate.

**Revision 4**, from a pass built entirely from real types. Two confirmed: the turn
future is `Send`, and `A: Sync` is absent from the closure, from `actor_with` and from
the `Saver` impl. Four changes:

1. §2.4 needs a **third** edit, `drop(inbox)`, before the end channel is filled.
   Without it `stop_drains_what_was_sent_before_it` fails 25 of 25 in one teardown
   order and passes 25 of 25 in the other, and nothing pins which order you get.
2. §2.4 also pins `wait_for` over `changed()`, and a dropped sender as ended. The first
   keeps `stop()` idempotent, and the second keeps it from hanging after `abort()`.
3. §2.7 prefixes **both** sides of `RemoteError::Failed`. Prefixing only the refusal is
   a guard that cannot fail, because a handler can panic with the refusal's exact text
   and produce an identical `Failed`.
4. §2.6 states the ordering constraint: the panic message is copied out before
   `reply.send(answer)` moves it.

Two counting faults: `LocalActor::ended` must be public, and `Stop` needs a
constructor because its blanket `From<E>` makes every other `From` impl a coherence
error.

---

## 12. Build order

Two commits. Each one is provable alone.

1. **`Observer` in keylink.** The trait, `TurnInfo`, `TurnOutcome`, `Stop`,
   `Stop::new`, `TurnLost`, `Ended`, `Panic::message`,
   `LocalActor::spawn_observed`, `LocalActor::read`, `LocalActor::ended`,
   `Handle::ended`, `actor_with`, the three-drop ordering of §2.4, the served-path
   mapping with **both** pinned prefixes, and the scope note on `spawn`. One counting
   observer in the tests, plus the shadowing probe. No storage anywhere.
2. **`keylink-durable`.** `Saver`, `open`, the moved test and example, the
   dev-dependency removed from keylink, and the measured number in the commit message.
   `durable_ledger.rs:131-134`'s comment says durability is the caller's, which stops
   being true — rewrite it with the move rather than carrying it across.

Step 1 is useful without step 2, for the reason §2.1 gives.

---

## 13. Revision 5 — rebased onto the correctness programme

**Appended, not edited.** Nothing above this line has been altered.

RFC-06 revisions 1–4 were written against `main` at `caf94e9`. This branch lands
four waves on top of that commit — the idempotency programme, caller identity and
actor safety, server push, and the admission port — and three of them touch the
exact files §12's step 1 touches. Every line reference above is therefore stale,
and three of the design's factual premises changed. None of the *design* changes;
this section records the deltas so an implementer does not discover them one
compile error at a time.

### 13.1 `ask_catching` no longer has two callers

§10 says: *"`ask_catching` has exactly two callers in the repo — `local.rs:202`
and `macros.rs:672` — and none in `tests/`, `demo/` or the README."*

The served path no longer calls it. Wave B added
`LocalActor::ask_catching_before(deadline, turn)` for `QueueTimeout`, which
refuses a message still queued and never one that has started, and the generated
actor dispatch calls **that**. So the callers are now:

| caller | which |
|---|---|
| `LocalActor::ask` | `ask_catching` |
| generated actor dispatch in `macros.rs` | `ask_catching_before` |

Both error types move to `TurnLost`. §2.7's mapping applies to
`ask_catching_before`, not to `ask_catching`, and `ask` still maps everything to
`MailboxClosed` exactly as §2.7 says.

### 13.2 How a refusal actually reaches the caller

§2.6's table says row 3 answers `MailboxClosed`, and §2.7 says a refused turn
reaches a *peer* as `Failed` naming the observer. Those two are only consistent
if the name survives the trip, and dropping the reply sender loses it — a dropped
sender is indistinguishable from an aborted actor, which is `TurnLost::Gone`.

So the loop **sends** the refusal rather than withholding the channel. The reply
payload becomes one enum:

```rust
/// What the loop puts on the reply channel.
enum Answered<T> {
    /// The turn ran. `Err` is a panic the actor survived.
    Ran(Result<T, Panic>),
    /// A queueing budget expired while the message waited. Nothing ran.
    Expired,
    /// An observer refused a completed turn. It ran and was not committed.
    Refused { name: &'static str },
}
```

`Queued<T>` keeps answering *did it run*; `TurnLost` answers *why there is no
value*. A dropped sender stays `Gone`. Row 3 of §2.6 is unchanged from the
caller's side: `ask` maps `TurnLost::Refused` to `MailboxClosed`.

### 13.3 A refused turn settles an idempotency key as the refusal

Wave A made the served path settle a key on **what ran**, using a progress marker
that generated dispatch sets inside the turn. An observer refusal arrives after
`completed()` has been marked, so the key settles as the `Failed` answer and a
retry replays `observer refused: {name}` rather than running the work again.

That is the right answer and it is worth stating, because §2.6 calls row 3's cost
"the same ambiguity `client.with_idempotency` exists for". On this branch the
ambiguity is *resolved* for a keyed call: the turn ran, at-most-once holds, and
the caller is told the same thing every time. For an unkeyed call the cost is
exactly as §2.6 describes.

### 13.4 Names checked against four waves of new surface

The waves added `Caller`, `QueueTimeout`, `Queueing`, `Queued`, `Idempotency`,
`KeyState`, `WireRequest`, `Subscribers`, `Admission`, `Admittance`, `Revocable`
and `Revocation`. None collides with §10's fourteen. `Queued` and `TurnLost` sit
beside each other in one rustdoc index and mean different things, which §13.2
names rather than leaves to a reader.

### 13.5 What did not change

`Handle::with_state` is untouched by the waves, so §5.2's routing at
`LocalActor::read` is as written. `Caller<'_>` as the first argument of every
generated actor method is invisible to an observer, which sees the state and the
outcome and never the request. §5.3's exclusion of the keyed `System` path is
unaffected: `macros.rs`'s `spawn()` still runs its own loop over `__actor_turn`.

### 13.6 Correcting §13.3 — a refused turn settles `Interrupted`, not the refusal

§13.3 above says a refused turn settles its idempotency key as the `Failed`
answer, that a retry replays `observer refused: {name}`, and that this "is the
right answer". **It is not, and this corrects it.**

Wave A settles a key on **what ran**, through a progress marker generated
dispatch sets. The marker had `completed()` inside the turn closure, where the
service method returns — and an observer runs *after* that and can refuse the
turn it just watched. So a row-3 refusal arrived with the marker already at
`Completed`, and `serve_one` recorded the veto as the call's settled reply.

That is RFC-05 §2's defect reached through a different door: an answer recorded
for something that did not land. The turn's mutation lived in memory and never
reached a store, and the actor is ending, so the effect is lost — which is
exactly the "it ran, and whether it took effect is unknown" that `Interrupted`
exists to say.

`completed()` now sits in the one dispatch arm holding an answer the turn itself
produced. Every other arm leaves the marker at `dispatched`. A panicking turn
already behaved this way, because the panic unwinds past the mark; a refusal now
matches it.

This also answers something §2.6 leaves open. It says row 3's cost "is real and
is not solved here — the turn **ran**, and the caller cannot tell whether it ran.
That is the same ambiguity `client.with_idempotency` exists for." On this branch
a keyed caller *is* told: `Interrupted` names the ambiguity rather than leaving
the caller to infer it, which is strictly more than §2.6 expected to be able to
offer. An unkeyed caller pays the cost §2.6 describes, unchanged.

Found by a review of this branch rather than by the implementation, which had
written §13.3 the wrong way round and would have shipped it.

---

## 14. Revision 6 — `keylink-persist` does add something, and it is not `revision`

§10 says *"`keylink-persist` adds nothing, because `Durable::revision` is cut."*
Issue #3's wave 4c makes the first half stale. The second half stands.

`keylink-persist` now has `Durable::VERSION`, `Durable::migrate`, a
`RestoreError` enum in place of the newtype, and
`versioned::Versioned` — a wrapper that owns a claim and is the only thing that
writes a twelve-byte header in front of a snapshot.

**This is not `revision` re-derived**, and the distinction is the reason §4.2
cut it. `revision` was a per-instance counter whose job was to *skip a store*,
and it was cut because nothing connects a mutation to a bump: an impl that
forgets one disables durability completely with every test still green. `VERSION`
is an associated `const` on the type, it skips nothing, and every store writes.
Forgetting to bump it cannot silence a store; it fails closed at the *next* load
as an unparseable `restore`, which the constant's own documentation states in as
many words. The failure mode §4.2 refused — a guard that moves rather than one
that is deleted — is absent, because there is no guard to move.

**§4.2 step 3 still reads `self.claim.store(&bytes).await`.** A `Saver` built
today holds a `Versioned<C>` and calls `store::<A>(&bytes)`, which frames the
same bytes and takes the same `&mut self`. Nothing else in §4.2 changes: the
snapshot is still taken synchronously outside the future, `A: Sync` is still
absent, and §5.3's argument from `Claim::store(&mut self)` on a non-`Sync` claim
is untouched — `Versioned` owns the claim and inherits both properties. A
compile-time probe in `keylink-persist/src/representability.rs` now pins the
`Send + 'static` half of that, which §5.3 previously only asserted in prose.
