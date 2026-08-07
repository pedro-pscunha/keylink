# RFC-05 — Idempotency: what settles a key, and what does not

Status: **accepted**, implemented in the correctness programme.
Supersedes nothing. Reverses one decision recorded in RFC-04 §3.3, and upholds
that section's actual objection while doing it.

RFC-04 §15 is a postscript recording where earlier sections of that document
turned out to be wrong. This one has its own, at §9, for the same reason: a
decision record that only records the decisions that survived is a marketing
document.

---

## 1. What the ledger is for, in one sentence

A call that fails mid-flight leaves the caller unable to say whether it ran, and
an `IdempotencyKey` removes the choice between a double booking and no booking.
Everything below is a consequence of taking that sentence literally.

The word that does the work is **ran**. Not *succeeded*, not *was answered* —
**ran**. Every defect in this document is the same mistake in a different
costume: the ledger recording something about a call that had not happened.

---

## 2. The defect: `Outcome::Err` cannot say who refused

`reply.rs` documents `Outcome::Err` as *"the call was refused, by the handler or
by a layer above it"*. That "or" is the bug.

`serve_one` used to record whatever the chain returned. So a layer that refused
a call **without ever calling `next`** — an authorization rule, a rate limiter, a
`Timeout` with a budget already spent — had its refusal written into the ledger
as the settled reply for that key. The caller then received that refusal on every
retry, for as long as the record lived, for a call that had **never run**, from a
rule that may have stopped applying an hour ago.

The reverse case is just as bad. A `Timeout` above a served **actor** does not
cancel the turn — a layer runs outside the mailbox, so its budget expiring drops
the *wait for the reply*, not the turn. The turn ran, mutated the state, and the
key was settled as `TimedOut`. The caller was told the work had not happened,
forever, while it had happened exactly once.

### 2.1 Why splitting `Outcome::Err` by provenance does not work

The obvious fix — "don't record an `Err` a layer produced" — is wrong, and the
counterexample is in the crate already. A panicking actor turn returns
`Outcome::Err(RemoteError::Failed(..))` from `macros.rs`. The turn **ran** and
may have mutated. Releasing that key would permit a re-run.

So the `Outcome` value genuinely cannot distinguish "a layer said no" from "the
base ran and failed". The information is not in it and cannot be put in it: a
layer that refuses does so precisely because it knows nothing about the method's
return type.

### 2.2 What was built: a progress marker on `Context`

`Context` carries a `Progress`, an `Arc<AtomicU8>` that every clone shares —
which matters, because `Context` travels **by value** down `Next` and each layer
holds its own copy. It has three states, and `fetch_max` makes them monotone so
a late mark cannot walk the value backwards.

| after the chain returns | what the key becomes |
|---|---|
| `NotDispatched` | released — nothing ran, nothing is owed |
| `Dispatched` | `Interrupted` — entered, no answer anybody received |
| `Completed` | the answer, recorded and replayed byte for byte |

Two things about this are worth more than the mechanism.

**The marking lives in generated dispatch, not in a wrapper around the base.**
The obvious place is a thin wrapper installed in `ServiceBuilder::build`. It is
the wrong place, and the actor path is why: the base is entered while the
message is still being *queued*, so a wrapper would report `Dispatched` for a
turn that a `QueueTimeout` then refused before it ever ran. Generated code knows
when the work begins, and it is the only thing that does. The `Handler` dispatch
marks around the handler call; the actor dispatch moves a clone of the marker
**into the turn closure**, which runs on the actor's own task.

**The decision lives in `Drop for Guard`, not at the call site.** The serving
task can be aborted — the connection ended, the server shut down — and then
there is nobody left to settle the key by hand. `Drop` is the only code that
still runs, so it has to be the code that decides. This is what makes a task
killed between accept and dispatch leave a claimable key rather than a
permanent `Interrupted`.

### 2.3 The design that was rejected: the ledger as a layer

An earlier draft of the programme said "make idempotency the innermost layer".
It must not be built, and the reason is one line of `rpc.rs`: a replay writes the
recorded `Bytes` **straight to the stream**, undecoded. A `Middleware` returns a
typed `(Outcome<Resp>, Body)`, so a ledger expressed as a layer would have to
decode the recorded frame and re-encode it.

That destroys the byte-for-byte property the ledger is built on. Postcard is
canonical, so the bytes would usually be identical and the defect would be
invisible until the day they were not. `a_replayed_reply_is_byte_identical` is
the executable pin; before this programme the property was asserted in three doc
comments and by nothing that runs.

---

## 3. Bounds: three of them, and why one was not enough

The ledger was bounded by volume alone — entries and recorded bytes. Three gaps.

**Running claims were invisible to both bounds.** A claimed-but-unsettled slot is
never evicted, and deliberately so: dropping one would let a concurrent duplicate
into the handler. But it was counted by nothing, so N connections admitted up to
100×N unaccounted claims in one ledger. Now there is a per-peer cap
(`MAX_RUNNING_PER_PEER`, set above QUIC's 100 concurrent streams so one
well-behaved connection never meets it) and a total cap (`MAX_RUNNING`, because
`Allowlist::Anyone` makes the peer count a property of the network).

These are counted **separately** from the settled records rather than folded into
`max_records`. `evict` returns early when no bucket has settled keys, so a bound
that counted running claims would silently stop being enforced rather than fire.

**A volume-only bound makes the at-most-once window a function of unrelated
traffic.** An operator could not say how long a retry stays safe without knowing
what every other caller was doing. `RECORD_TTL` makes it answerable. It runs on
`n0_future::time::Instant`, copying `actor.rs`'s `Eviction::Idle`, because
`std::time::Instant::now()` panics on `wasm32-unknown-unknown` and that shipped
once already.

**Eviction used to make a key vacant.** So a retry of an evicted key ran the work
a second time, silently — the exact failure the ledger exists to prevent, reached
through its own cache policy. Eviction now **downgrades**: the reply goes, the key
stays as a tombstone, and a retry is refused `Forgotten`.

Tombstones need their own pool and their own bound. A tombstone that still
counted as a record would leave `evict` unable to reach the bound it was trying
to enforce — an infinite loop rather than a leak. So there are two queues per
peer, `held` and `spent`, and `MAX_TOMBSTONES` is deliberately many times
`MAX_RECORDS`, because a tombstone is a key and nothing else.

Past `MAX_TOMBSTONES` a key really is vacant again. That is not hidden: it is
what `KeyState::Fresh` reports, and it is the one honest limit of a ledger that
lives in memory.

---

## 4. Attach: reversed, and §3.3 was right

`idempotency.rs` used to refuse Restate's *attach* — a second caller waiting on
the running invocation — citing RFC-04 §3.3.

**§3.3's objection was correct.** Its worry was *"whether a dropped `Notified`
can swallow a wakeup"*, and it is real and specific: `Notify::notify_waiters()`
wakes only the waiters registered at the moment it is called, so a waiter that
registers *after* the notification misses it forever. The failure is
load-dependent, which is to say it passes every test.

It also has a known answer, which is why the decision reverses rather than the
objection:

```rust
let notified = notify.notified();
tokio::pin!(notified);
notified.as_mut().enable();   // registered HERE, before the re-read
// re-claim under the lock; if it settled in between, answer without waiting
// only then await `notified`, bounded by n0_future::time::timeout
```

Reversing those two lines is the bug. The comment at that call site says so.

### 4.1 The shape: a modifier, never a method

```rust
client.with_idempotency(Idempotency::attaching_for(key, budget))
      .submit(order)
      .await?          // -> the ORIGINAL call's own reply
```

`attach` is a modifier on the call, so a retry is *literally the same call with
the same key* — which is what a caller does after a dropped connection — and it
gets back its own method's type rather than a variant to unwrap. It spends no
reserved name, leaves the generated `Handler`/`Request` correspondence untouched,
and changes neither trait: it is entirely a ledger and transport concern.

The budget lives **inside** the value that carries the key, so "attach without a
key" is unrepresentable rather than merely discouraged. One type serves the
client API and the envelope, so the two cannot drift — RFC-04 §14.6's rule,
applied before the fact rather than after. Millis on the wire, matching
`RemoteError::TimedOut { millis }`, so `Duration`'s `Schema` impl never enters
the hashed contract. `From<IdempotencyKey>` keeps every existing call site
reading the way it always has.

### 4.2 What an attaching call gets

| slot | answer |
|---|---|
| vacant | it runs; the budget is ignored |
| running | it waits, then whatever the running call settled as |
| a held reply | replayed immediately, no wait |
| `Interrupted` / unrecordable / tombstone | refused immediately, no wait |
| budget spent, still running | `InFlight` |

The last row is why attach needs **no new error variant**: it degrades to exactly
today's answer, and `InFlight` — "a call is in dispatch under this key now" — is
still the truth. One wait and one re-claim, never a loop, so the work is bounded
by the caller's own budget. `MAX_ATTACH` caps a peer that names an absurd one.

Cross-peer attach is impossible for free: the ledger is keyed by `(peer, key)`,
so one peer can never wait on another's call.

A reply that carries a body is unrecordable, so attaching to a `recv` answers
"it ran, and I cannot give you the reply". Correct, and still more than the
caller had. A compile-time restriction on `recv` was considered and **deferred**:
it is one kind, the runtime answer is already right, and it would cost a third
client type.

---

## 5. `key_state`: the one question a retry cannot ask

Four of the five states a caller might want are discoverable by retrying. The
fifth, `Fresh`, is not — discovering it by retrying means executing, and probing
must not require re-submitting a market order.

The query is **keylink's own question, not the user's**, so it does not touch the
service's `Request` enum and does not spend a `Handler` method. The request
direction became `WireRequest<Req>`, with a `Call` arm and a `KeyState` arm; the
response direction became a `WireResponse<Resp>` of the same shape. `Contract`
names both, so the hashed contract still equals the wire type.

It runs no handler and no layer, because there is nothing left to authorize past
the admission the connection already passed. It is peer-scoped **by
construction** rather than by a check: the id is the one iroh proved on the
connection the question arrived on, so a peer cannot name another's key.

Six variants, not five. `Unrecordable` is not `Forgotten`: one is a permanent
property of the method, the other is an eviction. Both mean "do not retry", and
merging them would make the query lie about why. `KeyState` is exhaustive — no
`#[non_exhaustive]` — because `postcard_schema` derives from the variant list and
ignores the attribute, so a seventh variant moves the ALPN and breaks every
handshake anyway. The attribute would advertise source compatibility on a hard
wire break.

---

## 6. The ALPN moved, and it should have

`keylink/Probe/a51c5347dacb0df5` → `keylink/Probe/aa77e29e5e1ccdc7`, for three
reasons that are all genuinely wire-visible: the two direction types above, and
`Envelope.idempotency` carrying `Idempotency` rather than `IdempotencyKey`.

A peer built before this change would decode a `KeyState` reply as a call reply,
which is precisely the postcard variant-confusion the derived ALPN exists to
refuse. Refusing it at the handshake is the correct outcome and costs nothing:
keylink is unpublished, so there is no fleet to split. That stops being true the
day somebody deploys it, which is the argument for moving it now.

---

## 7. Not promised: the retry does not get a timed-out turn's real reply

A `Timeout` above a served actor now settles `Interrupted` rather than recording
its own refusal. It does **not** hand the retry the turn's real answer.

Doing that needs the `Guard` to travel inside `Mail` and be settled by the turn
itself, which is a different and larger change: the encoding would move out of
`serve_one` into generated code, and the guard would outlive the caller.

The programme's own goal document claimed this shared "most of the same
machinery" as `attach` and concluded "build both or neither". **That claim is
wrong, and this is the correction.** `attach` waits on a `Notify` that the
*first* caller's guard fires when it settles; the guard never leaves `serve_one`.
Nothing about attach requires the guard to outlive its caller. The two are
independent, so building one does not oblige the other.

---

## 8. Considered and declined

A minimalism review recommended cutting four items as under-justified. All four
are kept, by decision, and the argument is recorded here so the question is
answered once rather than reopened.

- **`QueueTimeout`** — the objection is that §2 removes its stated purpose, and
  that a one-deep mailbox behind a 100-stream cap is not worth shedding. Kept:
  `Timeout` cannot express "refuse only what has not started", and that is the
  only bound that is safe to combine with a key.
- **The TTL** — the objection is that it attaches to no defect. Kept: see §3.
  A bound an operator cannot reason about is not a bound they can rely on.
- **`key_state`** — the objection is that a retry already answers four of the
  five states. Kept: see §5. The fifth is the one that matters.
- **The `Admission` port** — the objection is that `Arc<RwLock<Allowlist>>` is
  about twenty lines and loses nothing. Kept: the point is an external policy
  backend, which a lock around a local value cannot be.

---

## 9. Postscript: what this document got wrong on the way here

- The goal said "make idempotency the innermost layer". Wrong; §2.3.
- The goal coupled attach to delivering a timed-out turn's real reply. Wrong;
  §7.
- The goal's success criteria asked for a `ConfigError` where its scope section
  argued at length for a compile error, and asked for the retry to receive a
  timed-out turn's real reply where its scope section excluded it in bold. Scope
  won both times, and both restatements are named in the final report rather than
  quietly reworded.
- An earlier reading of `idempotency.rs`'s citation of RFC-04 §3.3 called it
  weak. It was not; §4.

---

## 10. Left for later

**A generated client trait with an associated error type.** `Client`, the
blocking `Client` and `Handle` share method names, argument order and success
types, and differ only in the error. A trait naming that shape would let a
caller be generic over "local or remote" without RFC-04 §5.5's rejected
unification — §5.5 refuses *one type* for both, which is a different proposal
from *one trait with two error types*. Not built, and not blocked.

**Durability.** The ledger dies with the process; a retry across a restart runs
the handler again. `keylink-persist` is deliberately not the answer — it says so
itself: *"No history. One state, one current value. A journal is a different port
with different operations."* The ledger is journal-shaped.

**A live wart, recorded rather than fixed.** A `NoSuchActor` refusal from an
actor system's `deliver` settles the key, so a retry after the actor is mounted
replays `NoSuchActor` forever. It is the same shape as §2 — a refusal recorded
for a call that never ran — one layer down, where the payload is opaque to
keylink by design and the progress marker therefore cannot reach. Fixing it means
the keyed actor system growing an idea of dispatch it does not have yet.

---

## 11. Two structures §3 left unbounded, and it said they were bounded

§3 is titled "Bounds: three of them, and why one was not enough". Two things it
does not mention grew without one. Both are the same mistake as §2 in a third
costume: the ledger was reasoning about the calls it *runs* and had stopped
looking at everything else it was holding.

### 11.1 The refusal that created what it refused

`claim` took the peer's bucket with `entry(peer).or_default()` and then ran the
cap checks. So a claim the **global** cap turned away — a peer this ledger had
never seen, refused because 4096 other calls were in dispatch — left an empty
`Bucket` behind in `inner.peers`, and nothing removes one: `release` and `evict`
prune only a bucket they took the last slot out of, and this one never had a
slot. With `Allowlist::Anyone` the number of distinct peers is a property of the
network, so the refusal path was unbounded memory in the module whose entire
purpose is bounds.

The fix is where the map is touched, not what is counted: the read-only checks go
through `inner.peers.get(&peer)`, and `entry(peer).or_default()` happens on the
one path that inserts a slot. No refusing or replaying path creates an entry.

`expire` and `evict` were checked for the same shape and do not have it. `expire`
never removes a slot — a record past its TTL is **downgraded** to a tombstone,
which is §3's own rule — so it cannot empty a bucket. `evict`'s record loop
downgrades for the same reason; its tombstone loop is the one place a slot really
is dropped, and it already prunes. Nothing there changed.

### 11.2 A parked attacher holds no claim, and the claim caps are what bound it

§3 says running claims used to be invisible to both volume bounds, and gave them
`MAX_RUNNING_PER_PEER` and `MAX_RUNNING`. §4 then added attach on top, and
reopened the hole one layer up.

A caller parked in `claim_key` waiting on `notified` **holds no claim**. It is
counted in neither `Bucket::running` nor `Inner::running`, so neither cap can see
it or the QUIC stream pair it is keeping open for up to `MAX_ATTACH`. One claimed
key, 40 connections of 100 concurrent bidirectional streams each, all attaching
to it: 4000 parked tasks and 4000 stream pairs for thirty seconds, and
`TooManyRunning` never fires. The argument in `MAX_RUNNING_PER_PEER`'s own doc
comment is exactly this argument, about the other half.

So there is a second pair, and it is a pair for the reason the first one is:
`MAX_WAITING_PER_PEER` because a peer can open connection after connection, and
`MAX_WAITING` because `Allowlist::Anyone` makes the peer count a property of the
network rather than of the configuration.

Three things about the shape are load bearing.

**The count lives in the ledger.** A per-connection counter cannot see the other
connections the same peer opened, which is the whole failure.

**A place to wait in is RAII**, a `Waiting` whose `Drop` gives it back. The
ordinary way a waiter stops waiting is that its task is *aborted* — the caller's
connection ended — and a decrement written on the return paths of `claim_key`
does not run then. It is the same argument `Drop for Guard` is built on, and the
failure it prevents is worse than a leak: a place that is never given back means
the cap is reached once and then refuses every attach, with nobody waiting, for
as long as the process lives.

**It reuses `TooManyRunning`.** `postcard_schema` hashes the variant list into the
ALPN, so a second spelling of "too many at once" would move the protocol identity
and break every handshake — §6's move was paid for by three genuinely wire-visible
changes, and this is not one. `keylink/Probe/aa77e29e5e1ccdc7` is unchanged, and
`the_contract_hash_has_not_moved` is what says so.

**This adds a row to §4.2's table.** That table lists what an attaching call gets
and its last row is "budget spent, still running → `InFlight`". There is now one
more: *no place to wait in* → `TooManyRunning`, returned **before** the task
parks, because a waiter refused after it has waited has already spent the task
and the stream pair the refusal exists to save. §4.2's sentence that attach needs
no new error variant is still true, and now carries twice the weight.

**§3's numbers, and why these two are not the same numbers.**
`MAX_WAITING_PER_PEER` is 128, the same as `MAX_RUNNING_PER_PEER`: above QUIC's
100 concurrent streams so one well-behaved connection never meets it, and it is
also the legitimate ceiling, since a `Peer` re-issue produces at most one
attacher per running claim and running claims are capped there. `MAX_WAITING` is
1024, a **quarter** of `MAX_RUNNING` rather than equal to it, because a waiter
produces nothing: it costs a task and a stream pair, it may still be refused when
its budget runs out, and a node should spend the smaller share of its budget on
calls it has already decided not to run.

`enable()`-before-re-read is untouched. §4's hazard and §4's answer are unchanged;
the bound is around that sequence, not in it.

### 11.3 What the probe checks now, and the one thing it cannot

`assert_agrees` recomputes every counter from the maps, and it grew two
assertions.

The first is §11.1's guard: no bucket in `inner.peers` is empty. It is stated
where the invariant lives rather than in one test, because the leak was reachable
from one path and the invariant holds on all of them.

The second is only half of what was wanted, and the half it is missing is worth
saying plainly. The waiter counts are **not recomputable from the maps**: a place
to wait in is a value on some other task's stack, and `Inner` records how many
exist rather than which. So the probe checks what it can — the per-peer entries
sum to the total, and no peer is left in the map at zero — and
`the_waiting_count_follows_the_live_guards` checks the counts against the guards
themselves by taking and dropping them one at a time. Two assertions, honestly
scoped, rather than one that claims more than it verifies.

### 11.4 A second live wart, recorded rather than fixed

`evict`'s record loop increments `inner.spent` unconditionally but pushes to
`bucket.spent` only when the key it popped from `held` is still in `slots`. If
those two ever disagreed, the counter would drift **above** the truth and the
tombstone loop would evict more than it should. The arm is unreachable today —
a key in `held` is always in `slots`, which is the invariant `assert_agrees`
checks — and it is the mirror image of the drift the exhaustive `match` in
`held_bytes` exists to prevent. Left alone, because fixing an unreachable arm on
the way past is how a correctness change grows a blast radius nobody reviewed.

### 11.5 A middleware can forge the progress marker, and this is the wrong layer to stop it

`Progress` and `Context::progress` are `#[doc(hidden)]` but they are `pub`,
because the code that marks them is expanded into the *caller's* crate and
`pub(crate)` does not reach across that boundary. So a `Middleware` holding a
`Context` can mark `completed()` and then refuse without calling `next`, and the
accept path records that refusal as the call's settled reply and replays it to
every retry — §2's defect, re-opened one layer up by the extension point.

**It cannot be sealed.** A token type only generated code could construct would
itself have to be `pub` to be named in the expansion, so the forgery moves by one
line and the surface grows by a type.

**Cross-checking against the chain's `Outcome` would be a regression, not a
fix.** The tempting shape is `Reached::Completed && matches!(outcome,
Outcome::Ok(_))`. But a layer that runs `next` and *then* rewrites `Ok` into
`Err` produced the frame the caller actually received, and replaying that frame
byte for byte is exactly right — at-most-once, with the answer the caller was
given. Demoting it to `Interrupted` would break an honest layer to inconvenience
a dishonest one, and the dishonest one loses nothing.

**What makes it tolerable is where the trust boundary is.** A `Middleware` is
not a plugin: you write it, you compile it into your binary, and it already sits
between every call and its handler. A layer willing to forge this marker can
return a wrong answer outright and save itself the work. The boundary this crate
defends is the **peer**, and no peer can reach any of this — every route to
`Progress` is in-process.

So it is documented rather than defended, at all three places a reader meets it:
on `Progress`, on `Context::progress`, and on the `Middleware` trait, which is
what someone about to write one actually reads.

---

## 12. Revision, 2026-08-07: the ALPN this document twice called settled has moved

Appended rather than edited in place, per CONTRIBUTING.md. §6 and §11.2 both name
`keylink/Probe/aa77e29e5e1ccdc7` — §6 as the value this programme moved the hash *to*,
§11.2 as evidence that bounding the waiter counts moved nothing. Both were true when
written. **The literal is now `keylink/Probe/d3fe34471921164e`.**

The cause is not in this document. The per-service keyed actor rewrite deleted
`RemoteError::ActorSchemaMismatch`, the fifth variant of six, which renumbers `Failed`
from index 5 to index 4; `RemoteError` sits inside every service's hashed contract
(`Envelope<WireResponse<Response>>` → `WireResponse::Call(Outcome<Resp>)` →
`Outcome::Err(RemoteError)`) and postcard encodes a variant by its declaration index. So
the move is wire-visible for every service, this document's `KeyState` path included, and
restoring the variant at index 4 reproduces `aa77e29e5e1ccdc7` exactly. §6's own three
reasons are untouched: the direction types and `Envelope.idempotency` are exactly as
described. `tests/protocol.rs::the_contract_hash_has_not_moved` is still the pin, and it
now records both moves and the step-1 answer for each.

Two smaller consequences for readers of this file. `schema_hash` — RFC-04 §14.3's shared
helper, referenced here as the source of an ALPN's trailing hex — is `pub(crate)` at
`node.rs:149` rather than `pub`; `derive_alpn` is its only caller. And §11.5's argument
that `Progress` cannot be sealed has grown a second instance worth reading beside it:
`Origin`, the mailbox's owned provenance, withholds `Clone` and has no public
constructor, and neither fact stops a caller from cloning the `Context` *before* two
deliveries and sharing one marker. See `docs/rfc-07-the-turn-model.md` §1.6, where that
claim has now been corrected twice. §11.5's conclusion — documented rather than defended,
because every route to `Progress` is in-process — is what covers this one too.
