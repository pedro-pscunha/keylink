# RFC-08 — Serving-side idempotency: which methods accept a key, and where its reply lives

Status: **accepted**, implemented as `Keyable` and `Records`.
Supersedes nothing. Sits beside `docs/rfc-05-idempotency.md`, which decided what
settles a key; this one decides what a *serving* side is allowed to say about keys
before any of that runs.

RFC-05 §9 is a postscript recording what that document got wrong on the way. This
one has §10 for the same reason, and it is already populated: the brief this was
built from proposed a name the crate had already refused.

---

## 1. The problem, in one sentence

RFC-05 gave the caller a key and gave the serving side no say in it: every method
of a served service accepted one, and one pool of `MAX_RECORDS` entries served the
whole service.

Two consequences, and both are about a decision nobody could take.

**A peer could spend another method's records.** A quote lookup called a thousand
times a second, each call carrying a key, evicts the records of the methods that
needed them. `evict` always sheds from the peer holding the most, so one peer
cannot flush another's — that bound holds and RFC-05 §3 is right about it — but
*within* one peer's bucket a worthless record and a load-bearing one are
indistinguishable. The ledger has no way to tell a record it should keep from one
it should never have taken, because nothing ever told it.

**At-most-once was only available bundled with an answer.** A key either recorded
up to `MAX_RECORDED_BYTES` of reply or was not sent at all. A caller whose only
requirement is *"do not run this twice"* — an idempotent write whose result it
reads out of its own domain — paid for a reply it was never going to read, and the
bytes it paid with were the bytes the previous paragraph is about.

Both are serving-side questions. A caller cannot answer either: it does not know
what else the service is carrying, and it is the party that benefits from saying
yes to everything.

---

## 2. `Keyable`: which methods accept a key

```rust
pub enum Keyable {
    Any,                            // the default, and what the crate did before
    Only(&'static [&'static str]),
    Except(&'static [&'static str]),
    None,                           // and no ledger is built
}
```

Set with `ServiceBuilder::keyable`, beside `layer` and `with_blobs`. Method names
are the `CamelCase` spelling `service!` writes into the generated `Request` enum,
which is what `Rpc::method` returns.

### 2.1 Refused, never ignored

A key on a method the service does not key is refused, with a new
`IdempotencyRefusal::NotKeyable { method: String }`.

Ignoring it was the cheaper option and it is dishonest. A caller that wrote
`with_idempotency` believes the call runs at most once. Running it and recording
nothing hands that caller a guarantee it does not have, silently, on the one path
it cannot check — and the discovery mechanism is a double booking rather than an
error. Every other variant of `IdempotencyRefusal` exists for the same reason: the
answer a caller can act on beats the answer that looks like success.

### 2.2 A `Middleware` could not have done this

The key is claimed **before** the layer chain runs — `serve_one` claims, then calls
`next` — so by the time the outermost layer sees the request the slot is already
taken. A layer can refuse the *call*, and RFC-05 §2 is the whole story of what that
means for the key, but it can never refuse the *key*. The decision has to live
where the claim is, which is why this is a property of the service rather than a
layer somebody installs.

This is also why the check sits **above** `IdempotencyKey::validate` in `claim_key`
rather than below it. `validate` is the first step on the road to `Ledger::claim`,
so a refusal that beats it cannot have claimed a slot or evicted a record — and
reporting `Malformed` to a caller whose real problem is "this method takes no keys"
would send it to fix the wrong thing. `a_refused_key_never_reaches_the_ledger` is
that ordering, asserted on a key that is refusable both ways.

### 2.3 `Keyable::None` builds no ledger

Not an empty ledger — no ledger. `Keys` holds `Option<Arc<Ledger>>` and the
`Option` is `None` for exactly that variant.

The invariant is one-directional and stated that way: **no ledger means no key is
accepted, and the converse is not claimed.** `Only(&[])` accepts nothing either and
still gets a ledger, because two spellings of the same absence would be two things
to keep in step. So `Keyable::accepts` decides every refusal, `None`'s included,
and the missing ledger is never the *cause* of one. What the `Option` buys is that
the claim path cannot reach a ledger that does not exist without an `unwrap`:
`Keys::ledger_for` returns the policy's verdict and the ledger together or neither.

`keyed_none_builds_no_ledger` asserts the absence on the field `build()` fills,
because from the wire a refusal looks identical whether the ledger was missing or
merely empty. That is a case where the behavioural test would have been the vacuous
one.

### 2.4 It reaches the keyed-actor path with no second mechanism

`keyed_service` builds a `ServiceBuilder<Addressed<Request>, Response>`, and
`Rpc::method` for `Addressed<T>` forwards to the inner request — so the same
`ServiceBuilder::keyable` covers both dispatch paths and Wave 1 needed no change to
`macros.rs`. `a_keyed_actor_service_applies_the_same_policy` proves it over two
nodes rather than asserting it here.

That forwarding is recent. `Rpc::actor`'s doc comment records that `method` used to
answer the constant `"Message"` over a keyed system, because the method name lived
inside a sealed payload. Had this RFC been written then, per-method policy would
have been impossible on the keyed path and the two would have needed separate
mechanisms.

### 2.5 What a typo does, and the check that is not there

Nothing validates these names against the service's own methods. `Only(&["Resrve"])`
accepts nothing and is refused on every call, discovered on the first call rather
than at `spawn`.

Checking would need the method list *at the builder*, which means a
`const METHODS: &'static [&'static str]` on `Rpc` that every generated service
carries — a change to `macros.rs` for a mistake whose refusal already names the
method it did not match. Declined, and recorded rather than left to be found.

### 2.6 `key_state` is per-service, and therefore unpoliced within one

`WireRequest::KeyState(key)` carries a key and **no method name**, so `Only` and
`Except` have nothing to check a query against. The policy covers the *call* path and
not the *query* path, and that asymmetry is a property of the wire type rather than an
oversight in the check.

It is not a leak, and the reasons are structural. The query is peer-scoped by
construction — the id is the one iroh proved on the connection the question arrived
on — and it can only report keys that were actually claimed, which under `Only` are
only keys on accepted methods. So there is nothing a peer can learn by asking about a
method it may not key: the answer is `Fresh`, which is the truth.

Under `Keyable::None` there is no ledger at all, and the query answers `Fresh` for
the same reason `answer_key_state` already answers `Fresh` for a **malformed** key:
*"such a key can never have been claimed, because `claim_key` refuses it before the
ledger sees it"*. Refusing the query instead was considered and rejected —
`NotKeyable { method: String }` needs a method name and a `KeyState` query has none,
so refusing means inventing a string, and inventing one to describe a query that
carries no method is worse than answering the fact.

What that costs is one sentence of `KeyState::Fresh`'s doc, which used to read *"A
call with this key would run"* and is false on a service that would refuse it. It now
separates the fact about the ledger from the prediction about the call, and
`a_key_state_query_on_a_service_that_keys_nothing_is_fresh` holds it.

---

## 3. `Records`: at-most-once without holding the caller's reply

```rust
pub enum Records {
    Memory,   // what the crate did before; written by `ServiceBuilder::new`
    None,     // the key still claims; every repeat is refused
}
```

### 3.1 The motive is retention, and the first version of this section was wrong

**Retracted, and left standing rather than deleted, because the retracted version
is the more instructive one.** This section originally argued that `Records::None`
saves memory: a caller who only needs *"do not run this twice"* was paying to store
up to a megabyte of reply it would never read.

That argument does not pay, and the reason is a constant doing double duty.
`MAX_RECORDED_BYTES` is used **twice**: `Guard::record` reads it as a per-record
ceiling (`frame.len() > limits.bytes`) and `evict` reads the same value as the
**total pool across every peer** (`inner.bytes > limits.bytes`). So the record pool
is 1 MiB *in total*, not per key, and what `Records::None` saves is that one
megabyte once per process — roughly what its own tombstones cost, since
`MAX_TOMBSTONES`'s doc puts that pool *"on the order of a megabyte"* too. A feature
justified on that saving would not have paid for itself.

**The motive is retention.** A reply frame is the caller's data, and under
`Records::Memory` it sits in this process's heap for up to `RECORD_TTL` — ten
minutes — so a retry can be answered with it. For most services that is exactly the
trade the ledger exists to make. For a service handling data it would rather not
hold, it is a ten-minute retention window nobody asked for, on bytes it was only
ever asked to pass through once. What `Records::None` removes is not a quantity of
bytes but a duration they are held for, and that is a privacy property rather than
a capacity one.

Two things follow from the correction, and both improve the rest of this document.
**§1's argument for `Keyable` gets stronger**: a 1 MiB total pool is filled by ten
100 KB replies, so the eviction pressure `Keyable` guards against arrives far sooner
than §1 implied. And **`MAX_RECORDS = 1024` is decorative above 1 KiB per reply** —
the byte bound binds first and the entry bound is never reached. Neither is this
RFC's to fix; both are recorded because the next person to read `MAX_RECORDED_BYTES`
as a per-key figure will make the same mistake this section did.

### 3.2 `Unrecordable`, not `Forgotten`, and not a new variant

**Also a reversal.** This settled `Slot::Forgotten` first, and widened
`Forgotten`'s doc to name a third cause: *"or never recorded, because the service
records none"*.

That put a configured choice under a name that means pressure, and RFC-05 §5 draws
the line explicitly:

> Six variants, not five. `Unrecordable` is not `Forgotten`: one is a permanent
> property of the method, the other is an eviction. Both mean "do not retry", and
> merging them would make the query lie about why.

`Forgotten` is produced by `expire` and by `evict`, and both are evictions —
`Slot::Forgotten`'s own doc comment in `idempotency.rs` carries the current list,
because that file owns it and this one does not.
A configured retention policy is a permanent property. So it settles
`Slot::Unrecordable`, whose own doc — *"the handler ran, and its reply is not
something this ledger can hold"* — is true word for word of a service configured to
hold none.

**Reusing it is what keeps the refusal and the query honest for free.** `Slot::state`
already maps `Unrecordable` to `KeyState::Unrecordable`, so a caller that *asks*
gets the same story as a caller that *retries*. That is the argument against minting
`Discarded`, and it is the reverse of how it first looks: a new refusal variant
would have left `key_state` answering the old one until `KeyState` gained a seventh
variant too — **two names on two enums to say what one existing name already says
truthfully.** The wire was already breaking for `NotKeyable`, so the price was low;
it was the benefit that was missing.

What reuse costs, stated rather than glossed: several doc sites widened and one
`Display` string changed. `IdempotencyRefusal::Unrecordable` used to render *"the
reply to this key carried a payload, which is not recorded"*, which was about to be
false on a service that had never declared a `recv` method. It now names no cause at
all, deliberately — which of the three applies is answerable from the service's own
configuration and does not belong in a message every service shares.

**That string is NOT wire-visible, and an earlier version of this sentence called it
that.** `IdempotencyRefusal` crosses the wire as a postcard variant index; the
receiving peer renders `Display` from *its own* build, so the text never travels. The
gate proved it by removal: with the new string in place, deleting only `NotKeyable`
restored the old hash exactly — so Wave 2 on its own would have moved no pin.

The real consequence is smaller and differently shaped: **two peers on different
builds render different text for the same variant, and no handshake catches it.**
Blast radius is small, because callers are told to match on variants rather than
parse messages and `examples/reliable_call.rs` does, but it is the honest statement
of what changing a `Display` impl costs.

**The arm order the earlier design forced does not exist any more.** Under
`Forgotten` the retention cause and the `carries_body` cause produced *different*
refusals, so choosing which to check first was a wire-visible decision, and putting
retention first would have made `Unrecordable` unreachable and deleted README limit
5. Both causes now land in the same slot, so there is nothing to sequence and limit 5
is a property of the method again — held by
`a_recv_method_is_refused_the_same_way_whatever_the_retention_policy`.

### 3.2.1 It is not free, and the earlier text said it was

The first version of this section said the key state *"is unaffected by this"*. The
set of states is unaffected. The **retention window is not**.

Under `Records::Memory` a settled key occupies a slot in the record pool and *then*
becomes a tombstone. Under `Records::None` it enters the tombstone pool directly and
forgoes the record pool's grace entirely — so the window is `MAX_TOMBSTONES` of
churn instead of `MAX_TOMBSTONES` **plus** `MAX_RECORDS`. That lands on the one
degradation README limit 4 says a caller **cannot detect**, because past
`MAX_TOMBSTONES` a key reads `KeyState::Fresh` and runs again.

**The operands and not the quotient**, per main's `5834881 docs(keylink): stop
writing derived figures, because that one rotted twice`. This section carried
`8192 / (8192 + 1024)` and "about 11%" before that rule reached this branch; both
are correct today and both would have rotted silently the first time either constant
moved, with nothing to catch it. The reader can divide.

Stated instead of sold as free. `records_none_shortens_the_at_most_once_window` holds
the **direction** — the window is strictly shorter — asserted as a difference between
two ledgers under one bound. It does not hold the ratio, and cannot, because it sets
its own bounds to stay fast; the direction is the claim worth guarding.

### 3.2.2 It opts out of a principle this module states

`idempotency.rs`'s own "What this takes from Restate" section says:

> Taken: the reply is **replayed**, not refused — a caller that lost its answer
> wants the answer, and refusing hands it nothing it did not already have.

`Records::None` does the refused thing. That is not a reversal of the reasoning:
"refusing hands it nothing it did not already have" is a claim about what is good
for the *caller*, and it holds. What `Records` adds is that the reply is the
caller's *data*, and holding it for ten minutes is a cost the serving side may not
be willing to pay on the caller's behalf. The module doc now carries that paragraph,
because a design principle a later feature opts out of has to say so where the
principle is stated rather than in a release note.

### 3.2.3 Attach still waits, and refusing early was rejected

An attacher on a `Records::None` service parks, the handler completes, and its
re-claim is refused `Unrecordable`. That looks like pure cost — a QUIC stream held
for up to `MAX_ATTACH` to be told no — and refusing at the policy check instead was
proposed on exactly that reading. **Rejected.**

The wait buys a *definitive* answer. `Unrecordable` means the work ran, there is no
reply, reconcile and stop. Refusing at once could only answer `InFlight`, whose
documented meaning is *ask again when it finishes* — so a caller told that asks
again, and again, for as long as the handler runs. One bounded park becomes a
re-dial loop: more load on both sides, ending at the worse of the two answers. The
"optimisation" inverts.

And the park is already priced. A waiter holds no claim, which is precisely why
`MAX_WAITING_PER_PEER` and `MAX_WAITING` exist — RFC-05 §11.2 is the section that
added them — and `MAX_ATTACH` caps any one of them at thirty seconds. There is no
unbounded cost here to remove.

Documented on `Idempotency::attaching_for`, where a caller meets it, and held by
`an_attacher_on_a_recordless_service_waits_and_is_told_it_ran`.

### 3.2.4 A retention settle is a tombstone

`settle` routes on `matches!(slot, Slot::Done(_))`, so `Unrecordable` lands in
`Bucket::spent` and is bounded by `MAX_TOMBSTONES` rather than `MAX_RECORDS`. The
right pool: a tombstone is what keeps the work from running twice and has to outlive
the reply it never had. `Slot::held_bytes` needed no edit because `Unrecordable`
already holds zero — checked against the match rather than assumed, and
`records_none_settles_the_key_and_holds_no_reply` asserts `inner.bytes == 0`.

### 3.2.5 No `Default`, and the selectors are exhaustive

`Keyable` and `Records` both had `#[derive(Default)]` and `#[non_exhaustive]` at
first. Both are wrong on the crate's own rules, and `Allowlist` is the precedent —
a per-service policy enum a caller constructs and hands to a builder, which is
`#[derive(Debug, Clone)]` with neither attribute, and whose doc says *"There is
deliberately no `Default`. Admitting everyone is a decision you make."*

`Keyable::Any` is the permissive setting and `Records::Memory` holds a caller's
payloads for ten minutes. Neither should be reachable by leaving a field out. The
default lives in `ServiceBuilder::new`, spelled out on one line, which satisfies
invariant 8 — every default reproduces today's behaviour — without an impl that
hands it to anybody who forgets. And `CONTRIBUTING.md` reserves `#[non_exhaustive]`
for **error** enums rather than for ones a caller constructs.

The cost is that "the default is `Any`" is no longer assertable from a unit test.
It is held over a real connection by `the_defaults_reproduce_todays_behaviour`
instead, which is the better guard anyway: it asserts the default *service*
behaves like the explicitly-configured one, which is the claim invariant 8 actually
makes.

### 3.3 `Clone`, not `Copy`

A tier below memory is the shape this would grow into, and it would hold a port —
an `Arc<dyn ..>` — rather than a discriminant. `Copy` today would have to be
withdrawn then, and withdrawing a trait impl is a breaking change no attribute
covers. Note that §8's cold tier is now cut, so this is caution against a shape
that may never arrive; it costs nothing, and `Records` is constructed once per
service.

---

## 4. The ALPN moved, and it should have

```
keylink/Probe/d3fe34471921164e   -> keylink/Probe/d75913b8a4742547
keylink/Inspect/7bbd34fa83c9060c -> keylink/Inspect/0572763878b7fb9d
```

`IdempotencyRefusal` derives `postcard_schema::Schema` and sits inside every
service's hashed contract by the route RFC-05 §12 spells for `RemoteError`:
`Envelope<WireResponse<Response>>` → `WireResponse::Call(Outcome<Resp>)` →
`Outcome::Err(RemoteError)` → `RemoteError::Idempotency(IdempotencyRefusal)`. A
seventh variant moves the variant list, so it moves the hash, for every service.

CONTRIBUTING.md's step 1 was answered before either literal was touched: the change
**should** be wire-visible, for the same reason every other refusal in that enum
is. A peer built before it cannot decode `NotKeyable`, and being refused at the
handshake is better than decoding a refusal as a variant that no longer means what
it did.

**This is the first time both pins moved in one commit, and that pairing is
evidence.** Each earlier entry in `the_inspect_contract_hash_has_not_moved` says
"`Probe`'s hash did not move" as proof the two pins discriminate. The converse
proof is this one: a change to a type every contract shares must move **both**, and
a change to one service's own request or response types must move exactly one. A
shared change that moved only one pin would mean a service was hashing something
other than what it carries — the drift `WireResponse`'s doc comment records as
having happened once already.

`Inspect` carries no idempotency key on any of its own methods and this still moves
its ALPN. That is the cost, paid once, pre-1.0, by a crate with no fleet.

### 4.1 Measured twice, and a rename is not free

Both literals were read off a failing run of `tests/protocol.rs`, not derived. The
variant was first written `NotKeyed`, which gave `keylink/Probe/68a16a96e45f8da8`
and `keylink/Inspect/7ebc3f421c33c62a`; renaming it to `NotKeyable` moved both
again. **The hash is sensitive to a variant's name and not only to the shape of
what it carries** — worth knowing before anybody assumes a rename is a source-level
change.

### 4.2 Two alternatives, both rejected

**Reuse an existing variant, and the wire stays still.** Rejected for being false.
`Malformed` says the key is empty or oversized, and it is neither. `Unrecordable`
says the reply carried a payload. `Forgotten` says it ran, and nothing ran. A
refusal that misdescribes itself sends the caller to fix the wrong thing, and the
hash exists precisely so the honest version can be paid for once. This is the same
judgement §3.1 makes in the other direction, and the two are consistent: reuse a
variant when it stays true, mint one when it would have to lie.

> **Marker, added on review.** *"`Unrecordable` says the reply carried a payload"* was
> true when this section was written and §3.2 retired it **earlier in this same
> document**: the retention setting settles `Unrecordable`, so its wording no longer
> names a payload. Left standing rather than rewritten, per `5834881`'s precedent —
> the conclusion is unaffected, because what rules `Unrecordable` out for `NotKeyable`
> is that **nothing ran**, which never depended on the wording.

**Carry the refusal as a `RemoteError::Failed` prefix.** Rejected. There are
exactly four prefixes, they are load-bearing as a `starts_with` discriminator, and
a guard asserts none is a prefix of another. A fifth would be adding wire-format
vocabulary to dodge a wire-format change — trading a break the handshake catches
for one it does not.

---

## 5. Not called `Keyed`, and the crate had already said so

`rpc.rs`'s `Claimed` was renamed **out of** the word `Keyed`, and its doc comment
records why: in this crate a keyed thing is one addressed by an `ActorId` —
`KeyedService`, `KeyedClient`, `keyed_service`, `keyed_alpn`, `run_keyed`. Two
unrelated keys, one word, one crate.

The brief this was built from specified `Keyed` for the policy type. Taking the
word back for the idempotency side is exactly what that rename refused, and
`keylink::Keyed` would have sat two lines under `keylink::KeyedService` in one
export list meaning a different key.

**`Keyable` is what the type decides**: whether a key may be attached to a method.
`Keyable::Any`, `Keyable::None` — nothing here is ever said of an actor, so there is
nothing to disambiguate. `Keying` keeps the stem a reader has to work through;
`Idempotent` reads as a property of the reply rather than of the method. The
refusal follows the type: `NotKeyable`, which is also the truer sentence, since the
call *was* keyed by the caller and the method is what is not keyable.

`Claimed`'s doc comment now records that the word was asked for twice and refused
twice, so the paragraph is a standing rule rather than history.

---

## 6. What every default does

Invariant 8 — every default reproduces today's behaviour — is satisfied by one line
of `ServiceBuilder::new`, which writes `Keyable::Any` and `Records::Memory` spelled
out. **Neither type has a `Default` impl**, and §3.2.4 is why: those are the
permissive and the ten-minutes-of-retention settings, and `Allowlist`'s doc makes the
argument for refusing to hand either one to somebody who left a field out.

So the claim is not assertable from a unit test, and it is stronger for it.
`the_defaults_reproduce_todays_behaviour` asserts it over a real connection as a
comparison — one service naming neither policy, one naming both values out loud, both
answering the repeat from the record and both reading `Done` when asked. Asserting
only the first would prove nothing the pre-existing tests do not; asserting
`Keyable::default()` would only have proved that a `derive` was present.

---

## 7. What this does not do

- **Per-actor key policy.** Reachable through `Rpc::actor`, and there is no use case
  yet. This ends at per-method.
- **A runtime-computed method list.** `&'static` is deliberate: which methods are
  worth recording is a property of the service's design, not of its data. A policy
  that must change while the service runs is `Admission`'s shape — a port with a
  live value behind it — and nobody has asked.
- **Durable idempotency across a restart.** Unchanged; RFC-05's second bullet and
  README limit 2 still stand, and `Records::None` makes them no worse.
- **Any change to the bounds.** `MAX_RECORDS`, `MAX_RECORDED_BYTES`,
  `MAX_TOMBSTONES` and `RECORD_TTL` are untouched. `Keyable` changes *what reaches*
  the pools, not how big they are.
- **A tier below memory.** Proposed, analysed, and **cut**. See §8, which records
  why raising a bound beat building a tier.
- **Any policing of `key_state` beyond the service level.** `WireRequest::KeyState`
  carries a key and no method name, so `Only` and `Except` have nothing to check it
  against and the query is unpoliced within a service. Not a leak — see §2.6 — but
  worth knowing before anybody treats the policy as covering every wire path.

---

## 8. The cold tier was analysed and cut, and a bound beat it

A third `Records` variant — a port to somewhere the reply bytes go when memory gives
them up — was specified, costed and **rejected**. The reasons are worth keeping,
because a cold tier is the obvious next idea and this is the second time it has been
proposed.

**What it was for does not exist at the size claimed.** §3.1's correction is the
whole argument: `MAX_RECORDED_BYTES` is 1 MiB and does double duty as the per-record
ceiling *and* the total pool across every peer. So the pool holds exactly **one**
maximum-size reply, and `MAX_RECORDS = 1024` is unreachable for any reply above
1 KiB. A cold tier is a port, a worker, a bounded channel, two new `Slot` states, an
on-disk bound, a startup truncate and a second eviction loop — to relieve one
megabyte. Raising the bound buys the same capacity for none of it.

**And the mechanics were worse than the brief that proposed them said.**

- `keylink_persist::Claim` could not have been the adapter, and not only because it
  has `load` and `store` and no delete. `.mise/tasks/framing` **fails the build** if
  `Claim` is named outside `keylink-persist` at all, `impl Claim for` excepted — so a
  cold module inside `keylink` could not legally have called it.
- `iroh-blobs` cannot be the adapter while `forget(id)` is in the port, and the
  reason is content addressing rather than a missing method. Two distinct record ids
  whose reply frames are byte-identical map to **one** hash, so deleting by hash for
  one id destroys the other's record. Refcounting — which is what tags are — is the
  feature, not a prerequisite that could be skipped.
- A digest-named record needs **insert-if-absent** to be safe, and the brief's own
  doc comment for it claimed the opposite. It said a collision *"refuses a fresh key
  as `Forgotten`: a false refusal, never a double apply"*. With an overwriting store
  it is the reverse: two colliding keys of one peer share one record, last writer
  wins, and a retry of key A is answered with **key B's reply frame** — which decodes
  cleanly, because there is one ledger per service and therefore one response type.
  A silently wrong answer, which is worse than a double apply. Recorded because
  nothing built here has that shape and the next digest to enter this module will.

**What replaces it: split the constant and raise the pool.** Not simply raise it —
the double duty means raising `MAX_RECORDED_BYTES` to 64 MiB would also permit a
single 64 MiB reply to be recorded. Two constants: a per-record ceiling that stays
1 MiB, and a total pool that rises, each argued against what it bounds. That also
makes `MAX_RECORDS` reachable for the first time, which is worth stating out loud
since today it is decorative above 1 KiB per reply.

**Not done in this change**, and deliberately held: it alters every existing
service's memory ceiling and changes what `evict` does under load, so it wants its
own commit with a measured argument per constant rather than riding on a policy
change. It is the one piece of the analysis above that is still owed.

**Since then, the first half has landed and the second is still owed.** The split
is done: `MAX_RECORDED_BYTES` is now the total pool alone, and `MAX_RECORDED_FRAME`
is the per-frame ceiling. So every present-tense sentence above that says the
constant *does double duty* describes the tree this section was written against and
not the tree now — including the one in §3.1, which is left exactly as it stands
because that section preserves a retracted argument on purpose and the double duty
is what makes the retraction instructive.

The ceiling did **not** stay at 1 MiB as this section predicted, and the four bytes
are the reason. `MAX_HEADER` bounds the postcard payload while the ledger stores the
whole frame, prefix included, so a ceiling of exactly 1 MiB would refuse four frame
sizes the wire was willing to carry. The truthful never-rejects value is
`MAX_HEADER + 4`.

**Still owed: the pool's value.** Raising it partly revives the memory argument
§3.1 retracted — at a larger pool, what `Records::None` saves is no longer *"roughly
what its own tombstones cost"*. That tension is flagged here rather than resolved,
because resolving it is the value-move commit's work.

---

## 9. What is guarded, and the two claims that are not

Every claim in this document carries an executable guard except two, and both say so
where they stand.

- **`serve_one` claims the key before it calls `next`.** The ordering is a
  reordering of two statements, which `scripts/break-once.sh` — a single-literal
  substitution — cannot express. Standing in for it:
  `rg -n 'claim_key\(&keys|let \(outcome, body\) = next\(' keylink/src/rpc.rs`,
  where the `claim_key` match must carry the lower line number.
- **`Rpc::method` for `Addressed<T>` forwards to the inner request.** The break
  lives in `actor/remote.rs`, and `break-once.sh` restores from a backup on
  `trap EXIT` — an interrupted run would write a stale copy over a file another
  branch is editing. Standing in for it:
  `rg -n -A1 'fn method\(&self\) -> &.static str \{' keylink/src/actor/remote.rs`,
  which prints the body rather than the signature, because a signature is identical
  either way.

One guard was found vacuous and fixed rather than shipped. `tests/api.rs` was to
hold that `keylink::Keyable` resolves from a consumer's position, and the break —
dropping the re-export — failed inside the *library*, because `rpc.rs` imported
`crate::Keyable`. The test could not run and therefore proved nothing. `rpc.rs` now
reaches the type through `crate::idempotency`, which leaves the root re-export with
exactly one guard.

### 9.1 What a break record carries, corrected twice

This section first said: break records carry the assertion message **and the
pass/fail counts**, and not `file:line:col`, because a line number moves every time
anybody edits a doc comment above it. The premise was right and the remedy was wrong,
and the merge gate measured it wrong **inside this same wave**.

**Counts rot faster than line numbers, and for a sharper reason.** A line number rots
per doc line edited *above* the citation. A whole-target count rots per **test added
to the target** — by anybody, for any reason, including a test that has nothing to do
with the break. Nine of the twelve counts recorded here were stale by the time the
gate ran, and the one that proves the mechanism is this: a Wave 2 test added to
`tests/idempotency.rs` silently falsified a Wave 1 record in the same file, which had
correctly enumerated four reddened tests and was now five.

It is the same rule this document already relies on twice, arriving a third time: **a
record that embeds a value it does not control rots — silently, and while still
reading like evidence.**

#### The rule, and it is wider than counts

The literal shape is not the defect. A second branch audited itself against the
whole-target-count rule, found it carried none, then searched for the *class* and
found two more instances — a rustdoc naming how many callers a function had, and a
dated measurement of a number a tracked task exists to drive to zero. Neither is a
count of tests. Both rot the same way.

> **A record may only assert values the record's own file controls.** A module-scoped
> count qualifies. A whole-target count does not. A caller count does not. And a dated
> measurement qualifies only when the thing measured is not itself the subject of a
> scheduled fix — otherwise the date licenses the staleness instead of preventing it.

The operational test is one question: **who can move this value without touching this
file?** If the answer is anybody, retire it. That is why a date is enough for
`persist.rs`'s 7.6 ms — a physical measurement does not move when somebody edits code
— and not enough for a figure something is *working* to change.

So, for a break record:

- **Drop whole-target counts.** They are not facts about your break.
- **Keep the assertion message**, which is.
- **Name the tests that go red**, which is also a fact about your break and is
  checkable by re-running it. Where a break is wide enough that naming every test is
  noise, say it is wide, say what makes it wide, and name the ones that matter.
- **Do not claim to have named them all.** *"A, B and C go red" is an existence
  claim and survives somebody adding test D. "A, B and C are the only ones" is a
  universal over a set the record does not own, and does not.* Several records in
  this wave were written the second way and corrected to the first — and one of them
  then said *five* and listed *four*, omitting the very test it had just named as the
  fifth. That is the same error one level down: a list presented as complete. The
  count goes; the list stays.
- **Module- and file-scoped claims stay**, because who adds a test *there* has to
  touch that file. The four counts in `mod policy` are of that kind, and so is
  `tests/idempotency.rs`'s "within this file, three tests and no more".

Two more instances of the class were found on this branch by the same question and
retired: an RFC asserting that `evict` has *"exactly one caller"* — a count of another
file's call sites — and a sentence pricing the retention saving against
`MAX_TOMBSTONES`' own *"on the order of a megabyte"*, which is a figure in a third
comment that nothing measures.

### 9.2 A quoted anchor rots three ways, and one of them is now a gate

`break-once.sh` anchors on a literal that must be **unique in the file it edits**, so
a record that quotes the line it breaks makes that file non-unique and the run aborts
with `expected exactly one occurrence, found N`. That fired repeatedly across this
branch, and every time the harness was working.

The rule is **"do not quote the literal when the record lives in the file being
broken"** — not "never quote". A record in `tests/idempotency.rs` describing a break
in `src/idempotency.rs` may quote freely: uniqueness in the target is untouched and
the quote is the more precise record.

A second failure mode makes describing the better default even across files. One
record here quoted `self.ledger.as_ref().filter(..)` as a single line; `rustfmt` split
it across three when the signature took `&'static str`, so the anchor stopped existing
and the recorded run became unreplayable — with nothing red to say so. **Name the
function and describe the edit; the formatter owns the line breaks and you do not.**

Note also that it is only the **search** literal that is constrained. `break-once.sh`
greps and counts `$from`; `$to` is never searched, so quoting the **replacement**
verbatim cannot affect uniqueness anywhere and should be done — a sibling branch's
over-wide reading of this rule left a reviewer hand-constructing working Rust to
reproduce a record that could simply have shown it. Describe what you searched for;
quote what you put in its place.

#### The default is QUOTE, and this document had it backwards

Everything above reads as though describing were the safe side. **It is the ungated
side, and that is the opposite.**

> Keep a quote wrongly → `mise run anchors` reddens: loud, cheap, one edit.
> Describe wrongly → **nothing checks it at all**, and a description that does not
> locate the edit is shippable.

That asymmetry cashed out in the *first three conversions ever made under this rule*.
One of them — `a_keyed_actor_service_applies_the_same_policy` — was converted to
*"the `.keyable(..)` call in its fixture above"*. That test has **no fixture**; it
builds inline, below the comment. What is above is a fixture four *other* tests use,
whose `.keyable(..)` takes the policy as an argument. Editing the line the description
sends you to gives `1 passed; 0 failed`.

**A reader following it would have concluded the guard was vacuous, which is false —
and that is worse than the rot it replaced.** A stale anchor aborts loudly at
`break-once.sh`'s precondition. A misdirecting description reddens nothing and looks
like a finding.

So:

> **Quote by default, because the gate covers you. Describe only where quoting
> provably cannot work — and replay a converted record from its description alone,
> before its entry leaves the list.**

The replay is the part with teeth. Dropping an entry is four deleted lines; re-quoting
means finding the new text. The incentive runs toward describing, so the conversion has
to carry a cost that catches a description which does not locate the edit — and only a
replay does.

**And the axis is post-hoc, which is worth admitting.** Calling an anchor "line-local"
means predicting no future edit reflows it, and `R26b`/`R26c` are function signatures —
the same construct that rotted on `ledger_for`. So the axis decides where quoting is
**impossible**, not where it is unnecessary. Where it is merely fragile, quote it and
let the gate carry the fragility.

#### The third mode: a positional anchor

A mechanical sweep after this wave merged found a rot mode neither rule above
reaches. `tests/api.rs` anchored on a slice of `lib.rs`'s `pub use idempotency::{..}`
list. A later wave inserted one more export into the middle of that slice, taking the
anchor to **count 0**. The record and its target are in different files, so the
same-file rule permits the quote; nothing rewrapped, so the formatter rule says
nothing. What moved was **a sibling's presence in an ordered list**.

#### The fourth mode: a fragment, or a line that prefixes another

Found on a sibling branch by the same sweep. Three anchors aborted for one reason: the
quoted span was **not a whole line**, so the documented remedy — *pass the line with
its indentation* — could not make it unique.

- `DIGEST_BYTES: usize = 16` sits at **column 0**: there is no indentation to add.
- `pop_front()` is **mid-line**: indentation is irrelevant to a fragment.
- `self.call` with its indentation is a **prefix of both** candidate lines, so
  following the remedy exactly still gives 2. It needs the **trailing newline**, which
  no record anywhere mentions.

So the rule gains a clause: **a quoted anchor must be a whole line, including its
indentation, and for a line whose text prefixes another it must include the trailing
newline.** The abort reads as the harness failing when it is the record that is wrong.

#### The fifth mode, which no resolution check can see

A record's watched message is a claim about **which assertion fires first**, so it
rots when an assertion is added **above** the recorded one. Found on a sibling branch:
a later commit inserted an on-disk check above where a value was read, so a different
assertion now fires. Nothing broke, nothing was red, and the guard was always correct
— **the record described a race it no longer wins.**

All four rules above permit it. Not same-file, nothing rewrapped, no ordered list, a
whole line. And `break-once.sh` reports failure either way, because the test still
fails. **Only comparing the recorded sentence against a fresh run finds it**, which is
what `scripts/replay-fate-breaks.sh` does on the set it covers.

`mise run anchors` cannot see this and says so in its own header. It answers the
narrower question of whether the break can be **started**. The check that finds mode
five is a replay, and the standing rule that follows is: **every branch re-runs its own
full record set during its rebase.** Seventeen break/test pairs measured at 69 seconds,
so cost is not a reason to skip it — the reason not to re-run *another* branch's set is
scope, never cost.

#### A record that quotes no message at all is unverifiable, and that is a failure

Three of four defects on that same branch were this shape: the record described the
*effect* — *"the two ids become equal"* — and never said what was watched. There is
nothing to compare a re-run against, so such a record can be shown neither stale nor
current.

**The design rule for any checker, including this one: "no message recorded" must be a
FAILURE, not a pass.** A checker keyed on mismatch alone calls all three green, which
is the silent-success case again — and it is the arm to write *first*, because these
are the records nobody can audit.

#### Never quote the value under test

Cite the guard, not the value, reaches one step further than we had it. A record beside
a digest test declined to quote the digest, because **quoting it makes the record rot on
exactly the change the test exists to catch.** *"`left` and `right` the same `CallId`"*
is the durable form. This is the same rule pointed at the guard's own subject.

#### The question that reaches all four

**Is the anchor's uniqueness a property of the line, or of its neighbours?** — with the
sharpening mode 4 forces, that **a fragment is not a line at all**. A `pub use` list
item's text is a property of its neighbours, and a `pub use` list is the most-added-to
construct in a crate, so re-quoting buys only until the next export lands and would put
that chore on whoever adds it. Describe those.

#### `mise run anchors`, because reading does not find these

Three records on `main` quoted anchors that no longer resolved and **not one was red**.
Two of them had already been through a review that checked the evidence beside the
anchor and not the anchor itself. So the check is a gate now, in `.mise/tasks/anchors`,
first in `gate` after `framing` because it only reads files: for every record that
quotes a search literal, that literal must occur **exactly once** in its stated
target, which is `break-once.sh`'s own precondition and nothing more.

Its list is written out by hand, and that is deliberate for the reason `local.rs`'s
`no_failure_prefix_is_a_prefix_of_another` spells out its five prefixes by hand: a
heuristic that parsed prose for *"replacing `X` with `Y`"* and missed a record would
check nothing while reporting success. **The honest limitation is that a record added
without an entry there is unchecked**, which is the cost of the hand-maintained list.

It earned itself immediately. Applying the three known findings, this document's author
fixed the wrong record in `idempotency.rs` — a neighbouring one whose search edit was
already described — and the gate reddened on the one that was actually stale.

#### The trade this makes, stated rather than left implicit

**A described record is rot-proof and cannot be driven by a script.**
`scripts/replay-fate-breaks.sh` replays a break and asserts the record's watched
substring appears in the run, and to do that it needs a `from`/`to` pair — quoted text.
Every record converted to prose therefore leaves the replayable set, and what stays
checkable about it is the assertion substring, by a human.

So it is decided per record, and the two axes are independent:

- **Same-file records: describe *when quoting cannot be made unique*, which is not
  always.** This said "always" and that was too strong. A sibling branch's same-file
  anchors resolve, because the record's quote sits in a `///` comment **without**
  indentation while the code carries it — so the indented search literal is unique and
  the record's copy is not a second occurrence. Same-file quoting is legitimate **when
  indentation disambiguates**.

  **And the exclusion is sharper than "column 0 or a fragment", which are symptoms:
  it fails exactly when the record's quote is BYTE-IDENTICAL to the code.** Those two
  are simply the ways that happens — code at column 0 has no indentation for the quote
  to lack, and a fragment has none to carry. State the property and the two cases fall
  out of it; state the two cases and a third way of colliding goes unnoticed.

  **A measured nuance that makes describing a stronger call rather than a forced one.**
  For one such anchor — a `const` at column 0 — a unique form does survive: taken
  *with its trailing newline* it is count 1, because the record's copy is followed by a
  closing backtick rather than a newline. So "no unique form exists" is the weaker
  argument and it is not quite true. The stronger one is that the surviving form's
  uniqueness rests on **a closing backtick sitting between the literal and the line's
  newline** — a dependency on the record's own punctuation, more fragile than the
  indentation dependency it would replace, and one fenced code block away from
  breaking. Describe it because the only quote that works is held up by a backtick,
  not because no quote works.
- **Positional anchors: describe.** The alternative is a gate that reddens for every
  unrelated export and trains people to re-quote mechanically, which is worse than no
  gate.
- **Cross-file, line-local anchors: keep the quote.** The fragility is real and it is
  now *caught* rather than silent, which is exactly what makes it affordable — a
  quoted anchor is worth its fragility precisely because `mise run anchors` fails
  loudly when it rots. That is the resolution of the tension: the gate is what buys
  the quotes.

And when auditing records, check that the **anchor still exists**, not only the
evidence beside it. Both unreplayable records found during this wave were found by
re-running the break rather than by reading it, and one had already been reviewed for
its counts while its anchor had been dead for some time.

---

## 10. Postscript: what the brief got wrong

- It specified `Keyed` for the policy type, which is the name `Claimed`'s doc
  comment in `rpc.rs` records the crate as having already refused for this exact
  meaning. §5.
- It did not price the refusal. `IdempotencyRefusal` is inside every service's
  hashed contract, so `NotKeyable` moves both ALPN pins — the first thing that had
  to be settled, and it was settled by running `tests/protocol.rs` rather than by
  reasoning. §4.
- It proposed asserting `keyed_none_builds_no_ledger` without saying how, and the
  obvious behavioural version is the vacuous one: a refusal looks identical whether
  the ledger is absent or empty. §2.3.
- It framed `Unrecordable` as a precedent *for* minting `Discarded`. Read closely it
  is a precedent against — `Slot::state` maps it to a `KeyState` variant for free, so
  a new refusal would have needed a second new name to stay honest. §3.2.
- It sold `Records::None` on **memory**, and the saving is about one megabyte once
  per process because `MAX_RECORDED_BYTES` is a total and not a per-key figure. The
  motive is retention. §3.1.
- It said the key state is *"not affected"* by `Records::None`. The set of states is
  not; the at-most-once window is shorter by `MAX_RECORDS` of churn. §3.2.1.
- It specified `Slot::Forgotten` for the retention settle, which puts a configured
  choice under a name that means pressure — the wrong side of the line RFC-05 §5
  draws. §3.2.
- Its `Keyed`/`Records` sketch carried `#[derive(Default)]` and `#[non_exhaustive]`,
  and `Allowlist` is the precedent against both for a policy enum a caller
  constructs. §3.2.4.
- It named `Service` as the struct holding the ledger. The ledger is on
  `RpcService`, and `Service` has type-erased it, so the brief's own goal — a reader
  seeing the allocation not happen — was unreachable where it asked for it. §2.3.
- Its fact table said `evict` runs inside `claim`. It does not: `claim` and
  `key_state` call `expire`, and `evict` is reached from `settle`. `expire` is also a
  producer of `Forgotten` the table never mentions. Stated as which function calls
  which rather than as a caller *count*, per §9.1 — a count of callers is a value
  this document cannot keep true.
