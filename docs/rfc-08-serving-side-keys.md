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

---

## 3. `Records`: a key without a stored reply

```rust
pub enum Records {
    Memory,   // the default, and what the crate did before
    None,     // the key still claims; every repeat is refused
}
```

The key **state** — running, done, interrupted, forgotten — is unaffected by this
and never leaves memory. That is what gives at-most-once and nothing here can
weaken it. `Records` is only about the reply bytes.

The gap, measured: a settled `Entry` is **56 bytes** — a 40-byte `Slot` plus a
16-byte `Instant` — beside a 24-byte `IdempotencyKey` and at most `MAX_KEY_LEN`
bytes of heap behind it, against up to `MAX_RECORDED_BYTES` of reply, which is
1 MiB. Roughly four orders of magnitude, and that gap is the whole reason the two
are separable. `std::mem::size_of`, aarch64-apple-darwin (Darwin 27.0.0, arm64),
rustc 1.97.1, 2026-08-07. The absolute number is not pinned by a test, because
`Instant` is not obliged to be 16 bytes on `wasm32-unknown-unknown`, which this
crate also builds for; the ratio is, by
`the_key_state_is_orders_of_magnitude_below_a_reply`.

So `Records::None` gives up the answer and keeps the guarantee. The work runs at
most once; the repeat is refused instead of replayed.

### 3.1 `Forgotten`, and not a new `Discarded`

`Forgotten` meant *"it ran and the reply expired or was evicted"*. Under
`Records::None` the reply never existed, so that sentence was about to become
false — and a false doc comment is this repo's most-corrected defect. The variant
is reused and its documentation now names three ways in: aged past `RECORD_TTL`,
evicted under pressure, or never recorded because the service records none.

**The decision was taken in the world where the wire had already moved.** Wave 1
added `NotKeyable` and moved both ALPN pins, so a `Discarded` variant would have
cost nothing extra to carry. It is still not here, for three reasons.

- `TooManyRunning`'s doc comment is this crate's standing ruling on exactly this
  trade — *one variant rather than two, because the caller acts on them
  identically* — and the caller's action here is `read the result out of your own
  domain, do not re-run`, which is README limit 3's own wording and is the same
  sentence for an eviction and for a service that records nothing. The open wire
  lowered the price and supplied no benefit.
- The operator distinction survives without a name. `Records::None` is a property
  of the **whole service** and static: such a service answers `Forgotten` to every
  keyed repeat, always, from the first one. There is nothing to disambiguate. This
  is what separates it from `Unrecordable`, which is per-**method** — within one
  service some methods record and some do not, and there a name is the only way to
  tell which you met. The precedent cuts the other way from how it first looks.
- One wire break, one cause. Carrying both variants in one commit would leave a
  future reader of `tests/protocol.rs` unable to say which moved the hash.

That per-method/per-service line is also why `Guard::record` short-circuits
`carries_body` under `Records::None` rather than checking it first. `Unrecordable`
blames the method, and under `Records::None` the method is fine.

### 3.2 A `Records::None` settle is a tombstone

`settle` routes on `matches!(slot, Slot::Done(_))`, so `Forgotten` lands in
`Bucket::spent` and is bounded by `MAX_TOMBSTONES` rather than `MAX_RECORDS`. That
is the right pool: a tombstone is what keeps the work from running twice and has to
outlive the reply it never had. `Slot::held_bytes` needed no edit, because
`Forgotten` already holds zero — checked against the match rather than assumed, and
`records_none_settles_the_key_and_holds_no_reply` asserts `inner.bytes == 0`.

### 3.3 `Clone`, not `Copy`

A tier below memory is the next thing this type grows, and it will hold a port —
an `Arc<dyn ..>` — rather than a discriminant. `Copy` today would have to be
withdrawn then, and withdrawing a trait impl is the breaking change
`#[non_exhaustive]` does not cover.

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

`Keyable::default()` is `Any`. `Records::default()` is `Memory`. Both reproduce the
behaviour that existed before either type did, and `ServiceBuilder::new` writes
`Keyable::default()` rather than `Keyable::Any` so the default lives in one place.

`the_defaults_reproduce_todays_behaviour` asserts it as a comparison over a real
connection — one service naming neither policy, one naming both defaults out loud,
both answering the repeat from the record — because asserting only the first would
prove nothing the pre-existing tests do not.

`#[default]` may sit on exactly one variant, so there is no state where a default is
unset or ambiguous. That half is the compiler's and neither test tries to break it.

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
- **A tier below memory.** `Records` is shaped to grow one and does not have one.
  See §8.

---

## 8. Left for later: a cold tier

`Records` exists as an enum with two variants because the third is a port —
somewhere the reply bytes go when memory gives them up. Three things are already
known about it and are recorded here so the next attempt starts from them rather
than rediscovering them.

- **An adapter that cannot delete is not an adapter.** A ledger tier that only
  grows is not a tier, it is a leak with an index. That rules out
  `keylink_persist::Claim`, which is one exclusive handle per `StateId` with
  `load`/`store` and no delete — a single-state atomic replace, not a keyed table.
- **`iroh-blobs` is not the first adapter either.** It has no public delete-by-hash,
  and every `add_bytes` creates a permanent auto-named tag whose name `blobs.rs`
  throws away, so every record would become a permanent GC root. Named or temp tags
  plus a GC-enabled store constructor come first, and that is its own piece of work.
- **Spill on eviction, never on record.** `Guard::record` runs before the reply
  frame is written, so a cold write there adds disk latency to every keyed reply.
  `evict` is where the bytes are being given up anyway — and it runs under a
  `std::sync::Mutex`, so it cannot await: take the bytes, mark the slot, hand them
  to a worker through a bounded channel, flip the slot when the write lands. **A
  worker that fails or a channel that is full settles the slot as `Forgotten`**, and
  that is the property that makes the whole tier safe: a cold tier can never break
  at-most-once, only the ability to answer.

Also known and unresolved: `Eviction` is already a public name in this crate for
actor eviction, so a type naming which record memory sheds first needs a different
word. §5 is the standard that decides it.

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

Break records in this wave carry the assertion message and the pass/fail counts and
**not** `file:line:col`. Two of this repo's recorded defects are break records
citing lines the tree no longer has, and a line number moves every time anybody
edits a doc comment above it.

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
  is a precedent against: the distinction it draws is per-method, and
  `Records::None` is per-service. §3.1.
