# Goal: keylink correctness programme — idempotency, caller identity, push, admission

> **Current testing note (2026-08-10):** Any ALPN literal-pin step below is
> historical and is superseded by `CONTRIBUTING.md`'s **The wire contract**.
> The literal-pin tests are ignored until keylink has a stable version. Do not
> run or update them, and do not order work around them. The active contract
> property tests remain in the gate.

## Intent

Close the twelve defects and gaps a design review found in keylink, so the crate
is honest about what it guarantees before anyone depends on it. The headline is a
silent correctness bug: a `Timeout` layer's refusal is recorded as an idempotency
key's settled reply while the work it refused runs anyway and mutates state — a
double-effect with a permanently wrong answer, in the one subsystem whose entire
purpose is at-most-once. Alongside it: caller identity is unavailable to a
`Handler`, a served actor can be held hostage by a stalled upload, a peer cannot
be revoked at runtime, and the accept side cannot push.

keylink is greenfield and unpublished. ALPN moves and breaking changes cost
nothing today and become impossible later, so every fix here is taken in its
correct form rather than its compatible one.

## Context

`/Users/pedrocunha/repos/keylink`, a Rust workspace of **three** members —
`keylink`, `keylink-persist` (landed `dab3b58`) and `demo` — plus
`keylink/examples/` and `docs/rfc-0{1..4}-*.md`, which are the decision record.
Default branch `main`. Gate is `mise run gate`.

**Item 4 touches 14 files** — every `impl …::Handler for` / `impl …::Actor for`
site: `demo/src/main.rs`, `keylink/examples/durable_ledger.rs`,
`keylink/src/macros.rs`, and eleven under `keylink/tests/`.
`keylink-persist` declares no `service!` block and is **not** affected.

The seams this work touches, all of which exist and should be reused rather than
reinvented:

- `keylink/src/rpc.rs` — `serve_one` (the accept path), the layer chain, the
  ledger claim at `:230` and record at `:267`, the serving-side accept loop at
  `:339-342`, and `RpcClient` which already owns a `Connection`.
- `keylink/src/idempotency.rs` — `Ledger`, `Slot`, `Guard`, `evict`. Bounded by
  volume only; has no notion of time.
- `keylink/src/middleware.rs` — `Middleware`, `Next`, `Context`, `Timeout`.
- `keylink/src/authz.rs` — `Allowlist`, `ProvenPeer`, `AdmittedPeer`.
- `keylink/src/macros.rs` — eleven kind-dispatching macros with no catch-all
  arms, plus the generated `Handler`, `Actor`, `Handle`, `Client` surfaces and
  the `DECLARES_BLOBS` compile-time fold read by `spawn`.
- `keylink/src/watch.rs` — `Watch`/`Departure`, the connection-ended signal.
- `keylink/src/actor.rs` — `Eviction::Idle(Duration)` and `Registry::now()`,
  already doing age-based eviction on `n0_future::time::Instant`, which is
  portable to wasm. Copy this; do not invent a second clock.
- `keylink/src/representability.rs` — the shadowing and coherence probes that
  turn "there is no way to do X" from prose into a compile error.
- `keylink-persist/` (workspace member, landed `dab3b58`) — **the repo's
  reference for how a port is written here.** `Snapshots`/`Claim`/`Durable`,
  with exactly one adapter, justified by a comparison. Three things to copy and
  one not to:
  - Copy: the port's promise is enforced by the **type**, not a comment —
    `store` is a method on `Claim`, so bytes cannot be written without holding
    exclusivity. `Admission` needs an equivalent for fail-closed.
  - Copy: one adapter on purpose, with the comparison written down.
  - Copy: `#![forbid(unsafe_code)]` and `#![warn(missing_docs)]` on anything new.
  - **Do not** copy its separate-crate placement. persist is a separate
    workspace member so `keylink` gains no dependency and no feature, because
    its adapters need a filesystem. `Admission` sits on keylink's own
    connection-accept path and its impls need nothing external, so the port and
    its impls stay **inside `keylink`**.
  - **Do not** reuse `Snapshots` for the idempotency ledger. persist says it
    plainly: *"No history. One state, one current value. A journal is a
    different port with different operations."* The ledger is journal-shaped.
    Durability for the ledger stays out of scope.

## Skills (load the matching one BEFORE working in its area)

**Review gates (always):** `brooks-review` and `brooks-sweep` run the waves;
`requesting-code-review` before the PR; `receiving-code-review` to triage every
finding. Add `brooks-audit` (this changes module structure — a new port, a new
registry, a layer moved) and `brooks-test` (several existing tests encode
intent that changes).

**Rust craft (always, this is a Rust crate):**
- `rust-best-practices` — error hierarchies with `thiserror`, `NewType` domain
  scalars, static vs dynamic dispatch, `Send`/`Sync` reasoning, type-state.
  Chapter 4 governs the new error variants; chapter 7 governs any absence this
  work claims.
- `ponytail` — run it on every wave. The single largest risk in a twelve-item
  programme is building more than the defect requires.
- `improve-codebase-architecture` — mandatory for the `Admission` port and the
  connection registry, which are the two new modules. Apply the deletion test.

**Process/meta:** `test-driven-development` (every fix here is a red test
first — see the red-test invariant), `systematic-debugging` (for anything that
does not reproduce on the first attempt), `writing-plans`,
`subagent-driven-development`, `using-git-worktrees`.

**Caveats — read this before reaching for a skill list:**
- **Every `arche-*` skill is irrelevant here.** They target a Python monorepo at
  `~/repos/arche` — Postgres, SQLC, FastAPI, pyright, `mise run p:full-check`.
  None of it applies to this Rust crate. Do not load them; do not import their
  gates. The `write-goal` skill's invariant library is written against that
  monorepo and its Python-specific invariants (Pydantic, `NewType` from `typing`,
  testcontainers, cassettes) are **replaced** by the Rust invariants below, not
  added to them.
- The RFCs are the decision record and several are **stale in known ways** —
  RFC-04 §15 is a postscript recording where §1.4, §2.2, §5.3 and §10.3 were
  wrong. Read §15 before trusting any earlier section of RFC-04.

## Scope

**In scope** — four waves. Only **D depends on C** (it reuses C's registry);
A, B and C are independent of one another and may be built in any order, or in
parallel worktrees. An earlier draft claimed each wave depended on the last,
which was false.

*Wave A — idempotency correctness (RFC-05 written first)*
1. **A layer's refusal must never settle a key as a final reply.** Fixes three
   defects at once: a layer refusal recorded and replayed forever; `Interrupted`
   reported when nothing ran; `Timeout` and `IdempotencyKey` not composing.

   The root cause is precise, and it is *not* where the ledger sits. It is that
   `Outcome::Err` cannot say **who refused** — its own doc reads *"refused, by
   the handler or by a layer above it"* (`reply.rs:80`) — so `serve_one` records
   whatever the chain returned, at `rpc.rs:259-269`, with no way to tell a
   handler's answer from a layer's veto.

   **An earlier draft of this goal said "make idempotency the innermost layer".
   That is wrong and must not be built.** A `Middleware` returns a *typed*
   `(Outcome<Resp>, Body)`; a replay writes a recorded `Bytes` frame straight to
   the stream without decoding it (`rpc.rs:127-132`). A ledger expressed as a
   layer would have to decode and re-encode, destroying the byte-for-byte
   property the ledger is built on.

   The requirement, therefore, stated as behaviour rather than mechanism:
   - An outcome produced by the **handler** settles the key, as today.
   - A refusal produced by a **layer that never called `next`** leaves the key
     claimable — nothing ran, so nothing is owed.
   - A refusal produced by a **layer that did call `next`** settles the key as
     unknown (`Interrupted`), because whether the work landed is genuinely not
     known at that point.
   - **Byte-for-byte replay survives.** Any design that decodes and re-encodes a
     recorded reply is rejected.

   **Splitting `Outcome::Err` by provenance does not work, and "just don't
   record a layer-produced `Err`" is wrong.** A panicked *actor turn* also
   returns `Outcome::Err(RemoteError::Failed(..))` (`macros.rs:443-448`) — the
   turn ran and may have mutated. Releasing that key would permit a re-run. The
   `Outcome` value genuinely cannot distinguish "a layer said no" from "the base
   ran and failed".

   The candidate mechanism, which the plan should cost first: a three-state
   signal rather than a position — a `pub(crate)` marker on `Context`, set to
   *entered* by a thin wrapper installed around `base` in `ServiceBuilder::build`
   (`rpc.rs:450-453`) before it calls the base, and to *completed* after. Then
   `rpc.rs:267` becomes: not entered → release the key; entered and completed →
   record; entered and not completed → drop the guard, which settles
   `Interrupted`. That needs a `Guard::release()` (settle without recording) and
   leaves `claim_key` and the replay path exactly where they are — no `Resp:
   DeserializeOwned` bound, no re-encode, no two-phase lookup.

   **Explicitly not promised:** the retry does *not* receive the real reply of a
   timed-out turn. That needs the `Guard` to travel inside `Mail` and be settled
   by the turn itself — a larger, separate item, and most of the same machinery
   as `attach`. Build both or neither.

   Whatever is chosen, `Outcome`'s doc comment stops conflating the two.
5. `Slot::Running` is bounded. Today it is invisible to both ledger bounds, so N
   connections admit up to 100×N unaccounted claims in one ledger.
   **Use a separate `running` counter in `Inner` plus a per-peer cap. Do NOT
   fold `Running` into `max_records`:** `evict` returns early when no bucket has
   settled keys (`idempotency.rs:284-286`), so a bound that counted running
   claims would silently stop being enforced rather than fire.
9. A TTL as a second bound, on `n0_future::time::Instant`, copying
   `actor.rs`'s `Eviction::Idle`. Bounds become configurable per service.
7. Eviction leaves a **tombstone** so a forgotten key is refused rather than
   silently re-executed. New `IdempotencyRefusal::Forgotten`.
3. A generated **key-status query** returning `Fresh | Running | Done |
   Interrupted | Forgotten`, peer-scoped by construction.
   *plus* **`attach`** — a second caller under a live key waits for the running
   invocation instead of receiving `InFlight`. Designed and reviewed as a
   feature distinct from the query.

   **The wakeup, and the hazard that killed this the first time.**
   `Slot::Running` gains an `Arc<Notify>`; `settle` calls `notify_waiters()`
   before replacing the slot; a waiter clones the handle, **drops the ledger
   lock**, waits on `n0_future::time::timeout(budget, …)`, then re-claims.
   Bounded by construction: one `Notify` per in-flight key, and in-flight keys
   are bounded by the transport.

   `idempotency.rs:41-46` rejects attach by citing RFC-04 §3.3, whose actual
   worry is *"whether a dropped `Notified` can swallow a wakeup"*. **That worry
   is real and specific**: `notify_waiters()` wakes only *currently registered*
   waiters, so a waiter that registers after the notification misses it forever.
   It also has a known answer — `tokio::sync::Notified::enable()`, which
   registers before the await:

   ```rust
   let notified = notify.notified();
   tokio::pin!(notified);
   notified.as_mut().enable();   // registered HERE, before the re-check
   // re-read the slot under the lock; if it settled, answer without waiting
   // only then await `notified`
   ```

   RFC-05 must show this pattern and say that §3.3's objection was correct about
   the hazard and is answered by `enable()`, rather than pretending the citation
   was wrong. A server-side `MAX_ATTACH` cap bounds a peer that sends an absurd
   budget.

   Two further things `attach` must confront, neither obvious:
   - **A reply that carries a body is `Unrecordable`** (`idempotency.rs:327`),
     so attaching to a `recv` or `blob` method has nothing to hand back and must
     degrade to `InFlight` rather than wait forever. Say so in the type or the
     doc, and test it.
   - It shares most of its machinery with "the retry receives the real reply"
     (excluded from item 1): both need the `Guard` to outlive the original
     caller and be settled by the work rather than by `serve_one`. If `attach`
     is built, revisit that exclusion.

   Also record in RFC-05, because it is a live wart either way: a `NoSuchActor`
   refusal from `deliver` settles the key, so a retry after the actor is mounted
   replays `NoSuchActor` forever.

*Wave B — caller identity and actor safety*
4. Every generated `Handler` method takes `cx: &Context` first; every generated
   `Actor` method takes `cx: Option<&Context>` first. `Actor::caller` is
   **deleted**.
6. A service declaring a `send` method **cannot be served as an actor**. Fold a
   `DECLARES_SEND` const the way `DECLARES_BLOBS` already folds, and assert it
   at **compile time** in the generated `Handle::service`/`Handle::serve` —
   **not** a runtime `ConfigError`, and **no opt-out**. A compile error beats a
   startup error, keeps `Result` off the generated serve site, avoids a new
   error variant, avoids rippling into `spawn_with`'s
   `Fn(&ActorId) -> Option<Mounted>`, and matches this goal's own invariant that
   an absence claim is a compile-time probe rather than a comment. Scoped to
   `send` alone on purpose: `recv` drains after the turn and `blob` streams
   later, so only `send` reads its body inside the serial turn.
8. `QueueTimeout` — refuses only while a message is still queued, so it sheds
   real load and can never refuse a message already in a turn.

   `Timeout` is **kept**: on a `Handler` it genuinely cancels, and `QueueTimeout`
   cannot. What this wave must decide — explicitly, not by default — is what
   stops `Timeout` being layered on an actor-served builder: a marker bound on
   the actor path's `layer`, a startup refusal, or a considered "documented, not
   prevented". Whichever is chosen, **`macros.rs:628-648` is rewritten in the
   same commit**: wave A fixes the idempotency half of what that paragraph
   describes and leaves the load-shedding half true, so it becomes a half-stale
   comment that is the only thing standing between a user and the bug.
10. A long-running actor turn can observe that its caller has gone, and bail at
    a safe point of its own choosing. The signal already exists and costs
    nothing: `Mail.reply` is a `oneshot::Sender`, closed exactly when the
    caller's future was dropped — no task, no timer, no cancellation.

    **One decision the plan must make explicitly, not by default.** If this is
    exposed on `Context`, `Context` is shared with `Handler`, where nothing
    wires the signal, so the method would silently always answer "no" for every
    handler. A method that lies to half its callers is worse than an absent one.
    Either put it somewhere only an actor turn can reach, or do not build it.

*Wave C — server-initiated messages*
2. RFC-04 §4.4's Design B: an accept loop on the dialing side, an admission
   decision on the dialing side, and **one** per-peer registry on the accept
   side, evicting on `Departure`. No new method kind and no `WireKinds` field —
   the reverse contract is an ordinary `service!` block with its own derived
   ALPN.

   **One registry, not two.** An earlier draft asked for "a per-peer
   inbound-connection registry" *and* `Subscribers<C>`. Those are the same
   `HashMap<EndpointId, _>`. Build it once; wave C uses it to fan out and wave D
   uses it to revoke. Its only non-obvious content — and the reason it is a type
   rather than a bare map at a call site — is that it evicts via `Watch`/
   `Departure` rather than `Connection::closed`, which is precisely the
   pin-its-subject-open leak `watch.rs:35-42` argues about. Write that
   comparison down, in the persist crate's style, or do not introduce the type.

*Wave D — runtime admission*
11. An **`Admission` port**; `Allowlist` becomes its canonical impl. The default
    dynamic impl is a snapshot behind a lock, not an actor.
12. Revoking a peer **closes its live connections**, reusing wave C's registry.

*Cross-cutting*
- **RFC-05** (new) records the idempotency programme, including `attach`'s
  reversal and the generated-client-trait follow-up.
- **Appends** to RFC-04 (§4.4 naming the NAT case, §2.6 on `&Context`, §6 on
  `QueueTimeout`, §10.4 on `send`-on-actor) and to RFC-02 (admission, beside
  capability A). Append — never edit.

**Considered and declined — build all four; RFC-05 records why**

A minimalism review recommended cutting `QueueTimeout` (8), the TTL (9), the
`key_state` query (3), and the `Admission` port (11) as under-justified. All
four are **kept**, by decision. RFC-05 records the argument against each and the
reason it did not carry, so the question is answered once rather than reopened:

- `QueueTimeout` — the objection is that item 1 removes its stated purpose and a
  1-deep mailbox behind a 100-stream cap is not worth shedding. It is kept
  because `Timeout` cannot express "refuse only what has not started", and that
  is the only bound that is safe to combine with a key.
- The TTL — the objection is that it attaches to no defect. It is kept because
  a volume-only bound makes the at-most-once window a function of unrelated
  traffic, which is not a property an operator can reason about.
- `key_state` — the objection is that a retry already answers four of the five
  states. It is kept because the fifth, `Fresh`, is the one a caller cannot
  obtain without executing, and because probing must not require re-submitting a
  market order.
- The `Admission` port — the objection is that `Arc<RwLock<Allowlist>>` is
  ~20 lines and loses nothing. It is kept because the point is an external
  policy backend, which a lock around a local value cannot be.

**Out of scope**
- The generated client trait with an associated error type. Recorded in RFC-05
  as a follow-up; not built.
- Supervision, restart, `Actor::Args` (RFC-04 §8 — "none. Not a smaller
  version. None.").
- Location transparency / unifying `Handle` and `Client` (RFC-04 §5.5,
  permanently rejected).
- Read-concurrency on actors. Every route to state is `&mut A`; adding a shared
  one means `A: Sync` and an `Arc`, which is the lock the actor model removed.
- Forced cancellation of an actor turn — it would drop `&mut A` mid-mutation.
- RFC-02 capability A (stop-serving an ALPN). Adjacent, designed-not-built, not
  requested.
- `iroh-blobs` integration; browser execution; any dependency version bump.
- Performance work. This programme measures correctness, not throughput.

**Blast radius**
- May modify: `keylink/src/**`, `keylink/tests/**`, `keylink/examples/**`,
  `demo/src/**` (these three must keep compiling — item 4 touches all of them),
  `docs/rfc-05-*.md` (new),
  `docs/rfc-04-actors.md` and `docs/rfc-02-runtime-router.md` (**append-only**),
  `README.md` (the Status section only, and only to keep it true),
  `keylink/Cargo.toml` (only if a wave genuinely needs a dependency —
  justify it, and run `mise run deny`).
- Must not touch: `keylink-persist/**` (it declares no `service!` and none of
  these twelve items reaches it — if you believe it must change, that is a
  finding to report, not an edit to make), `docs/rfc-01-blobs.md`,
  `docs/rfc-03-wasm.md`, `mise.toml`,
  `.mise/`, `.github/`, `rust-toolchain.toml`, `deny.toml`, `scripts/`,
  `Cargo.lock` beyond what a justified dependency requires, `.tmp/**` (scratch,
  gitignored, not part of this PR).
- Do not bump `rust-version`. Do not upgrade `iroh`.

## Public interface

Designed up front. No internal type — no `Slot`, no `Guard`, no `Ledger`, no
`Connection`, no `oneshot` — appears on any of it.

```rust
// ── wave A ────────────────────────────────────────────────────────────
/// What the ledger knows about one key, for one peer.
/// Exhaustive on purpose. `#[non_exhaustive]` here would be contradictory three
/// ways: this goal's own invariant reserves it for error enums and not for ones
/// a caller matches; `postcard_schema` derives from the variant list and ignores
/// the attribute, so a sixth variant moves the ALPN and breaks every handshake
/// anyway — the attribute would advertise source compatibility on a hard wire
/// break; and it forces the `_ =>` arm this crate spent eleven catch-all-free
/// macros deleting.
#[derive(Debug, Clone, Serialize, Deserialize, Schema)]
pub enum KeyState {
    /// No record. Either never used, or its record was evicted before a
    /// tombstone existed. Distinct from `Forgotten`.
    Fresh,
    /// A call is in dispatch under this key now.
    Running,
    /// Settled. The reply is held and would be replayed.
    Done,
    /// Entered and no reply came out. Whether it took effect is unknown.
    Interrupted,
    /// It ran; the reply is no longer held. At-most-once still holds.
    Forgotten,
}

/// A new refusal, distinguishing an evicted key from an unused one.
/// `IdempotencyRefusal` lives inside the hashed contract, so this moves the ALPN.
pub enum IdempotencyRefusal { /* … */ Forgotten }

// NO per-service bounds type. An earlier draft proposed `pub struct Retention`,
// which (a) collides hard with the existing `keylink::Retention` re-exported at
// `lib.rs:141` from `blobs.rs:344` and probed in `representability.rs:151`, and
// (b) has no caller — `Ledger::with_limits` already exists privately for tests
// at `idempotency.rs:203`. The two crate-level constants stay as they are.

// Generated per service, peer-scoped by construction:
//   client.key_state(key) -> Result<KeyState>

/// A key, and what to do when it collides with a call already in flight.
///
/// `attach` without a key is meaningless, so it is not representable: the budget
/// lives INSIDE the value that carries the key. This is also the wire type — one
/// type, client side and envelope side, so the two cannot drift.
///
/// `Envelope.idempotency` changes from `Option<IdempotencyKey>` to
/// `Option<Idempotency>`. Wire-visible; batch with `Forgotten` and `key_state`.
#[derive(Debug, Clone, Serialize, Deserialize, Schema)]
pub struct Idempotency {
    key: IdempotencyKey,
    /// How long the server may hold this call waiting for an in-flight one under
    /// the same key. `None` refuses immediately — today's behaviour.
    ///
    /// Millis rather than `Duration`, matching `RemoteError::TimedOut { millis }`,
    /// so `Duration`'s Schema impl never enters the hashed contract.
    attach_for_millis: Option<u64>,
}

impl Idempotency {
    /// Refuse on collision. Exactly what `with_idempotency(key)` does today.
    pub fn new(key: IdempotencyKey) -> Self;
    /// Wait up to `budget` for a call already in flight under this key, then
    /// answer with whatever it settled as. Capped server-side by `MAX_ATTACH`.
    pub fn attaching_for(key: IdempotencyKey, budget: Duration) -> Self;
    pub fn key(&self) -> &IdempotencyKey;
    pub fn attach_budget(&self) -> Option<Duration>;
}

/// Keeps every existing `with_idempotency(key)` call site compiling unchanged.
impl From<IdempotencyKey> for Idempotency { /* = Self::new */ }

impl Client {
    /// Signature widened; `attach` needs no second constructor and no new method.
    pub fn with_idempotency(&self, idem: impl Into<Idempotency>) -> Self;
}

// `attach` is a MODIFIER on the call, never a separate generated method. So a
// retry is literally the same call with the same key, and the caller's own
// method signature is what it gets back:
//
//     client.with_idempotency(Idempotency::attaching_for(key, budget))
//           .submit(symbol, Side::Buy, qty).await?   // -> Fill, the ORIGINAL one
//
// Consequences of the modifier shape, all of them wanted:
//  - no new reserved name (macros.rs:334-346), and the generated Handler/Request
//    1:1 correspondence is untouched. Only `key_state` spends a name.
//  - the handler and actor traits do not change at all. attach is entirely a
//    ledger + transport concern.
//  - `Peer::call`'s single re-issue starts composing with idempotency instead of
//    hitting `InFlight` — the case `peer.rs:26-29` currently sends you away from.
//
// What an attaching call gets, by slot state:
//   vacant       -> runs; the budget is ignored
//   Running      -> waits, then whatever it settles as
//   Done(frame)  -> replays immediately, no wait
//   Interrupted  -> refuses immediately, no wait
//   Unrecordable -> refuses immediately, no wait
//   tombstone    -> Forgotten, no wait
//   budget spent while still Running -> Refused(InFlight)
//
// That last row is why attach needs NO new error variant: it degrades to exactly
// today's answer, and `InFlight` already means "a call is in dispatch under this
// key now", which is true in both cases.
//
// A `recv` reply carries a body and therefore settles Unrecordable
// (idempotency.rs:327), so attaching to one answers "it ran, and I cannot give
// you the reply" — correct, and still more than the caller had. `blob` and
// `send` replies are header-only and attach normally. A compile-time
// restriction on `recv` was considered and DEFERRED: it is one kind, the runtime
// answer is already correct, and it would cost a third client type.
//
// Cross-peer attach is impossible for free: the ledger is keyed by (peer, key),
// so one peer can never wait on another's call.

// ── wave B ────────────────────────────────────────────────────────────
/// Where a message came from. Exhaustive, like `Departure`: `Local` is a named
/// state, not an absence.
///
/// NOT `Option<&Context>`. `authz.rs:33-40` records what an `Option` on an
/// identity costs, and this goal's own invariant says known value sets are
/// enums. `None` would also invite `cx.map(Context::peer)` at the one call site
/// where being wrong means authorising a local message against the last peer
/// that dialled.
pub enum Caller<'a> { Local, Peer(&'a Context) }

// Generated:
//   trait Handler { async fn f(&self,     cx: &Context,   a: A) -> R; }
//   trait Actor   { async fn f(&mut self, cx: Caller<'_>, a: A) -> R; }
//   // Actor::caller is DELETED.
//
// Two mechanical facts, so they are not rediscovered as blockers:
//  - `send` takes `body: BodyReader` LAST (macros.rs:1203), so the real shape is
//    `fn f(&self, cx: &Context, a: A, body: BodyReader)`. "First" alone is
//    ambiguous for that kind.
//  - `Next`/`Dispatch` take `Context` BY VALUE (middleware.rs:25, rpc.rs:72), so
//    the generated closure moves it into the async block and borrows inside.
//    That will look like a defect and is not.

/// Refuses a call still queued after its budget. Never refuses a turn already
/// running, so it cannot settle a key for work that then happens.
///
/// `Timeout` is KEPT, not replaced. On a stateless `Handler`, dropping the
/// future genuinely cancels the work and `QueueTimeout` cannot do that; deleting
/// `Timeout` would remove real cancellation from the path where it works. These
/// are two behaviours, not two doors to one thing, so "one way to build a thing"
/// does not apply.
pub struct QueueTimeout { /* … */ }
impl QueueTimeout { pub fn try_after(budget: Duration) -> Result<Self, QueueTimeoutError>; }

// ── wave C ────────────────────────────────────────────────────────────
/// Peers that dialled in, addressable by the accept side. Evicts on Departure.
/// As drafted this would not compile. `get` and `iter` return owned `C` from
/// behind a lock, so `C: Clone` is required; `iter()` is an eager snapshot and
/// must not be named as though it were lazy; and `len` without `is_empty` trips
/// `clippy::len_without_is_empty`, inside `clippy::all`, denied at
/// `Cargo.toml:34`.
pub struct Subscribers<C: Clone> { /* … */ }
impl<C: Clone> Subscribers<C> {
    pub fn get(&self, peer: EndpointId) -> Option<C>;
    pub fn snapshot(&self) -> Vec<(EndpointId, C)>;
    pub fn len(&self) -> usize;
    pub fn is_empty(&self) -> bool;
}

// ── wave D ────────────────────────────────────────────────────────────
/// Who may talk to a served protocol. `Allowlist` is the canonical impl.
///
/// NOTE the return type, and do not "fix" it back to `Option<AdmittedPeer>`.
/// `AdmittedPeer` is a newtype with a private field whose only constructor is
/// `Allowlist::admit` (`authz.rs:94-98`) — that privacy is the invariant, so a
/// trait returning it CANNOT BE IMPLEMENTED outside this crate, and making it
/// implementable would delete the property. keylink keeps the sole constructor
/// and converts at the one call site in `RpcService::accept`:
///
///     if admission.admits(&proven) { /* keylink mints the AdmittedPeer here */ }
///
/// This is not the `permits(&EndpointId) -> bool` that `authz.rs:183-188`
/// rejects. That rejection is about a bool escaping downstream, where "every
/// function downstream took that bare id". Here the bool never leaves the accept
/// path — it is consumed one line later — and everything downstream still
/// demands an `AdmittedPeer` that only a policy could have produced.
/// A `bool` is also wrong, for a second reason: it cannot say "the policy could
/// not be consulted". A failing backend would return `false`, byte-identical to
/// "not on the list", and an operator who cannot tell a dead authorizer from an
/// unlisted peer reaches for `Allowlist::anyone()` during an incident. That is a
/// fail-open arrived at through a diagnostic hole. `authz.rs:33-40` makes this
/// exact criticism of RFC-01's `Option`-keyed admission.
pub enum Admittance {
    /// A policy said yes. keylink mints the `AdmittedPeer`; this arm carries no
    /// value, which is what keeps the trait implementable outside the crate.
    Admitted,
    /// A policy said no.
    Refused,
    /// The policy could not be consulted. Refuses, and says why.
    Unavailable(String),
}

pub trait Admission: Send + Sync + 'static {
    fn admit(&self, peer: &ProvenPeer) -> Admittance;
}

/// An admission whose answer can change while connections are live. A plain
/// `Admission` is consulted at accept and never again, so revocation is a
/// capability in the type rather than a promise in a doc comment — otherwise
/// "an adapter could quietly not do it, and the caller could not tell which kind
/// it had" (keylink-persist/src/lib.rs:30-31).
pub trait Revocable: Admission {
    /// Swap the snapshot, **then** close every live connection the new list no
    /// longer admits, and return the peers closed.
    ///
    /// Not async: `Connection::close` does not await (`rpc.rs:322`); the
    /// awaiting variant is the separate `closed()` (`rpc.rs:855-856`).
    #[must_use = "a revocation that closed nothing is usually a mistake"]
    fn replace(&self, next: Allowlist) -> Vec<EndpointId>;
}
/// The ONE adapter keylink ships: a snapshot behind a lock. `Allowlist` is the
/// value it holds, NOT a second adapter — a static allowlist is a
/// `DynamicAdmission` nobody replaces. The reason to write a second adapter is
/// an external policy system, and that is the only reason.
///
/// No `Default` — admitting everyone stays something you type out.
pub struct DynamicAdmission { /* … */ }
impl DynamicAdmission { pub fn new(initial: Allowlist) -> Self; }
impl Admission for DynamicAdmission { /* … */ }
impl Revocable  for DynamicAdmission { /* … */ }

// Held as `Arc<dyn Admission>` on ServiceBuilder/RpcService — dyn-compatible on
// purpose, so `admit` is sync, non-generic and takes `&self`. Deliberately
// UNLIKE keylink-persist's `Snapshots`, which is RPITIT because `claim` does I/O
// at one monomorphized call site. `admit` runs once per CONNECTION (rpc.rs:321)
// and does no I/O; a backend needing I/O keeps its own snapshot and feeds it in.
// A generic `A` would not reach `Server` (Service erases to
// `Box<dyn DynProtocolHandler>`, rpc.rs:500-507) but WOULD reach the generated
// `service(&self, allow: Allowlist)` at macros.rs:661, putting `A` into every
// service! user's type errors for a call made once per connection.
//
// WHAT THIS DOES NOT PROMISE, in keylink-persist's style:
// - Nothing about a peer already inside a stream. Revocation closes the
//   connection; a call already dispatched runs to completion.
// - Nothing across nodes. `DynamicAdmission` is one process's view.
// - No history. There is no audit trail of past decisions; that is a different
//   port with different operations.
```

## Success criteria

Each is binary and names its proof. Every new guard must be **red-tested** —
break it, watch it fail, restore it — per CONTRIBUTING; a guard nobody watched
fail is not a guard.

**Wave A**
- [ ] A layer that never called `next` leaves the key claimable —
      `a_refusal_before_dispatch_leaves_the_key_claimable`.
- [ ] A layer that did call `next` settles the key as unknown, not as its own
      refusal — `a_refusal_after_dispatch_settles_interrupted`.
- [ ] Byte-for-byte replay survives the change, pinned by a test that did not
      exist before — `a_replayed_reply_is_byte_identical`. The property is
      asserted in three doc comments today and by nothing executable.
- [ ] On an actor, a `Timeout` above the ledger no longer records `TimedOut` for a
      turn that ran; the retry receives the real reply — `a_timed_out_actor_turn_records_its_real_reply`.
- [ ] A task killed between accept and dispatch leaves the key claimable rather
      than `Interrupted` — `an_abort_before_dispatch_does_not_report_interrupted`.
- [ ] `tests/idempotency.rs::a_refusal_is_recorded_and_replayed` is restated (not
      deleted) to say which refusals are recorded and why, and passes.
- [ ] Running claims are bounded — `running_claims_are_bounded_per_peer`.
- [ ] A record expires on its TTL, proven with `tokio::time::pause`, not a sleep
      — `a_record_expires_after_its_ttl`.
- [ ] An evicted key is refused `Forgotten`, never re-executed — `an_evicted_key_is_refused_not_rerun`.
- [ ] `key_state` reports each of the five states — `key_state_reports_every_state`.
- [ ] `key_state` cannot read another peer's key — `key_state_is_peer_scoped`.
- [ ] `attach` under a live key returns the running invocation's reply —
      `a_second_caller_attaches_to_a_live_key`; and gives up on its budget —
      `an_attach_that_outlives_its_budget_returns`.
- [ ] `tests/protocol.rs::the_contract_hash_has_not_moved` updated **after**
      deciding the change should be visible on the wire, with the reason in the
      commit message.

**Wave B**
- [ ] A `Handler` reads the calling peer — `a_handler_sees_the_calling_peer`.
- [ ] `Actor::caller` is gone — `rg 'fn caller' keylink/src` returns nothing.
- [ ] A local `Handle` call passes `None` and cannot be attributed to a peer —
      `a_local_call_has_no_caller`.
- [ ] `demo` and every test compile against the new signatures — `mise run gate` green.
- [ ] Serving a `send`-declaring service as an actor is refused at startup —
      `serving_a_send_method_on_an_actor_is_refused`, returning a `ConfigError`.
- [ ] `QueueTimeout` refuses before the mailbox and settles no key —
      `a_queue_timeout_refuses_before_the_mailbox`.
- [ ] An abandoned turn can observe it — `an_abandoned_turn_observes_its_caller_left`.

**Wave C**
- [ ] The accept side pushes to a peer that dialled in, over two real nodes —
      `a_server_pushes_to_a_client_that_dialled_in`.
- [ ] The reverse service enforces its own allowlist —
      `an_unauthorised_reverse_call_is_refused`.
- [ ] A subscriber is evicted when its connection ends —
      `a_subscriber_is_dropped_when_its_connection_ends`.
- [ ] No `WireKinds` field was added: `the_contract_hash_has_not_moved` is
      untouched by wave C.

**Wave D**
- [ ] A peer added at runtime is admitted without a restart —
      `a_newly_added_peer_is_admitted_without_a_restart`.
- [ ] A revoked peer's live connection is closed — `revoking_a_peer_closes_its_connection`.
- [ ] `Admission` has no blanket permissive impl and `DynamicAdmission` has no
      `Default` — held by a `representability.rs` probe, not by a comment.
- [ ] `Allowlist::from_env` still errors at startup when unset — existing test green.

**Cross-cutting**
- [ ] `mise run gate` green (fmt, clippy at both feature ends, tests, rustdoc as
      a gate). Output printed.
- [ ] `mise run deny` and `mise run msrv` green. Output printed.
- [ ] `mise run wasm` builds. Output printed.
- [ ] `docs/rfc-05-*.md` exists and covers wave A, the `attach` reversal, and the
      client-trait follow-up.
- [ ] RFC-04 and RFC-02 gained **appended** sections; `git diff` shows no line
      removed or altered above the append point.

## Invariants

### Universal (adapted to this crate — the Python invariant library does not apply)

- **No `unwrap`/`expect` outside tests.** The workspace denies both
  (`clippy::unwrap_used`, `clippy::expect_used`). Test modules opt out at the top
  of the file, which is the only place allowed.
- **`clippy::all` is deny.** `mise run lint` checks both ends of the feature
  lattice; the ends do not meet in the middle.
- **`missing_docs` is a gate.** `mise run docs` runs rustdoc with `-D warnings`.
- **Domain scalars are newtypes, not primitives.** The crate already does this
  (`IdempotencyKey`, `ActorId`, `BlobId`). Anything new follows.
- **Known value sets are enums.** `KeyState` is an enum, never a string.
- **Never leak internals.** No `Slot`, `Guard`, `Ledger`, `Bucket`,
  `Connection`, `oneshot` or `Bytes` frame on the public surface. Every new
  `pub` is a compatibility promise: `#[doc(hidden)]` if only the macro needs it,
  `pub(crate)` if nothing outside the crate does.
- **One way to build a thing.** No second construction path that quietly does
  less than the longer one.
- **Error enums are `#[non_exhaustive]`;** enums a caller constructs and matches
  are not. `Departure` stays exhaustive.
- **The hashed contract must equal the wire type.** RFC-04 §14.6's rule — every
  time the two drift, the ALPN gate stops covering something it claims to cover.
  Any change to what a stream carries updates `Contract` in the same commit.
- **Fail loudly, fail closed.** No silent fallback. `Admission` admits nobody by
  accident, and `Admittance::Unavailable` refuses the connection **and** stays
  distinguishable from `Refused` in the close code and the log — a backend that
  is down must never look like a peer that is not on the list.
- **`unsafe` is denied crate-wide.** `#![deny(unsafe_code)]` on `keylink`, with
  exactly one `#[expect(unsafe_code, reason = "…")]` at `body.rs:271`
  (`unsafe impl<R> Send for OnOneThread<R>`). `forbid` is impossible *because*
  of that line, which is why the claim "keylink contains exactly one `unsafe
  impl`" is prose in two places today (`lib.rs:82`, `:105`) and enforced
  nowhere. New crates use `#![forbid(unsafe_code)]`, as `keylink-persist` does.
  Check: `rg 'unsafe' keylink/src` returns the expect site and nothing else.
- **Poisoned locks recover, never propagate** — the
  `unwrap_or_else(PoisonError::into_inner)` pattern at `idempotency.rs:231-236`.
  A poisoned lock costs one request, not the process.
- **`#[must_use]` on every method that returns a decision** — an admission, a
  revocation, a claim. A decision computed and dropped is the failure
  `authz.rs:181-188` exists to prevent.
- **No `std::time::Instant`, no `std::thread`.** `n0_future::time` only.
  `actor.rs:520-534` records that `std::time::Instant::now()` panics on
  `wasm32-unknown-unknown` and that this shipped once. Check:
  `rg 'std::time::Instant|std::thread' keylink/src`.
- **No legacy or compat shims.** Greenfield, unpublished, single owner: change
  every call site and delete the old path. No aliases, no deprecation wrappers.
- **Fix at the root.** No workaround, no swallowed error, no sleep, no
  special-case branch. Use `systematic-debugging`.
- **Test the real boundary.** Two real nodes over a real handshake for anything
  transport-visible. No mock of the ledger, the mailbox or the connection.
- **No wall-clock assertions.** Clock-dependent behaviour is unit-tested with
  `tokio::time::pause`; integration tests assert only what load can strengthen.
- **Every new guard is red-tested.** Break it, watch it fail, restore it.
- **An absence claim is a compile-time probe,** not a comment — extend
  `representability.rs` rather than writing "do not add X".

### Context-dependent (decided for this goal)

- **Delivery:** one goal, four waves as atomic commits on one integration
  branch, **one PR** to `main`. Push as waves complete so CI stays current.
- **Wire visibility:** ALPN moves are free (greenfield, unpublished). Wave A
  moves it deliberately; the pinned literal is updated only after deciding the
  change *should* be visible, with the reason recorded. Wave C must **not** move
  it.
- **RFC discipline:** new `docs/rfc-05-*.md` for idempotency; **append-only** to
  RFC-04 and RFC-02, each append naming the section it answers.
- **Caller identity:** `Handler` takes `&Context`; `Actor` takes
  `Option<&Context>`. The asymmetry is the point — an actor is reachable two
  ways, a handler one. `Actor::caller` is deleted rather than kept alongside.
- **Admission:** a port. `Allowlist` is the canonical impl and keeps every
  fail-closed property (no `Default`, `from_env` errors at startup). The default
  dynamic impl is a snapshot behind a lock — **not** an actor: `admit` is a read
  on the connection-accept hot path, and routing it through a mailbox would add
  a queue and a `MailboxClosed` failure mode to the security path.
- **Revocation** closes live connections, reusing wave C's per-peer registry.
  Wave D therefore depends on wave C. **It swaps the snapshot BEFORE it closes**
  — closing first leaves a window in which a revoked peer reconnects and is
  admitted by the old list. That ordering is the correctness property; test it.
- **Ports & adapters:** `Admission` is the port. keylink ships **one** adapter,
  `DynamicAdmission`, and `Allowlist` is the value it holds rather than a second
  adapter. The reason to write a second adapter is an external policy system,
  and that is the only reason — stated in the persist crate's style, with what
  is *not* promised written down.
- **`attach` and the key-status query are two features,** designed and reviewed
  independently, not one method with a flag.
- **New modules** (`Admission` port, connection registry, `Subscribers<C>`) get
  an `improve-codebase-architecture` pass **during planning**, before code, and
  an enumerated public surface.
- **Ports & adapters:** `Admission` is the port; `Allowlist` and
  `DynamicAdmission` are the adapters. Name any further backend explicitly.

## Execution interaction policy

**Questions during execution: not allowed.** The executor annotates every
question and assumption inline as it goes and surfaces them all after finishing.
It never blocks mid-run. If genuinely blocked with no valid path, it stops and
reports the attempted paths, the evidence, the blocker and the open questions —
it does not ask.

## Process

Work in an isolated worktree created with worktrunk (`wt switch -c
<branch> -y`, never raw `git worktree`). Atomic commits, one logical change each;
push as waves complete.

**Explore → Plan → (Review plan ⇄ Improve plan)×N → Execute
(`subagent-driven-development`) → (Review execution ⇄ Apply fixes)×N → PR.**

**Two gates, discretion between them** — the plan, and the merge to `main`.
Per-wave reviews on the integration branch are **discretionary**: spend them on
waves A and D (behaviour-sensitive, money-adjacent, security-adjacent) and skip
them on mechanical ones. Do not run five review skills per wave; that is
ceremony, and the invariant checks below are the same four `mise` outputs the
success criteria already demand.

- **Review** at each gate is run by a **subagent on opus** using
  `brooks-review`/`brooks-sweep`, plus `brooks-audit` at the merge gate only
  (this changes module structure). Never fable.
- **A dedicated invariant-compliance wave** runs on the execution review: a
  separate subagent whose only job is to grade the PR diff against the
  `## Invariants` section item by item and evidence each one. It prefers machine
  checks and surfaces their output: `mise run gate`, `mise run deny`,
  `mise run msrv`, `mise run wasm`, `rg 'unwrap\(\)|expect\(' keylink/src`,
  `rg 'fn caller' keylink/src`, and the `git diff` proving the RFC appends
  removed nothing. An invariant it cannot evidence is treated as violated. It
  returns a per-invariant pass/fail verdict; any miss is Critical by definition
  and re-opens the loop.
- **Apply fixes** through `receiving-code-review`: triage every finding, fix what
  brings real value, do not gold-plate. Every invariant miss must be fixed, never
  triaged away.
- **Run `ponytail` on every wave** before review. Twelve items is exactly the
  shape that grows a second system.
- **Loop-break:** iterate until both the brooks quality wave and the
  invariant-compliance wave return no Critical/Severe/Major. Report
  Critical-vs-Major counts and the per-invariant verdict each wave.

## Definition of done

- [ ] Every success criterion proven, evidence attached.
- [ ] Diff inside the blast radius; no unrequested refactors; no `iroh` bump.
- [ ] `mise run gate`, `mise run deny`, `mise run msrv`, `mise run wasm` green.
- [ ] Review waves clean (no open Critical/Major); deviations reported.
- [ ] **Invariant-compliance wave clean** — per-invariant verdict with machine
      output attached.
- [ ] RFC-05 written; RFC-04 and RFC-02 appended, nothing above the append
      point altered.
- [ ] Every new guard red-tested, and said so.
- [ ] Inline annotations of every assumption and open question surfaced in the
      final report.
- [ ] **Final pass — re-read this goal end to end** and confirm each item against
      real evidence, not memory. If a plan exists, walk it task by task and
      confirm each is done or explicitly descoped.

## /goal completion condition

```text
Satisfy every success criterion and Definition-of-done item in
docs/goals/2026-08-06-correctness-programme.md. Work the four waves in order (A
idempotency correctness -> B caller identity and actor safety -> C server push ->
D runtime admission) as atomic commits on one integration branch, then open ONE
PR to main.

AS THE FINAL STEP, print the full proof state in ONE consolidated block:
`mise run gate` output (0 failures); `mise run deny`, `mise run msrv` and
`mise run wasm` output; `rg 'fn caller' keylink/src` (must be empty);
`rg 'unwrap\(|expect\(' keylink/src` (test modules only); `git diff` for
docs/rfc-04-actors.md and docs/rfc-02-runtime-router.md proving they were
APPENDED to with nothing above the append point altered; the new ALPN literal in
keylink/tests/protocol.rs with the one-line reason it moved; the
invariant-compliance wave's per-invariant pass/fail verdict; the list of every
new guard you red-tested; and every assumption and open question you annotated
along the way.

Stay inside the blast radius in that file. Do not bump rust-version or iroh. Do
not touch .tmp/, mise.toml, .mise/, .github/, deny.toml, rust-toolchain.toml,
docs/rfc-01-blobs.md or docs/rfc-03-wasm.md.

Do not ask questions mid-run: annotate every question and assumption inline and
surface them all in the final report. If blocked with no valid path, stop and
report the attempted paths, the evidence, the blocker and the open questions.

Stop when all criteria pass or after 60 turns.
```

## Deliverable

Isolated worktree, one PR to `main` with the agent session id in the body, all
checks green, RFC-05 and the two appends included.
