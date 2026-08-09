# Wave 1 — one failure vocabulary, both sides

Plan for `docs/goals/2026-08-09-one-failure-vocabulary.md`, written against
`unify-surfaces` at `929d201` after five read-only passes.

Read the goal's **appended corrections** first. Five of its claims are false
against the tree, and two of them change the size of the work.

## 1. What this is

Seven conditions are one fact on both sides of keylink. Locally each is a typed
variant; remotely each is a **string prefix inside `Failed(String)`**. The
prefixes are associated consts, which `postcard_schema` does not hash
(`reply.rs:119-124`), so the handshake cannot protect them. A drifted peer meets
a moved prefix as a `Failed` it silently stops recognising.

This wave gives those conditions one Rust object carried by both sides and
encoded on the wire, and deletes the prefix machinery.

It is net deletion in the crate and net *addition* in the tests, because nine
test assertions currently hard-code prefix text (§3.4).

## 2. Three decisions this plan makes, that the goal left open or got wrong

### 2.1 `Withheld` is NOT `#[non_exhaustive]`

The goal's sketch marks it `#[non_exhaustive]` with the comment *"error enums
ARE non_exhaustive here"*. That reads the rule at `console.rs:384-393` but stops
before the paragraph that decides this case.

The rule has two halves: error enums are `#[non_exhaustive]`; enums a caller
matches exhaustively are not. `Withheld` is both at once, so the halves have to
be broken apart by their *reasons*, and `console.rs:394-404` already does it for
exactly this shape:

> Either way the wire is not the lax half: a new arm moves `Inspect`'s ALPN, so
> an old console is refused at the handshake rather than handed a variant it
> cannot decode.

`Withheld` is on the wire. Adding a variant moves the ALPN — for **every**
service, since it rides `RemoteError`. An old peer therefore cannot ever be
handed an unknown `Withheld`; it is refused at the handshake first. The
protection `#[non_exhaustive]` exists to give is already supplied by the
handshake, and the cost — forcing a `_ =>` arm on every caller — is precisely
the "swallowed a signal" failure the second half of the rule refuses.

**Decision: no `#[non_exhaustive]`.** Same reasoning, same paragraph, same file
as `EndReason` beside it. Record it as an append to RFC-04, because it looks
like a violation of the rule and is an application of it.

### 2.2 `Withheld::Restarting` carries `EndReason`, which moves to `observe.rs`

Decided in goal item 11. The verification the item demanded is discharged and
resolves cheaply: **module paths are not hashed**, so the move is wire-free
(`postcard-schema-0.2.5/src/key/hash.rs:193-201`, plus an empirical probe;
see the goal's appended corrections).

`EndReason` reaches the wire through exactly one route — `Inspect`'s `Follow`
rpc → `EventPage` → `LifecycleEvent::Ended` (`console.rs:688,342,375,160-164`).
The `feed Events` at `console.rs:692` also names `LifecycleEvent`, but a feed is
outside the hash. After this wave it reaches the wire through `RemoteError` too,
which puts it in **every** service's graph rather than only `Inspect`'s. That is
a real widening and it is the price of the decision; state it in the RFC append.

### 2.3 `Undelivered` loses `Copy`, and that is the point

Today `Undelivered` is `Debug, Clone, Copy, PartialEq, Eq` (`turns.rs:3166`).
`Copy` is why `TurnLost::Restarting(Arc<Ended>)` **discards its reason** on the
way to `Undelivered` (`turns.rs:3909-3912`) — the exact defect goal item 1 of
the audit named, where a local caller is worse-informed than a peer about its
own actor.

Carrying the reason means carrying data. `Copy` cannot survive it. Neither can
`&'static str`: `Refused { name }` and `ObserverPanicked { name }` are
`&'static str` locally (`turns.rs:3230,3258`) and must be `String` on the wire.

**Costs, stated rather than discovered:**

- Every local refusal now allocates. Refusals are already the slow path — they
  run after an observer ran and a turn was abandoned — so this is not on the
  success path. Do not add a `Cow` to avoid it; `Cow<'static, str>` has no
  `Schema` impl and buying one is a larger change than the allocation it saves.
- `Copy` removal is a breaking change for downstream callers who bind by value
  twice. `Clone + PartialEq + Eq` survive, so `assert_eq!` sites are unaffected.
- `TurnLost` gains `Clone` and `PartialEq`? **No.** It still carries
  `Faulted(Panic)` (`observe.rs:717`), and `Panic` is not `Clone`. `TurnLost`
  keeps `Debug + thiserror::Error` only. The success criterion asserting
  "value equality of the two `Withheld`s" compares `Withheld`, not `TurnLost` —
  which is exactly why the criterion is written that way.

## 3. The shape

### 3.1 The types

```rust
// keylink/src/observe.rs — beside `Ended`, whose wire projection it is.
pub enum EndReason { /* unchanged, 6 variants, moved from console.rs:184 */ }

/// One object, both sides, and on the wire.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Schema, thiserror::Error)]
pub enum Withheld {
    Panicked         { message: String },   // the HANDLER
    Refused          { name: String },      // an observer vetoed
    ObserverPanicked { name: String },
    Dropped,
    Restarting       { reason: EndReason },
    Gone,
    Expired,
}

pub enum TurnLost   { Turn(Withheld), Faulted(Panic), Reentrant }
pub enum Unanswered { Turn(Withheld), Reentrant }   // was `Undelivered`
```

`Reentrant` stays **outside** `Withheld` in both, and that is principled rather
than incidental: every `Withheld` variant describes what happened to a turn,
whereas `Reentrant` describes what was wrong with the *caller* — the turn never
existed. Keeping it out is what lets `Withheld` be the shared vocabulary.

### 3.2 The rename, item 6

`Undelivered`'s first doc line (`turns.rs:3146`) is *"A local call that never
entered the mailbox, and why"*, which is false of `Dropped`, `Restarting`,
`Refused` and `ObserverPanicked` — all four ran. **Proposed: `Unanswered`**,
which is true of every variant including `Reentrant`, and which names the
caller's observation rather than the mailbox's history. Costs no ALPN (type
names are not hashed).

`NotQueued` keeps its name: it is send-shaped and its two variants genuinely
never queued.

### 3.3 `RemoteError` after the wave

```rust
#[non_exhaustive]                      // stays — it is not matched exhaustively
pub enum RemoteError {
    Unauthorized(String),
    TimedOut { millis: u64 },
    Idempotency(IdempotencyRefusal),
    NoSuchActor { id: String },
    Withheld(Withheld),                 // NEW
    AtCapacity { capacity: u64 },       // NEW — u64, never usize
    FollowRefused(FollowRefused),       // NEW — stop flattening Ahead/Crowded
    Failed(String),                     // now means: a caller's Middleware, and nothing else
}
```

`RemoteError` **stays `#[non_exhaustive]`**: it is a library error type a caller
handles rather than exhausts, which is the half of the rule that fits it.

`BadActorId { id: String }` from goal item 4 is folded into this list as a
`Withheld`-adjacent decision the implementer must make explicitly: the goal asks
for it, and `remote.rs:434` currently renders `ActorId::validate`'s error as a
bare `Failed`. Add it as its own variant — a malformed id is not a withheld
turn, it is a request that never addressed anything.

**Invariant to establish and guard: `Failed(String)` means a caller's own
`Middleware` and nothing else.** Four sites produce an unprefixed `Failed` today
and three of them stop: `turns.rs:4775` (reentrancy → `Withheld` route),
`remote.rs:353` (`AtCapacity` → its own variant), `remote.rs:434`
(`ActorId::validate` → `BadActorId`), `console.rs:1031` (`FollowRefused` → its
own variant).

### 3.4 The test debt this uncovers, which is the largest single item

Twelve non-guard sites consume a prefix. **Three** use the const and break at
compile time — `defer.rs:1199`, `singleton.rs:1117`, `singleton.rs:1121`.
**Nine** hard-code the literal text and will fail at *run* time with a message
about a string that no longer exists:

`defer.rs:718,920` · `supervision.rs:1155,1166,1380,1503,1514` ·
`actors.rs:735` · `singleton.rs:992`

Four more compare whole rendered strings rather than prefixes and must also move:
`singleton.rs:1018,1113,1267` (`format!("observer refused: {REFUSER}")`) and
`persist.rs:507` (`.contains("observer refused: persist")`).

**Every one of these becomes a match on a typed variant.** That is the point of
the wave — the test suite is currently the largest consumer of the string
channel this wave deletes, and rewriting it is not incidental cleanup, it is the
proof that the channel is gone.

## 4. The work, in commits

Atomic, in this order. The hashed contract moves **once**, at commit 5.

| # | Commit | Moves a pin |
|---|---|---|
| 1 | Rename private `Withheld` → `ObserverVerdict` (goal item 1) | no |
| 2 | Move `EndReason` `console.rs` → `observe.rs` (item 11) | no |
| 3 | Rename `Undelivered` → `Unanswered` (item 6) | no |
| 4 | Rename `deliver_to_local` (item 9) | no |
| 5 | **Add public `Withheld`; collapse `TurnLost`/`Unanswered`; reshape `RemoteError`; rename `NodeStatus::lost` → `dropped`** (items 2,3,4,7) | **BOTH** |
| 6 | Delete the prefix machinery and its two guards (item 5) | no |
| 7 | Rewrite the 16 test consumer sites onto typed variants (§3.4) | no |
| 8 | Replace the twin guard (§5) | no |
| 9 | RFC appends: RFC-04 §5.5 re-argued, the `#[non_exhaustive]` ruling, the `Gone`/`NoSuchActor` cost, the `EndReason` widening | no |

**Commits 1–4 are independent of each other and may run in parallel.** They are
pure renames and a pure move, none touches the wire, and each is mechanical.
Commits 5–9 are strictly sequential.

Commit 5 is where both pins move. Read both new literals **off one failing run**
of `cargo test -p keylink --test protocol contract_hash_has_not_moved` — a
positional substring that reaches both `the_contract_hash_has_not_moved`
(`protocol.rs:1200`) and `the_inspect_contract_hash_has_not_moved` (`:1304`).
Never compute a hash. Record both in the release note `CONTRIBUTING.md:197-210`
requires — and note that the rule names only the first test, so the Inspect pin
must be named explicitly or the next author will not know it moved.

## 5. The twin guard, which this wave dissolves rather than deletes

`every_turn_lost_except_faulted_has_one_undelivered_twin_with_the_same_words`
(`turns.rs:5101`) asserts **six** distinct things over two flat enums (full list
in the map). Its premise — variant-per-variant correspondence — stops existing
when both types share `Turn(Withheld)`. `slot()`, `EVERY` and `twin()`
(`turns.rs:4909-4946`) are invalidated together, not one at a time.

**Do not delete it and call the type system a replacement.** What the type
system now gives for free is that the *shared* arm cannot drift. What it does
NOT give, and what the guard must still hold:

1. `Unanswered::Reentrant` and `TurnLost::Reentrant` still render the same
   sentence, and they are two hand-written `#[error]` strings.
2. `NotQueued`'s two variants still render the same sentences as their
   `Unanswered` counterparts through the production `From` impl.
3. No two `Withheld` variants share a sentence (assertion 4 of the old guard —
   catches copy-paste that pairwise equality cannot see).
4. No `#[error]` text is empty (assertion 5 — refuses a vacuous pass).

Write the replacement to hold those four, and **watch it red-then-green** with a
break record. Assertions 3 and 4 of the old guard are the ones most likely to be
dropped as "the compiler handles it now"; the compiler does not handle either.

## 6. What is measured, and by what

| Claim | Proof |
|---|---|
| A peer and a local caller read the same `Withheld` | `a_local_caller_and_a_peer_read_the_same_withheld`, asserting **value equality of two `Withheld`s** for the six conditions reachable from both. `Panicked` goes through `ask_catching`, because `ask` resumes it |
| The prefix consts are gone | `rg 'PANICKED\|OBSERVER_PANICKED\|RESTARTING\|DROPPED\|REFUSED' keylink/src/reply.rs` empty. **Not** a `starts_with` count — that count was the goal's false claim |
| Every former prefix producer builds a typed variant | enumerate the five producers in `rendered` (`turns.rs:4547,4554,4562`) and `lost` (`:4618,:4639`); assert each by name in one test |
| `Failed` has one producer | `rg 'RemoteError::Failed\(' keylink/src` returns only the `Middleware` pass-through |
| The hash moved once | both pins updated in commit 5, values read off one failing run, release note written |
| Nothing else regressed | `mise run gate` (18 declared steps), `cargo nextest run --workspace`, `mise run anchors`, `mise run replay-anchors` N of N, `mise run docs` under `-D warnings` |

## 7. Refused, so nobody re-derives them

- **Distinguishing "factory refused" from "no factory".** Descoped to goal 3.
  The two converge at the `return` (`registry.rs:778` vs `:813`) and `Factory`'s
  refusal channel is `Option::None` (`registry.rs:533`) — no room for a reason.
  Fixing it changes the `Factory` signature and every caller's closure.
- **Making `TurnLost` `Clone`/`Eq`.** `Faulted(Panic)` forbids it, and the
  success criterion compares `Withheld` precisely to avoid needing it.
- **A `Cow<'static, str>` to dodge the allocation.** No `Schema` impl; buying
  one costs more than the allocation on an already-slow path.
- **Routing local calls through a fake `Dispatch`.** `middleware.rs:389-396`
  argues against it — precisely, through the `Context` argument rather than
  through `Dispatch` by name.
- **Touching what a panicking observer *does*.** `fatal: false` and the commit
  decision are defended in writing and were re-confirmed this week.

## 8. What this plan does not do

Goal 2 (one trait both sides) and goal 3 (peer administration) are separate
plans, written after this lands. Their shape depends on what `Withheld` actually
becomes, and writing them now would be speculation dressed as sequencing.

Two facts already known to correct them when they are written:

- **Goal 2's hazard framing is wrong on a detail.** `Dispatch` is a type alias
  (`rpc.rs:89-90`) with **three** construction sites (`macros.rs:705,1207,2147`),
  not one. `Handle` also has no `ask_catching` to expose — it is
  `LocalActor::ask_catching` (`turns.rs:3941`) behind a private field. And an
  async-trait impl for `blocking::Client` is **not** prevented by anything; it
  fails whenever a tokio runtime is current, not always (`blocking.rs:114-120`).
- **Goal 3's read gap is bigger than "a second connection".** `ActorStatus` is
  built only from a keyed registry entry (`registry.rs:839-866`), so a singleton
  actor yields **no row at all** — the question "which fields are meaningless
  for a singleton" has no field-level answer. And the load-bearing change is
  widening `Inspector`'s erasure, which today is an
  `Arc<dyn Fn() -> Option<Vec<ActorStatus>>>` that `console.rs:710-713` says
  "cannot mount, shut down, abort or address anything".
