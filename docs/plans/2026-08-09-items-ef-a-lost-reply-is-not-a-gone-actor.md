# Items E + F — a lost reply is not a gone actor

Date: 2026-08-09. Branch: `reply-not-gone`. Base: `99ce53f`.

E and F were two items. They are one wound, and one arm of it that neither item
named turned out to be the largest.

Revision 2, after three adversarial reviews. Every count below names its unit.
The first revision guessed five figures and four of them were wrong; where a
number appears here, the command that produced it appears with it.

---

## 1. The finding

`LocalActor::answered` ends with one line (`turns.rs:3642`):

```rust
answer.await.map_err(|_gone| TurnLost::Gone)
```

Its own doc (`turns.rs:3604-3620`) defends that as total:

> The sender is dropped *without sending* only when the loop is not there to
> send: `abort`, a runtime shutdown, or a panicking `Drop`.

**That enumeration is incomplete, and the crate already knows it.** `lost()`
(`turns.rs:4169-4199`) enumerates the causes of the same event on the served
path, and the second is *a handler took `reply.later()` and dropped the
`Deferred` — the sanctioned move, and what that type's `#[must_use]` asks for*.
`lost()` states why the three answers are three:

> The three answers are three different next moves for the caller, which is the
> only reason to tell them apart.

So keylink answers one condition two ways. A **peer** is told `dropped: the actor
is serving and this reply was never answered`. A **local caller in the same
process**, about the same actor and the same dropped reply, is told the actor is
no longer running.

`run_keyed_supervised`'s doc (`turns.rs:3003`) says `deliver_to_local` and
`deliver_to_actor` share `lost` so "the two paths cannot say different things
about one condition". The local path is the third caller, and it was left out.

### The escape hatch does not exist for the primary API

`Undelivered::Gone`'s doc says *"Use `ask_catching` to tell them apart"*.
`ask_catching` occurs **zero** times in `macros.rs` (`rg -c ask_catching
keylink/src/macros.rs`). A generated `Handle` — what every `service!` produces —
has no door to `TurnLost`. `Handle::ended()` calls itself "the second question",
and `LocalActor::ended`'s doc says *"A restart is **not** an ending"*
(`turns.rs:3774`), so it does not resolve for either live cause. `is_running()`
answers `true`.

### It is a **kind** of defect, not an instance — and that is what changed the plan

Review found a fourth member of the family, which is the one this plan now leads
with. `ask` folds `TurnLost::Refused` into `Undelivered::Gone` (`turns.rs:3546`),
and revision 1 defended that with *"a refused turn ends the actor"*. **False.**
`retryable()` (`turns.rs:2640-2657`) reads `Ended::Observer { fatal, .. } =>
!fatal`, so a **non-fatal** observer refusal is retryable and `turns.rs:2735`
returns `Standing::Restarting`. A local caller of a supervised actor whose turn an
observer refused is told *stop calling* while a supervisor rebuilds it.

Four defects, one shape, all at an editorial arm:

| collapsed | truth | fixed? |
|---|---|---|
| `Expired` → `Gone` in `ask_catching` | the actor is untouched | already, `turns.rs:3594` |
| `Reentrant` → `Gone` | the actor is running this turn | already, `turns.rs:3545` |
| a dropped reply → `Gone` | the actor is serving | **this plan** |
| a non-fatal `Refused` → `Gone` | the actor is restarting | **this plan** |

So the plan stopped patching the instance. **It deletes the kind.**

### The crate pre-registered this decision

`ask`'s body comment about `Expired` (`turns.rs:3536-3540`):

> it cannot be reported precisely here, because this method has one error type by
> design … if a later wave gives one a budget, **this is the call that has to
> widen** … **Named here so it is a decision somebody makes.**

This is that decision, made.

---

## 2. The shape

### Two axes, written down

`TurnLost` answers on two axes, and nothing in the crate says so. Naming the seam
is what stops a fifth editorial arm being invented across it later:

- **The turn axis** — `Refused`, `Faulted`, `Expired`, `Reentrant`. What happened
  to *this message*. Decided by the loop, carried on the reply channel.
- **The actor axis** — `Dropped`, `Restarting`, `Gone`. What is true of the
  *actor*. A projection of `Standing`, read only when the channel died.

An answer comes from exactly one axis. If the loop sent a verdict the channel
lived, so no `Standing` is read; if the channel died the loop sent nothing, so
there is no verdict. That is why the mapping is total rather than layered.

### Split by door

`Undelivered` today is the error of four doors that ask two different questions.
`notify` and `stop` ask *did this get queued?* — two states, always. `ask` and
`read` ask *did I get a value?* — every state. One type serving both is what
would put four unreachable variants on a public `stop`.

```rust
/// A message that never entered the mailbox.
pub enum NotQueued { Closed, Reentrant }
```

`send` (private, `turns.rs:4005`), `notify` and `stop` return it. Total over two
states at all five send sites, none of them dead.

### `Undelivered` becomes `TurnLost` minus `Faulted`

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum Undelivered {
    Gone,
    Dropped,
    Restarting,                      // unit: `Arc<Ended>` is not `Copy`
    Refused { name: &'static str },  // `&'static str` IS `Copy`, payload kept
    Expired,
    Reentrant,
}
```

`ask` resumes `Faulted` and maps the other six one-to-one. **There is no
editorial arm left anywhere in the crate.**

Verified by review, not assumed: `&'static str` is `Copy` for every referent, and
`PartialEq` for a reference forwards to the pointee, so `str` compares **bytes**
and no `assert_eq!` can go flaky. `tests/observe.rs:437` already depends on this.

`Expired` is included though it is unreachable from a local origin, on the
crate's own precedent (`observe.rs:622-632`) and its own words — *"written to be
correct if something reaches it"*. Excluding it would keep one editorial arm
alive on the exact state whose mis-folding is the canonical example. No
`unreachable!()`: `turns.rs:3581-3587` records why the crate refused that shape in
this same arm.

### `answered` reads the discriminator instead of guessing

```rust
// `RecvError` is a unit type, so the channel carries no cause. `Fate` does, and
// it is the same cell `lost` reads on the two served paths.
answer.await.map_err(|_| match self.fate.standing() {
    Standing::Running => TurnLost::Dropped,
    Standing::Restarting(reason) => TurnLost::Restarting(reason),
    Standing::Gone => TurnLost::Gone,
})
```

Exhaustive over `Standing`, so a fourth standing is a compile error here rather
than a silent fold into `Gone` — the shape that produced this defect.

### Why two types survive, stated where the dead reason was

`TurnLost`'s reason for existing dies with this change. `observe.rs:563-565` says
it "replaces `Undelivered` on the catching paths, because 'the actor is gone' and
'this turn was refused' ask a caller for different things" — and `Undelivered`
now says which it was.

The replacement is the only one that is true: **`TurnLost` carries two payloads
that make `Copy` and `Eq` impossible** (`Panic`, `Arc<Ended>`), and 19 assertion
sites want `Copy` and `Eq`. `Undelivered` is `TurnLost` with the payloads dropped
and `Faulted` removed, because `ask` resumes it.

`ask_catching` is repriced in the same breath: after this it buys the `Panic`
payload and the `Arc<Ended>` reason, **not** "tell them apart".

### `Display` strings — ruled before they are written

The two enums describe the same facts, so the six shared variants carry the
**same** string, and a guard checks it. `Undelivered::Gone`'s string returns to
`TurnLost::Gone`'s — *"the actor is no longer running"*.

That reverses a change this session made three days ago. It was right then: with
two variants, `Gone` covered three causes and "not delivered to a committed turn"
was the only sentence true of all three. `Gone` now has exactly one cause, and the
specific sentence is the honest one again.

**The revert was checked against every producer rather than assumed.** After the
change `Undelivered::Gone` has four producers, in two groups. `Standing::Gone`
means "there will be no next incarnation" by its own doc. A closed mailbox means
the same: `is_running()` is `!mail.is_closed()` (`turns.rs:3908`), `close()` has
exactly two callers on a local actor and both are give-up arms (`turns.rs:2738`,
`2904`), and `Inbox::close`'s own doc states the invariant — *"only on a path
where there will be no next incarnation"*. The restart arm returns before
reaching it (`turns.rs:2735-2736`), and `Lent` never drops the receiver anyway.
**No producer leaves the actor alive.**

Two of the six sentences are new, and **both are sentences this crate already
wrote**, which is what makes the shared-text rule literal instead of argued:

- `Dropped` is `lost()`'s wire sentence minus its prefix (`turns.rs:4192-4194`).
  A peer reads `dropped: the actor is serving and this reply was never answered`;
  a local caller reads the same sentence without the prefix.
- `Restarting` is `Standing::Restarting`'s own doc (`turns.rs:2408`).

Neither may be written **with** its wire prefix: two texts differing only by a
prefix is how a `starts_with` becomes ambiguous, which is what
`no_failure_prefix_is_a_prefix_of_another` exists about.

`Restarting` is the one variant where identical text and identical payload cannot
both hold — `TurnLost` carries `Arc<Ended>` and `Undelivered` cannot. **Both texts
stay payload-free**, which costs nothing: `lost` already renders the reason
separately, and it never goes through `Display`.

`Reentrant` is **already byte-identical across the two enums, by hand, with
nothing enforcing it.** The rule is half-kept already, which is the argument for
the guard rather than for the rule.

### One pre-existing gap this change makes matter

`no_other_served_failure_begins_with_one_of_the_four` (`actor/remote.rs:752-783`)
documents itself as covering *"every other `RemoteError::Failed` keylink itself
builds on a served path"*. Its list holds four entries and **omits
`Undelivered::Reentrant`**, which `turns.rs:4302` builds into a served `Failed`.
The claim is false today; nothing is broken only because that text happens to
begin with none of the four prefixes.

It matters now because `sent` becomes a `Result<(), NotQueued>`, so a bare
`refused.to_string()` there would silently start rendering a **different type's**
`Display` onto the wire — and §1's trace shows no test would see it. C1 renders
through `Undelivered::from(refused)` explicitly, and adds the missing entry so the
guard consumes the text it already claims to.

---

## 3. What is measured, and by what

| quantity | value | method |
|---|---|---|
| `assert_eq!` sites comparing an `Undelivered` | **11** | brace-matched every `assert_eq!`, kept those naming a variant; a 12th (`cycles.rs:643`) is a message string |
| exhaustive `match` sites that stop compiling | **7** | 6 over `Undelivered`, 1 over `TurnLost` |
| — of those, in `tests/cycles.rs` | **3** | `184-188`, `204-210`, `256-265` |
| `match` sites the compiler will **not** catch | **1** | `tests/cycles.rs:194-201`, `_ => TELL_SURPRISE` |
| existing tests that FAIL | **1** | `turns.rs:5163` |
| existing tests that rot **silently** | **2** | `defer.rs:332`, `defer.rs:408` — both assert only `is_err()` |
| doc claims falsified | **17–23** | two derivations; the union is the work |
| doc passages DELETED, not rewritten | **7** | the collapse has no referent left |
| doctests / README fences affected | **0** | the one fence naming either enum is ```` ```ignore ```` |
| other workspace crates affected | **0** | `rg 'Undelivered\|TurnLost' demo/ keylink-persist/` is empty |
| `RECORD_BLOCKS` today | **165** | `.mise/tasks/anchors:2965`, gate green today |

`Undelivered` grows from **1 byte to 24** (a fat pointer plus discriminant). Derived,
not measured — nothing in the workspace asserts it (`tests/actor_memory.rs`
measures `Mail` only). Measure it once during the work rather than treating it as
a blocker.

---

## 4. The work, in commits

Split so a regression in one does not implicate another. `Refused` is the biggest
behavioural win and is **independent of `Fate`** — a refusal is an answer, so it
never reads `Standing`.

**C1 — `NotQueued`.** New public two-state type. `send`, `notify`, `stop` return
it; `answered`, `read`, `deliver_to_local` adapt. `notify` (`turns.rs:3689-3693`)
returns the send result **directly**, with no `?` and no `map_err` — a `From` impl
does not cover that body. No behaviour changes.

**C2 — `Undelivered` gains `Refused { name }` and `Expired`.** `ask` maps them.
Seven `tests/observe.rs` assertions gain the observer's name — each already
asserts that name a second time through `ended()`, which is the argument for
keeping the payload. No `Fate` read yet.

**C3 — the `Fate` read.** `TurnLost::Dropped`, `TurnLost::Restarting(Arc<Ended>)`,
and their two `Undelivered` twins. This is item E.

**C4 — the guards** (§5).

**C5 — docs** (§6), including the compiled `#[must_use]`, and the RFC-07 appendix
(§7).

**C6 — `mise run anchors`.** Read the new `RECORD_BLOCKS` off the failing run.
Never compute it. Add a transition entry in the existing format. `anchors` runs in
**pre-commit**, so it fails before anything compiles.

---

## 5. Guards

Revision 1 listed five. One could not go red, one duplicated four existing tests,
and none covered the two new supervised cases.

| guard | what it pins |
|---|---|
| `a_live_actor_that_drops_a_deferred_is_told_dropped_and_not_gone` | the finding, on the local path — **and asserts the next call succeeds in the same test**, because liveness alone discriminates nothing |
| `a_handler_that_panics_before_consuming_its_reply_reads_dropped` | the cause that never was a `Deferred`. Fixture ready: `Book::boom`, `defer.rs:112-114` |
| `a_local_caller_can_tell_restarting_from_gone` | `Restarting` through a local `ask`. Mirror `tests/supervision.rs:1107` |
| `a_local_caller_is_told_refused_when_a_non_fatal_observer_refuses` | the fourth defect. `retryable()` makes it restart, and no local-door test reaches it today |
| `a_local_caller_and_a_peer_agree_about_one_dropped_reply` | the property `lost` exists for, extended to the third path. Must assert the local **variant** and the peer **prefix** — both `Err` passes for the wrong reason |
| `every_turn_lost_except_faulted_has_one_undelivered_twin_with_the_same_words` | the two-type relationship, mechanical instead of editorial: same variants, same `Display` |

**The control is not written.** Four tests already pin it — `turns.rs:4690`,
`4746`, `5009`, `singleton.rs:211`. Name them; a fifth is duplication.

### The twin guard, and what it does NOT check

**Rust has no reflection over enum variants, so no test can enumerate them.** Any
design claiming otherwise is wrong, and this one says what it rests on instead:

- a new `TurnLost` variant is `E0004` in the exhaustive `twin` match;
- a new `Undelivered` variant is `E0004` in the exhaustive `slot` match;
- the sample array is **fixed-size** and indexed by literals, so a seventh
  variant's natural `EVERY[6]` fires rustc's deny-by-default
  `unconditional_panic`. **Verify that in one line before trusting it** — if it
  only warns, the array becomes a `const` with an explicit length assertion.

Four assertions, each aimed at a way the test could pass by consuming nothing:
`covered.iter().all(..)` refuses a loop that ran zero times or over a subset;
`without_a_twin == 1` refuses a `twin` that returns `None` for everything and
skips every comparison; a set of the rendered texts refuses two variants sharing
one sentence; a non-empty check refuses `#[error("")]`, which satisfies equality
and says nothing. Both sides come from their own `Display` on values built from
literals — the `Undelivered` is never derived from the `TurnLost`'s text, which is
the tautology this shape invites.

**The residual, named rather than hidden:** an author who adds a variant and
points `slot` at an *existing* index gets past all three mechanisms. That belongs
in the test's own doc, not in a claim that the check is total.

Two break records, not one: one changes a character in `Dropped`'s `#[error]`
(exercises the equality), one points `slot`'s `Expired` arm at `0` (exercises the
coverage). A single record would leave half the test defended by prose.

Each behavioural guard is watched red-then-green via `scripts/break-once.sh` with
a break record, per `CONTRIBUTING.md`.

### Tests that must change

- **`turns.rs:5163 a_released_reply_is_gone_when_the_turn_that_released_it_panicked`
  FAILS.** The actor survives — the same test asserts `Faulted` for the turn's own
  caller at `5180` — so the answer becomes `Dropped`. The assertion, the **name**,
  and the break record at `5158-5161` all move.
- `defer.rs:332` and `defer.rs:408` keep passing on a different variant. Tighten
  both to the variant, or they rot silently.
- `defer.rs:163-168` and `193-196` are break records whose watched lines name the
  type and not the variant. Re-watch.
- **`tests/cycles.rs:194-201`** — the wildcard. Review by hand; a wildcard arm is
  what `TurnLost`'s own doc (`observe.rs:596-601`) calls the defect.

---

## 6. Docs

**Derived, not written** — revision 1 wrote the list and missed eight. Re-derive
before editing:

```
rg -n 'is gone|no longer running|TurnLost::Gone|Undelivered|fails one way|collapse|tell them apart' keylink/src/ keylink/tests/ README.md
```

Two independent derivations gave 17 and 23 sites; the union is the work. Known
members that revision 1 missed, listed because each is a different *kind* of
miss:

- `turns.rs:693-694` — the compiled `#[must_use]` on `Deferred`. **Compiler output
  a user reads**, quoted in prose at `defer.rs:547-548`.
- `turns.rs:86` — "`Undelivered` is a unit struct". Already false; it has been an
  enum since `Reentrant`.
- `parking.rs:125-126` — `Verdict::Faulted`, "the parked caller is told
  `TurnLost::Gone`". A behaviour claim, in a file revision 1 never opened.
- `tests/persist.rs:730-734` — "a restart … drains parked replies as
  `TurnLost::Gone`". Names the exact variant this plan adds.
- `README.md:184` — the word-for-word twin of two sites revision 1 did list.
- `macros.rs:1797-1799` — "a local call fails one way". `turns.rs:93-96` **already
  corrected this phrasing** and the macro never caught up, so the correction is
  written and only has to be applied.
- `defer.rs:609-613` — "a bare `Standing::Gone` occurs fourteen times in this
  file". Measured today: **16**, and C3 adds a 17th.

Seven passages are **deleted**, not rewritten, because the collapse they explain
stops existing: `turns.rs:3525-3548` (keep only the `Faulted` sentence),
`3059-3068`, `3070-3086`, `3604-3620`, `4707-4710`, `5095-5100`, and
`macros.rs:1815-1822`.

Two corrections to revision 1's own citations: **`TurnLost` lives in
`observe.rs:603`, not `turns.rs`**; and the append-only rule is
**`CONTRIBUTING.md:334`**, governing `docs/rfc-*.md` only — not `docs/` as a
whole, so `docs/plans/` is editable, which is why this file is a revision rather
than an appendix.

---

## 7. Item F — RFC-07 §4.8, re-priced

`docs/rfc-07-the-turn-model.md:939`. It states the cost as:

> the caller is told `Gone` — `MailboxClosed` through a generated `Handle` —
> rather than being handed the panic

Two things are wrong. `MailboxClosed` is `Undelivered` (renamed with item M; the
RFC names the old type **8** times, at `522`, `943`, `1526`, `1720`, `2272`,
`3025`, `3740`, `3998`). And the price it quotes — *the caller cannot tell this
from a dead actor* — is what this plan closes.

What §4.8 refused is still refused: the panic itself does not reach the caller.
That needed "a return channel and a `Drop` impl on the reply of every method".
So the re-pricing is exact: **the loss of the panic remains, the loss of the
distinction is closed.**

**The price was re-checked, not assumed.** A reader will ask whether item M's turn
chain is that return channel. It is not, on two grounds that are about the
mechanism rather than about effort. **Direction:** the chain travels caller →
actor, cloned into each `Message::Mail` at send; the refused feature needs the
reply's destination travelling actor → caller at drop time. **Content:** the
chain is actor identities for a membership test — `turns.rs:1339` says "One
actor, for **chain membership and nothing else**" — so it can name who is calling
and cannot name where an answer goes. `Reply` and `Deferred` still have no `Drop`
impl at all, which is exactly what §4.8 asked for and did not build. `Fate` fails
the same way and for the same reason it succeeds at the thing this branch uses it
for: it is a cell about the *actor*, with no payload and no reply identity, so it
can make the caller's sentence true and cannot hand it a `Panic`.

Appended as a dated section, per `CONTRIBUTING.md:334`. Drafted at
`.tmp/c5/rfc-07-appendix.md`.

**Two of the eight `MailboxClosed` occurrences are not renamed.** Lines 1526 and
1720 are quoted **break-record output** — transcripts of runs that happened and
printed that word. Six read as `Undelivered` from here on; those two stay,
because correcting a record of a real run would falsify it. `MailboxClosed` is
now zero occurrences in `keylink/src`; the RFC is the only place it survives.

**Sequencing:** the appendix states that
`a_deferred_handler_that_panics_costs_its_caller_the_answer_and_not_the_actor`
asserts `Dropped`. That is true only **after** the worklist's `tests/defer.rs`
rows land. Appending it earlier would give the RFC a claim the tree does not
hold — the defect this branch exists to fix. So the append is the **last** step
of C5, not the first.

---

## 8. What this plan does not do

- **No `Reply::detach`.** Item E named it. It would have let a handler opt into
  being reported as something other than `Gone`; every caller is now told
  something other than `Gone`, including in the accidental cases, so a method
  whose only effect is to opt into the truth has nothing left to do.
- **No reason on `Undelivered::Restarting`.** Revision 1 justified this by saying
  a local caller can get the reason from the handle. **There is no such door** —
  `Fate` is `pub(crate)`, `ended()` does not resolve during a restart, and
  `ask_catching` is absent from `macros.rs`. The unit variant stands on the
  `Copy`/`Eq` cost alone, and its doc says the reason is unreachable locally.
- **`Refused` does not carry the actor's fate.** A caller told `Refused` cannot
  learn whether the actor restarts; that depends on `Stop::fatal`. Stated in its
  doc rather than implied, because the paragraph that used to pretend otherwise
  is being deleted.
- **Neither race window.** `deliver_to_local` documents one (`4337-4344`): a
  restart whose next incarnation already published `Running` reads `Dropped`. Its
  mirror is documented at `turns.rs:2535-2542`: a caller's state whose `Drop`
  panics mid-restart leaves the cell `Restarting` for an actor that will never
  return. The local path inherits **both**, and RFC-07 §12.4 carries the
  generation counter that closes them.
- **`LocalActor::read` keeps its `Gone`, and it is exact.** Two reviewers
  disagreed; the trace settles it. `Lent` holds `&mut I` and **owns no receiver**
  (`turns.rs:2676-2681`), so `run_loop`'s `drop(inbox)` on the restart path drops
  a borrow and the queued read survives the incarnation. The `mpsc::Receiver`
  lives in a **parameter** of the frame that owns it — `run_loop`'s unsupervised,
  `supervise`'s under a supervisor — and each of those frames declares an armed
  `Floor` as a **body local**, which drops first. So the receiver cannot die
  before a `Gone` is in the cell. `Watched`, the only `Inbox` a local actor's
  receiver is wrapped in, takes `Inbox::ending`'s default of `Gone`
  (`turns.rs:1715-1717`); `Lent` is the only impl that returns `Restarting`, and
  it owns nothing.

  `read` gets no edit — it gets the paragraph that says why, **including what
  would break it**: give `Watched` an `ending` of its own, or move the receiver
  out of the floor-carrying frame. Neither is caught by a test today, and the
  crate's own standard (`turns.rs:5658-5663`) is that correct code protected only
  by prose is the shape to name.

  One true thing that is not a lie: on the restart path the queued read is
  answered by the **next** incarnation, off a state rebuilt from the last commit,
  and the caller is not told it crossed a restart. Recorded so it is not later
  mistaken for this question returning.
