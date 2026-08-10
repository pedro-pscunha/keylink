# `compose::Group` — fate sharing for a set of actors

> **Current testing note (2026-08-10):** Any ALPN literal-pin step below is
> historical and is superseded by `CONTRIBUTING.md`'s **The wire contract**.
> The literal-pin tests are ignored until keylink has a stable version. Do not
> run or update them, and do not order work around them. The active contract
> property tests remain in the gate.

Against `main@0ab8366`. Every citation was read in the tree on 2026-08-08.

**Revision 2.** Four adversarial reviews found 5 Critical and 13 Major defects in
revision 1. The thesis survived; the surface did not. The largest change is that
`Verdict::Restart` is gone — §3.3 carries the argument and the three independent
facts that killed it.

---

## 1. What this is

Issue #9 asks for group supervision written as a function. Its motivating
sentence is:

> The built-in supervision loop rebuilds **one** actor. It cannot say "if
> `Reserve` dies, take `Charge` down with it".

That is **fate sharing**: ending actors together, not restarting them together.
This design ships exactly that, and refuses the restart strategies issue #9's
sketch wraps around it.

`Group` watches a set of actors. When one ends for good, it asks a function the
caller wrote whether the group carries on without it or ends. That is the whole
feature.

---

## 2. The authority, because two documents disagree

Issue #3 refuses `OneForAll`/`RestForOne` **and** the mechanism this design uses.
Quoted in full, including the clause revision 1 cut:

> **`Supervisor`, `Strategy`, `OneForAll`/`RestForOne`, parent/child trees.** In
> OTP `one_for_all` exists because a crashed process leaves peers holding a
> **dead pid**. Here identity does not die with the incarnation — the outer loop
> keeps the `mpsc::Sender`, so a `Handle` held elsewhere keeps working across a
> restart. There is no stale reference to repair. What remains is *state*
> coupling, and a group restart is both too broad (discards a healthy actor's
> live state) and too narrow (cannot say *what* went stale) to fix it. That is a
> message, not a strategy. This also settles the kameo comparison:
> `RestartPolicy`, `SupervisionStrategy` and **`Links`-based death propagation**
> are the OTP model, and adopting them is a reversal of RFC-04 §8 rather than an
> addition to it.

`Links`-based death propagation **is** fate sharing. So issue #3, read alone,
refuses this feature.

**It is superseded on exactly this point.** RFC-04 §8 is marked *"SUPERSEDED AND
REMOVED, 2026-08-07. The current model is `docs/rfc-07-the-turn-model.md` §9"*
(`rfc-04-actors.md:995`), and RFC-07 §9.8 records the owner's decision
(`rfc-07-the-turn-model.md:2690-2695`), verbatim:

> **One caveat the owner recorded, and it is the next question rather than this
> wave's.** The dead-pid argument defeats sibling-*restart* strategies and does
> not touch **ordered shutdown, escalation, or lifecycle grouping**. Those are
> judged wanted. They are not built here, and nothing in this wave forecloses
> them: a group that shuts down in an order is a statement about several actors,
> and supervision as built is a statement about one.

That line divides on **what the thing does** — restart versus grouping — not on
which module it lives in. It is why this design is buildable and why the restart
half of issue #9's sketch is not.

All three wanted things are delivered here: **lifecycle grouping** is `Group`
itself, **ordered shutdown** is §4.6's teardown order, and **escalation** is a
group that is a member of another group (§5).

RFC-04 §8's surviving sentence — *"There is no process tree, no links, no exit
signals, and Wave 9 built none of them"* (`rfc-04-actors.md:1013`) — describes
what Wave 9 built. It reads as a live prohibition to anyone who opens that file
first, so §7 appends one line there pointing at §9.8.

---

## 3. What changed from revision 1, and why

### 3.1 The group is not an actor

Like `Names` (`names.rs:106`), it is a plain struct: an `Arc` of shared state and
one spawned task. **No mailbox, no `service!`, no ALPN, no turn.**

Three of issue #9's four named problems stop existing rather than being solved:

| issue #9 problem | what happens to it |
|---|---|
| 1 — a supervised `Group` panics and loses its list | There is no group actor to supervise |
| 2 — mailbox depth is 1, so ten deaths serialise | There is no mailbox. Endings are futures, bounded by the member count |
| 3 — `A → B → A` hangs | Narrowed and closed — §4.5 |
| 4 — teardown races a member in backoff | Solved by `abort()` + `ended()`, with no new runtime API — §4.6 |

kameo is the control group. It puts death notices in the ordinary bounded mailbox
(`mailbox.rs:985`, 64 slots) and pays three times: `drain_until_children_closed`
(`spawn.rs:392`), a `parent_shutdown` `AtomicBool` (`links.rs:107`), a branch in
`notify_links` (`links.rs:170`), plus a regression test named for the bug,
`stop_gracefully_does_not_deadlock_with_supervised_children` (`supervision.rs:1959`).

### 3.2 Keyed actors cannot be members, and that is a stated limit

`Membership` needs the member's `ended()`. **A keyed actor has no `ended()`.**
`names.rs:128-131`: *"a keyed actor has no `ended()` to ask for"*. RFC-07 §5.7
(`rfc-07-the-turn-model.md:1235`) lists it as missing: *"Give a keyed actor an
`ended()` channel. It is the one thing `spawn_wired` does not deliver."*

Revision 1 called a keyed member the *safe* shape. It is not a shape at all.
`Group` is local-actor-only today, `Membership` takes the ending as an argument
so it needs no change the day §5.7 lands — `Names::register` already carries that
sentence — and the limit is documented rather than discovered.

### 3.3 `Verdict::Restart` is gone

Revision 1 kept it. Three independent facts kill it.

1. **It cannot be bounded.** Revision 1 bounded group restarts with `Intensity`.
   `Intensity` exposes exactly one public item, `new` (`local.rs:2009`); `max`
   and `recovered_after` are private with no accessor, and `backoff()`
   (`local.rs:2028`) and `Budget` (`local.rs:2274`) are private. `compose` is a
   sibling module. So the promise *"nothing in `local.rs` changes"* and the
   promise *"group restarts are bounded by an `Intensity`"* could not both hold.
2. **It re-creates the stale reference this crate does not have.**
   `spawn_supervised` builds a fresh `mpsc::channel` (`local.rs:2924`), so a
   group rebuild produces a new mailbox and every handle a caller holds is dead.
   Issue #3's dead-pid argument is about a problem keylink avoids; a group
   restart would import it.
3. **It bought one shape and cost seven.** After §3.2 the only safe shape was a
   local member republished through `Names` — a call site that writes the same
   `ActorId` twice and calls `ended()` twice on one actor. It paid for: the
   `Intensity` argument, a re-entrant `FnMut` factory that can panic on the
   group's own task, a precondition nothing in the type system catches, an
   `OutOfRestarts` ending, a policy blind to the budget it spends, and a refusal
   row for adoption whose entire argument was that `Restart` could not answer it.

**What it does not cost.** RFC-07 §9.8 names escalation as wanted, and escalation
is delivered without it: a group whose policy says `StopAll` ends, and a parent
group holding it as a member sees that ending and decides in turn. That is OTP
escalation, built out of the two verdicts that remain.

Dropping it deletes four defects rather than repairing them, and it makes
**adoption the only mode**: `add` takes a `Membership` built from an actor that
is already running, so there is no factory at all.

---

## 4. The design

### 4.1 The public surface

```rust
/// What happens to the GROUP when one member ends for good.
///
/// A member's own restarts never reach here. This is asked once, when that
/// member's ending is final.
#[non_exhaustive]
pub enum Verdict {
    /// Carry on without it. The member leaves the group.
    Forget,
    /// End the group: every other member is retired, and `ended()` names this
    /// member and its reason.
    StopAll,
}

/// One actor the group watches, and the means to end it.
pub struct Membership { /* private */ }

impl Membership {
    /// `ended` is this actor's own `ended()`; `retire` ends it.
    ///
    /// Both must name the SAME actor. Prefer [`membership!`], which names it
    /// once and cannot cross them.
    pub fn new<F>(
        ended: impl Future<Output = Ended> + Send + 'static,
        retire: impl FnOnce() -> F + Send + 'static,
    ) -> Self
    where
        F: Future<Output = ()> + Send + 'static;

    /// End this member now, off the group's books.
    pub async fn retire(self);
}

/// Why a group ended. Every variant carries what that ending has, and nothing
/// it does not.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum GroupEnded {
    /// The policy said [`Verdict::StopAll`].
    Member { member: ActorId, reason: Ended },
    /// The policy panicked while deciding about this member.
    PolicyPanicked { member: ActorId, message: String },
    /// The future that was to report this member's ending panicked instead.
    /// Added in revision 3 — see §8.
    EndingPanicked { member: ActorId, message: String },
    /// [`Group::stop`] was called.
    Stopped,
    /// Every `Group` was dropped, so nothing decides any more.
    Dropped,
}

pub struct Group { /* private */ }

impl Group {
    /// A group that watches nothing yet.
    ///
    /// # Panics
    /// Outside a tokio runtime, like every other spawn in this crate.
    #[must_use = "dropping the `Group` stops watching; hold it"]
    pub fn new(
        policy: impl FnMut(&ActorId, &Ended) -> Verdict + Send + 'static,
    ) -> Self;

    /// Watch `member` under `name`, replacing whatever held that name.
    #[must_use = "this is the member the name displaced, and the group no \
                  longer watches it; retire it, or say `let _ =` to mean that"]
    pub fn add(&self, name: ActorId, member: Membership) -> Option<Membership>;

    /// Stop watching a member, without ending it and without asking the policy.
    #[must_use = "the group no longer watches this member; retire it, or say \
                  `let _ =` to mean that"]
    pub fn remove(&self, name: &ActorId) -> Option<Membership>;

    /// Every name, in the order it was added.
    #[must_use]
    pub fn names(&self) -> Vec<ActorId>;

    /// Resolve when the group ends, and say why. Callable more than once.
    pub fn ended(&self) -> impl Future<Output = GroupEnded> + Send + 'static;

    /// Retire every member and end the group, without asking the policy.
    pub fn stop(&self);
}

/// Build a [`Membership`] from a handle, naming it once.
#[macro_export]
macro_rules! membership { /* $handle -> Membership::new($h.ended(), || $h.abort()) */ }
```

**`Group` is not generic over a handle type**, and the reason is not `pool.rs`'s
trait refusal — it is that the motivating example is heterogeneous. `Reserve` and
`Charge` are different services with different `Handle` types, so there is no one
`H`. `Pool<H>` and `Names<H>` are homogeneous by construction; a group is not.

The call site:

```rust
let reserve = reserve::supervised(build_reserve, limit);
let charge  = charge::supervised(build_charge, limit);

let group = Group::new(|who, why| match why {
    Ended::Stopped => Verdict::Forget,
    _              => Verdict::StopAll,
});

let _ = group.add(ActorId::new("reserve")?, membership!(reserve));
let _ = group.add(ActorId::new("charge")?,  membership!(charge));
```

### 4.2 Naming, against the neighbours

`Outcome` is **taken**: `keylink::Outcome` is the middleware chain's result type
(`lib.rs:327`, `reply.rs:186`). Revision 1 declared a second one. It is gone —
`GroupEnded` is an enum and carries its own reasons, so there is no second type
to name and no field called `verdict` whose type is not `Verdict`.

`names()` and not `members()`, because `Pool::members()` (`pool.rs:185`) returns
handles while this returns names, and `Names::names()` (`names.rs:202`) is the
precedent for the shape and for the eager `Vec`.

### 4.3 `Verdict` is `#[non_exhaustive]`, and that is cosmetic

Revision 1 argued the opposite from `CONTRIBUTING.md:209-212` — *"Enums a caller
constructs and matches are not"* — and misread a conjunction. A caller constructs
a `Verdict` and never matches one; `#[non_exhaustive]` binds downstream matches
only, and the crate's own match stays exhaustively checked. The stated cost was
zero all along.

The honest ground is `observe.rs:430-431`: *"the bill is a major version per
ending once this crate is published, and today it is unpublished with no
downstream consumers at all."* So the choice is cosmetic, and it goes to
`#[non_exhaustive]` because that keeps restoring a refused variant additive.

### 4.4 Every entry is stamped

`add` on a live name replaces it, and the displaced member's ending future is
still pending. Without a stamp the conductor reports that ending under a name the
replacement now owns, and the policy answers `StopAll` about an actor the caller
already replaced.

`names.rs:64-72` documents this exact race and fixes it with a generation stamp
(`names.rs:146`, `names.rs:165-170`). `Group` copies both. An ending whose stamp
is no longer the one filed under that name is **discarded**, not decided on.

Refusing a taken name instead was considered and refused for `names.rs:54-62`'s
reason, which transfers verbatim: a name is freed asynchronously, so a registry
that refused a taken name would refuse a restart nondeterministically.

### 4.5 What `A → B → A` closure actually claims

Revision 1 said "unwritable". That is false: a synchronous closure cannot await,
but it can block — and a `std::sync::Mutex` an actor holds across an await needs
no dependency at all to do it.

*Correction, found while writing the RFC record:* revision 2 said
`n0_future::future::block_on` is "public and unconditional". It is public and
**conditional**. `n0-future` does not define it — it arrives through the glob
`pub use futures_lite::future::*` (`n0-future-0.3.2/src/lib.rs:79`), and the
function is `pub` under `#[cfg(feature = "std")]`
(`futures-lite-2.6.1/src/future.rs:52-53`). The claim it supports is unchanged,
because the `Mutex` route needs no feature at all.

The property that survives a hostile reader:

> **No cycle can be built out of keylink calls.** The policy has no await point,
> and the group is never *told* anything — it only observes `ended()`. There is
> no call from a member into the group to close a cycle with.

keylink's own synchronous API cannot do it either: `blocking::Runtime::run` calls
`guard()?` and returns `Error::BlockingInAsync` (`blocking.rs:78`), and there is
no `Handle::blocking_*` at all.

The residual is stated rather than denied: caller code that blocks inside the
policy stalls the group, because §4.8 gives it one decision at a time in one
task. That is a caller error with a symptom, not an impossibility.

### 4.6 Teardown: retire everything, then wait

`StopAll` and `stop()` both run two phases:

1. **Start every member's `retire`**, in reverse insertion order, concurrently.
2. **Await them all together**, in the `FuturesUnordered` the conductor holds.

Serial retire-then-await would let one member wedged in a turn that never yields
delay the *retirement* of every later member, which are then still serving — the
opposite of the fate sharing this feature exists for.

**`retire` takes a future so a caller can choose.** `abort` is synchronous
(`macros.rs:2123`) and `stop` is `async` (`macros.rs:2108`); a synchronous
`FnOnce()` would have made `abort` the only expressible answer and let the prose
take credit for a choice the signature had already made.

`membership!` uses `abort`, and the recommendation has evidence:
`stop` puts `Message::Stop` in the mailbox (`local.rs:3306`) and `supervise`
polls the inbox only inside `run_loop` (`local.rs:2596`), never across either
backoff sleep (`local.rs:2563`, `local.rs:2622`). So a `stop`-based teardown of a
member sitting in backoff cannot return until that backoff elapses.

> **Derived from the code, not measured.** No test in the tree covers `stop()`
> during a backoff. Task 9 measures it; if the measurement disagrees, this
> paragraph is wrong and changes.

**What awaiting an ending does and does not promise.** `abort()` + `ended()`
resolves when the task is gone, via the dropped-sender path (`local.rs:3265`). It
does **not** promise the member's observers released what they held: `stop`'s own
doc states the limit at `local.rs:3282-3284` — *"Waits for the receiver to be
dropped, which is the loop ending — not for the state's own `Drop` to have run.
A state that flushes in `Drop` needs its own signal."* A durable adapter's file
claim can still be open when a group ends.

### 4.7 The conductor

One task, spawned by `Group::new`, holding a `Weak` on the shared state —
`names.rs:151-153`'s argument: a group nobody holds is dropped even with members
still running, and the task then wakes once, finds nothing, and ends. It settles
`GroupEnded::Dropped` on the way out, so a caller holding the `'static` `ended()`
future is answered rather than left waiting. That mirrors `local.rs:3265`.

It waits on two inputs with **no `select!`** — `tokio`'s `macros` feature is off
for `wasm32-unknown-unknown`, so a `select!` would not compile for a browser
(`local.rs:1250-1253`). The house pattern is a hand-written `poll_fn`
(`local.rs:1586`).

The two inputs are an `n0_future::FuturesUnordered` of stamped member endings and
a `tokio::sync::Notify` that `add`, `remove` and `stop` fire.

**The `Notify` discipline, because the cited `poll_fn` argument does not
transfer.** `local.rs:1583-1585` can claim "it cannot lose a wakeup" because *"a
poll that returns `Pending` has taken nothing"*. A `Notified` that returns `Ready`
**has taken the permit**. So the loop is written:

```text
loop {
    drain staged members into the FuturesUnordered   // always first
    let notified = notify.notified();                // fresh each iteration
    poll_fn: endings, then notified
}
```

A permit stored by `notify_one` while the conductor is inside the policy is
consumed by the *next* iteration's fresh `Notified`, and every iteration drains
before it waits. **The invariant is: one wake means drain to empty.** `Notify`
coalesces — N adds can store one permit — so anything weaker loses members
silently. A test asserts it with N concurrent adds while the conductor is busy.

Members live in a `Vec`, so `names()` is insertion-ordered and teardown has a
direction. kameo's `RestForOne` filters `*child_id > &id` over a `HashMap`
(`kind.rs:294`), which stores its dependency order in a global id counter; this
keeps order where it belongs.

`ended()` is a `watch`, read with `wait_for` and not `changed()`, for
`local.rs:3255-3256`'s reason: *"a fresh receiver marks the current value as
seen, so `changed()` would wait forever on a second call."* `GroupEnded` derives
`Clone` so two awaiters both get one.

### 4.8 Panics, and the caveat the tree already carries

The policy runs on the conductor's task. It is wrapped in
`catch_unwind(AssertUnwindSafe(..))` — the shape `local.rs:3226` uses — and a
panic ends the group as `GroupEnded::PolicyPanicked`. Letting the conductor die
instead would leave every member running and unwatched, which is issue #9's
problem 1 arrived at from the other side. The group ends on the first panic, so
the policy is never called again.

> **`panic = "abort"` defeats this**, as it defeats every `catch_unwind` in the
> crate. Stated on the method, because the tree already states it twice
> (`local.rs:174`, `panic.rs:116`) and a third silent copy would be the drift
> those two exist to prevent.

### 4.9 Backpressure, which this module requires every pattern to state

| what a slow member costs | how it is bounded |
|---|---|
| A member slow to die delays the group's **next decision** | One decision at a time, in one task. Endings are futures, not queue entries: N members means at most N pending endings, and nothing is allocated per death |
| Pending `add`s while the conductor is deciding | Bounded by the number of `add` calls in flight, and drained to empty on the next wake (§4.7). This is a staging list, not a queue that grows against a slow consumer |

### 4.10 Lifecycle rules, stated because they surprised a reviewer

- **The group holds a handle clone**, inside `retire`. `membership!` clones, so
  the caller keeps its own and a dropped `Group` leaves members running. A caller
  that hands over its only handle gets the opposite, and that is `Names`'
  documented surprise (`names.rs:38-52`) one type over.
- **An empty group waits.** `Group::new` starts empty, and `Forget` on the last
  member returns to empty. Neither ends the group: `add` may follow.
- **`add`, `remove` and drop are three different things** — stop watching one,
  end all, stop watching all — and none is `schedule.rs:79-95`'s deleted `cancel`,
  which went because `drop` already meant it.

---

## 5. Refused, so nobody re-derives them

| refused | why |
|---|---|
| `Verdict::RestartAll` (`one_for_all`) | RFC-07 §9.8: the dead-pid argument defeats sibling-*restart* strategies. Also: rebuilding every member from its own commit point is **not** a consistent cut, because members commit independently and nothing coordinates them |
| `Verdict::RestartAfter` (`rest_for_one`) | Same. Its ordering is a *startup dependency*, which a group would be inferring from insertion order |
| `Verdict::Restart` | §3.3. Three facts, and it deleted four defects to drop it |
| A `Group` that is an actor | It is what makes issue #9's problems 1 and 2 exist. kameo pays three mechanisms and a named regression test for it |
| A trait over handle types | Not needed: `Membership` carries both capabilities as values, and `membership!` builds it without a trait. `pool.rs:177-183` refused a trait for a case with a one-line call-site alternative, which is **not** this case — a group retires members from its own task, where the caller is not — so that refusal is not cited as support |
| Supervision across nodes | Issue #9's own table: a remote node cannot tell "the other died" from "I cannot reach the other". `LifecycleEvent` already crosses the wire (`console.rs:134`) for anyone building it above with that difference in view |
| Handing the policy the dead member's last state | `ractor` does this (`messages.rs:92`). keylink's answer is `Persist`, measured at 7.6 ms per turn (`local.rs:2907`). A second path to "restart from something" is a second way to build a thing |

**Nesting is possible and lossy, and that is stated rather than claimed as
free.** Revision 1 said a group can be a member of another group because *"its
`ended()` is a future"*. `Group::ended()` yields `GroupEnded` and
`Membership::new` needs `Ended`; there is no total mapping, and `Ended` has no
variant meaning "a group ended". The caller writes the lossy one it wants:

```rust
parent.add(id, Membership::new(
    async move { match child.ended().await {
        GroupEnded::Member { reason, .. } => reason,
        _ => Ended::Stopped,
    } },
    move || async move { child.stop() },
));
```

That is escalation, and it is the shape `RestartAll` would have been if it were
kept — which is the argument for refusing the *variant* while allowing the
*composition*: the variant would make it the default, and issue #9's own sketch
proves the hazard by putting `RestartAll` on the `Ended::Panicked` arm, the most
common ending there is.

---

## 6. Tests

House style: one claim per test, the name **is** the claim, a doc comment naming
the edit that reddens it, and an entry in `.mise/tasks/anchors` — without one it
is unchecked (`CONTRIBUTING.md:293-301`). `Ended` has no `PartialEq`
(`observe.rs:449`), so every assertion on one is `matches!`.

| the claim | the test |
|---|---|
| a member ending for good reaches the policy once, with its name and reason | `a_member_that_ends_for_good_reaches_the_policy_once` |
| a member's own restart never reaches the policy | `a_members_own_restart_never_reaches_the_policy` |
| a refusal the author marked fatal reaches the policy without a restart | `a_fatal_refusal_reaches_the_policy_without_a_restart` |
| `StopAll` ends every sibling and names the member that caused it | `stop_all_ends_the_group_and_names_the_member_that_caused_it` |
| every member is retired before any is awaited | `every_member_is_retired_before_any_is_awaited` |
| `Forget` removes the member and leaves the others serving | `forget_removes_the_member_and_leaves_the_others_serving` |
| a displaced member's ending does not reach the policy as its replacement | `a_displaced_members_ending_does_not_reach_the_policy_as_its_replacement` |
| one wake drains every pending add | `adds_made_while_the_policy_runs_are_all_watched` |
| a policy that panics ends the group instead of orphaning it | `a_policy_that_panics_ends_the_group_rather_than_orphaning_it` |
| a group nobody holds answers its waiters instead of hanging | `a_dropped_group_ends_as_dropped_and_leaves_its_members_running` |
| members are reported in the order they were added | `members_are_reported_in_the_order_they_were_added` |
| a member can be added from inside another member's turn | `a_member_can_be_added_from_inside_another_members_turn` |

**Three of these have no one-line break, and that is recorded rather than
faked.** `scripts/break-once.sh:20-24` substitutes one literal, so a claim whose
defect is a *restructure* cannot be reddened by it. The precedent is exact —
`rfc-07-the-turn-model.md:2280-2285` says of `two_clones_of_a_pool_share_one_rotation`:
*"A break isolating sharing would have to move `at` out of `Inner`, which is a
restructure rather than a literal, so `break-once.sh` cannot express it. Said
here rather than counted as a proven guard."*

| test | why no break | what carries the claim instead |
|---|---|---|
| `a_member_can_be_added_from_inside_another_members_turn` | making `add` async is a compile error in the test, not a red test | a `compile_fail` doctest, the device `lib.rs:283-289` already uses for "the guard is an absence". It is gated twice — `mise run test` runs doctests and `.mise/tasks/features` runs them again at the all-off end |
| `members_are_reported_in_the_order_they_were_added` | the break is `Vec` → `HashMap`: a field type, an insert and a read | recorded as a regression pin, not a guard |
| `a_dropped_group_ends_as_dropped_and_leaves_its_members_running` | `Weak` → `Arc` is one line (`names.rs:153`), but it reddens only the "stops watching" half | name which half the break reddens, as `tests/compose.rs:575-581` does for the pool |

Every other record must price its break — file, literal, replacement — **before**
implementation, so task 9 measures rather than predicts.

One test carries the backoff claim without a clock:
`a_member_asleep_in_backoff_is_retired_without_waiting_out_its_backoff`. It
asserts that teardown *completes*, which load can only strengthen — never an
upper bound on wall-clock time (`AGENTS.md:105-112`).

Clock discipline: no wall-clock assertion in a direction load can break
(`AGENTS.md:105-112`). "Slow" is constructed with `tests/compose.rs`'s `Gate`,
and settling uses that file's bounded `settles`/`spun`, never a sleep.

---

## 7. Blast radius

| file | change |
|---|---|
| `keylink/src/compose/group.rs` | new |
| **`keylink/src/compose/pool.rs:56-67`** | **the sentence that denies this feature**: *"`compose` holds no supervision mechanism of its own, because composition and supervision compose rather than one containing the other."* The narrow half survives — `Pool` still restarts nothing and a member still restarts itself — and the second half must point at `Group`. No gate step sees this: the prose is valid markdown, so `mise run docs` exits 0 |
| `keylink/src/compose/mod.rs` | `mod group;`, `pub use`, a pattern-table row, a backpressure-table row. **The count prose needs care, not a bump**: the module says "five patterns" and "four types and two functions for the first four", and its fifth is deliberately *not a type* (`mod.rs:16-19`, with a whole section at `mod.rs:75-92` resting on it). `Group` makes it six patterns and five types. Also `mod.rs:41-44` — *"There is no third option"* — which §4.9's first row is a third shape of, and `mod.rs:23`'s "all four" |
| `keylink/src/lib.rs:54-56` and `lib.rs:198-200` | the crate summary names four compositions; the `pub mod` comment says *"these four"* |
| `keylink/tests/compose.rs` | line 1 says *"The four compositions"*; plus a `// ───────── the group` section marker padded to the file's 79-column right-aligned form, and one module-doc claim→test table row per test |
| **`.mise/tasks/anchors:1277`** | **`RECORD_BLOCKS = 127`**, a hard constant the task checks against a walk of every `*.rs` under `keylink/` and `keylink-persist/` (`anchors:1286-1332`). New break records redden `anchors`, which runs in **`pre-commit`** as well as `gate` — so this fails before anything compiles. **Read the new value off a failing run; never compute it.** `anchors:1298-1310` refuses a computed value because an agreeing subtraction reads as a derivation and is a coincidence. Re-measure after every rebase. Add a transition entry in `anchors:1243-1276`'s format |
| `docs/rfc-07-the-turn-model.md` | append §9.11. It must answer **two** existing rows by name: `rfc-07:2270-2278` (*"**Supervise anything.** … `compose` still holds no supervision mechanism of its own, and that is the design rather than an omission"*) and `rfc-07:2060-2062` (*"four types and two functions"*). **Append, never edit** (`CONTRIBUTING.md:322`) — both stay in the file and the append says they are now false |
| `docs/rfc-04-actors.md` | one appended line in §8 pointing "no links" (`rfc-04:1013`) at §9.8, and one against the refused-designs row at `rfc-04:57` (*"Supervision, restart, actor trees \| large \| **Reject.** Section 8"*), in the past tense that file already uses for its own overturned claims |

**Intra-doc links, because `mise run docs` is `-D warnings` and this is where it
bites.** `[`Ended`]`, `[`ActorId`]` and `[`Intensity`]` resolve from `group.rs`
**only if the file imports them**, as `names.rs:90` does. `[`Handle`]`,
`Handle::ended` and `Handle::abort` do **not** resolve at all: `Handle` is
generated per service, and `compose/mod.rs:94-97` makes those names work with a
link-reference block scoped to *that* doc comment, which does not reach a child
module. Write `crate::LocalActor::ended` explicitly, or repeat the block.
**CAUTION: do not add an explicit path where the bare link already resolves** —
`rustdoc::redundant_explicit_links` is denied, and 42 instances over 24 lines
were cleared one commit before `--document-private-items` went on;
`[tasks.docs]` records that transition.

**Nothing in `local.rs`, `macros.rs` or the supervision loop changes.** Revision 1
could not honour this and did not notice; dropping `Verdict::Restart` (§3.3)
restores it. No wire type, no ALPN, no `service!` output (`compose/mod.rs:4-7`),
so `the_contract_hash_has_not_moved` stays green untouched. `compose` has no
feature gates and must keep compiling under `--no-default-features` and for
`wasm32-unknown-unknown`.

*Citation fix from revision 1: the keyed mailbox depth is at
`keylink/src/actor/registry.rs:82`. There is no `keylink/src/registry.rs`.*

---

## 8. Revision 3 — four things implementation found that review did not

Recorded rather than edited away, because the value of the record is that it
shows what was believed at the time.

1. **`add` and `remove` cannot return a `Membership`.** An ending future has one
   owner, and `add` moves it into the conductor's `FuturesUnordered`. They return
   `Option<Retire>` instead — a separate type whose only method is
   `async fn retire(self)`. It is a distinct type on purpose: a `Membership` can
   be added and a `Retire` cannot, so they are not one type with a field that is
   sometimes absent. The first fix attempted was `Membership { ended: Option<..> }`,
   which makes a value that *looks* re-addable and, if re-added, files a member
   the policy can never be asked about. That is the unenforced promise this repo
   refuses, so it was replaced rather than documented.
2. **§4.7's "wakes once, finds nothing, and ends" had no wake source.** That
   sentence is `names.rs:151-153`'s, and there the wake comes from the actor's
   own ending. A group with two healthy members and no holder has none, so the
   conductor would have waited forever instead of publishing `Dropped`. The
   shared state's `Drop` fires the `Notify`, and the conductor holds the
   `Arc<Notify>` beside the `Weak`, dropping the upgraded `Arc` before every
   await.
3. **§4.6's "await them all together, in the `FuturesUnordered` the conductor
   holds" is not type-possible.** Endings and retirements have different item
   types. Teardown uses a second set. One set would need an enum wrapper on every
   ending, permanently, for a path that runs once.
4. **`every_member_is_retired_before_any_is_awaited` names an order
   `FuturesUnordered` does not promise.** The provable claim is that every retire
   future is *built* before any is polled and none waits on another to complete.
   The test is renamed `no_members_retirement_waits_on_another`.

`Group` derives `Clone` (a `GroupEnded::Dropped` meaning "every `Group` was
dropped" needs clones to mean anything, and `Names` is `Clone`); `Group` holds
the `watch::Receiver` while the conductor holds the `Sender`, because `Dropped`
must be published *after* the shared state is gone; and a replacement keeps the
displaced member's position in the `Vec`, because that position is its place in
the teardown order.

**`GroupEnded` has a fifth variant, `EndingPanicked`, and it closes the last
uncaught path.** §4.8 guarded the policy and nothing else, and quality review
found three more pieces of caller code running on the conductor's task: the
member's ending future, the retire closure, and the retire future. The failure
was proved rather than argued — with the catch removed, two panicking members
filed between two healthy ones orphan every member and answer `GroupEnded::Dropped`
to a caller **still holding a live `Group`**. A wrong answer, not a missing one.

The retire closure and its future are now caught with `panic.rs`'s existing
`catching` and `guarded`, so a panic there costs one member. The ending future
was the hard one: no `Ended` variant means *"the future that was going to say why
panicked instead"*, so reporting `Vanished` or `Panicked` would be a false
reason. The first answer was to document it as uncatchable. That was overruled —
documenting a silent group-wide orphan does not make it acceptable — and the
honest fix is a variant rather than a lie. `GroupEnded` is `#[non_exhaustive]`
and the crate is unpublished, which `observe.rs:430-431` prices as free.

**`add` on a group that has already ended files nothing and hands the membership
straight back** as a `Retire`. The spec was silent, and the alternative is a
leak: a member filed into a group whose conductor has exited is watched by
nothing, decided on by nothing, and retired by nothing — which is the failure
this whole feature exists to prevent, reached from the other side. The caller
gets the means to end it and a `names()` that stays empty.

---

## 9. What is still open

1. **`membership!` beside `Membership::new` is two doors.** `CONTRIBUTING.md:215`
   says one way to build a thing. The argument for both: the macro expands to the
   constructor rather than doing less, and the constructor is the only way to
   build a `Membership` for a hand-written handle. If review disagrees, the macro
   goes and the pairing misuse is documented instead.
2. **Whether `Verdict` should be two variants at all**, or whether fate sharing
   is better expressed as a `bool`-returning closure. Two named variants read
   better at the call site and leave room; that is a judgement, not a fact.
3. **Whether `StopAll` should retire in reverse insertion order or unordered.**
   RFC-07 §9.8 names "ordered shutdown" as wanted, which is the argument for
   reverse order; but every retire starts concurrently (§4.6), so the order is a
   statement about *issue* order only, and its observable effect is small.
