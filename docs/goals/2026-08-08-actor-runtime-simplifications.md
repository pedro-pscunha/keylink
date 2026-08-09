# Goal: four actor-runtime simplifications — review each, then execute the survivors

## Intent

The actor runtime is one runtime behind three axes: dispatch, identity and reach.
Four asymmetries remain across those axes. Two are missing capability. Two are
shape. This goal removes all four, in one branch, in one pull request.

The outcome a reader can observe:

- A keyed actor can be called from the same process, with no node and no QUIC
  connection.
- A caller can await the ending of a keyed actor, in the same vocabulary the
  local path already uses.
- A service generates fewer construction doors, and no compile-time guard is
  lost in the reduction.
- `local.rs` no longer holds the keyed loop under a name that says "local".

## Context

Repository `~/repos/keylink`, `main@0ab8366`, working tree clean except for one
untracked file, `docs/goals/2026-08-06-actor-runtime.md`.

**AMENDED 2026-08-09. That baseline is already stale, and the amendment is not
cosmetic — one of the four items is now partly built.** The branch
`compose-group` sits four commits ahead of `main` and is unmerged:
`573568b`, `7a48207`, `1db4965`, `0bce4be`. It adds `compose::Group` (issue #9),
and along the way it added `compose::Endings` and made `Lifecycle` chainable —
which is **a second answer to item B**. It also touches `registry.rs`, which item
B and item A both plan to touch.

This goal's own rule applies to this goal: *"Verify these five before you plan.
If any is false, the tree moved."* It has moved. **Land `compose-group` first,
re-read every citation here against the merged `main`, and treat the section
"The branch already in flight" below as binding on Wave 0.**

Measured in the same turn as this line: `keylink/src/local.rs` is 5,108 lines,
`keylink/src/macros.rs` is 3,929 lines, `keylink/src/actor/registry.rs` is 3,281
lines.

**Verify these five before you plan. If any is false, the tree moved. Re-verify
every citation in this goal before you write code.**

1. `keylink/src/actor/{mod,registry,remote}.rs` exist.
2. `registry.rs:1336` declares `pub struct System<M>`, with no `= Mail` default.
3. `remote.rs:425` declares `pub async fn deliver_to_actor<S: KeyedService>`.
4. `macros.rs:671` declares `struct SpawnableUnfed<A>`, and `:674` is its `CHECK`.
5. `local.rs:1737` is `run_loop`, `:1881` is `run_keyed`, `:2690` is
   `run_keyed_supervised`.

### The four items

**A — a keyed actor has no in-process call path.**
`System<M>` (`registry.rs:1336`) exposes `mount`, `shutdown`, `abort`,
`contains`, `len`, `is_empty`, `status` and `evict_idle`. It has no send. The
only ingress is `deliver_to_actor` (`remote.rs:425`). That function is
`#[doc(hidden)]` and it takes a `Context`. `Context` (`middleware.rs:221`) has no
public constructor. So a caller in the same process must dial its own node over
QUIC, and no test can call a keyed actor directly.

**B — `ended()` exists on one path only.**
A local caller awaits `Ended` through `LocalActor::ended` (`local.rs:3252`). A
keyed actor reports the same event through `Lifecycle::ended`
(`registry.rs:1270`), a system-wide callback. That is two vocabularies for one
event. `registry.rs:1519` and `local.rs:1877` and `local.rs:2634` each record the
gap in prose. RFC-07 §5.7 files it and names the cause: it needs a field on
`Mounted`.

**C — seven construction doors per service, and four of them differ by one
argument pair.**
Local: `actor` (`macros.rs:1580`), `actor_with` (`:1608`), `actor_wired`
(`:1645`), `supervised` (`:1693`). Keyed: `spawn` (`:1368`), `spawn_wired`
(`:1441`), `spawn_supervised` (`:1538`). `actor_with` and `actor_wired` differ
from `actor` only by which of the pair `(observers, feeds)` they take.
`spawn_wired` differs from `spawn` the same way. The repository already made this
argument once: `macros.rs:1680` records that `supervised` collapsed three doors
into one, "because a factory names its own triple and there is nothing left for a
shorthand to leave out".

**D — `local.rs` is 5,108 lines and its name is wrong.**
It owns the keyed loop, `run_keyed` (`:1881`) and `run_keyed_supervised`
(`:2690`). It also owns `Reply`, `Replied`, `Deferred`, `Fed`, `Feeds`,
`Intensity`, `Incarnation`, `LocalActor`, the panic policy and
`deliver_to_local`.

### The branch already in flight, and the three items it lands on

`compose-group` is unmerged as this is written. It exists to serve issue #9 —
`compose::Group`, fate sharing for a set of actors — and it collided with this
goal in three places without either document knowing about the other. Wave 0 must
rule on the first. The other two are work this goal absorbs.

**B is already answered once, by a different mechanism, and the crate must not
keep both.** `compose::Endings` is a `Lifecycle` that turns keyed endings into
futures: `endings.watch(&id)` hands back an `impl Future<Output = Ended>`. It
needs no runtime change, because the signal already existed — `Registry::reap`
reports a mounted actor's own ending to every installed `Lifecycle`, carrying the
true `Ended` including a supervised actor's `GaveUp`. `Lifecycle` was made
chainable in the same commit, because a `System` held exactly one and installing a
second sink silently displaced the first.

Shipping `System::ended(&id)` beside it gives the crate two ways to await one
event, which `CONTRIBUTING.md:215` refuses in as many words. **Wave 0 decides
which survives, and its verdict on B is therefore one of three:**

- **PROCEED** — build `System::ended` and *delete* `compose::Endings`, migrating
  its call sites. Justify the deletion of shipped, tested code.
- **PROCEED, AMENDED** — build `System::ended` and *rebuild* `Endings` on top of
  it, so the `Lifecycle` fan-out stays for callers who want a callback and the
  await has one implementation.
- **REFUSED** — `Endings` already closes B, and a field on `Mounted` buys too
  little for its cost. Record RFC-07 §5.7 as closed by the compose route.

The argument each way, so neither is re-derived. `Endings` has a **miss-window**:
`watch` must be filed before the ending arrives, and for a lazily-spawned actor
there is no caller-side moment to be "before". `System::ended` returning `Option`
makes that same miss **explicit** rather than a future that never resolves, which
is the stronger shape. Against that, a field on `Mounted` costs an allocation per
mounted actor, and `local.rs` deleted exactly such a channel once for having no
reader — so the cost is known and was already judged once.

**C must sweep the compose module, which this goal's scope did not name.**
`supervised` and `spawn_supervised` are used in `compose/group.rs`,
`compose/keyed.rs`, `compose/pool.rs`, `compose/topic.rs` and
`keylink/tests/compose.rs` — in rustdoc examples, in the `membership!` macro's
own documentation, and in tests. Deleting the doors without those sites is a
broken doc build, not a compile error, so `mise run docs` is the check that
catches it.

**D moves a file that a break record points into.** `compose-group`'s record for
`a_members_own_restart_never_reaches_the_policy` anchors on `local.rs`'s
`if !restarting {`. D's split moves that line to another file, which rots the
record's path. `mise run anchors` will say so; follow it rather than deleting the
row, exactly as `CONTRIBUTING.md:302` instructs.

### Three traps a previous parity proposal hit

Read these before you design A. A local-remote parity proposal failed here three
times, and each time on one of them.

1. **`deliver_to_local` (`local.rs:3662`) is a SERVED path, not the local-caller
   path.** It takes a `Context` and builds an `Origin` for a peer. It is the
   serving side of a singleton, and it is the twin of `deliver_to_actor`. The
   name says "local" and means "not keyed". The real local-caller path is
   `LocalActor::ask`, which builds `Origin::local()`. Citing `deliver_to_local`
   against `deliver_to_actor` proves that two SERVED paths converged. It proves
   nothing about local against served.
2. **`Caller` already exists.** `pub enum Caller<'a>` is at `middleware.rs:389`,
   `Origin::caller` is at `remote.rs:285`. Do not propose it.
3. **Local against served lives in two seams, not one.** A `Middleware` gets a
   `Context` by value. An actor handler gets a `Caller<'_>`. A non-actor
   `Handler` gets a `&Context`. `Origin` is narrower than `Context`, not wider.

### The crate-split bill, and a drift in it

`actor/mod.rs:92` calls RFC-04 §16.8 "the list of record" for six items that
block the crate split. **Two of those six are stale.** Items 1 and 2 describe
`M = Mail` type-parameter defaults. `registry.rs:213`, `:352`, `:1336` and
`:1751` declare `Mailbox<M>`, `Mounted<M>`, `System<M>` and `SystemBuilder<M>`
with no default at all, and `registry.rs:17` already records both as gone. The
real bill today is four items, not six.

A and B each add an item to that bill. Say so, in the RFC and in both module
headers, in the same commit.

## Skills — load the matching one before working in its area

**Every subagent runs on opus. Never fable, under any circumstance.**

**Review gates, always:**
- `brooks-review` — the plan gate and the merge gate.
- `receiving-code-review` — apply every finding. A reviewer never fixes its own.
- `requesting-code-review` — before the pull request.
- `brooks-audit` — D is a module-structure change, so this is not optional.
- `brooks-test` — every new guard needs a red test, and this skill judges them.

**Domain and language:**
- `rust-best-practices` — every item.
- `rust-illegal-states` — A's error type and C's builder. Both add new types.
- `rust-performance` — A and D. `tests/actor_memory.rs` asserts a slope, and both
  items can move it.
- `codebase-design` — C and D. Use the deep-module lens on the door count and on
  the file split.
- `ponytail` — before each review. Four items is the shape that grows a second
  system.

**Process:**
- `subagent-driven-development` — the execution loop.
- `systematic-debugging` — any gate failure. Fix the cause, never the symptom.
- `writing-plans` — one plan per item.

**Caveats. Read this list before you trust a skill name.**
- `improve-codebase-architecture` is named by `docs/goals/2026-08-06-actor-runtime.md`
  and by the `write-goal` template. It exists at
  `~/.agents/skills/improve-codebase-architecture`. It did NOT appear in this
  session's available-skills listing. If your listing does not carry it, use
  `codebase-design` and say which one you used.
- The `write-goal` template's Invariant Library is written for Python and for the
  `arche` repositories. Nothing in it applies here. This goal's own Invariants
  section replaces it in full.

## Scope

**In scope**
- Items A, B, C and D, in the order D → C → B → A.
- The documentation each item invalidates: `README.md`, `docs/rfc-04-actors.md`,
  `docs/rfc-07-the-turn-model.md`, and every module header that states a fact one
  of the four items changes.
- A new RFC section, or a new RFC, recording what each item decided and what each
  refused.
- **The `compose` module, which the original scope did not name.** C deletes doors
  that `compose/{group,keyed,pool,topic}.rs` and `keylink/tests/compose.rs` use in
  rustdoc examples, in the `membership!` macro's documentation and in tests. B
  must rule on `compose::Endings`. Both are in scope for the items that touch
  them, and neither is an unrequested refactor.

**Out of scope**
- Moving either ALPN. Both must be byte-identical before and after.
- Splitting `keylink` into two crates. Only the bill changes, never the split.
- A second QUIC connection model, a per-actor ALPN, or a reorder buffer. All
  three are refused designs. Do not re-derive them.
- Bumping `iroh` or `rust-version`.
- Removing the dead `Mail` variants for `send` and `recv`. Documented at
  `macros.rs:850`, and it needs a token muncher.
- Enabling GitHub Actions. CI is off by decision, and two things hold it off.

**Blast radius**
- May modify: `keylink/**`, `keylink-persist/**`, `demo/**`, `docs/**`,
  `README.md`, `CONTRIBUTING.md`, `mise.toml`, `.mise/tasks/**`.
- Must not touch: `Cargo.lock` beyond what a build writes, `rust-toolchain.toml`,
  `deny.toml`, `.githooks/**`, `scripts/break-once.sh`.

## The order, and why it is not negotiable

```
D  split local.rs            ← a pure move. First, so C, B and A land in the
                               new files instead of moving twice.
C  collapse the doors        ← before A, so A adds one door to a reduced set.
B  keyed ended()             ← adds a field to `Mounted`. A needs the same seam.
A  keyed local call path     ← last. It consumes B's field and C's builder.
```

D produces no behavior change, so it commits first and the three feature diffs
stay readable. If the review wave refuses D, run C, B and A in that order and
report the refusal.

## Public interface

### A — the local keyed door, symmetric with the client door

Generated per service, beside the existing `at(&KeyedClient, ActorId)`:

```rust
/// Address one actor of this service in this process.
pub fn at_local(system: &System, actor: ActorId) -> LocalRef<'_>;

pub struct LocalRef<'a> { /* borrows the system */ }

impl LocalRef<'_> {
    pub fn id(&self) -> &ActorId;
    // One method per declared `rpc` and `tell`, named as `Handle`'s are.
    // pub async fn get_item(&self, id: u64) -> Result<Option<Item>, ???>;
}
```

Three constraints bind the design, and the executor decides the rest.

1. **It routes exactly as the served path routes.** It goes through the same
   registry route the served path uses, so it triggers a `spawn_with` factory, it
   respects capacity, it respects the queue budget and it updates the eviction
   clock. A door that bypasses routing is a second delivery path, which is the
   defect this item exists to remove.
2. **It pushes the per-method `Mail` variants, never `Mail::Served`.** Those
   variants and their typed `Reply` already exist (`macros.rs:863`). The caller
   is local, so the `Origin` is `Origin::local()`, exactly as `LocalActor::ask`
   builds one.
3. **The error type is neither `crate::Error` nor `RemoteError`.** RFC-04 §5.2
   and §5.5 refuse a handle that is the same type locally and remotely, because
   such a handle either claims failures the local case cannot have or hides
   failures the remote case does. `Error::SchemaMismatch` has no meaning between
   two objects in one process. A local keyed call can still report four things a
   `MailboxClosed` cannot say: no such actor, registry full, queue budget
   expired, mailbox closed. Choose a shape, justify it against
   `rust-illegal-states`, and write the justification where a reader meets the
   type.

### B — the keyed ending

```rust
impl<M> System<M> {
    /// Await the reason this actor ended.
    pub fn ended(&self, id: &ActorId) -> Option<impl Future<Output = Ended> + Send + 'static>;
}
```

The signature above is a starting point, not a decision. Two properties are
decisions:

- It reports the same `Ended` the local path reports. Do not add a second
  lifecycle vocabulary.
- It coexists with `Lifecycle::ended` and does not replace it. One is a per-caller
  await, the other is a system-wide callback. State in the doc which question each
  answers.

### C — one builder per path

Target shape. `actor` and `spawn` keep their current meaning and their current
return type, because they are the common case and breaking them buys nothing:

```rust
pub fn actor<A: Actor>(state: A) -> Handle<A>;      // unchanged
pub fn spawn<A: Actor>(state: A) -> Mounted;        // unchanged

pub fn local<A: Actor>(state: A) -> Local<A>;       // the builder
impl<A: Actor> Local<A> {
    pub fn observe(self, observer: Box<dyn Observer<A>>) -> Self;
    pub fn feeds(self, feeds: Feeds) -> Self;
    pub fn supervised<N, Fut, F>(self, factory: N, limit: Intensity) -> Self;
    pub fn start(self) -> Handle<A>;
}

pub fn keyed<A: Actor>(state: A) -> Keyed<A>;       // the builder
impl<A: Actor> Keyed<A> { /* same four, ending in `build(self) -> Mounted` */ }
```

Deleted outright, with no shim and no `#[deprecated]`: `actor_with`,
`actor_wired`, `spawn_wired`, `supervised`, `spawn_supervised`. Seven doors
become four. Update every call site in `keylink/**`, `keylink-persist/**`,
`demo/**`, `README.md` and the RFCs in the same commit.

**CAUTION: `SpawnableUnfed::<A>::CHECK` is a `const` assertion evaluated at the
call site.** `macros.rs:671` declares it, and `:1384`, `:1581` and `:1612`
instantiate it. It refuses a service that declares a `feed` at a door that cannot
wire one, and a separate `CHECK` at `:645` refuses a service that declares
`send`. A builder moves the instantiation. If the refusal ends up somewhere a fed
service can reach, C has silently deleted a compile-time guard and replaced it
with nothing. Both refusals must still fail to compile after C, and each needs a
`compile_fail` doctest that was watched failing.

### D — the file split

No public name changes. No behavior changes. `local.rs` becomes `runtime/`, or
`runtime.rs` plus siblings, holding at least:

- the turn: `Turn`, `Reply`, `Replied`, `Deferred`, `turn`, `turn_deferred`,
  `dispatch`;
- the feeds: `Fed`, `Feeds`, `feed_turn`;
- supervision: `Intensity`, `Incarnation`, `supervise`, `run_keyed_supervised`;
- the loop and the inboxes: `run_loop`, `Inbox`, `Keyed`, `Watched`, `Lent`;
- the local handle: `LocalActor`, `MailboxClosed`, `deliver_to_local`.

`run_loop` stays one function. The module header's argument that the loop must be
singular survives a file split, so carry that argument to whichever file holds it
and do not weaken it.

## Success criteria

Each is binary and each names its proof. Print the proof.

**A**
- [ ] A test calls a keyed actor in-process with no `Node` and no `KeyedClient`,
      and asserts the reply — new test in `keylink/tests/actors.rs`, named in the
      final report.
- [ ] A test proves the local door triggers a `spawn_with` factory for an
      unmounted id, exactly as the served path does.
- [ ] A test proves the local door and the served path reach the same actor
      instance, by mounting once and calling both ways.
- [ ] `keylink::Error` and `keylink::RemoteError` do not appear in the local
      door's signatures — proved by `rg` over the generated door, output printed.
- [ ] Both ALPNs are unchanged. Print the two pinned hashes from
      `keylink/tests/protocol.rs` before and after.

**B**
- [ ] A test awaits the ending of a keyed actor and receives the same `Ended`
      variant the local path gives for the same cause.
- [ ] `Lifecycle::ended` still fires for the same event — asserted in one test
      that observes both.
- [ ] **Exactly one way to await a keyed ending exists on the final HEAD.** Print
      Wave 0's verdict on `compose::Endings`, and the `rg` proving whichever it
      refused is gone. If both survive, B has broken
      `CONTRIBUTING.md:215` and is not done.
- [ ] The miss-window is stated for whichever survives: `Endings` needs `watch`
      filed before the ending arrives, and `System::ended` returns `Option`. Both
      can miss; they differ in whether the miss is silent.

**C**
- [ ] `actor_with`, `actor_wired`, `spawn_wired`, `supervised` and
      `spawn_supervised` are gone from `macros.rs` — `rg` output printed, zero
      hits.
- [ ] A service declaring a `feed` still fails to compile at the shorthand door —
      `compile_fail` doctest, watched failing before it was added.
- [ ] A service declaring `send` still fails to compile at the actor doors — the
      existing `compile_fail` doctests stay red for the right reason.
- [ ] `keylink/tests/api.rs` is updated and green.
- [ ] The door count is stated in the final report: seven before, four after.
- [ ] **`mise run docs` is green**, which is the check that catches C's misses in
      `compose`: a deleted door named only in a rustdoc example is a broken doc
      build and not a compile error, so a green `cargo check` proves nothing here.

**D**
- [ ] `git diff --stat` for D's commit shows moves and no behavior change, and
      the commit message says so.
- [ ] `rg 'Turn::run|\.run\(' keylink/src` still returns exactly one
      turn-dispatch site. Print it.
- [ ] No file in the split exceeds 2,500 lines. Print `wc -l` for each.

**All four**
- [ ] `mise run gate` green. Print the tail of its output, and print the exit
      code line.
- [ ] `tests/actor_memory.rs` green, and the measured slope reported as a number
      in the final report and in the commit message.
- [ ] Every new guard was watched failing, and the break is recorded where
      `mise run anchors` checks it.
- [ ] `mise run docs` green, with no broken intra-doc link.

## Invariants

### Universal for this repository

- **No `unwrap` and no `expect` outside a test module.** The workspace denies
  both. A test module opts out at the top of the file, and nowhere else.
- **`#![warn(missing_docs)]`.** Every new public item carries documentation.
- **`#![deny(unsafe_code)]`.** There is exactly one lifted site in the crate. Do
  not add a second.
- **`n0_future::time` only.** `std::time::Instant::now()` panics on wasm, and
  that shipped once.
- **A guard you have not watched fail is not a guard.** Break it, see the red,
  restore it, and record the break where `mise run anchors` can check it.
  `CONTRIBUTING.md:294` states how the record must be written.
- **Do not assert on wall-clock time in a direction load can break.** A lower
  bound is fine. An upper bound reddens on a loaded machine. Put the
  clock-dependent half in a unit test with `tokio::time::pause`.
- **Cite the guard, the constant or the edit. Never the literal.** A record that
  embeds a value it does not control rots.
- **No legacy path and no compatibility shim.** Change every call site and delete
  the old one.
- **State a SHA, a count or a literal only after reading it in the same turn.**
- **The gate is the only gate.** Never kill a running gate. Never pass
  `--no-verify`, except for the two reasons `CONTRIBUTING.md` lists.
- **Report by `SendMessage(to: "main")`.** A subagent's final text reaches
  nobody, and a file on disk is not a report.

### Decided for this goal

- **The registry seam direction holds.** `remote.rs` may name registry types.
  `registry.rs` must not name transport types. The check is
  `rg 'super::|crate::' keylink/src/actor/registry.rs`, and it must not gain a
  transport name. Run it before and after, and print both.
- **Every new seam item goes on the bill.** A and B each add one. Record each in
  RFC-04 §16.8, in `actor/mod.rs`'s header and in `registry.rs`'s header, in the
  same commit. Correct §16.8's two stale items while you are there.
- **Nothing sized by a service's declarations is spelled out inside the loop's
  future.** RFC-07 §5.5 records that this rule cost three attempts and 1,987
  bytes per idle actor. `tests/actor_memory.rs` asserts the slope.
- **Neither ALPN moves.** A local door touches no wire type, and a builder
  touches no wire type. If either hash moves, the change is wrong.
- **One loop, one dispatch site.** D must not produce a second.
- **No new marker trait and no new enum without the `rust-illegal-states` lens.**
- **Test strategy: in-process first.** A test that can run with no `Node` must
  run with no `Node`. A transport in the way is a second explanation for a pass.

## Execution interaction policy

**Questions are not allowed.** Annotate every assumption and every open question
inline as you go, and surface all of them in the final report. Never block.

If you are genuinely blocked with no valid path, stop and report the paths you
attempted, the evidence, the blocker and the open questions. Do not ask.

## Process

**Land `compose-group` before this branch is cut.** It is four commits ahead of
`main`, it touches `registry.rs` — which A and B both plan to touch — and it
carries the second answer to B that Wave 0 must rule on. Cutting
`actor-simplifications` from `main` while `compose-group` is unmerged produces
two branches editing one file toward two designs for one event, and the merge
that follows is where the decision would get made by whoever rebases last rather
than by a review.

Worktree: `wt switch -c actor-simplifications -y`, from the merged `main`. Fetch
and fast-forward `main` first — `wt switch -c` branches from the local default
branch, and building on a stale base has happened here.

One atomic commit per item. One pull request to `main`.

**All subagents run on opus. Never fable.**

### Wave 0 — the review gate, and it can refuse

Before any code, one adversarial review of all four items against the current
tree. It has authority to refuse an item.

For each of A, B, C and D it returns one of three verdicts:

- **PROCEED** — the citations hold and the design is sound.
- **PROCEED, AMENDED** — the item is right and the shape in this goal is wrong.
  It names the correct shape and the reason.
- **REFUSED** — the item must not be built. It names the reason, and it names the
  refused design so a later author does not re-derive it.

A refused item is not executed. It is written into the pull request body and into
an RFC section as a refused design with its reasoning.

Run Wave 0 as parallel reviewers with distinct lenses, one pass. Re-review only on
a Critical or structural finding.

### Waves 1 to 4 — one per surviving item, in the order D → C → B → A

For each item:

1. **Explore** — an `Explore` subagent re-verifies this goal's citations for that
   item against the tree. Citations drift.
2. **Plan** — one plan, through `writing-plans`.
3. **Review the plan** — one adversarial pass, through `brooks-review`.
4. **Execute** — through `subagent-driven-development`.
5. **Review the execution** — `brooks-review` plus `brooks-audit` for D, plus
   `brooks-test` for every new guard.
6. **Apply** — through `receiving-code-review`. A reviewer never fixes its own
   findings.

Loop 5 and 6 until no Critical and no Major remains. Report the Critical count
and the Major count for every wave.

### The invariant-compliance wave

After the last item, one subagent whose only job is to grade the whole diff
against this goal's Invariants section, item by item, with machine output for
each. An invariant it cannot evidence is violated. Any miss is Critical and it
re-opens the loop.

### The merge gate

`brooks-review` plus `requesting-code-review` on the full diff, before the pull
request.

## Definition of done

- [ ] Every success criterion proven, with the machine output attached.
- [ ] Every refused item recorded, with its reasoning, in the pull request body
      and in an RFC.
- [ ] The diff stays inside the blast radius. No unrequested refactor.
- [ ] `mise run gate` green on the final HEAD.
- [ ] Review waves clean. No open Critical and no open Major.
- [ ] The invariant-compliance wave clean, with a per-invariant verdict.
- [ ] RFC-04 §16.8, `actor/mod.rs`'s header and `registry.rs`'s header agree on
      the crate-split bill, and the two stale items are corrected.
- [ ] A new RFC section records what each item decided and what each refused.
- [ ] **Final pass — re-read this goal end to end and confirm each item against
      real evidence, not memory.**

## Deliverable

An isolated worktree, and one pull request to `main`. The body carries the
per-item verdicts from Wave 0, the door count before and after, the measured
memory slope, and every open question the run accumulated.

## /goal completion condition

See the fenced block at the end of this document's hand-off. Reproduced here:

```text
Satisfy every success criterion and every Definition-of-done item in
docs/goals/2026-08-08-actor-runtime-simplifications.md. Run Wave 0 first: it may
REFUSE an item, and a refused item is recorded rather than built. Work in a
worktree via `wt switch -c actor-simplifications -y`. All subagents on opus,
never fable. AS THE FINAL STEP, print the full proof state in ONE block: `mise
run gate` output and its exit code, `wc -l` for every file split out of
local.rs, `rg 'Turn::run|\.run\(' keylink/src` (exactly one site), `rg
'actor_with|actor_wired|spawn_wired' keylink/src` (zero hits), the two pinned
ALPN hashes from tests/protocol.rs before and after, the measured
actor_memory.rs slope, the door count before and after, the per-invariant
verdict from the invariant-compliance wave, and the Wave 0 verdict for each of
A, B, C and D. Stay inside the blast radius in that file. Stop when all pass or
after 60 turns. Questions are not allowed: annotate assumptions inline and
surface them in the final report; if blocked with no valid path, stop and report
the attempted paths, evidence, blocker and open questions — do not ask mid-run.
```
