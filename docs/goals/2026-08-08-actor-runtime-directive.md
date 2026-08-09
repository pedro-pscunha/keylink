# Three actor-runtime simplifications, plus one rename

Operative directive, 2026-08-08. It supersedes
`2026-08-08-actor-runtime-simplifications.md` wherever the two disagree. That
file is still the reference for item A's public-interface sketch and for the
per-item exploration notes. This file is the one that decides.

## Intent

`keylink/src/local.rs` is named `local` yet exports two keyed entry points.
Keyed actors reach the registry through a different door than local ones. Seven
builder doors exist where four would do. Correct all four, in order, without
moving either ALPN hash.

## Merged, as of PR #29

`compose-group` carried `compose::Group`, `compose::Endings`, `Lifecycle` as a
list and not one slot, and `guarded` as a named future. **It is now MERGED**, at
merge commit `f74a508`. `git merge-base --is-ancestor 28fab9e main` returns zero
and `keylink/src/compose/group.rs` exists on `main`.

**This section said "committed and NOT merged" until the merge, and before that an
even earlier draft said "merged" when the tree did not support it.** Both readings
have now been true of this file at different times, which is exactly why the
instruction below survives the state changing: verify against your own checkout,
because a status sentence is the fastest-rotting thing in a directive.

**`Endings` is a second, working answer to B.** B reconciles with it or is
refused. Do not build a third.

## The four items

**D — rename `local.rs`. Do NOT split it.** Wave 0 refused the split.
`runtime` is taken: it names the tokio runtime `blocking.rs` owns — 64 lines
there, 22 in `macros.rs`, 13 in `registry.rs`. `local.rs` is 5,108 lines, 3,077
of them comments, so 35% code, the crate median, beside `macros.rs`'s 1,779.
That count measures how much this repository documents, not what the module
does. The defect is the false name: the file exports `run_keyed` and
`run_keyed_supervised`.

`git mv keylink/src/local.rs keylink/src/turns.rs`. Repoint `lib.rs:212`,
`lib.rs:316`, `actor/remote.rs:60`, `actor/registry.rs:67`, `reply.rs:130`,
`tests/supervision.rs:1306`. No name inside the `pub use` braces changes — that
block holds **17** names, not the 15 stated earlier, and none of them moves.

**`turn.rs` is refused, by measurement rather than by taste.** Three doc comments
link the function through an explicit path: `macros.rs:907` writes
`[`turn`](keylink::turn)`, and `macros.rs:3100` and `panic.rs:123` write
`[`turn`](crate::turn)`. A module named `turn` gives `crate::turn` a second
resolution, and `rustdoc` answers `` `crate::turn` is both a function and a
module `` — reproduced in a scratch crate under `mise.toml:475`'s real flags,
exit 101, against exit 0 for a module named `turns`. The plural is the word the
module header already uses: "A feed item and a mailbox message are both a
`Turn`, dispatched at one call site."

Two probe defects were caught before that result was trusted, and both are worth
knowing. The first probe printed exit 0 for every case because it omitted
`--document-private-items`, which is `CONTRIBUTING.md:236` exactly. The first
positive control failed on `E0428` rather than on the ambiguity lint, so it could
not have detected what it was looking for.

Runner-up `run.rs`, rejected because it names the act of entering the file rather
than what is in it. Also rejected: `runtime` (taken), `loop` (a keyword),
`actor`/`actors` (`keylink/src/actor/` exists), `dispatch` (`Dispatch` is a
public re-export from `rpc`), and `inbox`/`mailbox`/`supervise` (each names one
region of five).

**The anchors crash fix goes first, in its own commit.** Proven in a throwaway
clone: control green, then `git mv` alone gives exit 1 and a bare traceback at
`anchors:836` with not one check printed, then repointing the three rows gives
exit 0 with all 49 rows resolving. **Three reads take a registry path, not two** —
`:836` and `:920` read `target`, and `quoted_spans` at `:1014` reads `lives_in`.
One `registry_file()` helper guards all three in about 21 lines, red-tested
through `anchors-selftest`'s existing `run(source.replace(...))` idiom by
injecting a row whose target is a file that does not exist. Its own commit keeps
the rename's diff names-only, which is what makes the criterion below checkable.

**C — seven doors become four.** Delete `actor_with`, `actor_wired`,
`spawn_wired`, `supervised`, `spawn_supervised`. No shim and no `#[deprecated]`.
`actor` and `spawn` keep their meaning. Add `local()` and `keyed()` builders.
`SpawnableUnfed::<A>::CHECK` is a const assertion evaluated at the call site —
both refusals must still fail to compile, each with a `compile_fail` doctest
watched failing.

**B — the keyed ending.** Rule on `compose::Endings` first. Any `System::ended`
reports the same `Ended` the local path reports, and coexists with
`Lifecycle::ended`.

**A — the local keyed door.** `at_local(system, actor)`, routing exactly as the
served path routes, pushing per-method `Mail` variants with `Origin::local()`.
The error type is neither `crate::Error` nor `RemoteError`.

**M — every mailbox gets a configurable depth, and no path is pinned at 1 by an
argument that does not cover it.** This is one item, not two, and pricing it once
is the whole point.

`actor/registry.rs:893` creates every keyed mailbox at the system's configured
depth, and `actor/registry.rs:2008-2010` already exposes
`SystemBuilder::mailbox_depth(NonZeroUsize)`. **The file is
`keylink/src/actor/registry.rs`; `keylink/src/registry.rs` does not exist**, and
a reader following the shorter path gets an empty result. But
`local.rs:251` fixes `LOCAL_MAILBOX_DEPTH = 1`, deliberately not a knob, and
`local.rs:239` gives the reason: a deeper mailbox on an actor addressable from
its own turn "turns a documented always-deadlock into an intermittent one. An
intermittent deadlock is not a slower program; it is a different program in
production than in the tests." That argument is right and it stays.

**It does not cover the ingress that pays for it.** `Handle::service` creates no
mailbox — `deliver_to_local` pushes `Mail::Served` into the *same* channel the
local `Handle` pushes into, so a singleton served to peers inherits depth 1. Peer
traffic is another process. It cannot self-call, so the hazard pinning the
constant cannot arise on the wire.

**Split by INGRESS, never by local-versus-keyed.**

**CORRECTION, and an earlier draft of this file had it wrong: `run_loop` does NOT
take two inputs through `Inbox`.** It takes **one** through `Inbox` and a second
through a different trait, `Feeds<A>` (`local.rs:1265`). There is no "third
`Inbox` arm" move to repeat, and an executor sent looking for one will change
`run_loop`, which must not change.

The conclusion survives and gets cheaper. `Inbox` has four methods and only two
are required — `poll_next` (`local.rs:1359`) and `close` (`local.rs:1445`);
`mark_busy` and `ending` have defaults. **A composite `Inbox` impl hides two
receivers behind one `poll_next`, and `run_loop`, `next` and `Taken` do not
change at all.** Four obligations, each with a reason: `poll_next` returns
`Ready(None)` only when both receivers are drained AND closed, because
`run_loop:1783-1785` reads one `None` as "every sender is gone"; `close` closes
both, because `Lent`'s give-up arm needs every `Sender::is_closed()` true at
once; `mark_busy` forwards, because `Watched` holds the cell `is_busy` reads; and
the composite owns a **second** alternation flag, because `run_loop`'s
`mail_first` divides mailbox against feeds, not handle against wire. Flip that
flag when an item is RETURNED, never on a poll, or the choice depends on spurious
wakeups.

**THE HANDLE DEPTH IS NOT EXEMPT.** An earlier draft of this file left the handle
pinned at 1 and gave the knob only to the wire. That is refused. Every path gets
a configurable depth with a minimum of 1, the handle included. The reason the
constant existed is real and it is not a reason to keep it: **depth is not the
cause of the self-call deadlock. A blocking send issued from inside the target
actor's own turn is.** Depth only decides whether that failure is deterministic
or intermittent, and keeping it deterministic is settling for a reliable hang.
Make the send fail fast and loud — a named error, never an await — and no depth
can hang. See the design item below.

**Ordering across the two doors IS claimed, and the bill is small.**
`tests/singleton.rs:558-559` says "One mailbox, two doors. A local call and a
remote call to the same actor are serialized against each other", pinned by
`local_and_remote_messages_share_one_mailbox`. No RFC claims it. That test
survives — its 20 ms sleep puts the remote message inside its turn before the
local call exists, and turns stay serial at `run_loop:1772` under any split. What
does not survive is `entries_behind_a_held_turn`, which names FIFO as its method
twice (`singleton.rs:759-761`, `806-807`) and reads a remote message through a
local call. Resequence it onto a fact. Total: one doc sentence, one narrowed
claim, one helper.

**Two things break silently and neither is obvious.** `stop()` sends
`Message::Stop` on `self.mail` (`local.rs:3305-3306`) and `run_loop:1782` breaks
at once, so `Stop` stops sitting behind messages sent before it — the rule is
close the other receiver, drain what is buffered, then break, and *not* holding
`Stop` back while items exist, which never terminates under load. And
`buffered()` (`local.rs:3428-3439`) reads one sender, so it under-reports; for
the keyed path that is a documented floor, here it would simply be wrong.

**The shutdown budget arithmetic changes shape.** `registry.rs:1971-1982` says
raising depth raises the budget owed by up to N turns and nothing can check it,
because the budget is a `Duration` and the queue is a count. With two ingresses
the worst case is `W + 2` turns, not `N + 1`, because a split adds a second queue
rather than widening one. State that at the knob.

`unbounded` stays permanently refused, with `representability.rs:1056-1062`
holding two probes. **A second depth knob needs its own twin probe**, watched
failing, because `representability.rs:1062` pins only the existing parameter and
a new `usize` door would be unpinned.

**M blocks A.** `at_local` gives a keyed actor a local caller, and a keyed system
can already be built with `mailbox_depth(32)`. Together those are exactly the
pairing `local.rs:239` refuses. **Today it cannot arise, because keyed actors
have no local caller. A creates it.** Whichever shape A takes, `local.rs:239`'s
paragraph is rewritten in the same commit — it currently argues the hazard is
local-only, and after A that is false.

Note for anyone re-deriving this: that sentence wraps, so
`rg 'turns a documented always-deadlock'` returns nothing. Search
`always-deadlock` alone.

**E — `Reply::detach(work)`, so a spawned answer cannot lie.** Today a `#[defer]`
handler doing IO writes `reply.later()`, spawns, and hands the bare `Deferred` to
the task. Dropping it answers `Gone`, which a `Handle` renders as
`MailboxClosed` — "the actor is gone" — while the actor is alive, and a panicked
or aborted task produces exactly that lie. `#[must_use]` catches an accidental
drop and not a panic. `detach` spawns through `n0_future::task::spawn` so it
works in a browser, wraps in `panic::guarded`, and answers `Answered::Faulted` on
`Err`. `later()` stays, for park-in-state-answer-from-a-later-turn, which is a
different thing. No new types.

**"Nothing holds a bare `Deferred`, so nothing can drop one" is a claim this file
made and cannot support.** `later()` is public at `local.rs:444` and returns one.
The true claim is narrower: `detach` removes the *need* to hold one, so the
spawned-task path — the shape a service author actually writes for IO — can no
longer drop one. It reduces the exposure; it does not close it.

Three things `detach` needs that its sketch omits. It needs `T: Send + 'static`,
because `Reply<T>` crosses into the task, and `Deferred::send` already carries
that exact pair at `local.rs:702-705`. **It cannot route through
`Deferred::send`**: `park` takes `FnOnce(Verdict)` (`parking.rs:98`), `Verdict`
carries no `Panic`, and the `Verdict::Faulted` arm *drops* at `local.rs:713` — so
`detach` calls the private `Reply::answer` (`local.rs:485`) directly, which is
legal because it lives in the same module. Record that as a choice, or the next
author tries the parked route and finds it closed. And it breaks a bound `later()`
holds: a detached task outlives `stop()` and `abort()` and still answers its
caller. That can be right, because keylink owns the task, but it is new and needs
a sentence.

**`Guarded<F>` changes nothing here.** The spawned task is an `async move` block
whose type stays anonymous, so nothing becomes nameable; awaiting an opaque
future never needed a name. `Guarded` exists so `retire_all` can annotate a
`FuturesUnordered`, which `detach` has no equivalent of. Land it against
whichever form is on `main`; the call site is `guarded(work).await` either way.

**F — re-price RFC-07 §4.8 against the tree that now has the return channel.**
§4.8 refuses fixing a `#[defer]` handler that panics before consuming its
`Reply`, at a price of "a return channel and a `Drop` impl on the reply of every
method". `#[defer]` landed first and `parking.rs` landed after, so the return
channel that price assumed did not exist now does. **Verdict: BUILD LATER, after
E.**

**The `E0509` cost this file named was priced against the wrong type, and an
executor following it would have recorded REFUSE against a cost that is not
real.** Four probes compiled at rustc 1.97.1 settle it. `Drop` on `Deferred` plus
`let reply = self.0;` is indeed `E0509` — but §4.8 prices a `Drop` on **the
reply**, which is `Reply`, not `Deferred`, and with `Drop` on `Reply` alone
`Deferred::send` compiles clean, because moving a field out of a non-`Drop`
struct stays legal when the field's type implements `Drop`. The site that
actually breaks is `Reply::answer` at `local.rs:489`, and it is worse in kind,
because `answer` is on **every** reply path rather than the deferred one. And it
is avoidable entirely: `to: Option<Answerer<T>>` with `.take()` in both compiles
clean with no `unsafe`, at the price of one `Option` and a `None` arm that cannot
use `unwrap`.

**Two findings this file omitted are the ones that actually decide it.**
`Verdict` is `#[derive(Clone, Copy)]` at `parking.rs:106` and is passed by value
to every parked answer at `local.rs:926-928`; a `Panic` is neither `Copy` nor
cloneable, which `parking.rs:132-133` states, so the resumption cannot travel
through `Verdict` at all. And a `Drop for Reply<T>` **cannot tell the turn's own
reply from a `Deferred`'s inner one**, because `Deferred<T>` is `Deferred(Reply<T>)`
at `local.rs:665` — a turn that drops a parked `Deferred` resolves `Gone` today
and is tested, and a naive `Drop` would answer it `Faulted` with a panic
belonging to a different caller, which is exactly what `parking.rs:128-138`
refuses. Avoiding that needs a second discriminating field on `Reply<T>`, set by
`began` at `local.rs:1115-1126`.

The `thread::panicking()` trap holds and reinforces this: `Deferred::send`'s
`Verdict::Faulted => drop(reply)` at `local.rs:713` runs in the release loop
**after** `guarded` caught the panic, so `thread::panicking()` is false at the one
site whose whole meaning is "dropped because a turn faulted". Arm from the turn's
guard, never from the thread.

Full price: six sites, all on `Reply`, which is the universal reply path — §4.8
was right about that word. Against one failure mode that already has a documented
answer and a passing test at `tests/defer.rs:330`. **So: build E first, then
re-read §4.8 against what is left. Correct §4.8's text regardless**, because its
stated price is measurably wrong about the return channel, and name the
`thread::panicking()` trap there so the next author does not write the guard that
misses `local.rs:713`.

**A correction, recorded so it is not re-fixed.** "Answering from a later turn is
gated, answering from a task is not" is NOT a defect. Both are correct: an answer
a turn produced must wait for that turn's commit, so a receipt cannot outrun its
fsync, and an answer no turn produced has no commit to wait for. E gives the
second case a name, which is the whole of what was wrong.

**Two designs recorded rather than built.**

REFUSED — self-call detection through a task-local actor identity. It turns a
direct self-call from an always-hang into a loud error, which is strictly better
and is tempting as the answer to M. It catches **direct** self-calls only.
`cycles.rs:15` measures A → B → A at two actors and calls it "a harder failure
than the one the crate already warns about, and a quieter one". No depth setting
affects that case, and catching it needs a call chain threaded through the
mailbox that never crosses the network. Recorded as incomplete, not as rejected.

DEFERRED — `enqueue_<method>` returning a `Pending<T>`, separating "my message is
queued" from "the answer arrived". Against: a second generated method per `rpc`,
inside the goal whose item C takes seven doors down to four, and M and E remove
most of the motivation. For, narrowly and really: a future that is never polled
never sends, so nothing else in the crate expresses queue-now-collect-later. If
ever built, `Pending<T>` is a keylink newtype and never a
`tokio::sync::oneshot::Receiver`. **The record is at `local.rs:294-297`, not
`local.rs:382`** — 382 is `pub struct Reply<T> {`, the item that doc block
documents. It says `Answer<Resp>` was a transparent `oneshot::Sender<..>`, so
tokio's channel type sat in keylink's public API behind a `#[doc(hidden)]` name,
and a newtype cost one field and bought the promise back. **It names the
Sender, and `Pending<T>` would wrap a Receiver** — the principle carries
unchanged, because it is about tokio's channel type in a public signature, but
say so, or an author who greps for the receiver concludes the record does not
exist.

And record the open question that actually stops a re-derivation: `Pending<T>`
must resolve to four states, not two. `Answered` carries `Ran`, `Faulted`,
`Expired` and `Refused`. A `Pending<T>` resolving to `Result<T, MailboxClosed>`
re-collapses exactly what `ask_catching` exists to keep apart. So its await is
either `ask`-shaped, which resumes the panic, or `ask_catching`-shaped, which
returns `TurnLost`. That is a decision, not a mapping. Without it written down
the next author writes `Result<T, MailboxClosed>` and the conflation returns.

**The docs correction, which rides with M — and the misleading word is not the
one I named.** "Served singleton" appears in exactly one file in this tree: this
document, where the correction is written. The device that actually misleads is
the bare noun **`singleton`**, used **91 times across 22 files** as a countable
thing sitting beside "keyed". A reader who meets "the singleton" and "the keyed
system" as two columns reads two species, and then has to put `local` somewhere.
That is how a served actor becomes a row.

The ground truth, read from the code rather than from the correction's wording.
`Handle::service` **creates nothing**: it takes `&self`, clones the handle's
inner `LocalActor`, and returns the *same* `ServiceBuilder` type the free
`service` returns, on the *same* ALPN — which is why registering both is
`DuplicateAlpn`. It spawns no task and allocates no channel. A served actor has
**no mailbox of its own**: `deliver_to_local` pushes `Mail::Served`, one variant
of the same generated `Mail` enum, into the same `LocalActor` channel the
handle's own methods push into. **No type represents a served singleton.** There
is nothing for a taxonomy row to name.

The contrast is what proves it. A keyed actor *is* a second kind and the tree
says so: its own `keyed_alpn()`, its own hashed contract over
`Addressed<Request>`, its own registry. A served local actor has none of those.
And serving is symmetric across both kinds — `keyed_service` is the same move
over a `System` that already exists. **The kind is decided by `actor()` against
`spawn()`. Serving is a registration over whichever you built.**

Correct the coining site first, because every later use inherits it:
`rfc-04-actors.md:61` reads "Capabilities A to E are the **singleton actor**".
Then the §14.8 table whose heading row is `| The keyed system | The singleton |`
— a heading is what a reader scans. Then the three public-rustdoc sites, which
render in every downstream crate.

**Do not attempt the full rename in this pass.** Two vocabularies side by side
make a reader ask which is current, and a half-done rename is worse than none.
Correct the coining site, the table and the public rustdoc; leave the adjectival
uses; and if the whole sweep is wanted later it must include the test *names*
that carry the word.

Scope it by the right number. **91 is occurrences of the noun, not sites**, and
only about 39 of them use it as a countable thing — the rest are adjectival on a
path, a contract or an ALPN, and this pass deliberately leaves those. Calling it
a "91-site rename" tells a future author to expect 91 edits from an instruction
that asks for roughly 40.

**And one grammar split that must not be swept.** The phrase "served as a `<X>`"
makes the *service* the subject and `<X>` a costume, which is the becoming model.
Four of its eleven occurrences are defects, where the costume named is a kind
that does not exist. The other seven — `macros.rs:49`, `:648`, `:3350`,
`README.md:495`, `:498`, `rfc-04-actors.md:2423` and their neighbours — say
"served **as an actor**" inside the `send`-refusal argument, where the contrast
really is about the serving side, a mailbox against an `Arc` behind a concurrent
handler, and the compiler enforces it at `ServableAsActor::<A>::CHECK`. Those
words are correct. The sentence to copy is already in the tree at
`tests/singleton.rs:3-6`: "The local `Handle` is the base case; serving it is an
addition on top that changes nothing on the wire."

**Four sentences item M makes false, which only M's commit can carry.**
`macros.rs:1767-1772` argues a self-`tell` deadlocks because "a served actor
always has other senders" competing for the handle's single permit — M removes
that competition, so the warning survives and its stated reason does not.
`local.rs:233-241` writes the depth split as local-against-keyed when the real
axis is handle-against-wire, and its own premise already covers a served local
actor; that rewrite is scheduled under A above and M reaches it first.
`macros.rs:1903-1904` says "messages from both sides share the one mailbox",
which becomes flatly false. And `compose/mod.rs:52-59` names exactly two depth
constants and says every number in that module is chosen against them; M adds a
third, so the list becomes incomplete rather than wrong.

**Two places the CODE teaches it, which no prose fix reaches.**
`deliver_to_local` says "local" and means "served, and not keyed", while its twin
`deliver_to_actor` says "actor" and means "keyed" — together they encode a
singleton-against-keyed split rather than a local-against-keyed one. And
`keylink/tests/singleton.rs` is named after the kind that does not exist, while
its own header teaches the correct model. Both are separate decisions. Item D
renames the file and moves no exported name, so `deliver_to_local` survives it
untouched.

## Order

**D → M → C → B → A, then E, then F.** D is a rename with no behavior change, so
it carries no scheduling risk; its predicted size lives with the D criteria below
and is not restated here, because a figure restated in two places rots in one of
them. M comes before A because A is what creates the hazard M closes. The docs
correction rides with M, since M is the commit that changes the served ingress
and falsifies four sentences about it. F comes after E on measurement, not on
taste — see F.

## Traps, all measured

- **`.mise/tasks/anchors` has no `try` anywhere.** Its target cache calls
  `read_text()` — four sites on the merged tree — so a vanished `local.rs`
  raises `FileNotFoundError` and the task dies before printing one check. It
  runs in `pre-commit`, so **the rename commit cannot be created unless it
  repoints every row targeting that file.**

  **That is FOUR rows, not three.** This trap said "D1, D2 and D3" and PR #29
  added `G13`, which targets `keylink/src/local.rs` from
  `keylink/tests/compose.rs`. Re-measured on the merged tree: D1, D2, D3 and
  G13. A trap that enumerates its instances goes stale every time somebody adds
  one, so re-derive the list with `rg -c '"keylink/src/local\.rs"'
  .mise/tasks/anchors` rather than trusting this sentence.

  Make the task report a missing target instead — that is the crash-guard
  commit, and it goes FIRST, as its own commit. The cost of the crash is not one
  wrong answer but every check in the file, because the task dies rather than
  skipping a row.
- `scripts/replay-fate-breaks.sh` hardcodes `local.rs` on 16 lines, and no gate
  runs it. Repoint by hand and print the result.
- `RECORD_BLOCKS` must NOT move on a rename — the walk is path-insensitive. A
  bump is a defect, not routine.

## Success criteria

- [ ] D: **`git diff -U0 | rg '^[+-][^+-]' | rg -vi 'local|turns'` prints exactly
      one line** — the old header's first line. That is the criterion that
      actually enforces "no behavior change", and it is the one to keep if only
      one survives.
- [ ] D: `rg -n 'local\.rs|crate::local|\blocal::|\bmod local\b' keylink scripts
      .mise README.md CONTRIBUTING.md` returns zero. Baseline **102 lines across
      27 files**, re-measured on `f74a508` — the merge of PR #29. It was **89
      across 23** when first taken, and compose added the difference. Re-measure
      before starting: this baseline rots on every merge, not on every edit to
      `local.rs`.

**The sweep this replaces was wrong twice over, and the second way is the
instructive one.** `rg -n 'crate::local|src/local\.rs' .` does not return zero
today; it returns **84**, of which 44 sit under `docs/` — RFCs and older goals
that `CONTRIBUTING.md:322` forbids editing to match new code. So it demanded an
edit the house rules refuse.

**And it is unsatisfiable by construction: the line stating the criterion
contains the pattern it searches for.** A whole-tree sweep matches the document
that states it, and the plan written to satisfy it matched 24 more times. A check
whose own statement is a hit can never go green. Root the sweep at the
directories it can be true of, never at `.`.

Two misses the old pattern also had, both compile-critical and both found by
reading rather than by grepping: `reply.rs:130` and `tests/supervision.rs:1306`
spell it as a bare `local::tests::…`, which matches neither `crate::local` nor
`local\.rs`. `\blocal::` catches them and folds every module-path site into one
substitution, `s/\blocal::/turns::/g`. A sweep rooted at `.` also skips hidden
directories, so it saw none of the five `.mise/tasks/anchors` lines — name
`.mise` explicitly.
- [ ] D: `rg -n '\(\*turn\)\.run\(' keylink/src` returns exactly 1. Baseline 1,
      at `local.rs:1674`. `rg -c '^async fn run_loop'` returns 1, baseline 1.
      `ls keylink/src/*.rs | wc -l` returns 27, baseline 27.
- [ ] D: `RECORD_BLOCKS` absent from the rename commit's diff, and `docs/`
      absent from its file list.

**The "under 30 changed lines" estimate was low by 3x and is replaced.** The
figure has now been taken on two different trees, and BOTH are recorded, because
the delta is the useful part.

**On `0ab8366`: 89 name-carrying lines across 23 files outside `docs/`** — 4
compile-critical, **19 break-record paths rather than 7** (the other 12 live in
`supervision.rs`, `defer.rs`, `feeds.rs`, `persist.rs` and `cycles.rs`, not in
`local.rs`), 16 replay-script arguments, 45 prose lines inside code, and 5 in
`.mise/tasks/anchors`. Predicted diff was 93 changed lines, band 90-96.

**On `f74a508`, after PR #29 merged: 102 lines across 27 files** — 24 files under
`keylink`, 2 under `.mise`, 1 under `scripts`. `scripts/replay-fate-breaks.sh`
still carries 16, unchanged.

**The sub-breakdown above belongs to the 89-line tree and has NOT been
re-derived.** Only the total and the per-directory split were re-measured. Do not
carry those five sub-figures onto the new tree — 4 + 19 + 16 + 45 + 5 sums to 89,
so any of them reused against 102 is wrong by construction. Re-measure them, or
work from the total and drop the breakdown. Everything under `docs/` is left
alone per `CONTRIBUTING.md:322`.
- [ ] C: `rg 'actor_with|actor_wired|spawn_wired' keylink/src` returns zero, and
      **`mise run docs` is green** — a deleted door named only in a rustdoc
      example is a doc break and not a compile error.
- [ ] A: a test calls a keyed actor in-process, with no `Node` and no
      `KeyedClient`.
- [ ] All: `mise run gate` green, both ALPN hashes unmoved, and the
      `actor_memory.rs` slope reported as a number.
- [ ] Every new guard watched failing, and recorded where `anchors` checks it.

**Deleted criteria, with the reason kept so neither is re-derived.** The
2,500-line ceiling passed an arbitrary three-way cut that was worse than the
status quo. `rg 'Turn::run|\.run\('` returns 14 lines, 13 of them
`runtime.run(..)` on the blocking runtime, so it could not grade the one site it
was written for.

## Invariants

No `unwrap` and no `expect` outside tests. `#![deny(unsafe_code)]`.
`n0_future::time` only. No legacy path and no shim. Never `--no-verify`. Cite
the guard or the edit, never the literal. State a count only after measuring it
that turn.

**One writer per file.** A count read off a failing run went stale before the
commit consumed it, and that cost an hour here. Reading a number off a failure
stops you computing it. It does not stop the tree moving underneath.

**A deleted record is a guard the gate stops counting.** `anchors` is loud when
a record is added and silent when the tree is made to agree by removing one.

## Process

Wave 0 may REFUSE an item, and a refused item is recorded rather than built. One
atomic commit per item, one pull request. Subagents run on opus, never fable.
Questions are not allowed — annotate assumptions inline and surface them in the
final report.
