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

## In flight, and NOT yet merged

`compose-group` carries `compose::Group`, `compose::Endings`, `Lifecycle` as a
list and not one slot, and `guarded` as a named future. **It is committed and it
is not merged.** `git merge-base --is-ancestor compose-group main` returns
non-zero, and `keylink/src/compose/group.rs` does not exist on `main`. An earlier
draft of this file said "merged", which the tree did not support. Verify against
your own checkout before item B reconciles with anything.

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

`registry.rs:893` creates every keyed mailbox at the system's configured depth,
and `registry.rs:2009` already exposes `SystemBuilder::mailbox_depth`. But
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

**Split by INGRESS, never by local-versus-keyed.** The handle inbox keeps depth 1
and keeps its argument. The wire inbox gets the knob. `run_loop` already takes
two inputs through the private `Inbox` trait, so a third is the move Wave 3
already made. State two things where a caller meets them: raising the wire depth
raises the shutdown budget owed by up to N turns and nothing checks that, and
`unbounded` stays permanently refused with `representability.rs` holding the
probe.

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
`Err`. Nothing holds a bare `Deferred`, so nothing can drop one. `later()` stays,
for park-in-state-answer-from-a-later-turn, which is a different thing. No new
types.

**F — re-price RFC-07 §4.8 against the tree that now has the return channel.**
§4.8 refuses fixing a `#[defer]` handler that panics before consuming its
`Reply`, at a price of "a return channel and a `Drop` impl on the reply of every
method". `#[defer]` landed first and `parking.rs` landed after, so the return
channel that price assumed did not exist now does. Verdict required — BUILD NOW,
BUILD LATER or REFUSE — against three named costs: neither `Reply` nor `Deferred`
has a `Drop` today, adding one to `Deferred` breaks `Deferred::send`'s
`let reply = self.0;` with `E0509`, and `Reply` is dropped on non-unwinding paths
so a `thread::panicking()` guard must not misfire.

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
`tokio::sync::oneshot::Receiver` — `local.rs:382` records that leak and records
the newtype as its fix.

**The docs correction, which rides with M.** A served singleton is not a third
kind of actor. There are two kinds, local and keyed. `Handle::service` is a
method call on a handle you already hold, and reach over the network is an
add-on to a local actor rather than a species of one. The documentation reads as
a taxonomy with a served row beside `local` and `keyed`, which is what made a
careful reader list it as one. Correct the worst sites, and flag any place the
CODE teaches the wrong model — a name or a module layout does that as
effectively as a table.

## Order

**D → M → C → B → A, then E and F.** D is about 91 line edits and no behavior
change, so it carries no scheduling risk. M comes before A because A is what
creates the hazard M closes. The docs correction rides with M, because M is the
commit that changes the served ingress.

## Traps, all measured

- **`.mise/tasks/anchors` has no `try` anywhere.** Its target cache calls
  `read_text()`, so a vanished `local.rs` raises `FileNotFoundError` and the
  task dies before printing one check. It runs in `pre-commit`, so **the rename
  commit cannot be created unless it repoints rows D1, D2 and D3.** Consider
  making it report a missing target instead. One missing file currently
  suppresses every other row's check, so the cost is not one wrong answer but
  forty-eight absent ones.
- `scripts/replay-fate-breaks.sh` hardcodes `local.rs` on 16 lines, and no gate
  runs it. Repoint by hand and print the result.
- `RECORD_BLOCKS` must NOT move on a rename — the walk is path-insensitive. A
  bump is a defect, not routine.

## Success criteria

- [ ] D: **`git diff -U0 | rg '^[+-][^+-]' | rg -vi 'local|turns'` prints exactly
      one line** — the old header's first line. That is the criterion that
      actually enforces "no behavior change", and it is the one to keep if only
      one survives.
- [ ] D: `rg -n 'local\.rs|crate::local|\bmod local\b|pub use local::' keylink
      scripts .mise README.md CONTRIBUTING.md` returns zero. **The earlier sweep
      `crate::local|src/local\.rs` misses `mod local;` and `pub use local::{`** —
      verified, and both are compile-critical.
- [ ] D: `rg -n '\(\*turn\)\.run\(' keylink/src` returns exactly 1. Baseline 1,
      at `local.rs:1674`. `rg -c '^async fn run_loop'` returns 1, baseline 1.
      `ls keylink/src/*.rs | wc -l` returns 27, baseline 27.
- [ ] D: `RECORD_BLOCKS` absent from the rename commit's diff, and `docs/`
      absent from its file list.

**The "under 30 changed lines" estimate was low by 3x and is replaced.** Measured:
**91 insertions, 91 deletions across 23 files plus one rename**, in a band of
86-96 because `cargo fmt` re-sorts `use` groups. The earlier inventory counted 4
compile sites, 3 anchors rows, 16 script arguments and 2 prose lines. The real
inventory is 182 matching lines across 30 files: 4 compile-critical, **19
break-record paths rather than 7** — the other 12 live in `supervision.rs`,
`defer.rs`, `feeds.rs`, `persist.rs` and `cycles.rs`, not in `local.rs` — 16
replay-script arguments, 43 prose lines inside code, and 101 in `docs/**` which
are left alone per `CONTRIBUTING.md:322`.
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
