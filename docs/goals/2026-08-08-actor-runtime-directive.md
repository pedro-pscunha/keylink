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

## Already landed

`compose-group`: `compose::Group`, `compose::Endings`, `Lifecycle` as a list and
not one slot, `guarded` as a named future. **`Endings` is a second, working
answer to B.** B reconciles with it or is refused. Do not build a third.

## The four items

**D — rename `local.rs`. Do NOT split it.** Wave 0 refused the split.
`runtime` is taken: it names the tokio runtime `blocking.rs` owns — 64 lines
there, 22 in `macros.rs`, 13 in `registry.rs`. `local.rs` is 5,108 lines, 3,077
of them comments, so 35% code, the crate median, beside `macros.rs`'s 1,779.
That count measures how much this repository documents, not what the module
does. The defect is the false name: the file exports `run_keyed` and
`run_keyed_supervised`.

`git mv keylink/src/local.rs keylink/src/turn.rs` — justify the name, and not
`runtime`. Repoint `lib.rs:212`, `lib.rs:316`, `actor/remote.rs:60`,
`actor/registry.rs:67`, `reply.rs:130`, `tests/supervision.rs:1306`. No name
inside the `pub use` braces changes.

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

## Order

**D → C → B → A.** D is about 25 line edits, so it carries no scheduling risk.

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

- [ ] D: `git diff --stat` shows one rename and under 30 changed lines.
      `rg -n 'crate::local|src/local\.rs' .` returns zero.
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
