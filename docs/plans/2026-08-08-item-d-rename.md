# Item D — rename `local.rs` to `turns.rs` — implementation plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use `superpowers:subagent-driven-development`
> (recommended) or `superpowers:executing-plans` to implement this plan task by
> task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Rename `keylink/src/local.rs` to `keylink/src/turns.rs`, repoint every
name that points at the old path, and change no behavior.

**Architecture:** One `git mv`, four compile-critical repoints, and a token
substitution over the tree outside `docs/`. No file is split. No public name
changes. One preparatory commit first makes `.mise/tasks/anchors` report a
missing target instead of dying with a traceback.

**Tech Stack:** Rust 2021, `mise` tasks, `rustdoc` as a gate, Python 3 for the
anchors gate.

---

## 0. Reconciliation with the operative directive

`docs/goals/2026-08-08-actor-runtime-directive.md` is operative and supersedes
the long goal. This plan agrees with it on every point but two, and both are its
D success criteria at line 71-72. Each is refused on measurement.

**Refused: "under 30 changed lines".** The measured figure is **93**. See §6.
The directive inherited "about 25 line edits" from Wave 0, which counted 4
compile sites, 3 anchors rows, 16 script arguments and 2 prose lines. The tree
carries **89 name-carrying lines outside `docs/`**, measured in this run.

**Refused: `rg -n 'crate::local|src/local\.rs' .` returns zero.** Run verbatim
today it returns **84 lines, 44 of them under `docs/`**. Sixteen of those sit in
RFCs and older goals:

```
docs/rfc-04-actors.md:3643, 3644, 3647, 3759
docs/rfc-06-implementation-brief.md:19
docs/rfc-07-the-turn-model.md:2351
docs/goals/2026-08-06-rfc-06-observers-on-correctness-programme.md:50, 120, 121, 223
docs/plans/2026-08-08-item-d-wave-0.md:12, 83, 88, 102, 124
```

`CONTRIBUTING.md:322` forbids editing an RFC to match new code. So the criterion
demands an edit the house rules refuse.

**It is also unsatisfiable by construction.** The directive states the criterion
on line 72, and that line itself contains `src/local\.rs`. A whole-tree sweep
matches the document that states it. So does this plan, 24 times.

The replacement is in §6: the same sweep, widened to catch two spellings the
directive's own regex misses, and scoped to the code it can be true of.

**Accepted from the directive without change:** the rename over the split; `no
name inside the `pub use` braces changes`; the three traps; "consider making
`anchors` report a missing target"; the deletion of the 2,500-line ceiling and
the `Turn::run|\.run\(` criterion; one atomic commit per item; never
`--no-verify`.

**One directive invariant this plan can violate, and how it is avoided.** Line
95 reads *"One writer per file."* Task 2 writes `.mise/tasks/anchors`. If another
agent is editing that file for another item, Task 2 must wait. Check before
starting it.

**Line 99-101: "A deleted record is a guard the gate stops counting."** This plan
deletes no record and no `ANCHORS` row. It repoints three targets and 19 paths.

**On the directive's spelling of the name.** Line 31 writes `git mv
keylink/src/local.rs keylink/src/turn.rs`, and the same line says *"justify the
name"*. §2 is that justification, and it refuses `turn` on a measured rustdoc
failure. `turns` is the same word in the spelling that compiles.

## 1. What this plan executes, and what it refuses

`docs/plans/2026-08-08-item-d-wave-0.md` returned **PROCEED, AMENDED**. It
refused the split. This plan renames one file and splits nothing.

Do not re-derive the split. Wave 0 records its cost: 14 visibility widenings, a
carved 176-line header, 80 intra-doc links across a new boundary, and one
falsified design sentence.

`local.rs:49` calls `Inbox` a **private** trait. A rename keeps it private. That
sentence stays true, and keeping it true is one reason the rename wins.

## 2. The name is `turns.rs`, and `turn.rs` is refused by measurement

**Wave 0 suggested `turn.rs`. Measurement refuses it.**

Three doc links name the function through an explicit path:

```
keylink/src/panic.rs:123:  /// calls [`turn`](crate::turn), and the guard is inside *that*. So
keylink/src/macros.rs:907: /// Every arm is [`turn`](keylink::turn) or its deferring twin, which is
keylink/src/macros.rs:3100:/// Every arm is one call to [`turn`](crate::turn) or [`turn_deferred`](crate::turn_deferred)
```

`mise.toml:475` runs rustdoc as:

```
RUSTDOCFLAGS='-D warnings' cargo doc --no-deps --document-private-items --all-features -p keylink -p keylink-persist -p demo
```

A scratch crate reproduced the exact shape — `mod turn;`, `pub use turn::{turn,
Turn};`, and `[`turn`](crate::turn)` written in a sibling module. Run under the
same flags, rustdoc **fails**:

```
CASE 1: mod turn + pub use turn::turn + [`turn`](crate::turn) from a sibling
exit=101
error: `crate::turn` is both a function and a module
  |                    ^^^^^^^^^^^ ambiguous link

CASE 2: bare [`turn`] written in lib.rs, mod turn + fn turn both in scope
exit=101
error: `turn` is both a function and a module

CASE 3: module named 'turns' (control)
exit=0
```

The positive control in the same run (`pub struct Thing {}` plus `pub fn
Thing()`, linked as `[`Thing`]`) failed with `ambiguous link`, so the probe can
detect the defect it looked for.

**CAUTION: the first version of this probe omitted `--document-private-items`
and printed exit 0 for every case.** `CONTRIBUTING.md:236` states the general
form: `mise run docs` is the only gate that sees this class, so a green suite is
not evidence here.

### Why `turns` and not the runner-up

The module holds the turn (`Turn`, `turn`, `turn_deferred`, `dispatch`), the
feeds (`Fed`, `Feeds`, `feed_turn`), supervision (`Intensity`, `Incarnation`,
`supervise`), the loop and the inboxes (`run_loop`, `Inbox`, `Keyed`, `Watched`,
`Lent`), the local handle (`LocalActor`, `MailboxClosed`, `deliver_to_local`) and
both keyed entry points (`run_keyed`, `run_keyed_supervised`).

One sentence in the header covers all of it: *"A feed item and a mailbox message
are both a [`Turn`], dispatched at one call site."* `turns` is the word that
sentence already uses. `docs/rfc-07-the-turn-model.md` uses it as the title of
the model, on 27 lines that name this file.

**Runner-up: `run.rs`.** It is free, and `run_loop`, `run_keyed` and
`run_keyed_supervised` are every entry point the module publishes. It lost
because it names the act of entering the file and not what the file is about. A
reader who opens `run.rs` and meets `Reply`, `LocalActor` and `deliver_to_local`
learns nothing from the name.

### Every other candidate, and why it lost

| name | verdict | evidence |
|---|---|---|
| `turn` | refused | `crate::turn` becomes ambiguous, rustdoc exit 101 (above) |
| `runtime` | refused | Wave 0: `blocking.rs` owns the word, 64 lines there |
| `loop` | illegal | `error: expected identifier, found keyword `loop`` |
| `actor` / `actors` | refused | `keylink/src/actor/` already exists |
| `dispatch` | refused | `Dispatch` is a public re-export from `rpc`, meaning the served dispatcher |
| `inbox`, `mailbox`, `supervise` | refused | each names one region of a module holding five |

No file or directory named `turns` exists under `keylink/src/`. Verified in this
run against all eleven candidates above.

## 3. Measured baseline

Every figure below was measured on `2026-08-08` against this worktree at
`0ab8366`. Re-measure each one after the `compose-group` rebase (see §7).

| quantity | value |
|---|---|
| `wc -l keylink/src/local.rs` | 5,108 |
| `.rs` files directly in `keylink/src/` | 27 |
| names in `pub use local::{...}` | **17** (Wave 0's brief said 15) |
| compile-critical sites naming the module | 4 |
| `rg -n '\(\*turn\)\.run\(' keylink/src` | 1 (`local.rs:1674`) |
| `rg -c '^async fn run_loop' keylink/src/local.rs` | 1 |
| `RECORD_BLOCKS` in `.mise/tasks/anchors` | **127** here, **148** on `compose-group` |
| `ANCHORS` rows | 49 total, **3** targeting `local.rs` (D1, D2, D3) |
| `mise run anchors` on this tree | exit 0 |

The 17 names, read in this run from `keylink/src/lib.rs:316-319`:

```rust
pub use local::{
    deliver_to_local, feed_turn, run_keyed, run_keyed_supervised, turn, turn_deferred, Deferred,
    Fed, Feeds, Hooks, Incarnation, Intensity, LocalActor, MailboxClosed, Replied, Reply, Turn,
};
```

**No name inside those braces changes.** Only the module path before them
changes.

## 4. The edit inventory

**The edit is 89 lines across 23 files, outside `docs/`.** They split into five
classes.

| class | lines | files | action |
|---|---|---|---|
| compile-critical | 4 | 3 | repoint |
| break-record paths | 19 | 6 | repoint |
| `scripts/replay-fate-breaks.sh` arguments | 16 | 1 | repoint |
| prose inside code | 45 | 19 | repoint |
| `.mise/tasks/anchors` rows and prose | 5 | 1 | repoint |
| **total, the edit** | **89** | **23** | |
| `docs/**` (RFCs, goals, plans) | 44+ | 8+ | **leave alone** |

**The `docs/**` row grows with every document anybody writes, and this plan is
one of them — it accounts for 24 of the current matches.** It is not asserted on
anywhere. The five other rows are the edit. Re-measure them in Task 1 Step 2 and
ignore the sixth.

**CAUTION: a sweep rooted at `.` silently omits `.mise/`.** `ripgrep` skips
hidden directories by default, so an earlier count of this inventory reported 182
lines and contained none of the five `anchors` lines. Name `.mise` as an explicit
path argument, exactly as the corrected sweep below does.

### Two spellings no sweep in this repository has caught

**The module is named six ways, and both the directive's regex and this plan's
first sweep missed two of them.**

```
keylink/src/lib.rs:316            pub use local::{
keylink/src/reply.rs:130          /// `local::tests::no_failure_prefix_is_a_prefix_of_another` holds it, and a fifth
keylink/tests/supervision.rs:1306 /// `local::tests::an_ending_incarnation_settles_its_verdict_and_its_mailbox_before_its_state`,
```

A bare `local::` module path matches neither `crate::local` nor `local\.rs`.
The directive found the last two by citing them at line 33; a regex found
neither.

`rg -n '\blocal::'` outside `docs/` returns **5 lines**, and covers all three
above plus `remote.rs:60` and `registry.rs:67`. **One substitution rule handles
every one of them.**

**The corrected sweep, and the only one to use:**

```
rg -n 'local\.rs|crate::local|\blocal::|\bmod local\b' keylink scripts .mise README.md CONTRIBUTING.md
```

It returns **89 lines across 23 files**, measured in this run. That is the edit.

**Also outside the regex:** `.mise/tasks/anchors:372,374,391` carry the D1, D2
and D3 target column as `"keylink/src/local.rs"`, which the `local\.rs` arm does
catch, and which is counted in the 89.

### The four compile-critical sites

```
keylink/src/lib.rs:212           mod local;
keylink/src/lib.rs:316           pub use local::{
keylink/src/actor/remote.rs:60   use crate::local::{lost, rendered, Reply};
keylink/src/actor/registry.rs:67 use crate::local::{Fate, Standing};
```

### `docs/**` is left alone on purpose

`CONTRIBUTING.md:322` states the rule:

> Do not edit an RFC so that it matches new code. Append to it. The value of the
> record is that it shows what was believed at the time.

So 101 lines across `docs/rfc-04`, `docs/rfc-06-*`, `docs/rfc-07`, `docs/rfc-08`
and the goal and plan documents keep the name `local.rs`. State this in the
commit message. A later reader must read it as a decision and not as rot.

### One string that looks like the module and is not

```
keylink/src/actor/remote.rs:167:/// // Fails with `E0624: associated function `local` is private`, and
```

That `local` is `Origin::local`, not the module. **Do not touch it.** The
substitution rules in Task 3 cannot reach it, because every rule requires
`local.rs`, `crate::local::`, `mod local` or `pub use local::`.

## 5. The five traps

### Trap 1 — `anchors` crashes rather than fails, so the rename is not divisible

`.mise/tasks/anchors` contains **zero** occurrences of `try` — no `try:`
statement and no bare word. It contains exactly one line matching `except`, and
that line is prose:

```
993: # built on the exceptions breaks the convention.
```

**Quote that line whenever you assert the absence.** `rg -c 'try|except'`
returns 1 and reads as a contradiction. It is not one: there is no `try`
statement and no `except` clause anywhere in the 1,489 lines.

Three reads take a path from the `ANCHORS` registry and none of them is guarded:

```
836:  cache[target] = (ROOT / target).read_text()
920:  cache[target] = (ROOT / target).read_text()
1014: return set().union(*(spans(b) for b in comment_blocks((ROOT / path).read_text())))
```

Line 1014 runs through `quoted_spans(lives_in)` at line 1019, so a missing
`lives_in` crashes as well as a missing `target`.

Proven in a throwaway clone in this run. Control first, then the perturbation:

```
=== CONTROL: anchors on the UNMODIFIED clone ===
exit=0

=== after `git mv keylink/src/local.rs keylink/src/turns.rs` ===
exit=1
Traceback (most recent call last):
  File ".../clone/.mise/tasks/anchors", line 836, in <module>
    cache[target] = (ROOT / target).read_text()
FileNotFoundError: [Errno 2] No such file or directory: '.../clone/keylink/src/local.rs'
```

**Not one check printed.** `anchors` runs in `pre-commit`, so a commit that moves
the file without repointing rows D1, D2 and D3 cannot be created.

### Trap 2 — `scripts/replay-fate-breaks.sh` hardcodes the path 16 times

Measured: 16 lines, at 76, 85, 94, 111, 119, 127, 163, 172, 181, 192, 234, 244,
254, 261, 271 and 282. Each is one argument line reading `keylink/src/local.rs \`.

**No gate runs this script.** A search for `replay-fate-breaks` finds it named
only in prose: `keylink/tests/blocking_work.rs:137`, two lines of
`docs/rfc-08-serving-side-keys.md`, two lines of `docs/rfc-07-the-turn-model.md`,
and `scripts/break-once.sh:70`. Nothing will report this rot. Repoint it in the
same commit and run it by hand.

### Trap 3 — `RECORD_BLOCKS` must not move, and 127 is not the number to pin

The population walk globs and does not name files:

```python
for path in sorted(
    list((ROOT / "keylink").rglob("*.rs")) + list((ROOT / "keylink-persist").rglob("*.rs"))
):
    all_blocks += comment_blocks(path.read_text())
harness_blocks = [b for b in all_blocks if HARNESS in b]
```

`HARNESS = "break-once.sh"`. The walk is path-insensitive, and the marker it
counts does not contain the path. So the rename moves no block, and repointing
the 19 record paths moves no block either.

Proven in the same clone. After `git mv` plus repointing only the three rows,
with `RECORD_BLOCKS` left at 127:

```
anchors exit=0
ok: 49 REGISTERED rows resolve to exactly one occurrence
    40 of the 127 harness blocks carry a registered row, so 87 do not
```

**Treat any change to `RECORD_BLOCKS` in D's commit as a defect.**

**CAUTION: the expected-unchanged value is not 127 after the rebase.**
`compose-group` sets `RECORD_BLOCKS = 148`. Read the value off the rebased tree
before the rename, and assert the same value after. Never compute it.

### Trap 4 — two success criteria in the goal cannot grade this item

`rg 'Turn::run|\.run\(' keylink/src` returns **14 lines**, measured verbatim in
this run: 6 in `macros.rs`, 7 in `blocking.rs`, 1 in `local.rs`. Thirteen are
`runtime.run(..)` on the tokio runtime. §6 replaces this criterion.

The 2,500-line ceiling grades nothing, because nothing is split. §6 replaces it.

### Trap 5 — 19 break records name the path, and nothing checks them

Seven live inside `local.rs` itself (3778, 3809, 4623, 4729, 4922, 4971, 5059).
Twelve more live in tests: `supervision.rs` 4, `defer.rs` 5, `feeds.rs` 1,
`persist.rs` 1, `cycles.rs` 1. Two more mentions sit in `anchors` prose, at lines
116 and 959.

`anchors` never parses the path out of a record, so none of these is checked.
Each one is an instruction a human runs. After the rename, every one of them
names a file that does not exist.

`cycles.rs:254` spells it `` `local.rs` `` with no directory. The substitution in
Task 3 catches both spellings.

## 6. Success criteria — two replaced, all with baselines

Delete the goal's second and third D criteria. Keep the rest. The full list:

- [ ] **One dispatch site.** `rg -n '\(\*turn\)\.run\(' keylink/src` returns
      exactly one line. **Baseline measured: 1**, at `keylink/src/local.rs:1674`.
      After: 1, at `keylink/src/turns.rs`.
- [ ] **One loop.** `rg -c '^async fn run_loop' keylink/src/turns.rs` returns 1.
      **Baseline measured: 1** in `local.rs`.
- [ ] **Nothing was split.** `ls keylink/src/*.rs | wc -l` returns 27, unchanged.
      **Baseline measured: 27.**
- [ ] **The commit is a rename.** `git show --stat --find-renames=100% HEAD`
      lists `keylink/src/local.rs => keylink/src/turns.rs`.
- [ ] **No line changes anything but a name.**
      `git show -U0 --format= HEAD | rg '^[+-][^+-]' | rg -vi 'local|turns'`
      prints exactly one line, and that line is
      `-//! An actor that never leaves the process.`
- [ ] **No spelling of the old name survives outside `docs/`.**
      `rg -n 'local\.rs|crate::local|\blocal::|\bmod local\b' keylink scripts .mise README.md CONTRIBUTING.md`
      returns zero. **Baseline measured: 89 lines across 23 files.** This
      replaces the directive's `rg -n 'crate::local|src/local\.rs' .`, which
      cannot reach zero — see §0.
- [ ] **`docs/` is untouched.** `git show --name-only HEAD | rg '^docs/'` returns
      zero.
- [ ] **`RECORD_BLOCKS` did not move.** `git show HEAD -- .mise/tasks/anchors | rg 'RECORD_BLOCKS'`
      returns zero.
- [ ] **`mise run anchors` green.** Print the summary line.
- [ ] **`mise run docs` green.** This is the criterion the name was chosen
      against.
- [ ] **`mise run gate` green.** Print the tail and the exit code.
- [ ] **`scripts/replay-fate-breaks.sh` run by hand**, before and after, with
      both exit codes printed.
- [ ] **`tests/actor_memory.rs` slope reported**, and unchanged.
- [ ] **Changed-line count inside the predicted band.** See §Task 3 Step 8.

### The predicted changed-line count

**93 changed lines — 93 insertions and 93 deletions — across 23 files plus one
rename.** Derived, not estimated:

| term | lines | source |
|---|---|---|
| name-carrying lines outside `docs/` | 89 | the corrected sweep, §4 |
| the module header's first line | +1 | §Task 3 Step 5 |
| the `pub use` block moving as a unit | +3 | 4 lines move, 1 of them already counted |
| **predicted total** | **93** | |

Per file, the 89:

| file | lines | file | lines |
|---|---|---|---|
| `scripts/replay-fate-breaks.sh` | 16 | `keylink/src/reply.rs` | 4 |
| `keylink/src/{local.rs => turns.rs}` | 7 | `keylink/tests/persist.rs` | 4 |
| `keylink/src/actor/registry.rs` | 7 | `keylink/src/actor/remote.rs` | 4 |
| `keylink/tests/defer.rs` | 7 | `keylink/src/observe.rs` | 3 |
| `keylink/tests/supervision.rs` | 7 | `keylink/src/panic.rs` | 3 |
| `.mise/tasks/anchors` | 5 | `keylink/tests/actors.rs` | 2 |
| `keylink/src/lib.rs` | 5 | `keylink/tests/feeds.rs` | 2 |
| `keylink/src/representability.rs` | 4 | `keylink/tests/singleton.rs` | 2 |

The remaining six carry one line each: `macros.rs`, `idempotency.rs`,
`compose/mod.rs`, `compose/topic.rs`, `tests/cycles.rs`, `tests/observe.rs`,
`tests/actor_memory.rs`.

**The directive's "under 30 changed lines" is low by a factor of three.** It
inherited Wave 0's count of four compile sites, three anchors rows, 16 script
arguments and two prose lines. That count omits 45 prose lines and 17 of the 19
break-record paths.

Check the prediction with `git diff --cached --shortstat --find-renames=100%`.
**Accept 90 to 96.**

The floor is 90: that is the count if `rustfmt` leaves the `pub use` block where
it is, so only its first line changes. The ceiling allows a re-wrap. Moving a
`use` line inside a sorted block costs nothing extra — one deletion and one
insertion, which an in-place edit already costs.

**Let `cargo fmt` decide every placement, and do not fight it.**
`mise run fmt-check` is the arbiter.

If the count lands outside the band, stop and find out why before committing. Do
not adjust the band.

## 7. Sequencing and the `compose-group` rebase

`compose-group` is unmerged and lands first. Its tip in this run is `e801cca`,
five commits ahead of `main`.

It touches 12 files. **This rename also touches 5 of them:**

| shared file | `compose-group` hunks | this rename touches | conflict |
|---|---|---|---|
| `keylink/src/lib.rs` | 56, 198, 200 | 212, 316-319, plus 3 prose lines | none — line drift only |
| `keylink/src/actor/registry.rs` | 638 to 2105 | 67, and prose at 49, 89, 145, 191, 283, 3180 | none — line drift only |
| `keylink/src/panic.rs` | 20 to 232 | prose at 12, 26, 152 | none — line drift only |
| `keylink/src/compose/mod.rs` | 1 to 132 | prose at 54 | none — line drift only |
| `.mise/tasks/anchors` | one hunk at 1277 | rows at 372, 374, 391; prose at 116, 959 | none |

**The rebase costs no conflict resolution. It costs re-measurement.** Every line
number in this plan drifts. `RECORD_BLOCKS` changes from 127 to 148. Task 1 Step
1 re-derives all of them.

`compose-group` does not touch `keylink/src/local.rs`,
`keylink/src/actor/remote.rs` or `scripts/replay-fate-breaks.sh`.

---

# Tasks

## Task 1: rebase, and re-measure every citation

**Files:** none modified.

- [ ] **Step 1: Fetch and rebase onto the merged `main`**

```bash
cd /Users/pedrocunha/repos/keylink.actor-simplifications
git fetch origin
git rebase origin/main
git log --oneline -3
```

Expected: `compose-group`'s five commits are in the history, and the rebase
reports no conflict.

- [ ] **Step 2: Re-measure every number this plan states**

```bash
cd /Users/pedrocunha/repos/keylink.actor-simplifications
echo "--- module size ---";        wc -l keylink/src/local.rs
echo "--- files in src ---";       ls keylink/src/*.rs | wc -l
echo "--- compile sites ---";      rg -n 'crate::local|^mod local;|^pub use local::' keylink/src
echo "--- RECORD_BLOCKS ---";      rg -n '^RECORD_BLOCKS' .mise/tasks/anchors
echo "--- anchors D rows ---";     rg -n '"keylink/src/local.rs"' .mise/tasks/anchors
echo "--- replay script ---";      rg -c 'local\.rs' scripts/replay-fate-breaks.sh
echo "--- dispatch site ---";      rg -n '\(\*turn\)\.run\(' keylink/src
echo "--- run_loop decls ---";     rg -c '^async fn run_loop' keylink/src/local.rs
echo "--- total sweep ---";        rg -c 'local\.rs|crate::local|mod local|super::local' --glob '!target' . | awk -F: '{s+=$2} END{print s}'
```

Write every answer into the report. If a number differs from §3, the tree moved.
Use the new number.

- [ ] **Step 3: Record the pre-rename control runs**

```bash
cd /Users/pedrocunha/repos/keylink.actor-simplifications
mise run anchors; echo "anchors exit=$?"
mise run docs;    echo "docs exit=$?"
bash scripts/replay-fate-breaks.sh 2>&1 | tee /tmp/replay-before.txt; echo "replay exit=${PIPESTATUS[0]}"
```

Expected: `anchors` exit 0, `docs` exit 0. **Record the replay exit code
whatever it is.** A red baseline is still a baseline, and without it the
post-rename run means nothing.

**WARNING: do not pipe a gate through `tail`.** Use `tee`. A pipe discards the
output the gate exists to print.

## Task 2: make `anchors` report a missing target instead of crashing

**Verdict: do this, and do it in its own commit, before the rename.**

The rename forces rows D1, D2 and D3 to be repointed anyway, so this fix buys
nothing for D itself. It is worth doing for what it buys everybody else. A gate
that dies with a traceback reports nothing about the other 48 rows, and the next
contributor who moves any of the 14 target files meets a Python stack trace
instead of a named failure with a remedy. The file already argues this against
itself at lines 825-830: *"A passing gate advertising rot."* Every other check in
it prints its own remedy.

It is its own commit so that the rename's diff stays a diff of names only, which
is what makes the §6 criterion *"no line changes anything but a name"* checkable.

**Files:**
- Modify: `.mise/tasks/anchors` (three read sites plus one helper)
- Modify: `.mise/tasks/anchors-selftest` (two new checks)

- [ ] **Step 1: Write the failing check first**

Add this to `.mise/tasks/anchors-selftest`, after the existing `RECORD_BLOCKS`
perturbation block. The file already holds `source = TASK.read_text()` at line
52, and `run()` and `check()` are defined at lines 56 and 70.

```python
# ── a registry path that no longer exists must REPORT, not crash ──────────────
#
# Three reads take their path from `ANCHORS` - two `cache` reads for `target` and
# `quoted_spans` for `lives_in` - so any rename can point a row at nothing.
# `read_text` raised there and the task died with a traceback BEFORE printing one
# check: a gate that reports nothing at all about the other rows, which is worse
# than reporting one failure. Watched red by injecting the row below and reading
# `Traceback` out of the output.
#
# The row is INJECTED rather than edited in place, so this check does not carry a
# path of its own and cannot rot when a file is renamed.
INJECTED = '("ZZ", "keylink/tests/cycles.rs", "keylink/src/no-such-file.rs", "zz"),'
code, out = run(source.replace("ANCHORS = [\n", f"ANCHORS = [\n    {INJECTED}\n", 1))
check(
    "a missing TARGET file fails the gate",
    code == 1,
    f"exit {code}, expected 1",
)
check(
    "a missing TARGET file does not crash the task",
    "Traceback" not in out,
    "the task died with a traceback, so it reported nothing about the other rows",
)
check(
    "a missing TARGET file is named as itself",
    "DOES NOT EXIST" in out and "keylink/src/no-such-file.rs" in out,
    "expected a FAIL naming the path that is gone",
)

LIVES = '("ZY", "keylink/tests/no-such-record.rs", "keylink/src/lib.rs", "zy"),'
code, out = run(source.replace("ANCHORS = [\n", f"ANCHORS = [\n    {LIVES}\n", 1))
check(
    "a missing LIVES_IN file does not crash the task",
    code == 1 and "Traceback" not in out,
    f"exit {code}; `quoted_spans` reads `lives_in` and is the third unguarded read",
)
```

- [ ] **Step 2: Run it and watch it fail**

```bash
cd /Users/pedrocunha/repos/keylink.actor-simplifications
mise run anchors-selftest; echo "exit=$?"
```

Expected: FAIL on `a missing TARGET file does not crash the task`, and the detail
line reports the traceback. **If it passes, the check is not exercising the
defect. Stop and fix the check before touching `anchors`.**

- [ ] **Step 3: Add the guard to `.mise/tasks/anchors`**

Insert this helper immediately after the two cache declarations at lines 795-796:

```python
def registry_file(path: str, record: str, role: str) -> str | None:
    """Read a path that came from `ANCHORS`, or REPORT that it is gone.

    Three reads below take their path from the registry, so a rename anywhere in
    the tree can point a row at nothing. `read_text` raises there, and this task
    then dies before printing a single check - a gate that reports nothing at all,
    which is strictly worse than one that reports a failure.
    """
    if (ROOT / path).is_file():
        return (ROOT / path).read_text()
    print(f"FAIL {record}: the {role} file DOES NOT EXIST", file=sys.stderr)
    print(f"       path: {path}", file=sys.stderr)
    print("       A rename moved it, or it was deleted. Repoint this row to where",
          file=sys.stderr)
    print("       the code lives now. Do NOT delete the row - that leaves the record",
          file=sys.stderr)
    print("       unchecked rather than correct.", file=sys.stderr)
    print(file=sys.stderr)
    return None
```

Then guard each of the three reads. At the first cache read:

```python
for record, lives_in, target, literal in ANCHORS:
    if target not in cache:
        body = registry_file(target, record, "TARGET")
        if body is None:
            bad += 1
            unresolved += 1
            continue
        cache[target] = body
    found = cache[target].count(literal)
```

At the second cache read, inside the fragment loop:

```python
for record, lives_in, target, literal in ANCHORS:
    if target not in cache:
        body = registry_file(target, record, "TARGET")
        if body is None:
            continue  # already reported by the resolution loop above
        cache[target] = body
    body = cache[target]
```

And at `quoted_spans`, change its body to take the same route:

```python
def quoted_spans(path: str, record: str) -> set[str] | None:
    """Every backticked span inside a comment of `path`, whitespace-flattened."""
    text = registry_file(path, record, "LIVES_IN")
    if text is None:
        return None
    return set().union(*(spans(b) for b in comment_blocks(text)))
```

with its caller:

```python
for record, lives_in, target, literal in ANCHORS:
    if lives_in not in spancache:
        got = quoted_spans(lives_in, record)
        if got is None:
            bad += 1
            continue
        spancache[lives_in] = got
```

- [ ] **Step 4: Run the selftest and watch it pass**

```bash
cd /Users/pedrocunha/repos/keylink.actor-simplifications
mise run anchors-selftest; echo "exit=$?"
mise run anchors;          echo "exit=$?"
```

Expected: both exit 0, and the selftest prints the three new `ok:` lines.

- [ ] **Step 5: Record the break where `anchors` checks it**

Add this record immediately above the `INJECTED` line in `anchors-selftest`:

```python
# WATCHED RED. The break is a DELETION and spans the whole helper, so no
# one-line anchor exists and this record DESCRIBES the edit rather than quoting
# it - `CONTRIBUTING.md:294`'s second case. To reproduce: delete `registry_file`
# from `.mise/tasks/anchors` and restore both cache reads to
# `cache[target] = (ROOT / target).read_text()`. The three checks below then read
#
#     FAIL a missing TARGET file does not crash the task
#          the task died with a traceback, so it reported nothing about the other rows
#
# and the task's output ends at `FileNotFoundError` with no check printed.
```

**CAUTION: this record moves `RECORD_BLOCKS` if it names the harness.** The text
above does not contain the string `break-once.sh`, so the population count does
not change. If you rewrite it and it does name the harness, `mise run anchors`
fails and prints the number to set. Read that number off the failing run. Never
compute it.

- [ ] **Step 6: Commit**

```bash
cd /Users/pedrocunha/repos/keylink.actor-simplifications
git add .mise/tasks/anchors .mise/tasks/anchors-selftest
git commit -m "fix(anchors): a registry path that is gone crashed the gate, so it reported nothing"
```

Expected: `pre-commit` runs `anchors` and `anchors-selftest` and both pass.

**A `RECORD_BLOCKS` change is legitimate in THIS commit and never in Task 3's.**
The §6 criterion that the constant did not move grades the rename commit alone.

## Task 3: the rename

**Files:**
- Rename: `keylink/src/local.rs` to `keylink/src/turns.rs`
- Modify: `keylink/src/lib.rs`, `keylink/src/actor/remote.rs`,
  `keylink/src/actor/registry.rs`, and 19 more files carrying the name
- Modify: `scripts/replay-fate-breaks.sh`, `.mise/tasks/anchors`

- [ ] **Step 1: Move the file**

```bash
cd /Users/pedrocunha/repos/keylink.actor-simplifications
git mv keylink/src/local.rs keylink/src/turns.rs
git status --short
```

Expected: one line, `R  keylink/src/local.rs -> keylink/src/turns.rs`.

- [ ] **Step 2: Repoint the module path in code**

```bash
cd /Users/pedrocunha/repos/keylink.actor-simplifications
rg -l 'crate::local::' keylink/src | xargs perl -pi -e 's/crate::local::/crate::turns::/g'
rg -n 'crate::turns::' keylink/src
```

Expected: exactly two lines, in `actor/remote.rs` and `actor/registry.rs`.

- [ ] **Step 3: Repoint every path spelling outside `docs/`**

```bash
cd /Users/pedrocunha/repos/keylink.actor-simplifications
rg -l 'local\.rs' keylink scripts .mise | xargs perl -pi -e 's/local\.rs/turns.rs/g'
rg -c 'turns\.rs' keylink scripts .mise
```

Expected: `scripts/replay-fate-breaks.sh` 16, `.mise/tasks/anchors` 5, and the
per-file counts from §6.

**CAUTION: the pattern is `local\.rs` with an escaped dot, and never bare
`local`.** The words `deliver_to_local`, `LocalActor`, `Origin::local` and
`LOCAL_MAILBOX_DEPTH` all contain `local` and must not change. A rename whose old
name prefixes a sibling rewrites text no compiler reads.

- [ ] **Step 4: Repoint the two `lib.rs` declarations, by hand**

Delete `mod local;` and add `mod turns;` in its alphabetical place, between
`mod testing;` and `mod watch;`.

Delete the four-line `pub use local::{...};` block and add it back as
`pub use turns::{...};` in its alphabetical place, after
`pub use subscribers::Subscribers;`. **The 17 names inside the braces do not
change.**

- [ ] **Step 5: Rewrite the header's first line, and only that line**

`keylink/src/turns.rs:1` currently reads:

```rust
//! An actor that never leaves the process.
```

Replace it with a line that is true of both paths:

```rust
//! Every actor's turn: one loop, one dispatch, and both ways in.
```

**Leave the other 175 header lines alone.** In particular, leave line 49's
*"its mailbox through a private [`Inbox`] trait"* exactly as it is. That sentence
is one of this rename's reasons.

- [ ] **Step 6: Prove no spelling of the old name survives**

```bash
cd /Users/pedrocunha/repos/keylink.actor-simplifications
rg -n 'local\.rs|crate::local|\bmod local\b|pub use local::' keylink scripts .mise README.md CONTRIBUTING.md
echo "exit=$?"
```

Expected: no output, exit 1. **`rg` exits 1 when it matches nothing, so exit 1 is
the pass here.**

- [ ] **Step 7: Format, then compile**

```bash
cd /Users/pedrocunha/repos/keylink.actor-simplifications
cargo fmt
git diff --stat -- keylink/src/lib.rs keylink/src/actor/remote.rs keylink/src/actor/registry.rs
cargo check --all-features
```

Expected: `cargo check` passes. If `cargo fmt` moved a `use` line, keep its
placement.

- [ ] **Step 8: Check the changed-line prediction before committing**

```bash
cd /Users/pedrocunha/repos/keylink.actor-simplifications
git add -A
git diff --cached --shortstat --find-renames=100%
git diff --cached --stat --find-renames=100% | tail -5
```

Expected: 23 files changed, 86 to 96 insertions, 86 to 96 deletions, and the
rename shown as `keylink/src/{local.rs => turns.rs}`.

If the number falls outside the band, stop. Find the cause. Do not widen the
band.

- [ ] **Step 9: Prove the diff carries names and nothing else**

```bash
cd /Users/pedrocunha/repos/keylink.actor-simplifications
git diff --cached -U0 | rg '^[+-][^+-]' | rg -vi 'local|turns'
```

Expected: exactly one line,
`-//! An actor that never leaves the process.`

Any other line is a behavior change that slipped in. Remove it.

- [ ] **Step 10: Prove `RECORD_BLOCKS` did not move**

```bash
cd /Users/pedrocunha/repos/keylink.actor-simplifications
git diff --cached -- .mise/tasks/anchors | rg 'RECORD_BLOCKS'
echo "exit=$?"
```

Expected: no output, exit 1.

- [ ] **Step 11: Prove `docs/` is untouched**

```bash
cd /Users/pedrocunha/repos/keylink.actor-simplifications
git diff --cached --name-only | rg '^docs/'
echo "exit=$?"
```

Expected: no output, exit 1.

- [ ] **Step 12: Run the gates the rename can break**

```bash
cd /Users/pedrocunha/repos/keylink.actor-simplifications
mise run anchors; echo "anchors exit=$?"
mise run docs;    echo "docs exit=$?"
```

Expected: both exit 0. `mise run docs` is the criterion the name was chosen
against. If it reports `ambiguous link`, the name is wrong and not the links.

- [ ] **Step 13: Run the harness no gate runs**

```bash
cd /Users/pedrocunha/repos/keylink.actor-simplifications
bash scripts/replay-fate-breaks.sh 2>&1 | tee /tmp/replay-after.txt
echo "replay exit=${PIPESTATUS[0]}"
diff <(rg -o 'exit=[0-9]+' /tmp/replay-before.txt) <(rg -o 'exit=[0-9]+' /tmp/replay-after.txt) && echo "IDENTICAL exit sequence"
```

Expected: the same exit sequence as the Task 1 Step 3 baseline. A break that was
red before must be red after, at the same recorded sentence.

- [ ] **Step 14: Commit**

Write the message to `.tmp/item-d/commit-msg.txt` first, then commit from the
file. Never pass prose through an inline shell string.

```text
refactor(turns): local.rs held both keyed entry points under a name that says local

`local.rs` declared `run_keyed` and `run_keyed_supervised`, both re-exported from
`lib.rs`. A reader looking for the keyed loop does not open a file called `local`.

A rename and nothing else. No file is split: Wave 0 refused the split and recorded
why - 14 visibility widenings, a carved 176-line header, 80 intra-doc links across
a new boundary, and `local.rs:49`'s claim that `Inbox` is private turned false.

The 17 names re-exported at `lib.rs` are unchanged, so no public path moves.

NOT `turn.rs`, and the reason is measured rather than aesthetic. Three doc comments
link the function as `[`turn`](crate::turn)`. A module named `turn` gives that path
a second resolution, and rustdoc refuses it under the flags `mise run docs` uses:

    error: `crate::turn` is both a function and a module
      |                    ^^^^^^^^^^^ ambiguous link

`--document-private-items` is what makes that visible. Without it the same tree
documents green, which is `CONTRIBUTING.md:236`'s point about this gate.

`docs/**` keeps the old name on 101 lines, deliberately. `CONTRIBUTING.md:322`:
an RFC is appended to and never edited to match new code.

RECORD_BLOCKS is unchanged. The population walk globs `keylink/**/*.rs` and counts
blocks containing `break-once.sh`, so it is path-insensitive and a rename moves
no block.
```

## Task 4: the full gate and the proof block

- [ ] **Step 1: Run the whole gate**

```bash
cd /Users/pedrocunha/repos/keylink.actor-simplifications
mise run gate 2>&1 | tee /tmp/gate.txt
echo "gate exit=${PIPESTATUS[0]}"
tail -30 /tmp/gate.txt
```

Expected: exit 0.

**Never pass `--no-verify`.** `CONTRIBUTING.md:156` lists the only legitimate
reasons, and none of them is a rename.

- [ ] **Step 2: Report the memory slope**

```bash
cd /Users/pedrocunha/repos/keylink.actor-simplifications
cargo test --test actor_memory -- --nocapture 2>&1 | tee /tmp/slope.txt
rg -i 'slope|bytes per' /tmp/slope.txt
```

Report the number in the final report. Wave 0 measured that module placement
cannot reach generator layout, so an unchanged slope is the expectation and not a
surprise.

- [ ] **Step 3: Print every criterion from §6 with its machine output**

Collect the output of each command in §6 into one block. Print it. Do not
paraphrase a result.

---

## Assumptions

1. **`compose-group` merges before this branch rebases.** Every line number here
   comes from `0ab8366` and drifts after that merge. Task 1 Step 2 re-derives
   them.
2. **`docs/**` keeps the name `local.rs`,** on the authority of
   `CONTRIBUTING.md:322`. 101 lines across 8 files are left deliberately stale as
   a historical record.
3. **The prose class is repointed in full,** all 43 lines. The alternative is 43
   sentences in shipped module documentation naming a file that does not exist.
4. **`scripts/replay-fate-breaks.sh` is edited,** although `scripts/**` is not in
   the goal's "May modify" blast radius. Wave 0's amended shape, item 5,
   instructs this edit by name, and Wave 0 is authoritative for item D. The goal
   forbids only `scripts/break-once.sh`, which this plan does not touch.
5. **`cargo fmt` decides `use` placement.** The 86-to-96 band exists for that.
6. **The new header first line is the only prose line rewritten** inside the
   moved file, beyond the 7 break-record paths.

## Open questions, annotated rather than asked

1. **Two commits or one?** The goal says "One atomic commit per item". This plan
   uses two: the `anchors` reporting fix, then the rename. The reason is that the
   §6 criterion *"no line changes anything but a name"* is not checkable if the
   gate fix rides along. **If the one-commit rule is held strictly,** squash Task
   2 into Task 3 and amend that criterion to allow the `.mise/tasks/` hunk.
2. **Is the `anchors` fix in scope for item D at all?** It is a gate change and
   not a rename. It is proposed because the rename is what exposed the crash, and
   because the crash costs the next contributor more than it costs this one. It
   can be split into its own pull request without changing anything else in this
   plan.
3. **`replay-fate-breaks.sh` has no measured baseline in this plan.** Running it
   replays roughly 23 breaks and rebuilds the crate for each. Task 1 Step 3
   records the baseline before any edit. **Until that runs, no claim about its
   pass or fail state is supported.**
4. **The `pub use turns::{...}` block's final position is not decided here.**
   Global alphabetical order puts it between `subscribers` and `watch`. A
   `#[cfg(feature = "persist")]` attribute at `lib.rs:325` splits the `use` group
   in two, so `rustfmt` may not be able to place it there. Task 3 Step 7 settles
   it.
5. **Wave 0's brief said the `pub use` block re-exports 15 names. It is 17,**
   measured in this run. Nothing depends on the difference, because none of the
   names changes.

## What would make this plan wrong

If `mise run docs` reports an ambiguous link under `turns`, the probe in §2 did
not model the tree. Then the name is wrong, and the fix is a different name, and
never a `fn@` disambiguator added to make one name fit.
