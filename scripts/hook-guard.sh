#!/usr/bin/env bash
# Do the hooks gate the thing they are ACTUALLY about?
#
# `.githooks/pre-commit` is about the commit it is writing. `.githooks/pre-push`
# is about the commit it is delivering. Each hook decides one thing — gate, or do
# not — and this drives that decision from the outside, in a throwaway
# repository, and watches which way it went. It never runs cargo: a stub `mise`
# on `PATH` records that the gate was reached and returns, so what is asserted is
# the hook's DECISION and not clippy's verdict. That is the property the hooks
# own; clippy has its own tests.
#
# ONE harness for both, because both need the same three things: a throwaway
# repository, a stub `mise` that says whether the gate was entered, and a
# counter nobody writes down. A second script would copy all three, and the
# second copy is where the stub stops matching the hook it stands in for.
#
# ══ PART ONE — .githooks/pre-commit ═════════════════════════════════════════
#
# # The case this exists for
#
# `commit --amend` replaces HEAD, so the commit it writes is built on HEAD's
# PARENT. A hook that compares the index against HEAD therefore sees an empty diff
# whenever an amend stages nothing new, and skips — which means **an amend cannot
# repair a commit that was never gated**, one made with `--no-verify` or by an
# older version of this hook's own early exit. Scenario 1 is that case, and it is
# the reason for the file.
#
# # Made red, in both directions
#
# A guard that only proves "the gate ran" is satisfied by a hook that always runs
# it, which would delete the fast path this hook exists to have. So the pair:
#
# EACH RECORD BELOW NAMES THE ROWS IT REDDENS AND COUNTS NOTHING, and that is a
# repair rather than a style. Records 1 and 2 used to say "the other three" and "the
# two negatives". Both were true at five `check` calls and both went stale the
# moment `a8a7343` took it to eight — silently, because a break record is read and
# found plausible rather than executed. **The footer one screen below had exactly
# this defect and was fixed by deriving the number; the fix for prose is to name
# the rows, because a row name survives a new scenario and a count cannot.**
#
# 1. **Revert the fix.** The literal this record searches, carried inline as a
#    backticked span:
#    `amend_base="$(git rev-parse --verify --quiet HEAD~1 || git hash-object -t tree /dev/null)"`
#
#    In `.githooks/pre-commit`, replace that line
#
#        amend_base="$(git rev-parse --verify --quiet HEAD~1 || git hash-object -t tree /dev/null)"
#
#    with `amend_base=""`. Watched: it reddens the amend-of-an-ungated-Rust-commit
#    row, and that row only —
#
#        FAIL an amend of an ungated Rust commit RE-RUNS the gate — the gate did NOT run
#
# 2. **Make it gate everything.** Replace the `if ! grep -qE ... <<<"$linted"; then`
#    line with `if false; then`. Watched: it reddens **every row
#    that expects a skip** — the two ordinary negatives AND the reword, which is the
#    one this record omitted while the reword scenario existed:
#
#        FAIL an ordinary commit staging no linted file skips the gate — the gate ran and should not have
#        FAIL an amend of a commit with no linted file skips the gate — the gate ran and should not have
#        FAIL a rebase -i reword of a Rust commit does NOT run the gate (a replay is not authoring) — the gate ran and should not have
#
#    **THIS RECORD HAS NO REGISTRABLE LITERAL, and that is stated rather than left to
#    be discovered.** Its citation ELIDES the middle of the line with an ellipsis, so
#    there is no literal to register, and inventing one would register a line this
#    record does not quote. Record 4 quotes that SAME line in full, so its anchor already
#    covers the rot this record would be watching for. A record that declares itself
#    covered elsewhere costs a reader nothing; one that looks registrable and is not
#    costs them a measurement.
#
# 3. **Break the rebase arm, either way.** TWO substitutions, so TWO literals and TWO
#    rows — 3a and 3b. **The split is by REPLACEMENT, never by citation**: a row
#    carries one literal, and two breaks that happen to redden the same row are still
#    two breaks. Both carried inline as backticked spans:
#
#    3a `if [ "$rebasing" = no ] && ps -o args= -p "$PPID" 2>/dev/null | grep -qw -- '--amend'; then`
#    3b `if [ -d "$(git rev-parse --git-path rebase-merge)" ] ||`
#
#    In `.githooks/pre-commit`, either drop the guard from the detector by replacing
#
#        if [ "$rebasing" = no ] && ps -o args= -p "$PPID" 2>/dev/null | grep -qw -- '--amend'; then
#
#    with the same line minus its `[ "$rebasing" = no ] && ` — or, and this is the
#    interesting one, replace
#
#        if [ -d "$(git rev-parse --git-path rebase-merge)" ] ||
#
#    with `if [ -d .git/rebase-merge ] ||`. Watched, and BOTH produce the same
#    single row:
#
#        FAIL a rebase -i reword of a Rust commit does NOT run the gate (a replay is not authoring) — the gate ran and should not have
#
#    The second break is why scenarios 5 and 6 run in a linked worktree. In an
#    ordinary clone the literal spelling is correct and this row stays green, so
#    the guard would certify a hook that is dead in every worktree this repo is
#    developed in.
#
# 4. **Restore the pipe, which is the shape this file used to carry.** `main` carries
#    the HERE-STRING, verified by reading `main:.githooks/pre-commit` rather than
#    inferred: the fix went in with the merge that took this hook to the here-string,
#    and the two branches' code is identical once comments are stripped. This clause
#    used to say `main` carried the pipe, which read as "the 64 KiB bypass is live on
#    the default branch" — the strongest possible reason to hurry a merge, and false.
#    The literal
#    this record searches, carried inline as a backticked span — and it is the same
#    line record 2 cites with an elision, which is why that record has no row of its
#    own and points here:
#    `if ! grep -qE '\.rs$|Cargo\.toml$|Cargo\.lock$|rustfmt\.toml$' <<<"$linted"; then`
#
#    In `.githooks/pre-commit`, replace that line
#
#        if ! grep -qE '\.rs$|Cargo\.toml$|Cargo\.lock$|rustfmt\.toml$' <<<"$linted"; then
#
#    with
#
#        if ! printf '%s\n' "$linted" | grep -qE '\.rs$|Cargo\.toml$|Cargo\.lock$|rustfmt\.toml$'; then
#
#    Watched — one row, and it is a row no other scenario in this file can reach:
#
#        FAIL a commit staging Rust beside a path list larger than one pipe buffer runs the gate — the gate did NOT run
#
#    Every other row here stages a handful of files, which is under one pipe buffer
#    and therefore green against the defective spelling. That is why scenario 7
#    exists at all: the bug is invisible at every size the rest of this script uses,
#    and it fails in the direction that reports success.
#
# 5. **Weaken the LIST the commit hook execs.** The literal this record searches,
#    carried inline as a backticked span:
#    `    exec mise run pre-commit`
#
#    In `.githooks/pre-commit`, replace that line with `    exec mise run fmt-check` —
#    one task instead of five. Watched, one row, and it is the row added for it:
#
#        FAIL the commit hook runs the WHOLE pre-commit list and not a subset
#
#    **Before that row existed this break reddened NOTHING**, from a green control.
#    `check yes` asks whether the stub was REACHED and never which task reached it, so
#    every `check yes` in Part One stayed green while the hook ran a single formatter.
#    H13 is the same hole in the push hook, where it is worse.
#
# Quoting those literals is safe for the reason `break-once.sh` cares about: this
# record is not in the file whose literal it quotes, so the anchor stays unique in
# its target.
#
# ── THESE ROWS ARE REGISTERED. WHAT FOLLOWS IS HISTORY, AND IT IS KEPT AS HISTORY
#
# **Read this before you touch a row.** If `mise run anchors` sent you here with
# `this task's literal is NOT QUOTED by its own record`, the remedy is the rule two
# paragraphs down — quote the literal inline. **It is never to delete the row.** An
# earlier version of this heading said none of these rows was in `.mise/tasks/anchors`
# and that registering them was not possible, which pointed a reader who followed the
# gate's own instruction at exactly the one action the registry argues against.
#
# Every record in this file is registered EXCEPT record 2, which has no literal to
# register and says so where a reader meets it. The rows are named rather than counted,
# because a count here rots the next time a record is added: **H1 through H12** against
# `.githooks/pre-push`, **records 1, 3a, 3b and 4** against `.githooks/pre-commit`, and
# **I1, I2 and I3** against `scripts/install-hooks.sh`. `.mise/tasks/anchors` is the
# source of truth for the current set; this list is a map, not a ledger.
#
# What changed, since the old text argued from two facts that were true when written:
#
#   * `markers_for` now picks the comment markers per FILE, so a hash-commented file
#     is read with `#` and a `.rs` file keeps the three strict markers. A shell record
#     is therefore visible to the record-quote check at all, which it was not.
#   * the population walk covers what a record can live in rather than `*.rs` under
#     the two crates, so these blocks are in the denominator too.
#
# **The count that used to sit here is deleted rather than re-measured, and that is
# the lesson worth keeping.** It said `0` spans in each shell file "against 228 in
# `keylink/src/rpc.rs`". The 0 was true of the old marker set and is false of the new
# one. The 228 matched no unit at either end of this branch — `rpc.rs` reads 304
# distinct spans and 552 occurrences at the tip — and it was a figure about a file this
# record does not own, which is the whole reason it rotted unnoticed. A quantity you do
# not control does not belong in a record; the mechanism does.
#
# **And the ordering is the finding, not a detail:** the commit that registered these
# rows touched `.mise/tasks/anchors` and nothing else. The enabling commit never opened
# the file it enabled, so every claim in this header stayed as it was written while the
# thing it described was replaced underneath it.
#
# **THE RULE FOR EVERY RECORD IN THIS FILE, stated as a property rather than as a list,
# because the next author's record is not in anybody's list:** a literal that is
# registered, or may be, carries an INLINE backticked span somewhere in its record —
# whatever else that record does to present it. The record-quote check compares
# backticked spans by EQUALITY, so an INDENTED DISPLAY BLOCK is invisible to it: a
# record showing its literal that way gets accused of not quoting a literal it quotes
# plainly, and every remedy the failure prints is then wrong. Keep the display block
# where it reads better and put the inline span beside it. A long literal may WRAP
# across comment lines, because the reader joins a block's lines before it cuts spans
# out — H11's is wrapped for exactly that reason.
#
# Two consequences that are not about backticks at all:
#
#   * A record naming TWO substitutions needs TWO spans and registers as TWO rows,
#     because a row carries one literal. **The split is by REPLACEMENT, never by
#     citation** — record 3 is the one here, and its rows are 3a and 3b.
#   * A record whose citation ELIDES part of the line has NO literal to register, and
#     manufacturing one would register a line the record does not quote. Record 2 is
#     that case; it says so where a reader meets it, and names the record whose anchor
#     covers the same line.
#
# Measured with the shipped reader lifted out of `.mise/tasks/anchors` rather than
# reasoned about, over EVERY record in this file: records 1, 3a, 3b, 4, H11 and H12
# displayed their literal and matched NOTHING, not even by containment, until this rule
# was applied to each; H1, H4 and the other H rows already matched inline, as did I1,
# I2 and I3. Record 2 is the only record here that cannot comply, for the reason above.
# `scripts/break-hook-record.sh` remains the on-demand half: a replay aborts when a
# quoted literal is missing or no longer unique, which is the same precondition
# `anchors` now checks on every gate. What neither of them checks is REDNESS — that the
# break still fails the row its record names.
#
# `rebasing=no` counts **1** in `.githooks/pre-commit`, measured with `grep -cF`
# after this record was written. The near-miss is worth naming, because the first
# draft of this paragraph asserted it counted 2 and was wrong: the test below the
# assignment reads `[ "$rebasing" = no ]`, which does NOT contain the literal
# `rebasing=no`. A plausible second occurrence that is not one — counted, not read.
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
hook="$root/.githooks/pre-commit"
push_hook="$root/.githooks/pre-push"
for candidate in "$hook" "$push_hook"; do
    [ -x "$candidate" ] || { echo "no executable hook at $candidate" >&2; exit 2; }
done

scratch="$(mktemp -d)"
trap 'rm -rf "$scratch"' EXIT

# A stub `mise` that records being reached. `command -v mise` in the hook finds
# this one, so the hook takes its `exec mise run pre-commit` branch and the marker
# is proof the gate was entered.
mkdir -p "$scratch/bin"
cat >"$scratch/bin/mise" <<'STUB'
#!/usr/bin/env bash
echo "$*" >>"$GATE_MARKER"
STUB
chmod +x "$scratch/bin/mise"

repo="$scratch/repo"
mkdir -p "$repo"
cd "$repo"
git init -q .
git config user.email guard@example.com
git config user.name Guard
git config commit.gpgsign false
# The hook under test, and NOTHING chained behind it: `hooks.chain` is
# deliberately unset here so a developer's global secret scanner is not invoked
# against a throwaway repository.
mkdir -p "$scratch/hooks"
cp "$hook" "$scratch/hooks/pre-commit"
git config core.hooksPath "$scratch/hooks"
git config --unset-all hooks.chain 2>/dev/null || true

export GATE_MARKER="$scratch/gate-ran"
export PATH="$scratch/bin:$PATH"

fresh() { : >"$GATE_MARKER"; }
ran() { [ -s "$GATE_MARKER" ]; }

fail=0
# COUNTED rather than written down. The footer used to say "all four decisions
# correct" while five rows printed above it — a count contradicting the very
# output it summarised, in the one file whose job is to check a claim against
# behaviour. Deriving it means the next scenario cannot make it wrong again.
checks=0
check() { # check <expect ran: yes|no> <description>
    checks=$((checks + 1))
    if [ "$1" = yes ]; then
        if ran; then echo "  ok   $2"; else echo "  FAIL $2 — the gate did NOT run"; fail=1; fi
    elif ran; then
        echo "  FAIL $2 — the gate ran and should not have"; fail=1
    else
        echo "  ok   $2"
    fi
}

# `check` asks one question and Part Two needs others — an exit status, a
# message, two files being byte-identical. `assert` takes the claim as a COMMAND
# and uses its exit status, so a row reads as the thing it measures rather than
# as a boolean somebody computed above it. It shares `checks`, so the footer
# stays derived across both parts.
assert() { # assert <description> <command...>
    local description="$1"
    shift
    checks=$((checks + 1))
    if "$@"; then
        echo "  ok   $description"
    else
        echo "  FAIL $description"
        fail=1
    fi
}

echo "── PART ONE: .githooks/pre-commit gates the commit it is writing"

# A base commit, so HEAD~1 exists for the amends below.
echo "readme" >README.md
git add README.md
fresh
git commit -qm "base"

# ── 1. THE CASE. A Rust file committed with the gate bypassed, then amended with
#       nothing newly staged. The amend must gate: it is writing a commit whose
#       tree carries Rust that this hook has never seen.
echo "fn a() {}" >a.rs
git add a.rs
fresh
git commit -q --no-verify -m "add a.rs, ungated"
check no "a --no-verify commit does not run the gate (it is a bypass, by design)"
fresh
git commit -q --amend --no-edit
check yes "an amend of an ungated Rust commit RE-RUNS the gate"

# ── 2. An ordinary commit that stages Rust. The path that always worked; here so
#       that a fix breaking it cannot pass.
echo "fn b() {}" >b.rs
git add b.rs
fresh
git commit -qm "add b.rs"
check yes "an ordinary commit staging Rust runs the gate"
#       AND THE MARKER'S CONTENT, not merely its existence. `ran()` asks only whether
#       the stub was reached, so until this row existed the hook could `exec mise run`
#       ANY task and every `check yes` in Part One stayed green. Measured: replacing
#       this hook's `exec mise run pre-commit` with `exec mise run fmt-check` reddened
#       NOTHING in this file. One row per hook is enough, because one line in each hook
#       decides what the marker holds — see record 5.
assert "the commit hook runs the WHOLE pre-commit list and not a subset" \
    grep -qx 'run pre-commit' "$GATE_MARKER"

# ── 3. An ordinary commit that stages nothing linted. The fast path, which is the
#       reason this hook has a filter at all.
echo "notes" >notes.txt
git add notes.txt
fresh
git commit -qm "add notes.txt"
check no "an ordinary commit staging no linted file skips the gate"

# ── 4. An amend of a commit that touched nothing linted. Proves the amend arm is
#       still a filter and not an unconditional gate.
fresh
git commit -q --amend -m "add notes.txt, reworded"
check no "an amend of a commit with no linted file skips the gate"

# ── 5 and 6. A REBASE IS A REPLAY, and these two run IN A LINKED WORKTREE on
#       purpose. The hook asks "is a rebase in progress?" through `git rev-parse
#       --git-path`, and the reason is only observable here: in a linked worktree
#       `.git` is a FILE, so a literal `[ -d .git/rebase-merge ]` is false even in
#       the middle of a rebase. In an ordinary clone both spellings work, so this
#       pair would pass against the broken one and be decoration.
#
#       Both scenarios replay commits that DO touch Rust. That is the whole
#       design: a non-Rust commit would skip on the filter alone and prove
#       nothing, so Rust makes the rebase arm the only thing that can explain the
#       outcome.
#
#       `awk` rather than `sed -i`, because in-place `sed` needs an empty argument
#       on macOS and refuses it on GNU, and this script should not pick a side.
cat >"$scratch/bin/todo-first" <<'ED'
#!/usr/bin/env bash
# Rewrite the first `pick` in a rebase todo to $TODO_VERB. $1 is the todo file.
awk -v verb="$TODO_VERB" 'NR==1 && $1=="pick" { $1=verb } { print }' "$1" >"$1.new"
mv "$1.new" "$1"
ED
cat >"$scratch/bin/set-msg" <<'ED'
#!/usr/bin/env bash
printf 'reworded by the guard\n' >"$1"
ED
chmod +x "$scratch/bin/todo-first" "$scratch/bin/set-msg"

wt="$scratch/wt"
git worktree add -q -b replay "$wt" HEAD
cd "$wt"
echo "fn r1() {}" >r1.rs
git add r1.rs
git commit -q --no-verify -m "r1, ungated Rust"
echo "fn r2() {}" >r2.rs
git add r2.rs
git commit -q --no-verify -m "r2, ungated Rust"

# ── 5. A `reword` step. The sequencer spawns `git commit --amend ... ` WITHOUT
#       `--no-verify`, so the hook does run and must decide to skip. Reverting the
#       rebase arm makes this the failing row, because the amend detector then
#       fires and the replayed commit carries Rust.
fresh
TODO_VERB=reword GIT_SEQUENCE_EDITOR=todo-first GIT_EDITOR=set-msg \
    git rebase -i HEAD~2 >/dev/null 2>&1
check no "a rebase -i reword of a Rust commit does NOT run the gate (a replay is not authoring)"

# ── 6. An `edit` stop, where the developer really is authoring: they stage Rust
#       and amend by hand. This must STILL gate, and it is why the rebase arm
#       clears the detector rather than exiting the hook — the ordinary
#       index-against-HEAD path sees the staged file and runs.
fresh
TODO_VERB=edit GIT_SEQUENCE_EDITOR=todo-first \
    git rebase -i HEAD~2 >/dev/null 2>&1
check no "the edit stop itself writes no commit, so no gate"
echo "fn newly_authored() {}" >>r1.rs
git add r1.rs
fresh
git commit -q --amend --no-edit
check yes "an amend at an edit stop that STAGES Rust still runs the gate"
GIT_EDITOR=: git rebase --continue >/dev/null 2>&1
cd "$repo"

# ── 7. A STAGED LIST LARGER THAN ONE PIPE BUFFER, which every scenario above is
#       too small to see. The filter used to be `printf ... | grep -qE ...`, and
#       `grep -q` exits at its first match and closes the pipe — so `printf` took
#       SIGPIPE, `pipefail` failed the pipeline, `!` inverted that, and the hook
#       skipped the gate on a commit staging Rust while exiting 0.
#
#       THE RUST FILE HAS TO SORT EARLY, and that is the whole scenario rather than
#       a detail. Git lists staged paths in sort order, so with the match at the END
#       grep reads the entire list, never closes the pipe early, and this row stays
#       green against the defect. `aa.rs` sorts before `big/`.
#
#       Sized well above a 64 KiB pipe rather than just over it: Linux allows a pipe
#       to be enlarged to 1 MiB, so a payload tuned to this machine's capacity would
#       be a row that passes on somebody else's. No figure is written here — the row
#       PRINTS the size it actually built, which cannot go stale the way a number in
#       this comment would.
#
#       Index entries written straight to the index with `--index-info`, so twelve
#       thousand paths cost one process and no files on disk. It is LAST in Part One
#       because it leaves those paths in the index with no working-tree file behind
#       them; Part Two builds its own repositories and never sees this one.
blob="$(git hash-object -w --stdin </dev/null)"
python3 - "$blob" <<'PY' | git update-index --index-info
import sys
blob = sys.argv[1]
pad = "q" * 200
for i in range(12000):
    sys.stdout.write(f"100644 {blob}\tbig/{pad}{i:06d}.txt\n")
PY
echo "fn wide() {}" >aa.rs
git add aa.rs
wide_bytes="$(git diff --cached --name-only --diff-filter=ACMR | wc -c | tr -d ' ')"
fresh
git commit -qm "a wide commit that also stages Rust"
check yes "a commit staging Rust beside a path list larger than one pipe buffer runs the gate"
# Printed rather than asserted against a threshold. The size that matters is the
# kernel's pipe capacity, which this script cannot read, so the row's job is to be
# far above any of them and the number's job is to let a reader confirm it was.
echo "       (the staged path list measured $wide_bytes bytes)"

# ══ PART TWO — .githooks/pre-push ═══════════════════════════════════════════
#
# A push has TWO subjects, and this hook is the only place they can be compared.
# Git resolves the refspec to a sha and writes that sha on the hook's stdin; the
# gate builds the WORKING TREE. So "the gate went green" and "the ref update was
# delivered" are statements about different commits unless something checks that
# they are the same commit.
#
# Real pushes, to bare repositories under the scratch directory. A push to a
# local bare repo runs `pre-push` exactly as a push to GitHub does, with the same
# stdin, and it needs no network and leaves no remote branch to clean up.
#
# One scenario cannot use a push and says so where it runs: git never writes ZERO
# update lines, so the only way to present the hook with an empty read is to
# invoke it directly.
#
# ── MADE RED, ONE BREAK PER CLAIM, AND EVERY BREAK BELOW WAS RUN ─────────────
#
# EACH RECORD NAMES THE ROWS IT REDDENS AND COUNTS NOTHING, for the reason Part
# One's header gives: a row name survives a scenario being added and a count does
# not.
#
# The breaks were applied to a COPY. Both hooks and this script were copied into a
# temporary directory — `root` above resolves them relative to this file, so a
# copy of the three is enough — and the perturbation went into the copy. The tree
# was never written to, so an interrupted run could not leave a stale restore
# behind. Quoting the literals here is safe for `break-once.sh`'s reason: this
# record is not in the file whose lines it quotes, so each anchor stays unique in
# its target.
#
# H1. **Revert the capture.** In `.githooks/pre-push`, replace
#     `updates="$(cat)"` with `updates=""`. Watched — every row that depends on
#     knowing what the push carries:
#
#         a deletion-only push SKIPS the gate
#         a skipped gate says it was skipped
#         a skipped gate says why
#         a deletion-only push with a chain configured still SKIPS the gate
#         the chained hook is handed the deletion marker
#         the chained hook is handed the deleted ref
#         a delivered sha that is not HEAD never reaches the gate
#         a delivered sha that is not HEAD is REFUSED
#         the refusal says what it refused
#         the refusal names the delivered sha
#         the refusal names HEAD
#         the refusal names the bypass
#         the chained hook receives the bytes git sent
#         a deletion-only push in a SHA256 repository SKIPS the gate
#         the sha256 deletion-only push says it skipped
#         a push from a linked worktree of the OWNING checkout's HEAD never reaches the gate
#         a push from a linked worktree of the OWNING checkout's HEAD is REFUSED
#         the refusal names the worktree's HEAD and not the owning checkout's
#         an ANNOTATED tag at HEAD never reaches the gate
#         an ANNOTATED tag at HEAD is REFUSED, git having sent the tag object's sha
#         the tag refusal names an OBJECT rather than a commit
#         a force-push REWINDING to an older commit never reaches the gate
#         a force-push of an older commit is REFUSED
#
#     Twenty-three rows, re-measured when scenario 13 was added rather than assumed
#     to be unchanged, exactly as scenario 12 forced a re-measurement before it: a new
#     scenario silently widens the redness of every earlier break that disables the
#     machinery it depends on. The last five rows are those two widenings — three from
#     scenario 12, two from scenario 13.
#
#     `a deletion-only push RUNS the chained hook` does NOT move, and that is
#     worth more than the list: the chain is still REACHED, it is only handed
#     nothing. Reaching a hook and feeding it are two claims, and H5 is the one
#     that separates them.
#
# H2. **Treat an unknown read as all-deletions.** Replace
#     `if [ "$updates_read" -gt 0 ] && [ "$deletions" -eq "$updates_read" ]; then`
#     with `if [ "$deletions" -eq "$updates_read" ]; then` — zero equals zero, so
#     an empty read skips the entire gate. Watched — four rows, the last two added by
#     scenario 14 and measured rather than inferred, because a new scenario widens every
#     earlier break that disables the machinery it depends on:
#
#         empty stdin RUNS the gate
#         the empty read is announced as unknown
#         an UP-TO-DATE push reads empty stdin and RUNS the gate
#         the empty read names NOTHING TO PUSH before it names unknown
#
#     **Scenario 14 is what makes this break's cost concrete.** Before it, every row
#     here drove the hook directly, so this break looked like a guard against a capture
#     bug. It is also a guard against an ordinary `git push` on a current branch
#     silently skipping the gate.
#
# H3. **Skip on ANY deletion instead of every.** Replace the same line with
#     `if [ "$updates_read" -gt 0 ] && [ "$deletions" -gt 0 ]; then`. Watched:
#
#         a delete plus an update in one push RUNS the gate
#
#     **H2 holds the only inline span of the line both records search, so a re-quote
#     there reddens H3 as well.** The record-quote check is per `lives_in` FILE rather
#     than per record, which is why H3 passes today on a span it does not carry — and
#     why widening or rewording H2's citation fails BOTH rows, while the failure text
#     sends the reader to H3, whose record never quoted the anchor. Measured over the
#     registered rows: H3 is the only one with zero own-quote occurrences.
#
# H4. **Drop the comparison against HEAD.** Replace
#     `        if [ "$sha" != "$head_sha" ]; then` with `        if false; then`.
#     Watched — fourteen rows, the last five of them added by scenarios 12 and 13 and
#     measured rather than inferred:
#
#         a delivered sha that is not HEAD never reaches the gate
#         a delivered sha that is not HEAD is REFUSED
#         the refusal says what it refused
#         the refusal names the delivered sha
#         the refusal names HEAD
#         the refusal names the bypass
#         a push from a linked worktree of the OWNING checkout's HEAD never reaches the gate
#         a push from a linked worktree of the OWNING checkout's HEAD is REFUSED
#         the refusal names the worktree's HEAD and not the owning checkout's
#         an ANNOTATED tag at HEAD never reaches the gate
#         an ANNOTATED tag at HEAD is REFUSED, git having sent the tag object's sha
#         the tag refusal names an OBJECT rather than a commit
#         a force-push REWINDING to an older commit never reaches the gate
#         a force-push of an older commit is REFUSED
#
# H5. **Stop re-delivering stdin to the chain.** Replace
#     `        "$chain/pre-push" "$@" <<<"$updates"` with
#     `        "$chain/pre-push" "$@"`, which is what the line was before this
#     change. The chained hook still runs and reads EOF on its first line.
#     Watched:
#
#         the chained hook is handed the deletion marker
#         the chained hook is handed the deleted ref
#         the chained hook receives the bytes git sent
#
#     This is the break the whole forward exists for, and the shape of its
#     redness is the argument: a chained scanner that is reached and starved
#     exits 0, so nothing but a byte comparison can tell it from one that
#     checked.
#
# H6. **Skip the HOOK rather than the GATE.** Replace
#     `    if [ -n "$updates" ]; then` with
#     `    case "$updates" in "(delete) "*) exit 0 ;; esac; if [ -n "$updates" ]; then`
#     — a hook that returns early for a deletion instead of only skipping the
#     gate. Watched:
#
#         a deletion-only push RUNS the chained hook
#         the chained hook is handed the deletion marker
#         the chained hook is handed the deleted ref
#
# H7. **Re-deliver unconditionally.** Replace
#     `    if [ -n "$updates" ]; then` with `    if true; then`, so an empty
#     capture takes the here-string arm and `<<<""` sends a lone LF. The indented
#     spelling counts once; the unindented one guarding the classification loop is a
#     different line. Watched:
#
#         empty stdin hands the chained hook nothing, not a blank line
#
# H11. **Restore the PIPE to the chain.** The literal this record searches, carried
#      inline as one backticked span — the reader joins a block's comment lines before
#      it cuts spans out, so a span may wrap across lines, and this one has to:
#      `if [ -n "$updates" ]; then "$chain/pre-push" "$@" <<<"$updates" else
#      "$chain/pre-push" "$@" </dev/null fi`
#
#      Replace the two-arm forward
#
#          if [ -n "$updates" ]; then
#              "$chain/pre-push" "$@" <<<"$updates"
#          else
#              "$chain/pre-push" "$@" </dev/null
#          fi
#
#      with `    redeliver_pipe | "$chain/pre-push" "$@"`, defining
#      `redeliver_pipe() { if [ -n "$updates" ]; then printf '%s\n' "$updates"; fi; }`
#      above it — the spelling this file carried before. Watched:
#
#          a push whose chained hook never drains stdin still SUCCEEDS
#
#      **Only that row, and only because of its payload.** Every other chain row
#      here pushes a handful of refs, which fits in one pipe buffer and completes
#      before the reader is gone, so all of them stay green against the pipe. The
#      scenario forces the payload past the buffer and gives the chain a hook that
#      exits without reading, which is what turns the writer's SIGPIPE into a
#      refused push under `pipefail`.
#
# H8. **Hard-code forty zeros.** Replace
#     `        "" | *[!0]*) return 1 ;;` with a first arm matching a 40-character
#     zero sha and a `*) return 1 ;;` arm under it. A sha1 repository is
#     unaffected, which is the point. **The REPLACEMENT here is described rather than
#     quoted, which is legal — `break-once.sh` never searches a replacement — but it
#     means a registered row for H8 guards the SEARCH literal only, and nothing checks
#     that the described replacement still says what it says.** Watched:
#
#         the sha256 deletion-only push succeeds
#         the sha256 deletion-only push says it skipped
#
# H9. **Delete the chain forward.** Replace
#     `if [ -n "$chain" ] && [ -x "$chain/pre-push" ]; then` with
#     `if false; then`. This is what pins the four rows that assert the chain was
#     REACHED, which H5 and H6 each pin only in part. Watched:
#
#         a deletion-only push RUNS the chained hook
#         the chained hook is handed the deletion marker
#         the chained hook is handed the deleted ref
#         empty stdin still RUNS the chained hook
#         the chained hook receives the bytes git sent
#
# H10. **Read HEAD from the checkout that owns the hooks directory.** Replace
#     `    head_sha="$(git rev-parse HEAD)"` with
#     `    head_sha="$(git --git-dir="$(git rev-parse --git-common-dir)" rev-parse HEAD)"`.
#     Watched — and in BOTH directions at once, which is what makes this the most
#     expensive defect the file can catch: the worktree's own push is wrongly
#     REFUSED, and the owning checkout's sha is wrongly ACCEPTED:
#
#         a push from a linked worktree gates against the WORKTREE's own HEAD
#         a push from a linked worktree is not refused
#         a push from a linked worktree of the OWNING checkout's HEAD never reaches the gate
#         a push from a linked worktree of the OWNING checkout's HEAD is REFUSED
#         the refusal names the worktree's HEAD and not the owning checkout's
#
#     Every other row stays green, so a standalone repository cannot see this at
#     all — scenario 10 is the only thing that can.
#
#     **The `--git-dir=` spelling is the measurement, not a stylistic choice, and
#     the first attempt at this break FAILED TO REDDEN ANYTHING.** It used
#     `git -C "$(git rev-parse --git-common-dir)/.."`, which looks like it reads
#     the owning checkout and does not: git EXPORTS `GIT_DIR` into the hook
#     pointing at the pushing worktree's own gitdir, and `GIT_DIR` overrides
#     directory-based discovery, so `-C` anywhere still answers the worktree's
#     HEAD. That is also why the hook is CORRECT as written. Only an explicit
#     `--git-dir=` beats the exported variable.
#
# H12. **Refuse a rewind, which is the guard a reader assumes this hook already
#     has.** The literal this record searches, carried inline as a backticked span:
#     `delivering="$delivering$local_ref $local_sha"$'\n'`
#
#     In `.githooks/pre-push`, replace that line
#
#         delivering="$delivering$local_ref $local_sha"$'\n'
#
#     with that same line followed by
#
#         [ -z "${_remote_sha//0/}" ] || git merge-base --is-ancestor "$_remote_sha" "$local_sha" || exit 1
#
#     — the fourth field consulted, which is the only thing that would make this
#     hook's decision depend on the tip the REMOTE holds. Watched — six rows, all of
#     them scenario 13's:
#
#         a force-push of a rewritten history reaches the gate
#         a force-push of a rewritten history is ACCEPTED
#         a force-push after --amend reaches the gate
#         a force-push after --amend is ACCEPTED
#         a --force-with-lease push reaches the gate
#         a --force-with-lease push is ACCEPTED
#
#     **Every row outside scenario 13 stays green, and the reason is THREE facts about
#     this file rather than luck.** A DELETION never reaches the added line, because
#     `is_deletion` takes the branch above it — scenarios 2, 3, 4 and 9. An UP-TO-DATE
#     push never reaches it either, because git sends no update line and the loop is
#     not entered at all — scenario 14, which pushes to a ref the remote already holds
#     and is still unaffected. And every REMAINING push here creates its remote ref, so
#     the fourth field is all zeros and the added test short-circuits. Scenario 13 is
#     the only place a real sha reaches that comparison.
#
#     The three-way reason is itself a repair: this paragraph first said scenarios 2,
#     3, 4 and 9 were the only rows pushing to a ref the remote already holds, and
#     scenario 14 falsified that the day it was added — while the replay went on
#     printing the same six rows, because the CONCLUSION was right and the reason was
#     not. A replay agreeing with a reading does not check the reading.
#
#     The rewind rows do NOT move, and that pair is the part worth keeping: the
#     rewind is still refused, only for a different reason and with no message. A
#     refusal satisfies `check no` whatever produced it, so what pins this break is
#     the three ACCEPTING rows and not the refusing one.
#
#     This is one of only TWO breaks here that make the hook refuse MORE rather than
#     less — H11 is the other, and its shape is a payload past one pipe buffer rather
#     than a rewind. That is why the paragraph below no longer says every break here
#     narrows the hook: it was already wrong about H11 before H12 arrived.
#
# H13. **Weaken the gate the hook EXECS — one word, and it was the false green this
#     file could not see.** The literal this record searches, carried inline as a
#     backticked span:
#     `    exec mise run pre-push`
#
#     In `.githooks/pre-push`, replace that line with `    exec mise run pre-commit`.
#     The hook then runs the five-task commit list on every push and exits 0. Watched —
#     ONE row, the one added for it:
#
#         FAIL the push hook runs the WHOLE gate and not a subset
#
#     **Before that row existed this break reddened NOTHING, and three checkers agreed
#     it was fine**, each measured from a control made green first: `scripts/hook-guard.sh`
#     exit 0 with every assertion holding; `mise run anchors` unmoved, because none of
#     the twelve registered `.githooks/pre-push` literals covers this line; and
#     `.mise/tasks/task-lists` green, because it checks the `pre-commit` list and reads
#     no line of this hook. CI is off by decision, so nothing downstream existed.
#     Consequence: every push in every worktree runs 5 tasks instead of 18 — `test`,
#     `docs`, `deny`, `msrv`, `features`, `examples`, `package` and all four wasm steps
#     absent, silently, at exit 0.
#
#     **The defect was `ran()`, not this line.** `ran()` is `[ -s "$GATE_MARKER" ]`: it
#     asks whether the gate was REACHED and never which gate, so any substitution that
#     keeps the stub reachable is invisible to it. Two rows now read the marker's
#     CONTENT — this one and record 5's — and two is the right number rather than a
#     compromise, because ONE `exec` line per hook decides what the marker holds. A
#     third row in another scenario would re-measure the same line.
#
#     **And the coverage arithmetic is the uncomfortable part.** This hook holds 93 code
#     lines; the twelve registered literals touch 12 of them. The anchors exist to make
#     a hook edit visible, and the line they left uncovered was the one that names the
#     gate. Two other uncovered lines were driven to check the script catches them
#     anyway — inverting `if is_deletion "$local_sha"` gives 23 FAIL rows, and
#     `if [ -n "$delivering" ]` replaced by `if false` gives 15 — so uncovered is not
#     the same as unguarded. This line was the one where it was.
#
# ── AND THE ROWS NO BREAK ABOVE REDDENS, NAMED RATHER THAN LEFT TO LOOK COVERED
#
# Two kinds, and neither is decoration:
#
#   * POSITIVE CONTROLS. `an ordinary push of HEAD runs the gate`, `an ordinary
#     push of HEAD succeeds`, `a deletion-only push still succeeds`, `the mixed
#     push succeeds`, `empty stdin does not refuse the push` and `the push whose
#     chain was measured also reached the gate`. They exist so that no fix can
#     satisfy this file by refusing everything or by gating everything — a
#     failure mode MOST breaks above cannot produce, because they make the hook do
#     LESS. **Two make it refuse MORE, each in one narrow shape:** H11 for a
#     payload past one pipe buffer, H12 for a rewind. This paragraph used to say
#     every break here narrows the hook, and H11 was already the counter-example
#     before H12 was written. Scenario 11's row and scenario 13's three accepting
#     rows are what catch those two.
#   * ANTI-VACUITY PRECONDITIONS on the measurement itself: `git delivered an
#     update line to compare against`, `git delivered exactly one line, its LF
#     included`, `the linked worktree and its owning checkout sit at different
#     commits`, `the wide push's update payload exceeds one 64 KiB pipe buffer`,
#     scenario 12's pair `the annotated tag's OBJECT sha differs from HEAD` and `the
#     annotated tag nevertheless POINTS at HEAD`, and scenario 13's `the force-push is
#     a rewind: the remote's tip is not an ancestor of HEAD`. They are claims about
#     what git did, not about the hook. Two are there because `cmp` of two empty files
#     passes; the third is there because scenario 10 asserts nothing at all if the two
#     HEADs happen to agree; the fourth is there because scenario 11 passes against the
#     very spelling it exists to reject as soon as its payload drops below one buffer;
#     the tag pair is there because a tag refusal is only interesting once the tag is
#     known to point at HEAD; the last is there because `--force` on a push that would
#     fast-forward anyway is accepted for a reason that has nothing to do with force,
#     which would leave every accepting row in scenario 13 green and empty.
#   * CANARIES ON GIT ITSELF, both in Part Three: `a RELATIVE core.hooksPath runs
#     the pushing worktree's own hook` and `an ABSOLUTE core.hooksPath runs the
#     owning checkout's hook instead`. No break in this repository can move them,
#     and that is the point — they pin the external rule that
#     `scripts/install-hooks.sh` argues its choice from. If a future git changes
#     the rule, they redden and that argument gets re-opened instead of inherited.

# The hook under test, alone in its own hooks directory. Part One's directory
# holds a `pre-commit`, and a commit made while setting up a push scenario would
# fire it and write the marker that says "the gate ran".
push_hook_dir="$scratch/push-hooks"
mkdir -p "$push_hook_dir"
cp "$push_hook" "$push_hook_dir/pre-push"

# A chained pre-push that DUMPS what it reads. Its existence proves the chain was
# reached; its contents are the bytes it was handed. Both halves are needed: a
# forward that reaches the chained hook while feeding it nothing is the failure
# this file exists to catch, and it looks identical to success from the outside.
mkdir -p "$scratch/chain"
cat >"$scratch/chain/pre-push" <<'CHAIN'
#!/usr/bin/env bash
cat >"$CHAIN_STDIN"
CHAIN
chmod +x "$scratch/chain/pre-push"

pushlog="$scratch/push.log"
pushed_status=0
push() { # push <git push args...>: status in `pushed_status`, output in `$pushlog`
    pushed_status=0
    git push "$@" >"$pushlog" 2>&1 || pushed_status=$?
}

# `hooks.chain` set to the EMPTY STRING rather than unset. A value in the
# developer's GLOBAL config cannot be unset from inside a repository, only
# overridden, and the forward tests `[ -n "$chain" ]` — so an empty local value is
# what actually keeps a global secret scanner away from a throwaway repository.
newrepo() { # newrepo <dir> [git init args...]
    local dir="$1"
    shift
    mkdir -p "$dir"
    cd "$dir"
    git init -q -b work "$@" .
    git config user.email guard@example.com
    git config user.name Guard
    git config commit.gpgsign false
    git config core.hooksPath "$push_hook_dir"
    git config hooks.chain ""
}

echo
echo "── PART TWO: .githooks/pre-push gates the commit it is delivering"

newrepo "$scratch/push"
echo "one" >one.txt
git add one.txt
git commit -q -m "one"
echo "two" >two.txt
git add two.txt
git commit -q -m "two"
head_sha="$(git rev-parse HEAD)"
older_sha="$(git rev-parse HEAD~1)"

git init -q --bare "$scratch/remote"
git remote add origin "$scratch/remote"

# ── 1. The ordinary push. HEAD goes to the remote, so the tree the gate builds is
#       the tree the push delivers, and the gate must run.
fresh
push origin work
check yes "an ordinary push of HEAD runs the gate"
assert "an ordinary push of HEAD succeeds" test "$pushed_status" -eq 0
#       THE MARKER'S CONTENT, and this row is the one that closes a measured FALSE
#       GREEN. `ran()` tests `-s` alone, so replacing this hook's `exec mise run
#       pre-push` with `exec mise run pre-commit` left every row in this file green,
#       `mise run anchors` unmoved and `.mise/tasks/task-lists` green — while every
#       push in every worktree ran the five-task list instead of the whole gate. CI is
#       off by decision, so nothing downstream existed to catch it. See H13.
assert "the push hook runs the WHOLE gate and not a subset" \
    grep -qx 'run pre-push' "$GATE_MARKER"

# ── 2. THE CASE. A branch deletion, with nothing else in the push. There is no
#       tree to build, so every step of the gate is a step about an unrelated
#       commit. Measured against the hook as it was, with the same stub: a
#       deletion-only push reached `mise run pre-push`, and the recorded run of
#       it went on to fail on a cargo lock — a red push for a deletion that had
#       nothing to be red about.
git push -q --no-verify origin work:refs/heads/doomed
fresh
push origin --delete doomed
check no "a deletion-only push SKIPS the gate"
assert "a deletion-only push still succeeds" test "$pushed_status" -eq 0
assert "a skipped gate says it was skipped" grep -q "SKIPPING the gate" "$pushlog"
assert "a skipped gate says why" grep -q "carries no tree" "$pushlog"

# ── 3. The GATE is what a deletion skips, and never the HOOK. A protected-ref
#       guard belongs in a chained hook, and an early `exit 0` in the hook would
#       make every such guard dead code on exactly the push it exists to stop. So
#       the chain must still be reached, and must still be handed the deletion.
git push -q --no-verify origin work:refs/heads/doomed2
git config hooks.chain "$scratch/chain"
export CHAIN_STDIN="$scratch/chain-on-delete"
rm -f "$CHAIN_STDIN"
fresh
push origin --delete doomed2
check no "a deletion-only push with a chain configured still SKIPS the gate"
assert "a deletion-only push RUNS the chained hook" test -f "$CHAIN_STDIN"
assert "the chained hook is handed the deletion marker" grep -qF "(delete)" "$CHAIN_STDIN"
assert "the chained hook is handed the deleted ref" grep -qF "refs/heads/doomed2" "$CHAIN_STDIN"

# ── 4. A delete AND an update in one push. The push delivers a tree, so the whole
#       gate is owed on it — `every update is a deletion` and `any update is a
#       deletion` differ here and nowhere else.
git push -q --no-verify origin work:refs/heads/doomed3
git config hooks.chain ""
fresh
push origin :refs/heads/doomed3 work:refs/heads/extra
check yes "a delete plus an update in one push RUNS the gate"
assert "the mixed push succeeds" test "$pushed_status" -eq 0

# ── 5 and 6. EMPTY STDIN IS UNKNOWN, NOT EMPTY. Driven directly here, and the
#       reason used to be stated as "git always writes at least one update line, so
#       an empty read is not a push git can produce" — **which is FALSE, and
#       scenario 14 below is the push that produces it.** An up-to-date push writes
#       no update line at all. What driving the hook directly still buys is
#       CONTROL over the chained hook's stdin, which scenario 14 does not have.
#
#       Treating an empty read as all-deletions would skip the whole gate on the one
#       input that means "I could not tell", which is the false green this change
#       exists to remove.
#
#       The same run answers a second question: the chained hook must receive
#       NOTHING rather than a blank line, because a lone LF is one malformed
#       update to a reader that splits on newlines.
git config hooks.chain "$scratch/chain"
export CHAIN_STDIN="$scratch/chain-on-empty"
rm -f "$CHAIN_STDIN"
fresh
empty_status=0
"$push_hook_dir/pre-push" origin "$scratch/remote" </dev/null >"$pushlog" 2>&1 ||
    empty_status=$?
check yes "empty stdin RUNS the gate"
assert "empty stdin does not refuse the push" test "$empty_status" -eq 0
assert "the empty read is announced as unknown" grep -q "unknown is not a deletion" "$pushlog"
assert "empty stdin still RUNS the chained hook" test -f "$CHAIN_STDIN"
assert "empty stdin hands the chained hook nothing, not a blank line" test ! -s "$CHAIN_STDIN"

# ── 7. THE GUARD READING STDIN UNLOCKS. `HEAD~1` is pushed to a new branch, so
#       the delivered sha is not HEAD while the working tree — the only thing the
#       gate can build — is HEAD's. A green gate would be a statement about a
#       commit this push does not deliver, so the push is refused before the gate
#       runs rather than after it has produced the false green.
git config hooks.chain ""
fresh
push origin "$older_sha:refs/heads/older"
#       The sha is looked for BEHIND AN ARROW, and that is a repair. Git's own
#       success line for this refspec is `* [new branch]      <sha> -> older`,
#       which contains the sha in full — so the first version of this row, a
#       `grep` for the bare sha, was green while the hook refused nothing.
#       Measured by disabling the comparison against HEAD: the bare-sha form
#       stayed green with no refusal printed at all. Only the refusal writes the
#       sha with `-> ` in front of it.
check no "a delivered sha that is not HEAD never reaches the gate"
assert "a delivered sha that is not HEAD is REFUSED" test "$pushed_status" -ne 0
# "an object" and not "a commit": for an annotated tag the refused sha is the TAG
# OBJECT's, which is not a commit. The message was reworded once this row's own
# scenario 12 measured that case, and the assertion follows the message.
assert "the refusal says what it refused" \
    grep -qF "delivers an object that is not HEAD" "$pushlog"
#       `--` because the pattern starts with `-`, which grep reads as an option
#       and reports as a usage error. That direction is the safe one: the row
#       went red rather than green when the arrow was first added.
assert "the refusal names the delivered sha" grep -qF -- "-> $older_sha" "$pushlog"
assert "the refusal names HEAD" grep -qF "HEAD -> $head_sha" "$pushlog"
assert "the refusal names the bypass" grep -qF "git push --no-verify" "$pushlog"

# ── 8. THE FORWARD IS BYTE-EXACT, measured rather than reasoned about. Two bare
#       remotes, NEITHER holding the branch, so both pushes produce the identical
#       update line: the same local ref, the same sha, the same remote ref, and an
#       all-zero remote sha because the branch is new on both. The remote's name
#       and URL arrive as arguments and never on stdin, so they cannot differ.
#
#         `raw`     — `core.hooksPath` points at the dumper, so what it writes is
#                     what GIT delivers.
#         `chained` — `core.hooksPath` points at the real hook and `hooks.chain`
#                     at the dumper, so what it writes is what the CHAIN receives.
#
#       Non-emptiness is asserted first, and that is not ceremony: `cmp` of two
#       empty files passes, so a capture that silently wrote nothing would report
#       byte-identity as a success. The line count then pins the trailing LF,
#       which is the byte a `$(cat)` capture strips and a re-delivery has to put
#       back.
raw_hooks="$scratch/raw-hooks"
mkdir -p "$raw_hooks"
cp "$scratch/chain/pre-push" "$raw_hooks/pre-push"
git init -q --bare "$scratch/remote-raw"
git init -q --bare "$scratch/remote-chained"
git remote add raw "$scratch/remote-raw"
git remote add chained "$scratch/remote-chained"

sent="$scratch/git-sent"
got="$scratch/chain-got"
git config core.hooksPath "$raw_hooks"
export CHAIN_STDIN="$sent"
git push -q raw work:refs/heads/bytes >/dev/null 2>&1
git config core.hooksPath "$push_hook_dir"
git config hooks.chain "$scratch/chain"
export CHAIN_STDIN="$got"
fresh
push chained work:refs/heads/bytes
assert "git delivered an update line to compare against" test -s "$sent"
assert "git delivered exactly one line, its LF included" \
    test "$(wc -l <"$sent" | tr -d ' ')" -eq 1
assert "the chained hook receives the bytes git sent" cmp -s "$sent" "$got"
check yes "the push whose chain was measured also reached the gate"

# ── 9. A SHA256 REPOSITORY, where a deletion's zero sha is 64 characters. The
#       detector matches all-zeros by SHAPE, and this is the only scenario that
#       can tell that apart from a hard-coded 40: in a sha1 repository the wrong
#       spelling is indistinguishable from the right one, so scenario 2 would
#       certify a detector that reads every deletion here as an ordinary update
#       and gates a tree the push does not carry.
git config hooks.chain ""
newrepo "$scratch/push256" --object-format=sha256
echo "one" >one.txt
git add one.txt
git commit -q -m "one"
git init -q --bare --object-format=sha256 "$scratch/remote256"
git remote add origin "$scratch/remote256"
git push -q --no-verify origin work:refs/heads/doomed
fresh
push origin --delete doomed
#       All three rows, because `check no` alone is ONE-SIDED here: a detector
#       that reads the 64-zero sha as an ordinary update refuses the push, the
#       gate does not run, and "the gate did not run" is satisfied by the
#       refusal. Measured — under a hard-coded 40 the `check no` stays green and
#       only the two rows below it move.
check no "a deletion-only push in a SHA256 repository SKIPS the gate"
assert "the sha256 deletion-only push succeeds" test "$pushed_status" -eq 0
assert "the sha256 deletion-only push says it skipped" \
    grep -q "SKIPPING the gate" "$pushlog"

# ── 10. A LINKED WORKTREE, which is the shape this hook ships in and the one
#       scenario whose absence would have been expensive.
#
#       This repository stores `core.hooksPath` as an ABSOLUTE path in
#       `.git/config`, and linked worktrees SHARE that file. So every worktree
#       runs ONE hook file, owned by one checkout, while sitting at its own HEAD
#       — and a merge to the default branch puts the new hook in front of every
#       worktree at once, with no per-worktree opt-in.
#
#       If the guard resolved HEAD from the checkout that owns the hooks
#       directory rather than from the pushing worktree, it would refuse every
#       push from every worktree simultaneously. **No scenario above can see
#       that**, because they all run in standalone repositories where the two
#       answers are the same commit. That is the same trap Part One's scenarios 5
#       and 6 exist for, arriving through a different question.
git config hooks.chain ""
newrepo "$scratch/shared"
echo one >one.txt
git add one.txt
git commit -q -m "one"
git init -q --bare "$scratch/remote-shared"
git remote add origin "$scratch/remote-shared"
owner_head="$(git rev-parse HEAD)"
git worktree add -q -b side "$scratch/shared-side" HEAD
cd "$scratch/shared-side"
echo two >two.txt
git add two.txt
git commit -q -m "two"
side_head="$(git rev-parse HEAD)"
assert "the linked worktree and its owning checkout sit at different commits" \
    test "$owner_head" != "$side_head"
fresh
push origin side
check yes "a push from a linked worktree gates against the WORKTREE's own HEAD"
assert "a push from a linked worktree is not refused" test "$pushed_status" -eq 0
fresh
push origin "$owner_head:refs/heads/ownerhead"
check no "a push from a linked worktree of the OWNING checkout's HEAD never reaches the gate"
assert "a push from a linked worktree of the OWNING checkout's HEAD is REFUSED" \
    test "$pushed_status" -ne 0
assert "the refusal names the worktree's HEAD and not the owning checkout's" \
    grep -qF "HEAD -> $side_head" "$pushlog"

# ── 11. A CHAINED HOOK THAT NEVER DRAINS ITS STDIN, on a payload past one pipe
#       buffer. Every chain row above hands the chain a few hundred bytes and a stub
#       that reads all of them, so every one of those rows is green whether the
#       forward is a pipe or a here-string. This is the only row that tells them
#       apart.
#
#       The forward used to be `redeliver | "$chain/pre-push" "$@"`. A chain that
#       exits without reading closes the read end, the writer takes SIGPIPE, and
#       `pipefail` with `set -e` turns that into a REFUSED push whose entire output
#       is `printf: write error: Broken pipe` — indistinguishable from a crash.
#       Measured at 401 refs and 116,698 bytes: the pipe spelling refused, the
#       here-string succeeded. At 402 bytes both spellings succeed, which is why the
#       payload here is deliberately large.
#
#       EVERY BRANCH SITS AT HEAD, so the not-HEAD guard cannot be what refuses.
#       Without that this row would pass for the wrong reason.
mkdir -p "$scratch/chain-nonreader"
printf '#!/usr/bin/env bash\nexit 0\n' >"$scratch/chain-nonreader/pre-push"
chmod +x "$scratch/chain-nonreader/pre-push"
newrepo "$scratch/push-wide"
echo "one" >one.txt
git add one.txt
git commit -q -m "one"
git init -q --bare "$scratch/remote-wide"
git remote add origin "$scratch/remote-wide"
wide_pad="$(printf 'b%.0s' $(seq 1 90))"
for i in $(seq 1 400); do git branch "$wide_pad$i"; done
# The payload git is about to write, reconstructed in git's own shape. An
# anti-vacuity precondition: below one buffer the pipe spelling passes too, so
# without this the row could quietly stop testing anything.
wide_bytes="$(git for-each-ref \
    --format='%(refname) %(objectname) %(refname) 0000000000000000000000000000000000000000' \
    refs/heads | wc -c | tr -d ' ')"
assert "the wide push's update payload exceeds one 64 KiB pipe buffer" \
    test "$wide_bytes" -gt 65536
git config hooks.chain "$scratch/chain-nonreader"
fresh
push --all origin
assert "a push whose chained hook never drains stdin still SUCCEEDS" \
    test "$pushed_status" -eq 0
git config hooks.chain ""

# ── 12. TAGS, where the sha git writes on stdin is not the sha a reader expects.
#       Four places in this repository now tell a contributor that an annotated tag
#       at the tip is refused, and until this scenario existed nothing checked it.
#
#       A LIGHTWEIGHT tag names a commit, so at HEAD it sends HEAD and passes. An
#       ANNOTATED tag is its own object, and git writes THAT object's sha — so the
#       comparison below can never equal HEAD, no matter where the tag points. The
#       two preconditions say exactly that about git: the object sha differs from
#       HEAD while the tag still resolves to HEAD. Without them a green refusal row
#       could be a refusal for some other reason entirely.
#
#       This is the most ordinary tag push there is, and it is refused. The refusal
#       is deliberate — see the comment above the message in `.githooks/pre-push` —
#       and CONTRIBUTING.md lists it as a legitimate `--no-verify`.
newrepo "$scratch/push-tags"
echo "one" >one.txt
git add one.txt
git commit -q -m "one"
git init -q --bare "$scratch/remote-tags"
git remote add origin "$scratch/remote-tags"
tag_head="$(git rev-parse HEAD)"
git tag light
git tag -a ann -m "annotated"
assert "the annotated tag's OBJECT sha differs from HEAD" \
    test "$(git rev-parse refs/tags/ann)" != "$tag_head"
assert "the annotated tag nevertheless POINTS at HEAD" \
    test "$(git rev-parse 'refs/tags/ann^{commit}')" = "$tag_head"
fresh
push origin light
check yes "a LIGHTWEIGHT tag at HEAD reaches the gate"
assert "a LIGHTWEIGHT tag at HEAD is accepted" test "$pushed_status" -eq 0
fresh
push origin ann
check no "an ANNOTATED tag at HEAD never reaches the gate"
assert "an ANNOTATED tag at HEAD is REFUSED, git having sent the tag object's sha" \
    test "$pushed_status" -ne 0
assert "the tag refusal names an OBJECT rather than a commit" \
    grep -qF "delivers an object that is not HEAD" "$pushlog"

# ── 13. A FORCE-PUSH, which is a claim about the tip the REMOTE holds and not
#       about what the push delivers — so this hook does not consult it, and until
#       this scenario existed nothing said so either way.
#
#       Git writes the remote's current sha as the FOURTH field of each update line,
#       and that field is the only thing distinguishing a rewind from a
#       fast-forward. This hook reads the first two fields and discards the fourth,
#       so a rewrite, an `--amend` and a `--force-with-lease` all hand it the same
#       input an ordinary push does. Measured with a hook that only dumps what it
#       was sent, on a forced update over a branch the remote held: one update line,
#       whose fourth field was that branch's old sha rather than the zeros a new ref
#       carries. The non-ancestry is asserted separately, in the row below, because
#       that is the half which makes the push a rewind at all.
#
#       THE GATE-RAN QUESTION IS ASKED FOR EACH OF THE THREE, because the sentence
#       that justified asking it once was FALSE. It said the three accepting forms
#       deliver the identical update line, same ref and same sha. Measured with a dump
#       hook over these very three pushes: **one distinct ref, but THREE distinct
#       delivered shas and three distinct fourth-field shas, none of them zero.** Each
#       `--amend` writes a new commit, so each push carries a sha the previous one did
#       not. What the three genuinely share is the SHAPE — one ref, a non-zero fourth
#       field, and a delivered sha equal to HEAD — and the shape is what the argument
#       needs. But a shape argument pins nothing, and a `check` costs one line, so
#       each form now asks the gate-ran question for itself: a change that accepted the
#       amend WITHOUT gating it used to stay green here.
#
#       THE ACCEPTING ROWS ARE WHAT PINS THIS, not the refusing one. A hook that
#       grew a rewind guard would satisfy every other row in this file, and it would
#       block the amend-then-force-push this repository does as ordinary practice —
#       discovered by a developer rather than here. H12 is that break.
#
#       And the rewind IS refused, which scenario 7 cannot say: there the branch is
#       new on the remote and the fourth field is all zeros. Here the branch exists
#       and the fourth field is a real sha, which is the only shape in this file
#       where a reader could expect the hook to consult it.
newrepo "$scratch/push-force"
echo "one" >one.txt
git add one.txt
git commit -q -m "one"
echo "two" >two.txt
git add two.txt
git commit -q -m "two"
git init -q --bare "$scratch/remote-force"
git remote add origin "$scratch/remote-force"
git push -q --no-verify origin work
remote_tip="$(git rev-parse HEAD)"
#       The rewrite: the tip the remote holds is dropped rather than built on.
git reset -q --hard HEAD~1
echo "three" >three.txt
git add three.txt
git commit -q -m "a rewritten tip"
#       An ANTI-VACUITY PRECONDITION, and the row this scenario would be worthless
#       without: `--force` on a push that would fast-forward anyway is accepted for
#       a reason that has nothing to do with force, and every row below would stay
#       green. Spelled as a merge base rather than as `! git merge-base
#       --is-ancestor`, because `assert` runs its argument list as a command and a
#       leading `!` is a reserved word only when bash PARSES it.
assert "the force-push is a rewind: the remote's tip is not an ancestor of HEAD" \
    test "$(git merge-base "$remote_tip" HEAD)" != "$remote_tip"
fresh
push --force origin work
check yes "a force-push of a rewritten history reaches the gate"
assert "a force-push of a rewritten history is ACCEPTED" test "$pushed_status" -eq 0
#       An `--amend` and then a force-push, which is this repository's own workflow
#       and the form a refusal would cost the most.
echo "more" >>three.txt
git add three.txt
git commit -q --amend --no-edit
fresh
push --force origin work
check yes "a force-push after --amend reaches the gate"
assert "a force-push after --amend is ACCEPTED" test "$pushed_status" -eq 0
#       `--force-with-lease`, which compares the remote against the tracking ref
#       instead of overwriting blindly. The row above is what leaves that ref
#       matching, so the lease holds and the push reaches this hook at all.
echo "yet more" >>three.txt
git add three.txt
git commit -q --amend --no-edit
fresh
push --force-with-lease origin work
check yes "a --force-with-lease push reaches the gate"
assert "a --force-with-lease push is ACCEPTED" test "$pushed_status" -eq 0
#       A rewind to an OLDER commit, over the branch the remote holds. Git allows it
#       because it was forced; this hook refuses it because the tree the gate would
#       build is not the tree the push delivers.
force_older="$(git rev-parse HEAD~1)"
fresh
push --force origin "$force_older:refs/heads/work"
check no "a force-push REWINDING to an older commit never reaches the gate"
assert "a force-push of an older commit is REFUSED" test "$pushed_status" -ne 0

# ── 14. AN UP-TO-DATE PUSH, which is the input scenarios 5 and 6 called impossible.
#       Git writes one update line per ref it will UPDATE, and a push with nothing to
#       deliver updates none — so `git push` on a current branch runs this hook with
#       ZERO bytes on stdin. Measured across five spellings before this row existed:
#       `git push origin work`, a bare `git push`, `--all`, an explicit
#       `HEAD:refs/heads/work` and `--force` each read zero bytes and zero lines.
#
#       So the empty read has an ORDINARY producer, not only a bug in the capture, and
#       this is the row that stops that being rediscovered. The hook's decision is
#       unchanged and deliberately so: unknown runs the gate, because a capture bug
#       must never skip it silently. **The cost is real and this row is where it is
#       visible** — an already-landed push pays the whole gate for a push that
#       delivers nothing.
#
#       The FIRST assertion is the anti-vacuity one: without it, a row that pushed
#       something after all would still be green on the two rows below it.
newrepo "$scratch/push-current"
echo "one" >one.txt
git add one.txt
git commit -q -m "one"
git init -q --bare "$scratch/remote-current"
git remote add origin "$scratch/remote-current"
git push -q --no-verify origin work
fresh
push origin work
assert "the up-to-date push really had nothing to deliver" \
    grep -qF "Everything up-to-date" "$pushlog"
check yes "an UP-TO-DATE push reads empty stdin and RUNS the gate"
assert "the up-to-date push still succeeds" test "$pushed_status" -eq 0
assert "the empty read names NOTHING TO PUSH before it names unknown" \
    grep -qF "NOTHING TO PUSH" "$pushlog"

# ══ PART THREE — scripts/install-hooks.sh, and WHICH hook file runs ═════════
#
# Every scenario above asks what a hook DOES. These ask which file git runs at
# all, which is the question `core.hooksPath` answers and the one the installer
# writes an answer to.
#
# ── two of these rows assert GIT's behaviour, not ours, and that is deliberate ─
#
# `install-hooks.sh` chooses a RELATIVE value, and its header argues that choice
# from one measured fact: git resolves a relative `core.hooksPath` against the
# working-tree root, so each worktree runs its own branch's hooks, while an
# absolute value makes every worktree run one checkout's. No break in this
# repository can redden that pair — it is a canary on an external dependency. If a
# future git changes the rule, these rows go red and the decision in that header
# has to be re-argued rather than quietly inherited.
#
# The other three rows are ours: the installer must write the relative value, and
# must never chain our hooks to a copy of themselves.
#
# ── MADE RED, RUN AND WATCHED, NAMING THE ROWS ────────────────────────────────
#
# Same method as Part Two: the perturbation goes into a copy of
# `scripts/install-hooks.sh` beside copies of the hooks and this script, and the
# unperturbed control is run first and must be green.
#
# **`scripts/break-hook-record.sh` is what does that, and until it existed this
# sentence was written in a voice that implied something did.** It copies the tree,
# runs this script AND `.mise/tasks/task-lists` as a control, refuses to report a
# perturbed result unless both come back with no failures, and fails a perturbation
# that reddens nothing.
#
#     scripts/break-hook-record.sh .githooks/pre-push from-literal.txt to-literal.txt
#
# **Which records have been through it, named rather than implied: 4, H1, H4, H11 and
# H12.** Those are the ones a session added or whose row list it changed, and the
# printed sets matched them exactly — 1, 23, 14, 1 and 4 rows. H1's and H4's figures
# are the re-measured ones: scenario 13 widened both, and the replay is what said by
# how much rather than a reader deciding it looked unchanged. The rest predate the
# script and their row lists remain one author's watched run, which is the whole
# reason the script exists. Replaying them is worth doing and is not done here.
#
# Comparing the rows it prints against the rows a record names is still the reader's
# job — these records are prose, and nothing parses them.
#
# I1. **Restore the literal-only test, which IS the historical defect.** In
#     `scripts/install-hooks.sh`, replace
#     `if [ -z "$previous" ] || [ "$ours" = yes ]; then` with
#     `if [ -z "$previous" ] || [ "$previous" = .githooks ]; then`. Watched:
#
#         FAIL the installer leaves a foreign hooks.chain alone
#         FAIL the installer treats a SIBLING worktree's .githooks as ours too
#
# I2. **Compare only the current worktree.** Replace
#     `done < <(git worktree list --porcelain | sed -n 's/^worktree //p')` with
#     `done <<<"$PWD"`. Watched — one row, which is what makes the sibling case
#     worth its own scenario rather than an assumption:
#
#         FAIL the installer treats a SIBLING worktree's .githooks as ours too
#
# I4. **Delete the blast-radius warning, which is what a tidy-up does to it.** The
#     literal this record searches, carried inline as a backticked span:
#     `    if [ "$worktrees" -gt 1 ]; then`
#
#     In `scripts/install-hooks.sh`, replace that line with `    if false; then`, so the
#     script writes `core.hooksPath` across every worktree and says nothing about it.
#     Watched:
#
#         FAIL the installer says how far the core.hooksPath write reaches
#
#     **This row exists because the WARNING was the fix.** F5 was that one installer run
#     re-points five worktrees silently; a sentence announcing it, with nothing pinning
#     the sentence, is one cleanup away from the silence coming back. The row reads the
#     count the script DERIVED from `git worktree list` rather than a number written
#     down, so it also fails if that derivation is replaced by a literal.
#
# I3. **Write an absolute value, reversing the decision.** Replace
#     `git config core.hooksPath .githooks` with
#     `git config core.hooksPath "$PWD/.githooks"`. Watched:
#
#         FAIL the installer writes the relative core.hooksPath
#
#     This is the row that pins the CHOICE rather than an implementation detail,
#     which is why it has a break at all: the argument for relative lives in that
#     script's header, and a silent flip to absolute would leave the header arguing
#     for a value the script no longer writes.
echo
echo "── PART THREE: which hook file runs, and what the installer writes"

# Two checkouts carrying DELIBERATELY DIFFERENT hook files, so the question "which
# one ran" has an observable answer. Neither is the real hook: what is under test
# is resolution, and a stub that names itself is the only way to see it.
newrepo "$scratch/paths"
mkdir -p .githooks
printf '#!/usr/bin/env bash\ncat >/dev/null\necho "RAN: owner"\n' >.githooks/pre-push
chmod +x .githooks/pre-push
git add -A
git commit -q -m "hooks in the owning checkout"
git init -q --bare "$scratch/remote-paths"
git remote add origin "$scratch/remote-paths"
git worktree add -q -b side "$scratch/paths-side" HEAD
cd "$scratch/paths-side"
printf '#!/usr/bin/env bash\ncat >/dev/null\necho "RAN: worktree"\n' >.githooks/pre-push
chmod +x .githooks/pre-push
git add -A
git commit -q -m "different hooks in the linked worktree"

git config core.hooksPath .githooks
push origin side:refs/heads/rel
assert "a RELATIVE core.hooksPath runs the pushing worktree's own hook" \
    grep -q "RAN: worktree" "$pushlog"
git config core.hooksPath "$scratch/paths/.githooks"
push origin side:refs/heads/abs
assert "an ABSOLUTE core.hooksPath runs the owning checkout's hook instead" \
    grep -q "RAN: owner" "$pushlog"

# ── the installer must not chain our hooks to a copy of themselves ────────────
#
# Measured before this row existed: with `core.hooksPath` already an ABSOLUTE path
# to the repository's own `.githooks`, the installer wrote that path into
# `hooks.chain` — losing the global scanner it exists to preserve, and pointing the
# hook at itself. The next commit hung, killed at a timeout with no output. The
# trigger was the value's SPELLING, so this row uses the absolute spelling.
cd "$scratch/paths"
mkdir -p scripts "$scratch/global-hooks"
cp "$root/scripts/install-hooks.sh" scripts/
chmod +x scripts/install-hooks.sh
printf '#!/usr/bin/env bash\necho SCANNER\n' >"$scratch/global-hooks/pre-commit"
chmod +x "$scratch/global-hooks/pre-commit"
git config core.hooksPath "$scratch/paths/.githooks"
git config hooks.chain "$scratch/global-hooks"
#       CAPTURED rather than discarded, because the installer's blast radius is the
#       thing it was silent about. This repository has five worktrees sharing one
#       `core.hooksPath`, so one run re-points all five — and if the sentence saying so
#       is itself unpinned, a tidy-up deletes it and the silence comes back. This
#       scratch repository has TWO worktrees, and the count is derived from
#       `git worktree list` rather than written down, so the row reads the number the
#       script computed.
./scripts/install-hooks.sh >"$pushlog" 2>&1
assert "the installer says how far the core.hooksPath write reaches" \
    grep -qF "reaches all 2 worktrees" "$pushlog"
assert "the installer leaves a foreign hooks.chain alone" \
    test "$(git config --get hooks.chain)" = "$scratch/global-hooks"
assert "the installer writes the relative core.hooksPath" \
    test "$(git config --get core.hooksPath)" = ".githooks"

# The sibling-worktree spelling, which is the live shape when the script is run
# from a linked worktree rather than from the checkout the path names.
git config core.hooksPath "$scratch/paths-side/.githooks"
git config hooks.chain "$scratch/global-hooks"
./scripts/install-hooks.sh >/dev/null 2>&1
assert "the installer treats a SIBLING worktree's .githooks as ours too" \
    test "$(git config --get hooks.chain)" = "$scratch/global-hooks"

if [ "$fail" -ne 0 ]; then
    echo "── FAILED" >&2
    exit 1
fi
echo
echo "── all $checks assertions hold, across both hooks and the installer"
