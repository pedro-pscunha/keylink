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
# 1. **Revert the fix.** In `.githooks/pre-commit`, replace
#
#        amend_base="$(git rev-parse --verify --quiet HEAD~1 || git hash-object -t tree /dev/null)"
#
#    with `amend_base=""`. Watched: it reddens the amend-of-an-ungated-Rust-commit
#    row, and that row only —
#
#        FAIL an amend of an ungated Rust commit RE-RUNS the gate — the gate did NOT run
#
# 2. **Make it gate everything.** Replace the `if ! printf '%s\n' "$linted" |
#    grep -qE ...; then` line with `if false; then`. Watched: it reddens **every row
#    that expects a skip** — the two ordinary negatives AND the reword, which is the
#    one this record omitted while the reword scenario existed:
#
#        FAIL an ordinary commit staging no linted file skips the gate — the gate ran and should not have
#        FAIL an amend of a commit with no linted file skips the gate — the gate ran and should not have
#        FAIL a rebase -i reword of a Rust commit does NOT run the gate (a replay is not authoring) — the gate ran and should not have
#
# 3. **Break the rebase arm, either way.** In `.githooks/pre-commit`, either drop
#    the guard from the detector by replacing
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
# Quoting those literals is safe for the reason `break-once.sh` cares about: this
# record is not in the file whose literal it quotes, so the anchor stays unique in
# its target.
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
#
#     `a deletion-only push RUNS the chained hook` does NOT move, and that is
#     worth more than the list: the chain is still REACHED, it is only handed
#     nothing. Reaching a hook and feeding it are two claims, and H5 is the one
#     that separates them.
#
# H2. **Treat an unknown read as all-deletions.** Replace
#     `if [ "$updates_read" -gt 0 ] && [ "$deletions" -eq "$updates_read" ]; then`
#     with `if [ "$deletions" -eq "$updates_read" ]; then` — zero equals zero, so
#     an empty read skips the entire gate. Watched:
#
#         empty stdin RUNS the gate
#         the empty read is announced as unknown
#
# H3. **Skip on ANY deletion instead of every.** Replace the same line with
#     `if [ "$updates_read" -gt 0 ] && [ "$deletions" -gt 0 ]; then`. Watched:
#
#         a delete plus an update in one push RUNS the gate
#
# H4. **Drop the comparison against HEAD.** Replace
#     `        if [ "$sha" != "$head_sha" ]; then` with `        if false; then`.
#     Watched:
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
#
# H5. **Stop re-delivering stdin to the chain.** Replace
#     `    redeliver | "$chain/pre-push" "$@"` with
#     `    "$chain/pre-push" "$@"`, which is what the line was before this
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
# H6. **Skip the HOOK rather than the GATE.** Replace the same line with
#     `    case "$updates" in "(delete) "*) exit 0 ;; *) redeliver | "$chain/pre-push" "$@" ;; esac`
#     — a hook that returns early for a deletion instead of only skipping the
#     gate. Watched:
#
#         a deletion-only push RUNS the chained hook
#         the chained hook is handed the deletion marker
#         the chained hook is handed the deleted ref
#
# H7. **Re-deliver unconditionally.** Replace
#     `    if [ -n "$updates" ]; then` with `    if true; then`, so an empty
#     capture sends a lone LF. The indented spelling counts once; the unindented
#     one guarding the classification loop is a different line. Watched:
#
#         empty stdin hands the chained hook nothing, not a blank line
#
# H8. **Hard-code forty zeros.** Replace
#     `        "" | *[!0]*) return 1 ;;` with a first arm matching a 40-character
#     zero sha and a `*) return 1 ;;` arm under it. A sha1 repository is
#     unaffected, which is the point. Watched:
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
# ── AND THE ROWS NO BREAK ABOVE REDDENS, NAMED RATHER THAN LEFT TO LOOK COVERED
#
# Two kinds, and neither is decoration:
#
#   * POSITIVE CONTROLS. `an ordinary push of HEAD runs the gate`, `an ordinary
#     push of HEAD succeeds`, `a deletion-only push still succeeds`, `the mixed
#     push succeeds`, `empty stdin does not refuse the push` and `the push whose
#     chain was measured also reached the gate`. They exist so that no fix can
#     satisfy this file by refusing everything or by gating everything — a
#     failure mode the nine breaks above cannot produce, because each one narrows
#     the hook rather than widening it.
#   * ANTI-VACUITY PRECONDITIONS on the measurement itself: `git delivered an
#     update line to compare against`, `git delivered exactly one line, its LF
#     included`, and `the linked worktree and its owning checkout sit at different
#     commits`. They are claims about what git did, not about the hook. Two are
#     there because `cmp` of two empty files passes; the third is there because
#     scenario 10 asserts nothing at all if the two HEADs happen to agree.

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

# ── 5 and 6. EMPTY STDIN IS UNKNOWN, NOT EMPTY. Driven directly rather than
#       through `git push`, because git always writes at least one update line —
#       so an empty read is not a push git can produce, it is what a bug in the
#       capture produces. Treating it as all-deletions would skip the whole gate
#       on the one input that means "I could not tell", which is the false green
#       this change exists to remove.
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
assert "the refusal says what it refused" \
    grep -qF "delivers a commit that is not HEAD" "$pushlog"
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

if [ "$fail" -ne 0 ]; then
    echo "── FAILED" >&2
    exit 1
fi
echo
echo "── all $checks assertions hold, across both hooks"
