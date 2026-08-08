#!/usr/bin/env bash
# Does `.githooks/pre-commit` run its gate on the commit it is ACTUALLY writing?
#
# The hook decides one thing — gate, or exit 0 — and this drives that decision
# from the outside, in a throwaway repository, and watches which way it went. It
# never runs cargo: a stub `mise` on `PATH` records that the gate was reached and
# returns, so what is asserted is the hook's DECISION and not clippy's verdict.
# That is the property the hook owns; clippy has its own tests.
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
# 1. **Revert the fix.** In `.githooks/pre-commit`, replace
#
#        amend_base="$(git rev-parse --verify --quiet HEAD~1 || git hash-object -t tree /dev/null)"
#
#    with `amend_base=""`. Watched — scenario 1 alone, and the other three stay
#    green:
#
#        FAIL an amend of an ungated Rust commit RE-RUNS the gate — the gate did NOT run
#
# 2. **Make it gate everything.** Replace the `if ! printf '%s\n' "$linted" |
#    grep -qE ...; then` line with `if false; then`. Watched — the two negatives,
#    while 1 and 2 stay green:
#
#        FAIL an ordinary commit staging no linted file skips the gate — the gate ran and should not have
#        FAIL an amend of a commit with no linted file skips the gate — the gate ran and should not have
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

hook="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)/.githooks/pre-commit"
[ -x "$hook" ] || { echo "no executable hook at $hook" >&2; exit 2; }

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

echo "── .githooks/pre-commit gates the commit it is writing"

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

if [ "$fail" -ne 0 ]; then
    echo "── FAILED" >&2
    exit 1
fi
echo "── all $checks assertions hold, each one about whether the gate ran"
