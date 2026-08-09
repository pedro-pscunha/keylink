#!/usr/bin/env bash
# Point this repository's git hooks at the versioned ones in .githooks/.
#
#   ./scripts/install-hooks.sh
#
# Run it once per clone. The hooks live in the repository rather than in
# .git/hooks so that a change to the gate arrives with a `git pull` instead of
# by word of mouth.
#
# ── THE VALUE IS RELATIVE ON PURPOSE, AND IT IS A BEHAVIOURAL FORK ────────────
#
# `core.hooksPath` lives in `.git/config`, which every linked worktree SHARES —
# so this one line decides hook resolution for all of them at once. Measured on
# git 2.54.0, with two checkouts carrying deliberately different hook files:
#
#   `.githooks` (relative)   git resolves it against the WORKING-TREE root, so
#                            each worktree ran ITS OWN branch's hook.
#   `/abs/path/.githooks`    every worktree ran the hook belonging to that one
#                            checkout, whatever branch it was on.
#
# Relative is chosen, and the absolute reading has a real case that has to be
# argued down rather than ignored:
#
#   * FOR absolute — one gate for every worktree, and no branch can weaken the
#     hook that gates it. Against: `--no-verify` already makes that soft, and
#     uniformity is what the MERGE is for, not what a config path is for.
#   * FOR relative, decisively — the hooks `cd` to the working-tree root and then
#     `exec mise run ...`, so the TASK DEFINITIONS always come from the pushing
#     worktree. With an absolute value the hook LOGIC comes from one checkout and
#     the tasks from another: a combination that exists in no commit, and is
#     therefore untested by construction. Relative makes both halves come from the
#     same commit.
#   * FOR relative — a change to a hook becomes testable on the branch that makes
#     it. Under the absolute value it is not, and a hook defect reaches every
#     worktree the moment `main` moves.
#   * FOR relative — this file's own first paragraph. A `git pull` cannot carry a
#     gate change into a worktree whose hooks resolve to a different checkout.
#
# **THE HAZARD RELATIVE CARRIES, MEASURED RATHER THAN REASONED: on a branch whose
# tree has no `.githooks/` directory, NO hook runs and the push succeeds.** Git
# treats a missing hook as nothing to do. Nothing in this repository can detect it,
# because the thing that would report it is the hook that is absent — so it is
# declared here instead of worked around. It takes deleting the directory or
# checking out a commit older than it.
#
# To test a hook change without touching any of this, run `mise run hook-guard`:
# it drives both hooks in throwaway repositories and never reads this config.
set -euo pipefail

cd "$(git rev-parse --show-toplevel)"

# A hooks path already in effect is almost always a global one — a secret
# scanner installed across every repository. `core.hooksPath` REPLACES it
# rather than adding to it, so record it and let .githooks/* chain to it.
# Without this step, installing keylink's hooks would quietly switch off a
# guard the developer set up everywhere.
#
# ── AND IT MUST NEVER CHAIN TO ONE OF OUR OWN HOOK DIRECTORIES ────────────────
#
# The test used to be the literal string `.githooks`, and the live configuration
# on this developer's machine was an ABSOLUTE path to that same directory. That
# spelling matched neither arm, so running this script did two things, both
# measured in a throwaway repository:
#
#   * it OVERWROTE `hooks.chain`, losing the global secret scanner it exists to
#     preserve; and
#   * it pointed `hooks.chain` at our own hooks, so the hook chained to itself.
#     The next `git commit` hung — killed at a ten-second timeout with no output.
#
# So the documented once-per-clone command was a landmine, and the trigger was the
# value's SPELLING rather than anything about it. Every worktree of this repository
# is compared, not just this one: a value naming a SIBLING worktree's `.githooks`
# is still one of ours, and chaining to it loops the same way.
previous="$(git config --get core.hooksPath || true)"
resolved=""
case "$previous" in
    "") ;;
    /*) resolved="$previous" ;;
    *) resolved="$PWD/$previous" ;;
esac

# Process substitution and not a pipe: a piped `while` runs in a subshell, and
# `ours=yes` would be discarded when it exited — leaving the loop looking correct
# and deciding nothing.
#
# Two comparisons, because either can be the one that matches. The string test
# catches the ordinary case; `-ef` compares device and inode, which is what sees
# through a symlinked path or a second spelling of one directory, and it needs the
# path to exist. Written as separate arms rather than `A || B && C`, which the
# shell reads as `(A || B) && C`.
ours=no
while read -r worktree; do
    [ -n "$worktree" ] || continue
    if [ "$resolved" = "$worktree/.githooks" ]; then
        ours=yes
    elif [ -e "$resolved" ] && [ "$resolved" -ef "$worktree/.githooks" ]; then
        ours=yes
    fi
done < <(git worktree list --porcelain | sed -n 's/^worktree //p')

if [ -z "$previous" ] || [ "$ours" = yes ]; then
    : # Nothing to preserve, or one of our own hook directories is already there.
else
    git config hooks.chain "$previous"
    echo "chaining to the hooks already configured at: $previous"
fi

# Relative, for the reasons in the header. Also the only spelling that makes
# re-running this script idempotent, which the absolute one is not.
#
# ── AND THE WRITE REACHES EVERY WORKTREE, SO IT SAYS SO ──────────────────────
#
# `core.hooksPath` is not per-worktree unless `extensions.worktreeConfig` is on, and
# it is off here. So this one line lands in the SHARED config and every linked
# worktree reads it. Going from an absolute value to the relative one therefore
# switches every worktree at once — from one checkout's hooks to each worktree's own
# branch's hooks — and the header's hazard then applies to each of them: a branch
# whose tree carries no `.githooks/` runs NO hook and the push succeeds.
#
# That was silent until this printed it. The count is derived from `git worktree
# list` rather than written down, and the previous value is read BEFORE the write,
# because after it there is nothing left to compare against.
hooks_path_before="$(git config --get core.hooksPath || true)"
worktrees="$(git worktree list --porcelain | grep -c '^worktree ' || true)"
git config core.hooksPath .githooks
chmod +x .githooks/*

# Read BACK rather than restating the value written above. A message that repeats the
# literal from the line before it cannot report that line changing; this one reads what
# git now holds, so it stays true if the write above is ever edited.
hooks_path_after="$(git config --get core.hooksPath || true)"

if [ "$hooks_path_before" != "$hooks_path_after" ]; then
    echo "core.hooksPath: ${hooks_path_before:-<unset>} -> $hooks_path_after"
    if [ "$worktrees" -gt 1 ]; then
        echo "  this is SHARED config (extensions.worktreeConfig is off), so the"
        echo "  change reaches all $worktrees worktrees. Each now runs the hooks in"
        echo "  ITS OWN branch's .githooks/ — and a branch without that directory"
        echo "  runs no hook at all."
    fi
fi

echo "installed: .githooks/pre-commit, .githooks/pre-push"
echo
echo "  pre-commit  framing, anchors, anchors-selftest, fmt-check, lint-fast"
echo "  pre-push    the full gate: every target, both feature ends,"
echo "              every feature subset, the tests, the docs,"
echo "              and the browser target"
echo "              — skipped for a push that only DELETES branches, and"
echo "                refused for a push delivering an object that is not HEAD."
echo "                Any ANNOTATED tag lands there, one at HEAD included,"
echo "                because git sends the tag object's own sha."
echo
echo "bypass with --no-verify; see CONTRIBUTING.md for when that is legitimate."

# It said both hooks would "run the checks directly and SKIP the browser
# target", which was one sentence covering two hooks that no longer agree:
# `pre-push` REFUSES without mise, because it claims to be the whole gate and
# a subset of the whole gate is a false green.
if ! command -v mise >/dev/null 2>&1; then
    echo
    echo "note: 'mise' is not installed. pre-commit runs its checks directly;"
    echo "      pre-push REFUSES, because it is the only gate there is and a"
    echo "      weaker subset of it would be a false green. Install mise:"
    echo "      https://mise.jdx.dev"
fi
