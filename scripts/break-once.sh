#!/usr/bin/env bash
# Break one line, run one test, restore. Aborts if the edit did not match —
# a break harness that silently no-ops reports "green" and proves nothing.
#
#   break-once.sh <file> <literal-to-replace> <replacement> <test-filter> [cargo args...]
#
# The runner is `cargo test` unless `BREAK_RUNNER` says otherwise. That override
# is not decoration: the gate runs `cargo nextest run`, and some guards exist
# ONLY under it — `.config/nextest.toml`'s `drift-builds` group is one, because
# it replaces a `OnceLock` that nextest's process-per-test voids. Under plain
# `cargo test` that group is never read, so this harness could not reach the
# guard at all and would report a green that proves nothing.
#
#   BREAK_RUNNER="cargo nextest run" break-once.sh … --test drift
#
# Filter syntax differs between the two runners: `cargo test` takes a substring,
# nextest takes a filterset. Pass the filter each one expects.
set -euo pipefail

file="$1"
from="$2"
to="$3"
filter="$4"
shift 4

# NOT `grep -qF`, which this line used for its first three months and which
# CANNOT express the precondition. POSIX gives `-F` a pattern LIST separated by
# newlines, so a multi-line literal matches when ANY ONE of its lines does, and a
# literal ending in a newline hands grep an EMPTY pattern that matches every file
# unconditionally. Measured with the `/usr/bin/grep` this runs under, BSD grep
# 2.6.0: a file holding only `alpha` satisfies `grep -qF -- $'alpha\nbeta'` while
# Python's `.count()` of the same literal returns 0, and a file holding neither
# line satisfies `grep -qF -- $'alpha\n'`. So the check was inert for exactly the
# literals it was most needed for — registry rows do pass multi-line ones — and
# the run went on to the replace step, which then refused. Nothing was lost,
# because `trap restore EXIT` below is what protects the file; what was lost is
# the abort happening where this script says it happens.
#
# ONE count, over the WHOLE literal, BEFORE the backup. The replace step below
# used to repeat it as an `assert` after the backup was taken. Both counts were
# the same expression on the same bytes, so the second could not fail where the
# first passed — and having two invited the reading that they check different
# things. Now every count that is not 1 aborts before a backup exists.
occurrences="$(python3 - "$file" "$from" <<'PY'
import sys
print(open(sys.argv[1]).read().count(sys.argv[2]))
PY
)"
if [ "$occurrences" != 1 ]; then
    # One line, and it keeps both `ABORT` and `expected exactly one` in it: that is
    # the filter `scripts/replay-fate-breaks.sh` echoes matching lines through, so a
    # count on a second line would not reach a replay's console.
    abort="BREAK HARNESS ABORT: expected exactly one occurrence, found $occurrences"
    echo "$abort in $file: '$from'" >&2
    exit 2
fi

backup="$(mktemp)"
cp "$file" "$backup"
restore() { cp "$backup" "$file"; rm -f "$backup"; }
trap restore EXIT

python3 - "$file" "$from" "$to" <<'PY'
import sys
path, old, new = sys.argv[1], sys.argv[2], sys.argv[3]
text = open(path).read()
open(path, "w").write(text.replace(old, new))
PY

echo "── broken: $file"
# Word-split on purpose — `BREAK_RUNNER` is a command prefix such as
# "cargo nextest run", not a single executable.
read -r -a runner <<< "${BREAK_RUNNER:-cargo test}"
echo "── runner: ${runner[*]}"
# NOT `tail -15`, which is what this was. A consumer searches this output for the
# sentence a record says it watched, and `tail` discards the output BEFORE it is
# searched — so a panic that scrolls past fifteen lines reads as a break that did
# not redden. That is a false failure produced by the harness, in a harness whose
# whole purpose is to tell a real failure from a green break.
#
# `tee` keeps the whole run on stdout, where every consumer already looks, and
# leaves a copy behind. The copy is NOT deleted: it is the evidence for whatever
# the searcher concluded, and its path is printed for that reason.
#
# **PREVENTIVE, and the margin is one line.** Measured 2026-08-08 over the nine
# breaks `REDNESS` registers: every watched sentence fell inside the fifteen-line
# window, so `tail` hid none of them — and `A1`'s sat at line 26 of 40, one line
# inside it. One more line of runner output before the panic and that row would
# have reported a break that did not redden.
log="$(mktemp -t break-once)"
set +e
"${runner[@]}" "$@" "$filter" 2>&1 | tee "$log"
status=${PIPESTATUS[0]}
set -e
echo "── exit=$status (non-zero means the test caught it)"
echo "── log: $log"
exit 0
