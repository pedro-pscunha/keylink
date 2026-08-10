#!/usr/bin/env bash
# Break one line, run one test, restore. Aborts if the edit did not match —
# a break harness that silently no-ops reports "green" and proves nothing.
#
#   break-once.sh <file> <literal-to-replace> <replacement> <test-filter> [cargo args...]
#
# The gate's aggregate target does not replace these named files: a focused
# break selects the original target with `--test <name>`, which keeps the failure
# local and preserves the file's deliberate process isolation.
#
# The filter is a bare libtest substring appended after Cargo's arguments. Keep
# registry filters bare, fully qualified test names; options for libtest itself
# belong after `--` in the cargo arguments.
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
#
# **`cargo fmt` OWNS THE LINE BREAKS, so count an anchor AFTER formatting.** An
# anchor quoted off a multi-line block can be unique when you measure it and gone
# when the formatter collapses that block onto one line — the count goes 1 → 0 and
# nothing says so, because `mise run anchors` checks only REGISTERED rows. Twice in
# one day: `D11` in `#41`, when a shorter type name let a block collapse, and `O3`
# on `observer-crashed`, which was dead at its own first commit and stayed dead
# through a merge gate. `.mise/tasks/anchors` calls this rot mode 2. The remedy is
# sequencing and nothing cleverer: format first, then count.
#
# **A READ FAILURE PRINTS THE SAME ONE-LINE ABORT, and `set -e` is not what
# reports it.** The first `python3` version let a missing target raise, so the
# assignment failed and `set -e` killed the script: a `FileNotFoundError`
# traceback and exit 1, with no `ABORT` line anywhere. Both replay consumers echo
# every line this script prefixes `── `, plus `ABORT` and `expected exactly one`
# from the abort below, which carries no prefix because it goes to stderr. So a
# rotted `target` path — a rot mode the registry names — produced ZERO matching
# lines, and `replay-anchors` then told the reader to open a log the run never
# wrote. Measured against `keylink/src/does-not-exist.rs`: the `grep` this
# replaced exited 2 and printed one `ABORT` line, and that first version exited 1
# and printed none.
if ! occurrences="$(python3 - "$file" "$from" <<'PY'
import sys
try:
    text = open(sys.argv[1]).read()
except OSError as err:
    print(f"BREAK HARNESS ABORT: cannot read {sys.argv[1]}: {err.strerror}",
          file=sys.stderr)
    raise SystemExit(2)
print(text.count(sys.argv[2]))
PY
)"; then
    exit 2
fi
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
runner=(cargo test)
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
# **PREVENTIVE, and the six tightest rows had three lines of slack.** Measured
# 2026-08-08 (-03:00) over the breaks `REDNESS` registers: `tail` hid none of them.
#
# WHICH STREAM, before any figure, because naming the wrong one is how this paragraph
# went wrong twice. Every number below is the PIPED stream ALONE: the runner's own
# combined output, which is exactly the bytes `tail -15` used to receive and exactly
# what `tee` writes to `$log`. The `──` lines this script prints are NOT in it — two
# are echoed before the pipeline and the rest after it — so none of them ever reached
# `tail`, and none of them can consume its window. (It said FOUR until the count
# below added a fifth and a warning of its own. The claim never depended on the
# number, only on which side of the pipeline they sit, so the number is gone rather
# than re-counted: a figure nothing rests on is a figure that goes stale unwatched.)
#
# What decides survival is how many lines FOLLOW the sentence, because `tail -15`
# counts from the END: a line survives while at most fourteen follow it. Counted in
# each row's `$log`, where each of these sentences occurs exactly once — L3 has 9
# lines after it, L4 and A2b have 10, and L5, A1, A2, A3, A4 and A5 have 11. So the
# six tightest rows would have survived three further printed lines, and a fourth
# would have hidden the sentence.
#
# **THIRD VERSION OF THIS PARAGRAPH, and each earlier one measured something the
# consumer never read.** The first quoted the distance from the TOP, "A1's sat at
# line 26 of 40": the wrong end, and the compiler and the test runner talking rather
# than the margin. The second corrected the end and kept the wrong stream — it read
# "11 to 13 lines after, six at 13", which is this script's WHOLE stdout, the runner's
# lines plus the two `──` echoes that follow the pipeline, so every row reads two
# high. Either error alone is fatal to the conclusion, and the second carries one
# more: `── log:` is printed by the SAME diff that deleted the `tail`, so a figure
# taken off the combined output cannot describe the pre-`tee` behaviour even in
# principle. It also contradicted its own figures, whichever stream they belonged to:
# it put six rows at 13 and made fourteen the last surviving position, so the "one
# more line" it warned about still survived. No row was hidden under either reading.
#
# Named after the test filter, because nine `break-once.XXXXXX` files in one `TMPDIR`
# say nothing about which break each one is the evidence for. Nothing reaps them, and
# that is the same decision as the one above: the log outlives the run because it is
# what a searcher's conclusion rests on.
log="$(mktemp -t "break-once-${filter//[^A-Za-z0-9_]/-}")"
set +e
"${runner[@]}" "$@" "$filter" 2>&1 | tee "$log"
status=${PIPESTATUS[0]}
set -e
echo "── exit=$status (non-zero means the test caught it)"
echo "── log: $log"

# ── HOW MANY TESTS THE FILTER SELECTED, WHICH THE EXIT STATUS CANNOT SAY ───────
#
# A break record names ONE guard, and `cargo test` resolves its filter as a
# SUBSTRING. So a test whose name is a strict PREFIX of a sibling's selects BOTH,
# and a run where only the SIBLING reddens exits non-zero and prints the
# sibling's panic. Every consumer of this script searches this output for the
# sentence a record says it watched, and that sentence can then come out of a
# test the record does not name — a break that reddened the wrong test, read as
# evidence for the right one.
#
# **COUNTED HERE because there are THREE consumers and they were not equally
# guarded.** `scripts/replay-fate-breaks.sh` counted for itself, the
# `replay-anchors` task did not count at all, and a hand-run of this script — the
# way most of the registered records are re-checked — counted nothing either. One
# count, printed once, on the one path all three go through. What to DO about a
# count that is not 1 stays with each consumer, because only they know whether a
# record is claiming one guard: this script warns and never aborts, since running
# one break against several tests by name is a legitimate exploratory use and the
# `replay-anchors` header records somebody doing exactly that.
#
# SUMMED over every totals line rather than read off the first, because a filter
# with no target runs one binary per target and prints one line each. `?` is NO
# totals line anywhere: an abort, a tree that did not compile, or a runner whose
# summary neither pattern knows. **That is not zero**, and a consumer must never
# read it as zero — zero is a filter that named nothing, which is a different
# fault with a different remedy.
selected="$(python3 - "$log" <<'PY'
import re
import sys

text = open(sys.argv[1], errors="replace").read()
counts = [int(passed) + int(failed) for passed, failed in
          re.findall(r"^test result: \w+\. (\d+) passed; (\d+) failed", text, re.M)]
print(sum(counts) if counts else "?")
PY
)"
echo "── selected=$selected"

# **THE REMEDY IS THE FULL PATH, and `-- --exact` ALONE is the WRONG one for most
# of this tree.** `--exact` matches the WHOLE listed name; a `--lib` test lists as
# `module::tests::name`; and every registered record passes a BARE name. So adding
# `--exact` to a bare name trades "selected two" for "selected none" — it moves the
# fault rather than fixing it. Measured 2026-08-09 on
# `the_recorded_bytes_stay_inside_their_bound` under `-p keylink --lib`: 1 as a
# substring, **0** with `-- --exact`, and 1 with `-- --exact` and the full path
# `idempotency::tests::bounds::the_recorded_bytes_stay_inside_their_bound`. An
# integration binary only looks like an exception, because there the full path IS
# the bare name.
#
if [ "$selected" != 1 ] && [ "$selected" != "?" ]; then
    echo "── WARNING: this filter selected $selected tests, and a break record names ONE."
    if [ "$selected" = 0 ]; then
        echo "──   Nothing ran, so this break cannot redden and never could. Either the"
        echo "──   name exists nowhere, or the target cannot reach it: a \`#[cfg(test)]\`"
        echo "──   test inside \`src\` needs --lib and is unreachable under --test."
    else
        echo "──   A break that reddens ANY of them satisfies a record naming one, so"
        echo "──   nothing downstream can say WHICH test printed the watched sentence."
        echo "──   Take the FULL name — \`cargo test\` with these same cargo args and"
        echo "──   \`-- --list\` — pass THAT as the filter, and add \`-- --exact\` to"
        echo "──   the cargo args. \`--exact\` matches the WHOLE name, so a BARE name under --lib"
        echo "──   selects zero with it."
    fi
fi
exit 0
