#!/usr/bin/env bash
# Replay one break record from `scripts/hook-guard.sh` or `.mise/tasks/task-lists`:
# run the CONTROL first, then the perturbation, and print the rows that reddened.
#
#   break-hook-record.sh <file-under-root> <from-literal-file> <to-literal-file>
#
# The literals come from FILES rather than arguments, because every one of them is
# shell source containing `$`, quotes and sometimes newlines, and passing that
# through an argument list is how a break silently perturbs something else.
#
# ── WHY THIS EXISTS, AND IT IS NOT CONVENIENCE ────────────────────────────────
#
# Two files in this repository instructed a replayer to "always run the unperturbed
# control first", and nothing ran one. `scripts/break-once.sh` — the driver for the
# Rust records — aborts on a missing literal, asserts a single occurrence, and runs
# the test ONCE; it never compares against an unperturbed run. So the control-first
# discipline was prose, and `.mise/tasks/task-lists` documents that discipline
# failing: five breaks were recorded as caught before anybody tried the control,
# which came back red too. A break recorded against a red control proves nothing,
# and it fails in the reassuring direction.
#
# ── WHAT THIS DOES AND DOES NOT AUTOMATE ──────────────────────────────────────
#
# It automates the control: both checkers must come back with ZERO failures before
# any perturbed run is believed, and a perturbation that reddens NOTHING is reported
# as a failed replay rather than as a pass. **Comparing the printed rows against the
# rows the record claims is still the reader's step.** The records are prose and this
# script does not parse them. That is a real limit and it is stated rather than
# implied — an unattended run of this script cannot tell you a record is honest, only
# that the break reddens something from a green start.
#
# ── IT NEVER WRITES TO THE WORKING TREE ───────────────────────────────────────
#
# Every perturbation goes into a `mktemp -d` copy. Nothing restores anything, so
# there is no backup to be orphaned and no `trap` that can overwrite a live file
# with a stale copy later. The live tree is opened read-only, and the script prints
# its checksum at the end so that is visible rather than promised.
set -euo pipefail

if [ "$#" -ne 3 ]; then
    sed -n '2,6p' "${BASH_SOURCE[0]}" >&2
    exit 2
fi

target="$1"
from_file="$2"
to_file="$3"

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
for f in "$from_file" "$to_file"; do
    [ -f "$f" ] || { echo "break-hook-record: no such literal file: $f" >&2; exit 2; }
done
[ -f "$root/$target" ] || { echo "break-hook-record: no such file: $root/$target" >&2; exit 2; }

work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT

# Everything either checker reads. Copied whole rather than selectively, because a
# checker that silently finds a file missing reports a failure about the wrong thing.
mkdir -p "$work/.githooks" "$work/scripts"
cp "$root/.githooks/pre-commit" "$root/.githooks/pre-push" "$work/.githooks/"
cp "$root/scripts/hook-guard.sh" "$root/scripts/install-hooks.sh" "$work/scripts/"
cp "$root/mise.toml" "$root/CONTRIBUTING.md" "$work/"
cp -R "$root/.mise" "$work/.mise"
chmod +x "$work/.githooks/pre-commit" "$work/.githooks/pre-push" \
    "$work/scripts/hook-guard.sh" "$work/scripts/install-hooks.sh"
# `task-lists` resolves its root with `git rev-parse --show-toplevel`, so the copy
# has to be a repository for the copy to read the copy.
git init -q "$work"

# `python3` on a developer's PATH is often a mise SHIM, and a shim loads mise config
# from the cwd — so an UNTRUSTED `mise.toml` in the copy makes the interpreter exit 1
# before `task-lists` runs a line. That failure looks exactly like a caught break.
#
# **BOTH paths, and the second one was measured the hard way.** Naming only the copy
# makes this variable the whole trust list, which UNTRUSTS the real repository — and
# the first run of this script died on exactly that, at the perturbation step, after
# printing a green control. Every `python3` call below also runs with its cwd inside
# the copy, so the shim reads a config it has been told to trust either way.
export MISE_TRUSTED_CONFIG_PATHS="$work:$root"

run_checkers() { # run_checkers <outfile>: every FAIL row from both checkers
    : >"$1"
    ( cd "$work" && bash "$work/scripts/hook-guard.sh" 2>&1 ) | grep -E '^\s*FAIL' >>"$1" || true
    ( cd "$work" && python3 "$work/.mise/tasks/task-lists" 2>&1 ) | grep -E '^FAIL' >>"$1" || true
    sed -e 's/^[[:space:]]*//' -e 's/^FAIL //' "$1" | sort -u >"$1.rows"
}

echo "── CONTROL, unperturbed. An exit code means nothing until this is green."
run_checkers "$work/control.log"
if [ -s "$work/control.log.rows" ]; then
    echo "── CONTROL IS RED. Every row below is a pre-existing failure, so no" >&2
    echo "── perturbed result from this tree would mean anything. Stopping." >&2
    sed 's/^/     /' "$work/control.log.rows" >&2
    exit 1
fi
echo "   control green: both checkers report no failures"

from_abs="$(cd "$(dirname "$from_file")" && pwd)/$(basename "$from_file")"
to_abs="$(cd "$(dirname "$to_file")" && pwd)/$(basename "$to_file")"
( cd "$work" && python3 - "$work/$target" "$from_abs" "$to_abs" <<'PY'
import sys
path, from_file, to_file = sys.argv[1], sys.argv[2], sys.argv[3]
old, new = open(from_file).read(), open(to_file).read()
text = open(path).read()
n = text.count(old)
if n != 1:
    sys.exit(
        f"break-hook-record: the search literal occurs {n} times in {path}, and a "
        "break has to name exactly one line. Widen the literal until it is unique."
    )
open(path, "w").write(text.replace(old, new))
PY
)
echo "── PERTURBED: $target, one occurrence replaced"

run_checkers "$work/broken.log"
if [ ! -s "$work/broken.log.rows" ]; then
    echo "── THE BREAK REDDENED NOTHING, which is a failed replay and not a pass." >&2
    echo "── Either the guard does not cover this line, or the replacement is inert." >&2
    exit 1
fi

echo "── ROWS THAT WENT RED (compare these against the record yourself):"
sed 's/^/     /' "$work/broken.log.rows"
echo "── $(wc -l <"$work/broken.log.rows" | tr -d ' ') row(s) reddened from a green control."
echo "── the live tree was never written to; $target still hashes to:"
echo "     $(shasum -a 256 "$root/$target" | awk '{print $1}')"
