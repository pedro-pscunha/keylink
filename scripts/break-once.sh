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

grep -qF -- "$from" "$file" || {
    echo "BREAK HARNESS ABORT: '$from' is not in $file" >&2
    exit 2
}

backup="$(mktemp)"
cp "$file" "$backup"
restore() { cp "$backup" "$file"; rm -f "$backup"; }
trap restore EXIT

python3 - "$file" "$from" "$to" <<'PY'
import sys
path, old, new = sys.argv[1], sys.argv[2], sys.argv[3]
text = open(path).read()
assert text.count(old) == 1, f"expected exactly one occurrence, found {text.count(old)}"
open(path, "w").write(text.replace(old, new))
PY

echo "── broken: $file"
# Word-split on purpose — `BREAK_RUNNER` is a command prefix such as
# "cargo nextest run", not a single executable.
read -r -a runner <<< "${BREAK_RUNNER:-cargo test}"
echo "── runner: ${runner[*]}"
set +e
"${runner[@]}" "$@" "$filter" 2>&1 | tail -15
status=${PIPESTATUS[0]}
set -e
echo "── exit=$status (non-zero means the test caught it)"
exit 0
