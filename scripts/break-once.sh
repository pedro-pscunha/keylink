#!/usr/bin/env bash
# Break one line, run one test, restore. Aborts if the edit did not match —
# a break harness that silently no-ops reports "green" and proves nothing.
#
#   break-once.sh <file> <literal-to-replace> <replacement> <test-filter> [cargo args...]
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
set +e
cargo test "$@" "$filter" 2>&1 | tail -15
status=${PIPESTATUS[0]}
set -e
echo "── exit=$status (non-zero means the test caught it)"
exit 0
