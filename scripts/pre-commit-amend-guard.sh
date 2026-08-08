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
# Quoting those literals is safe for the reason `break-once.sh` cares about: this
# record is not in the file whose literal it quotes, so the anchor stays unique in
# its target.
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
check() { # check <expect ran: yes|no> <description>
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

if [ "$fail" -ne 0 ]; then
    echo "── FAILED" >&2
    exit 1
fi
echo "── all four decisions correct"
