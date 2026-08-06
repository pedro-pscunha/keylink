#!/usr/bin/env bash
# Point this repository's git hooks at the versioned ones in .githooks/.
#
#   ./scripts/install-hooks.sh
#
# Run it once per clone. The hooks live in the repository rather than in
# .git/hooks so that a change to the gate arrives with a `git pull` instead of
# by word of mouth.
set -euo pipefail

cd "$(git rev-parse --show-toplevel)"

# A hooks path already in effect is almost always a global one — a secret
# scanner installed across every repository. `core.hooksPath` REPLACES it
# rather than adding to it, so record it and let .githooks/* chain to it.
# Without this step, installing keylink's hooks would quietly switch off a
# guard the developer set up everywhere.
previous="$(git config --get core.hooksPath || true)"
case "$previous" in
    ""|.githooks)
        # Nothing to preserve, or we are already installed.
        ;;
    *)
        git config hooks.chain "$previous"
        echo "chaining to the hooks already configured at: $previous"
        ;;
esac

git config core.hooksPath .githooks
chmod +x .githooks/*

echo "installed: .githooks/pre-commit, .githooks/pre-push"
echo
echo "  pre-commit  cargo fmt --check, clippy on the library     (seconds)"
echo "  pre-push    the full gate: every target, both feature ends,"
echo "              the tests, the docs, and the browser target"
echo
echo "the browser target needs two rustup pieces:"
echo "  rustup target add wasm32-unknown-unknown"
echo "  rustup component add llvm-tools"
echo
echo "bypass with --no-verify; see CONTRIBUTING.md for when that is legitimate."

if ! command -v just >/dev/null 2>&1; then
    echo
    echo "note: 'just' is not installed, so the hooks will run the checks"
    echo "      directly. Install it for the one-command gate: 'just gate'."
fi
