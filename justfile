# keylink's gate, defined once.
#
# CI runs these same recipes, so a green `just gate` locally means a green CI
# run — there is no second copy of the commands to drift from this one.
#
#   just            # list every recipe
#   just gate       # everything CI runs
#   just pre-commit # the fast subset the commit hook runs
#   just pre-push   # the heavy subset the push hook runs

_default:
    @just --list

# ── the whole gate ──────────────────────────────────────────────────────────

# Everything CI runs, in CI's order. Cheapest failures first.
gate: fmt-check lint features test docs wasm

# ── what the hooks run ──────────────────────────────────────────────────────

# Fast enough that nobody is tempted to bypass it. Seconds, on a warm cache.
pre-commit: fmt-check lint-fast

# The heavy gate. Slow is acceptable here: a push is rarer than a commit.
pre-push: gate

# ── formatting ──────────────────────────────────────────────────────────────

fmt:
    cargo fmt --all

fmt-check:
    cargo fmt --all -- --check

# ── linting ─────────────────────────────────────────────────────────────────

# The library alone, so the commit hook stays under a few seconds. It catches
# the lint that a `--all-targets` run would also catch, minus the test and
# example targets that dominate the build time.
lint-fast:
    cargo clippy -p keylink --lib --all-features -- -D warnings

# Both feature ends. `--no-default-features` is scoped to the library on
# purpose: the integration tests use the `blocking` API, so they cannot compile
# without the feature that provides it.
lint:
    cargo clippy --all-targets --all-features -- -D warnings
    cargo clippy -p keylink --lib --no-default-features -- -D warnings

# ── feature combinations ────────────────────────────────────────────────────

# Every subset of the three optional features.
#
# `lint` checks the two ENDS of the lattice — all features on, all features off
# — and they do not meet in the middle. That gap is not theoretical: gating
# `bind_from_env` behind `env` made `--features blocking` alone stop compiling,
# because `blocking::Node::bind_from_env` calls it, and neither end noticed.
#
# `check` rather than `clippy`, because `lint` already lints both ends and what
# this recipe is looking for is a combination that does not build at all.
features:
    #!/usr/bin/env bash
    set -euo pipefail
    for f in "" blocking fs env blocking,fs blocking,env fs,env blocking,fs,env; do
        echo "── features: ${f:-none}"
        cargo check -p keylink --lib --no-default-features --features "$f"
    done

# ── tests ───────────────────────────────────────────────────────────────────

test:
    cargo test --workspace

# ── documentation ───────────────────────────────────────────────────────────

# `#![warn(missing_docs)]` is a warning, and a warning nothing denies is
# decoration. This is what turns it into a gate, and it catches broken
# intra-doc links in the same pass.
docs:
    RUSTDOCFLAGS='-D warnings' cargo doc --no-deps --all-features -p keylink

# ── the browser target ──────────────────────────────────────────────────────

# `build`, never `check`. `cargo check` stops before the link step, and the
# failure this catches IS a link failure: `ring` compiles its C to an archive,
# and an archive written by Apple `ar` carries an index `rust-lld` cannot read,
# so every `ring_core_*` symbol comes back undefined. A `check` job passes while
# the target is broken.
#
# `llvm-ar` ships with the `llvm-tools` rustup component. The path is derived
# from the host triple so this works on a developer's Mac and on CI's Linux
# without either naming the other.
wasm:
    #!/usr/bin/env bash
    set -euo pipefail
    ar="$(rustc --print sysroot)/lib/rustlib/$(rustc -vV | sed -n 's/^host: //p')/bin/llvm-ar"
    if [ ! -x "$ar" ]; then
        echo "llvm-ar not found at $ar" >&2
        echo "install it with: rustup component add llvm-tools" >&2
        exit 1
    fi
    AR_wasm32_unknown_unknown="$ar" \
        cargo build --target wasm32-unknown-unknown -p keylink --no-default-features

# ── supply chain ────────────────────────────────────────────────────────────

# Advisories and licences over the whole tree. See deny.toml for why this one
# earns a place in a crate that parses bytes a stranger sent.
deny:
    cargo deny check
