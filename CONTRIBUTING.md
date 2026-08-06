# Contributing to keylink

## The gate

```sh
./scripts/install-hooks.sh   # once per clone
just gate                    # everything CI runs
```

You need stable Rust, [`just`](https://github.com/casey/just), and two rustup
pieces for the browser target:

```sh
rustup target add wasm32-unknown-unknown
rustup component add llvm-tools          # provides llvm-ar; see below
```

The gate is defined once, in the `justfile`. CI calls those same recipes, so a
green `just gate` locally means a green run. There is no second copy of the
commands to drift.

| | runs | why |
|---|---|---|
| `just pre-commit` | fmt, clippy on the library | Seconds. A commit hook people bypass is not a gate. |
| `just pre-push` | the whole gate | A push is rarer than a commit, and a red gate here costs one local run instead of a CI round trip. |
| CI | the same recipes, plus `cargo deny check` | `deny` needs the network, so it lives in CI rather than in a hook. |

## When `--no-verify` is legitimate

Both hooks point here. The short version is "the hook itself is broken", never
"the check is inconvenient".

Legitimate:

- The hook cannot run — `just` is missing and the inline fallback is wrong, or a
  rustup component is not installed.
- The push died in transport *after* the gate ran green on the same HEAD. That
  is a network failure, and running the gate again proves nothing.

Not legitimate:

- The gate is slow.
- "I will fix it in the next commit."
- The failure looks unrelated to your change. Find out why before you decide
  that.

If you bypass, say so in the pull request, with which of the two reasons applied.

## The wire contract

`keylink/tests/protocol.rs::the_contract_hash_has_not_moved` pins the ALPN as a
string literal. When it goes red, in this order:

1. Decide whether this change **should** be visible on the wire.
2. Only then update the literal, and say so in the release notes.

Never step 2 without step 1. Updating a literal to turn a red test green is how
a deployed fleet silently splits in half.

The pin exists because every other test compiles both peers from one source
tree. A wire change moves both sides, the handshake still agrees, and the whole
suite stays green. A test cannot catch a change it participates in.

## Public API

Every `pub` item is a compatibility promise. Before you add one:

- If only the `service!` macro needs to name it, keep it `pub` and add
  `#[doc(hidden)]`. Generated code must reach it; a reader must not meet it.
- If nothing outside the crate needs it, make it `pub(crate)`. The test is
  `rg '\$crate::Name' keylink/src/macros.rs` plus a search of `tests/`, `demo/`
  and the README — not a count of callers inside `src/`.
- Error enums are `#[non_exhaustive]`, so a new variant is not a breaking
  change. Enums a caller constructs and matches are not. `Departure` is
  deliberately exhaustive: it names exactly three outcomes — we closed it, the
  peer closed it cleanly, it broke — and the open-ended part lives inside
  `Transport`, which is `#[non_exhaustive]`.
- One way to build a thing. `RpcService::new` and the crate-root `serve` were
  removed because they were a second construction path that could not take
  middleware layers, so the shorter door quietly did less than the longer one.

`just docs` runs rustdoc with `-D warnings`, which turns `#![warn(missing_docs)]`
into a gate and fails on a broken intra-doc link in the same pass.

## The browser target

`just wasm` **builds**, never checks. `cargo check` stops before the linker
runs, and the failure this recipe exists to catch is a link failure: `ring`
compiles C into an archive, and an archive written by Apple `ar` carries an
index `rust-lld` cannot read, so every `ring_core_*` symbol comes back
undefined. A `check` job passes green on a tree whose wasm target does not link.
That happened here.

The recipe derives `llvm-ar` from the host triple, so a Mac and CI's Linux both
work without either naming the other.

Three features need an operating system facility a browser does not have, and
each refuses the target with its own `compile_error!`: `blocking`, `fs`, `env`.
CI asserts that all three actually fire. A `compile_error!` nobody triggers is a
comment.

What is claimed: the crate compiles and links for `wasm32-unknown-unknown`.
Nothing has ever run in a browser.

## Tests

- A guard you have not watched fail is not a guard. Break it, see the red,
  restore it. A check that cannot fail looks exactly like a check that passes.
- Do not assert on wall-clock time. Tests share a machine with a compiler and a
  test runner, and a load spike is not a bug in your code. Put the
  clock-dependent half in a unit test with `tokio::time::pause`, and leave the
  integration test an assertion that load can only strengthen.

## RFCs

`docs/rfc-*.md` record decisions, including the ones that were reversed. Add one
when a change rests on a judgement somebody will question in six months.

Do not edit an RFC so that it matches new code. Append to it. The value of the
record is that it shows what was believed at the time.
