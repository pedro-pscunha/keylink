# Contributing to keylink

## The gate

```sh
./scripts/install-hooks.sh   # once per clone
mise run gate                # everything CI runs
```

You need [`mise`](https://mise.jdx.dev) and rustup. Nothing else is a manual
step: `rust-toolchain.toml` requests the `wasm32-unknown-unknown` target and the
`llvm-tools` component, and rustup installs both on first use. `mise.toml` pins
`cargo-deny`. A cold clone should reach a green gate without reading a list of
setup commands, because a setup command in a README is a step somebody skips.

The gate is defined once, in `mise.toml` and `.mise/tasks/`. CI calls those same
tasks, so a green `mise run gate` locally means a green run. There is no second
copy of the commands to drift.

| | runs | why |
|---|---|---|
| `mise run pre-commit` | fmt, clippy on the library | Seconds. A commit hook people bypass is not a gate. |
| `mise run pre-push` | the whole gate | A push is rarer than a commit, and a red gate here costs one local run instead of a CI round trip. |
| CI | the same tasks, plus `mise run deny` and `mise run msrv` | `deny` fetches the advisory database and `msrv` needs a second toolchain, so both live in CI rather than in a hook. |

`mise run gate` runs its steps **one at a time**, cheapest first. That is why the
task lists sub-tasks rather than using bare `depends`: mise parallelizes
dependencies, and two concurrent cargo invocations contend for one target
directory and one machine's memory.

Run `mise run deny` by hand whenever you change a dependency. It is the one check no
hook runs, which is exactly why it was broken for three commits without anybody
seeing it: a licence, an advisory or a wildcard arrives with a dependency and
with nothing else.

`mise run msrv` compiles the workspace on 1.91, the oldest Rust
`keylink/Cargo.toml` claims. That number is not inherited from a dependency's
manifest — it was compiled. Raise it only when something actually needs a newer
compiler, and re-run the task rather than reasoning about it.

`mise run features` builds every subset of `blocking`, `fs` and `env`. Clippy checks
the two ends of that lattice — all on, all off — and the ends do not meet in the
middle. When you put an item behind a feature gate, check what calls it:
gating `Node::bind_from_env` behind `env` broke `--features blocking` on its
own, because `blocking::Node::bind_from_env` calls it, and neither end of the
lattice noticed.

## When `--no-verify` is legitimate

Both hooks point here. The short version is "the hook itself is broken", never
"the check is inconvenient".

Legitimate:

- The hook cannot run — `mise` is missing and the inline fallback is wrong, or a
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

`mise run docs` runs rustdoc with `-D warnings`, which turns `#![warn(missing_docs)]`
into a gate and fails on a broken intra-doc link in the same pass.

## The browser target

`mise run wasm` **builds**, never checks. `cargo check` stops before the linker
runs, and the failure this recipe exists to catch is a link failure: `ring`
compiles C into an archive, and an archive written by Apple `ar` carries an
index `rust-lld` cannot read, so every `ring_core_*` symbol comes back
undefined. A `check` job passes green on a tree whose wasm target does not link.
That happened here.

The task derives `llvm-ar` from the host triple, so a Mac and CI's Linux both
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
- Record the break beside the guard, and **`mise run anchors` decides how.** If the
  record quotes the literal `break-once.sh` searches for, that literal must resolve
  exactly once in the file the break edits, and the record needs an entry in
  `.mise/tasks/anchors` — without one it is unchecked. Quoting cannot work at all when
  the record lives in the file being broken, and it rots by itself when the anchor's
  uniqueness depends on its neighbours, as a `pub use` list item's does. Describe the
  search edit in those two cases. **Quote the replacement either way**: the harness
  never searches it. Three records reached `main` unable to find their own anchors,
  and none of them was red, which is why this is a gate.
- **A red `anchors` means the anchor moved. Follow it.** Re-quote it against the line
  as it now reads — that is the fix for a cross-file, line-local anchor, and the fix
  whenever a rebase moved the target. **Delete its entry only** in the two cases above,
  where quoting cannot work at all, and describe the edit instead. Deleting is the
  shortest path to green and it is usually wrong: it leaves the record unchecked rather
  than correct. The task prints this at you, because the moment you need it is
  mid-rebase and not while reading `CONTRIBUTING`.
- Do not assert on wall-clock time. Tests share a machine with a compiler and a
  test runner, and a load spike is not a bug in your code. Put the
  clock-dependent half in a unit test with `tokio::time::pause`, and leave the
  integration test an assertion that load can only strengthen.

## RFCs

`docs/rfc-*.md` record decisions, including the ones that were reversed. Add one
when a change rests on a judgement somebody will question in six months.

Do not edit an RFC so that it matches new code. Append to it. The value of the
record is that it shows what was believed at the time.
