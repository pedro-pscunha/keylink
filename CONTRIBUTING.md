# Contributing to keylink

## The gate

```sh
./scripts/install-hooks.sh   # once per clone
mise run gate                # the whole gate
```

**GitHub Actions is disabled. `mise run gate` is the only gate**, and the
`pre-push` hook runs it — which makes `./scripts/install-hooks.sh` load-bearing
rather than a convenience. Skip it and nothing checks your pushes.

Two consequences worth knowing before you hit them:

- **`mise` is now required, not recommended.** The `pre-push` hook used to fall
  back to a weaker set of checks when mise was missing, on the grounds that CI
  ran the rest. It refuses now, because a gate that passes while covering less
  than it claims is a false green.
- **The gate runs on your machine, on your OS.** CI ran Linux. Nothing checks the
  other platform any more.

The `pre-push` hook reads git's list of ref updates before it runs any of that,
and it answers differently for two shapes of push. A push whose updates are ALL
deletions skips the gate and prints that it did: a deletion carries no tree, so
building one checks nothing about it. A push delivering a commit that is not
`HEAD` is **refused**, because the gate builds the working tree — a green result
would be a statement about a tree the push does not carry. A tag pointing at an
older commit lands there, and so does `git push --all`.

You need [`mise`](https://mise.jdx.dev) and rustup. Nothing else is a manual
step: `rust-toolchain.toml` requests the `wasm32-unknown-unknown` target and the
`llvm-tools` component, and rustup installs both on first use. `mise.toml` pins
`cargo-deny`, and `msrv` pulls Rust 1.91 on its first run — a one-time download
that used to be CI's to pay.

The gate is defined once, in `mise.toml` and `.mise/tasks/`. That was already
true while CI existed, which is why removing the workflow changed no command:
it called those same tasks rather than restating them.

| | runs | why |
|---|---|---|
| `mise run pre-commit` | fmt, clippy on the library | Seconds. A commit hook people bypass is not a gate. |
| `mise run pre-push` | the whole gate | A push is rarer than a commit, and a red gate here costs one local run instead of a CI round trip. |

There is no third row. `deny`, `msrv` and `wasm-guards` used to be CI's, and they
moved into `gate` when the workflow was deleted — so `mise run pre-push` is now
the whole of it. Nothing else checks this repository.

`mise run gate` runs its steps **one at a time**, cheapest first. That is why the
task lists sub-tasks rather than using bare `depends`: mise parallelizes
dependencies, and two concurrent cargo invocations contend for one target
directory and one machine's memory.

The tests inside `mise run test` are the exception, and only because they do not
share that directory the same way: `cargo nextest run` executes test binaries as
parallel PROCESSES, which is where most of the gate's time used to go. One file
needs protecting from that — see `.config/nextest.toml`.

`mise.toml` pins `sccache` and sets `RUSTC_WRAPPER`, so a cargo command run under
mise compiles through a shared cache. `mise install` fetches it; there is still
no manual setup step. It also sets `CARGO_INCREMENTAL=0`, because sccache cannot
cache an incremental build — export `CARGO_INCREMENTAL=1` in a shell where you
are iterating by hand rather than gating.

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
- You are pushing something the gate cannot check, and the `pre-push` hook has
  refused it for that reason: a tag pointing at an older commit, or a branch
  that is not the one checked out. Nothing built those trees in this run, and
  the refusal is the only part of the system that knows.

Not legitimate:

- The gate is slow.
- "I will fix it in the next commit."
- The failure looks unrelated to your change. Find out why before you decide
  that.

If you bypass, say so in the pull request, and name which reason applied.

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

**Publishing a previously-private type? Run `mise run docs` before you believe the
change compiles.** Making a type `pub` promotes its **whole** rustdoc to public
documentation, so every intra-doc link in it that pointed at a private neighbour —
legal while the module was private — becomes an error at once. You did not edit
those links; publishing one item revoked their legality. Name the private item in
backticks instead of linking it.

A related convention, which the tree follows without stating why: `keylink_persist`
is optional, and **only the `#[cfg(feature = "persist")]` module links to it**.
Always-compiled files that mention it — `call.rs`, `observe.rs` — use backticks.
Follow that rather than adding a link from a module that compiles without the
feature.

**`cargo check`, `cargo clippy --all-targets`, the whole test suite and
`mise run anchors` all pass with those errors in the tree.** `mise run docs` is the
only gate that sees them, so a green suite is not evidence here.

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
`mise run wasm-guards` asserts that all three actually fire — that check was
CI's until the workflow was deleted, and it moved into `gate` in the same
change. A `compile_error!` nobody triggers is a
comment.

`mise run wasm-browser` is the fourth task and the only one that loads a module.
It builds `keylink/tests/browser.rs` for the target, and
`wasm-bindgen-test-runner` serves it to headless Chrome over a loopback port and
calls each test inside the page. Both tools are pinned in `mise.toml`; the
chromedriver pin tracks the **installed Chrome's major version**, not keylink,
so a Chrome that auto-updates past it reddens the task until the pin follows.

**What now runs in a browser**, all four assertions under `--no-default-features
--features web`:

- `derive_alpn` — the schema hash is computed, is stable across two calls, and
  still moves when a field is added.
- `postcard` encode and decode of `WireResponse<u32>`, in both the `Ok` and the
  `Err(RemoteError)` arm.
- `Body::from_web_file` — a real `File` becomes a body that declares its length.
- `Body::from_web_stream` — a `ReadableStream` with no BYOB reader is refused as
  `Error::NotAByteStream` rather than thrown out of.

The task found a defect on its first run, which is the shape of what the three
build tasks cannot see: keylink asked for `wasm-streams 0.6` while reqwest, which
iroh-relay pulls in on this target, asks for `0.5`. Both export the same
`#[wasm_bindgen]` symbols, so `rust-lld` refused the module with twenty
`duplicate symbol` errors — **no consumer of the `web` feature could link a wasm
binary.** `mise run wasm-web` was green throughout and was not wrong: it builds a
library, and an rlib is never linked.

**What still does not run in a browser.** No `Node`, no `Server`, no `Client`, no
connection, no actor turn. A page cannot open a UDP socket, so QUIC is out of
reach whatever keylink does, and no test here tries. Nothing from
`keylink-persist` runs either — the version frame and the migration chain compile
for this target and are executed only on a native host. `keylink/tests/browser.rs`
carries the same list beside the tests.

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
  and none of them was red, which is why this is a gate. **Registration is opt-in and
  the gate says so**: it checks the records listed in that task, not every record in
  the tree, and nothing will notice a record you do not add.
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
