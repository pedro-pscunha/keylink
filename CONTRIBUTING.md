# Contributing to keylink

## The gate

```sh
./scripts/install-hooks.sh   # once per clone
mise run gate                # the whole gate
```

**CI is off by decision, and `mise run gate` at `pre-push` is the gate.** The
`pre-push` hook runs it, which makes `./scripts/install-hooks.sh` load-bearing
rather than a convenience. Skip it and nothing checks your pushes.

### CI is off by decision, and TWO independent things keep it off

The decision is not a fallback and it is not an accident. Commit `afdbeb3` —
"build: disable GitHub Actions, and move what only it ran into the gate" — did both
halves at once, and moving what only CI ran into `gate` is what made switching it
off safe. It stays off; the gate does this job.

Two reasons rather than one, which is what stops this paragraph depending on either
of them. Read back on 2026-08-08, and reproducible as written — `gh` resolves
`{owner}/{repo}` from the local remote:

1. **The repository setting is off.**
   `gh api repos/{owner}/{repo}/actions/permissions` answers
   `{"enabled":false,"sha_pinning_required":false}`. Read back rather than inferred
   from a write's exit code.
2. **`main` carries no workflow file.** `origin/main` has no `.github` directory,
   so a pull request based on `main` would have nothing to start even with the
   setting on.

Either alone would hold. Both is the state, and writing both down is what lets a
reader who finds one of them changed know that the other is still holding.

**The residue, and it is inert.** Six unmerged REMOTE branches still carry
`.github/workflows/ci.yml`, and five LOCAL gate branches carry it as well — eleven
refs across the two namespaces, counted on 2026-08-08. Both namespaces are named
because this sentence used to say "six unmerged branches" while counting only
`refs/remotes/origin`, which reads as a count of the repository and is not one.

Ten of those eleven share blob `77fda9a`, whose trigger is `push` to `master` plus a
bare `pull_request:` — that second half is where CI runs used to come from, and as
measured on 2026-08-08 the repository had recorded twenty runs, all on
`pull_request`, seventeen of them red, the last at 13:05Z. Those figures are past
tense with a date on purpose: nothing can join that set while the setting is off.
**So those files are dead weight rather than a live trigger** — merging one starts
nothing today. Delete them as their branches land, and know they are there before
anybody considers the setting again.

**The eleventh ref carries blob `0b01d7e`, and it is NOT the special case this
paragraph used to claim.** The claim was that this copy is `push`-only against a
`master` branch that does not exist, so it "could never fire at all". Measured
rather than read — `diff <(git cat-file -p 77fda9a) <(git cat-file -p 0b01d7e)`
reports exactly two differing lines, both `- run:` steps, and the two `on:` blocks
are byte-identical. **All six remote copies carry the same bare `pull_request:`.**
So that copy is inert for the one reason the other ten are, the setting being off,
and for no reason of its own. No ref named `master` exists, which disarms the `push`
half of all eleven and none of their `pull_request` halves.

Two consequences worth knowing before you hit them:

- **`mise` is now required, not recommended.** The `pre-push` hook used to fall
  back to a weaker set of checks when mise was missing, on the grounds that CI
  ran the rest. It refuses now, because a gate that passes while covering less
  than it claims is a false green.
- **The gate runs on your machine, on your OS.** CI ran Linux. Nothing checks the
  other platform any more.

**In a worktree, the hooks are your branch's own.** `./scripts/install-hooks.sh`
writes `core.hooksPath` as the relative `.githooks`, and git resolves that against
the working-tree root — so each worktree runs the hooks of the branch checked out
in it, and a gate change arrives in a worktree when that worktree rebases. The
script's header carries the measurement and argues the choice against the absolute
value, which makes every worktree run one checkout's hooks instead. One hazard
comes with it: a branch whose tree has no `.githooks/` runs **no hook at all**, and
the push succeeds. To exercise a hook change without touching any configuration,
run `mise run hook-guard`.

The `pre-push` hook reads git's list of ref updates before it runs any of that,
and it answers differently for two shapes of push. A push whose updates are ALL
deletions skips the gate and prints that it did: a deletion carries no tree, so
building one checks nothing about it. A push delivering an object that is not
`HEAD` is **refused**, because the gate builds the working tree — a green result
would be a statement about a tree the push does not carry.

**That refusal is about the REFS a push carries, not about the command.** Measured
on 2026-08-08 against the hook:

| push | result |
|---|---|
| `git push --all` with every branch at `HEAD` | accepted, gate ran, both refs landed |
| `git push --all` with one branch behind | refused |
| a lightweight tag at `HEAD` | accepted, gate ran |
| **any annotated tag, including one at `HEAD`** | refused |
| an annotated tag at an older commit | refused |
| `git push --force` after rewriting history | accepted, gate ran, the new tip landed |
| `git push --force` after `git commit --amend` | accepted, gate ran |
| `git push --force-with-lease` | accepted, gate ran |
| `git push --force` rewinding a branch to an older commit | refused |

`git push --all` is refused as soon as any branch it carries is not at `HEAD`, and
not before. The annotated tag is the case worth knowing before you hit it: git sends
a tag's OWN object sha on the hook's stdin, so `git tag -a v1.0 && git push origin
v1.0` at the tip is refused even though the tag points at `HEAD`. Push it with
`--no-verify`.

**A force-push is not a case at all, because the hook never asks what the remote
held.** Git writes that tip as the fourth field of each update line, and the hook
reads that field and never looks at it again — so a rewrite, an `--amend` and a
`--force-with-lease` are each judged only on the sha they deliver. All three deliver
`HEAD`, so all three run the gate and land. A forced rewind to an *older* commit is
refused for the ordinary reason: what it delivers is not `HEAD`. All four were
measured against the hook, and `scripts/hook-guard.sh`'s scenario 13 pins them.

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
| `mise run pre-commit` | framing, anchors, anchors-selftest, fmt-check, lint-fast | Seconds. A commit hook people bypass is not a gate. |
| `mise run pre-push` | the whole gate | A push is rarer than a commit, and a red gate here costs one local run instead of a CI round trip. |

There is no third row. `deny`, `msrv` and `wasm-guards` used to be CI's, and they
moved into `gate` when the workflow was deleted — so `mise run pre-push` is now
the whole of it. Nothing else checks this repository.

`mise run gate` runs its steps **one at a time**, cheapest first. That is why the
task lists sub-tasks rather than using bare `depends`: mise parallelizes
dependencies, and two concurrent cargo invocations contend for one target
directory and one machine's memory.

`mise run test` keeps Cargo invocations serial too. Its speedup is inside one
libtest process instead: `keylink/gate-tests/gate.rs` includes the native
integration suites that can safely share a process, so iroh's endpoint teardown
is paid once rather than once per file or once per test. Process-wide
measurements, the nested Cargo build, and the scheduling-sensitive reconnect
suite remain separate.

The task runs three Cargo commands. The first covers workspace library and binary
tests, the aggregate, and the isolated native suites. The second uses `--test
'*'` for integration targets in every workspace member except `keylink`. The
third runs workspace doctests explicitly. Browser tests belong to
`wasm-browser`, and examples belong to `examples`; both tasks are separate gate
steps.

The aggregate inventory fails closed in three directions. Every Rust file under
`keylink/tests/` must be included in the aggregate or named as isolated. Every
isolated native suite must appear as an exact `--test` selector in `[tasks.test]`.
The non-native `browser` suite must appear as an exact selector in
`.mise/tasks/wasm-browser`. Add a test file without wiring its runner and the
aggregate fails.

`mise.toml` pins `sccache` and sets `RUSTC_WRAPPER`, so a cargo command run under
mise compiles through a shared cache. `mise install` fetches it; there is still
no manual setup step. It also sets `CARGO_INCREMENTAL=0`, because sccache cannot
cache an incremental build — export `CARGO_INCREMENTAL=1` in a shell where you
are iterating by hand rather than gating.

While iterating on a dependency change, run `mise run deny` as the focused check.
The pre-push hook also runs it through the full gate, so dependency policy is not
a separate manual-only check.

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
- **You are pushing an annotated tag, including one created at `HEAD`.** Git sends
  the tag object's own sha, which is never `HEAD`, so the hook refuses it and no
  remedy short of this flag applies. This is the most ordinary tag push there is,
  and it is listed here so that hitting it is not read as a fault.

Not legitimate:

- The gate is slow.
- "I will fix it in the next commit."
- The failure looks unrelated to your change. Find out why before you decide
  that.

If you bypass, say so in the pull request, and name which reason applied.

## The wire contract

A generated service negotiates `<crate>/<Service>/<label>/<shape hash>`; an addressed service adds `/keyed` before the label. The hash is derived from the complete wire shape, not the label. `previous` blocks pin frozen shapes and supply adjacent conversions; see README's **Contract windows**. The per-service `/versions/<probe hash>` protocol reports an admitted peer's window when no version matches.

Keylink has no stable version and has never run in production. A contract-hash
move therefore costs nothing: do not group commits around one, avoid the correct
type shape to preserve one, or report one as a compatibility cost.

`keylink/tests/protocol.rs` retains two literal-pin tests for the day a stable
version is cut: `the_contract_hash_has_not_moved` for `keylink/Probe`, and
`the_inspect_contract_hash_has_not_moved` for `keylink/Inspect`. Both are
`#[ignore]`d. Do not run them as a gate and do not update their literals while
the crate is unstable.

The active property tests make a different claim. They prove that different type
sets derive different contracts and that a peer built from other types is refused
at the handshake. Those tests stay on because they check the mechanism rather
than freezing its current output.

When keylink gains a stable version, remove the ignores deliberately. Read new
literals from the failing run then; never compute them. That is also the point at
which a contract move becomes a compatibility decision again.

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
  - **One exception, and it is about features rather than about errors:**
    an enum with a **feature-gated variant** is `#[non_exhaustive]` whatever
    else it is. `Records` is the case — its `Durable` variant is behind
    `persist`. Cargo unifies features across a build, so a downstream crate
    matching it exhaustively compiles while nothing enables `persist` and stops
    compiling when an unrelated crate in the same build turns it on. That break
    is not the downstream author's to cause or to prevent.
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

**And there is one broken link `mise run docs` does not see either.** rustdoc does
not resolve an intra-doc link to a `#[doc(hidden)]` item, and does not warn about
it: it emits the raw path as the `href` and leaves it. `-D warnings` is green
while the rendered page carries `href="crate::Origin"`, which goes nowhere. The
one gate that catches a broken link is blind to this kind, so read the generated
HTML instead of the exit code:

    rg -o 'href="(crate|self)::[A-Za-z:_]+"' target/doc/keylink/ | sed 's/.*href="//;s/"//' | sort -u

Every path it prints is a dead link. Ten are dead this way at `5198ecb`, which is
a measurement and not a target: the check is here so that an *eleventh* is
noticed, and clearing the ten is its own pass. Name a hidden item in backticks
rather than linking it — and say in the doc that that is why, because the next
author who sees a bare backticked type reads it as an oversight and links it.

That last clause is the general rule, and it is worth more than this instance:
**a fix that removes a defect without recording why is half a fix, and the half
that rots.** The sentence naming the defect is what stops the next author
reintroducing it, and it is the first thing a later rewrite drops, because it
reads as commentary while the fix reads as code. These two links were un-linked
once with the reason recorded, and a rewrite three commits later kept the fix and
dropped the reason — which left the tree correct and the next author unwarned.

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

**What now runs in a browser**, under `--no-default-features --features web`:

- `trust::Chain::verify` — two-link delegation and caveats, without sockets.
- `derive_alpn` — the schema hash is computed, is stable across two calls, and
  still moves when a field is added.
- `postcard` encode and decode of `WireResponse<u32>`, in both the `Ok` and the
  `Err(RemoteError)` arm.
- `Body::from_web_file` — a real `File` becomes a body that declares its length.
- `Body::from_web_stream` — a `ReadableStream` with no BYOB reader is refused as
  `Error::NotAByteStream` rather than thrown out of.
- `Patience` — portable constructor boundaries and the topic timeout path run
  on the browser clock.
- `BroadcastDepth`, `DropOldest`, and `DropNewest` — constructor boundaries and
  both bounded lossy fan-out policies run in Chrome.

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

`keylink-http` owns its unit tests and real loopback HTTP integration tests. Run
one async or liveness test by its exact name before you run its test binary.
The workspace test task selects all integration targets outside `keylink` with
`--test '*'`, so a new adapter test file needs no second test inventory.

The docs task names `keylink-http` explicitly because the crate denies missing
documentation through `RUSTDOCFLAGS`. The examples task runs the adapter example through `.mise/tasks/http-smoke`.
The task uses `curl`; a build without execution does not verify HTTP behavior.

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
- Order two real calls by something you can observe, never by a sleep. "Sleep 80 ms
  so the first call is inside its turn" is a bet that a QUIC round trip fits in
  80 ms. Wait for the actor to say so (`status().busy`, `status().queued`, an
  `entered` channel, `key_state`), hold the turn on a semaphore until the order you
  need has been observed, and bound every such wait at a value no stall can cross.
- A window the test's own steps must beat is the same bet: a restart's
  `recovered_after`, an idle-eviction `after`, a `Timeout` shorter than the dispatch
  in front of it. Give it a value load cannot spend, and where the window decides
  the order, let the test decide the order itself.

## RFCs

`docs/rfc-*.md` record decisions, including the ones that were reversed. Add one
when a change rests on a judgement somebody will question in six months.

Do not edit an RFC so that it matches new code. Append to it. The value of the
record is that it shows what was believed at the time.

One carve-out: an identifier a break record names — the test it runs, the file it
edits, the script that drives it, the literal it searches for — may be renamed in
place to keep the record runnable, because that changes a pointer and not a claim.
**The shape of the record does not matter.** Some are table rows and some are
bullets; both point, and the carve-out is about pointing. What a record *asserts*
— a count, a watched message, the verdict a break produced — is still appended,
never edited in. And nothing here permits editing an RFC so that it matches new
code: that is the rule above, and this carve-out does not reach it.

**The search literal is in that list for the same reason the rest of it is.**
`scripts/break-once.sh` substitutes one literal and refuses one that does not
occur exactly once in its target, so a record whose literal names a symbol that
was renamed under it does not run at all. The carve-out exists to keep a record
runnable, and a stale search literal is the commonest way one stops being: it is a
pointer into a source file, spelled in that file's own words instead of as a path.
Reading it as a claim because it is written in code font protects nothing and
costs the record the only thing it was for.

**Re-quote a rename, never a restructure.** The permission is to spell the same
line the way the tree now spells it. Where the arm a record broke was split,
merged, or turned into a different shape, there is no rename to apply, and writing
a fresh literal invents a break nobody ran. Append that the literal is spent and
name what replaced the code — the same treatment a deleted test gets, and for the
same reason: a record that cannot be re-expressed is still a record of something
that happened.

**The search-literal widening was written by the wave it governs, and no record has
used it yet.** The branch that added it then met
`docs/rfc-07-the-turn-model.md` §18.2, which found that both records the widening
was written for had been *restructured* rather than renamed — so both were declared
spent under the paragraph above, and neither literal was re-quoted. The widening is
live, deliberate and **unexercised**: it has passed no test of itself, and the first
record that uses it will be the first evidence it works. That is stated here rather
than left as a silence, because a rule adopted and never fired reads exactly like a
rule nobody agreed to, and the next author is entitled to know which this is.

**Write a figure with the commit it was measured at.** A count of anything in the
tree — tests in a file, variants in an enum, rows in a registry — is a measurement
of the moment it was taken, and an RFC keeps it for ever. *"Fifteen tests, at
`75155e3`"* costs two words and cannot rot, because it stops claiming anything
about today; the bare form reads as current for as long as the file exists, and a
reader who counts and disagrees cannot tell whether the record went stale or the
tree moved. `.mise/tasks/anchors` does this with an explicit pin over a block of
figures. RFCs have no such mechanism, so the sentence has to carry it. Better
still, cite the guard rather than the count, and there is nothing to pin.

This is a rule for writing a **new** figure. A bare one already in a file is
corrected the way everything else is — by appending, never by editing the number
in place. RFC-07 §13, §15.1 and §16 are three worked examples.

## HTTP gateway and documents

Two smokes, and they are not redundant. `mise run http-smoke` drives the
singleton `rpc` surface — the small example, the fast check. `mise run
http-smoke-full` drives every route in the crate table against a second example
that mounts a singleton surface, a keyed surface and the two document endpoints
in one process: streaming both ways, blob bytes, ranges, conditional GETs,
encoded actor ids, and both document media types. Both run in `gate`.

They use real curl on purpose. Everything else in this workspace goes through
`reqwest` or `tower`, and both normalise things a deployment's client will not.

`keylink-openapi`'s snapshots under `tests/snapshots/` are the published
contract. `KEYLINK_OPENAPI_BLESS=1 cargo nextest run -p keylink-openapi` rewrites
them; read the diff before committing it, because a change there is a change to
what every generated client will be built from.

A document is built from the same `HttpSurface` the gateway is mounted with. If
you find yourself constructing a second surface to describe the first, the
document and the runtime have already diverged.
