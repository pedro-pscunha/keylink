# RFC-06 step 1, adapted to the `correctness-programme` branch

> **Current testing note (2026-08-10):** Any ALPN literal-pin step below is
> historical and is superseded by `CONTRIBUTING.md`'s **The wire contract**.
> The literal-pin tests are ignored until keylink has a stable version. Do not
> run or update them, and do not order work around them. The active contract
> property tests remain in the gate.

This replaces `keylink-rfc06-implement-prompt.md`. The design does not change —
RFC-06 is still the design and its implementation brief is still the order of
work. What changes is **where the work lands**, because the branch it lands on
rewrote the two files step 1 mostly lives in.

Everything the original prompt said that is not contradicted below still holds.

---

## Target

Not `/Users/pedrocunha/repos/keylink` on `main`. Work in the worktree:

```
/Users/pedrocunha/repos/keylink.correctness-programme     branch correctness-programme
```

That branch is four commits ahead of `main` (`9a368ab`, `982305e`, `84a8310`,
`e92956e` — the correctness programme, waves A–D). `mise run gate`,
`mise run deny`, `mise run msrv` and `mise run wasm` are all green on it.

**`main` is owned by another session.** Do not commit to it, do not rebase onto
it, do not `git checkout` in `/Users/pedrocunha/repos/keylink`.

## Step 0 — bring RFC-06 onto the branch

The RFC and its brief are on `main` at `836179c` and are **not** in this
worktree. That commit is docs-only, so:

```sh
cd /Users/pedrocunha/repos/keylink.correctness-programme
git cherry-pick 836179c
```

The scratch material the brief tells you to read stays in the base repo's
`.tmp/`, which is per-worktree and gitignored. Read it there:

```
/Users/pedrocunha/repos/keylink/.tmp/probe-observer/{probe,owned,realpanic}.rs
```

## Read before you write anything

The brief's list, with two corrections.

1. `docs/rfc-06-observers.md` — whole. §2.6 and §2.7 are where the collisions are.
2. `CONTRIBUTING.md`.
3. `keylink/src/local.rs` — end to end. **It is not the file the brief describes.**
   It gained `Queued<T>` and `LocalActor::ask_catching_before` in wave B.
4. `keylink/src/middleware.rs` — the `Progress` / `Reached` marker, and
   `Queueing`. The brief does not know these exist and one of them decides
   whether step 1 is correct. See collision 1.
5. `docs/rfc-05-idempotency.md` §2 — the defect the progress marker exists to fix.
   Read it before touching anything that marks progress.
6. `/Users/pedrocunha/repos/arche/.tmp/keylink-impl/peak-rust.md` and
   `.tmp/probe-observer/`, as the brief says.

---

## The collisions

### 1. Critical — the observer chain runs *after* the progress marker says the turn completed

**This is the one that matters. Implementing §2.6 verbatim reintroduces the
exact defect wave A removed.**

Wave A gave every call a three-state progress marker on `Context`, and the
idempotency ledger settles the key from it: nothing dispatched → the key is
released; dispatched and unfinished → `Interrupted`; completed → the answer is
recorded and replayed. The marks are made by generated dispatch, and the actor
path marks them **inside the turn closure** (`keylink/src/macros.rs:775-790`):

```rust
.ask_catching_before(__queueing.map($crate::Queueing::deadline),
    move |__state: &mut A| Box::pin(async move {
        __progress.dispatched();
        let __turned = __actor_turn(__state, $crate::Caller::Peer(&cx), req, body).await;
        __progress.completed();          // <-- HERE
        __turned
    }))
```

RFC-06 §2.6 wraps that closure and runs the observers **after it returns**. So
by the time an observer refuses, `completed()` has already been marked. A row-3
refusal — turn completed, observer refused, nothing committed — would then be
recorded as the key's settled reply, and every retry would be answered
`observer refused: saver` forever, for a turn whose state was never committed.
That is RFC-05 §2's defect, one layer down.

**Required change, and it is small.** Move `__progress.completed()` out of the
turn closure and into the dispatch's match arm that actually has a committed
answer — the `Queued::Ran(Ok(..))` arm, after `ask_catching_before` returns.
`dispatched()` stays inside the closure, where it must be, because only the turn
knows it started.

- Do not walk the marker back on refusal. `Progress` is monotone by `fetch_max`
  on purpose (`middleware.rs:85-90`); a downgrade path would be a second way to
  reach a state, and the marker's whole value is that it has one.
- The result is that a refused turn leaves the key `Interrupted`, which is
  exactly right: RFC-06 §2.6 says of row 3 that *"the turn ran, and the caller
  cannot tell whether it ran — that is the same ambiguity `client.with_idempotency`
  exists for."* On this branch that ambiguity has an answer, and this is it.
- **Append a revision note to RFC-06** saying so. It is a strengthening of §2.6
  row 3, not a deviation to hide. Do not edit §2.6 in place.

Prove it: a served durable actor, an observer that refuses, a retry under the
same idempotency key gets `Interrupted` and **not** the refusal. Break it by
moving `completed()` back inside the closure and watch the retry replay the
refusal.

### 2. Major — `ask_catching` has three callers on this branch, not two

The brief says *"Two callers exist in the whole repo: `local.rs:202` and
`macros.rs:672`."* There are three:

| caller | now at |
|---|---|
| `LocalActor::ask` | `keylink/src/local.rs:218` |
| `LocalActor::ask_catching_before` | `keylink/src/local.rs:294` (the `None`-deadline path) |
| the served actor dispatch | `keylink/src/macros.rs:771` (via `ask_catching_before`) |

`ask_catching_before` is wave B's `QueueTimeout` support. Its error type changes
with `ask_catching`'s, and its `Turn<A>` closure needs the second parameter too.

### 3. Major — `Queued<T>` and `TurnLost` are two different questions; compose, do not merge

`Queued<T>` (`local.rs:145`) says whether the turn **started**:
`Ran(Result<T, Panic>)` or `Expired`. `TurnLost` says why the value did not come
back. They are orthogonal and both are needed. The composition that works:

```rust
pub async fn ask_catching_before<T, F>(..) -> Result<Queued<T>, TurnLost>
```

with `TurnLost::Gone` replacing today's `MailboxClosed` and `TurnLost::Refused`
new. Do not flatten `Expired` into `TurnLost` — `Expired` means *nothing ran*,
which is the opposite of what every `TurnLost` arm means, and the ledger settles
them differently.

### 4. Major — §2.6's table has three rows and the dispatch now has four arms

A message refused by `QueueTimeout` while still queued **never runs**, so no
observer runs for it. That is correct by construction and needs no table row —
but the served dispatch match at `macros.rs:795-840` already has four arms
(`Ran(Ok)`, `Ran(Err(panic))`, `Expired`, `Err(gone)`) and `TurnLost::Refused`
makes five. Do not add an `Expired` row to §2.6; do make sure the `Expired` arm
keeps marking nothing on the progress marker, which is what leaves the key
claimable.

### 5. Major — the actor trait signature changed

Wave B deleted `Actor::caller` and gave every generated method a caller argument:
`Handler` takes `cx: &Context` first, `Actor` takes `cx: Caller<'_>` first
(`send` keeps `body: BodyReader` last). Any probe, example or test the brief has
you write against the `Actor` trait must use the new shape. `Observer` itself is
unaffected — it takes `&A`, not a request.

Also: `Handle::service` now opens with `let () = ServableAsActor::<A>::CHECK;`, a
post-monomorphization const assert that refuses to compile for a service
declaring a `send` method. **`actor_with` must carry the same line** — it is a
second door to the same actor path, and a door without the check is the hazard
RFC-04 §16.3 exists to close.

### 6. Minor — the ALPN pin moved, and the "do not add a variant" argument got stronger

§2.7 cites `protocol.rs:772` and the old literal. Current coordinates:

```
keylink/tests/protocol.rs:811   const PINNED: &str = "keylink/Probe/aa77e29e5e1ccdc7";
```

Wave A moved that hash deliberately once, for three genuinely wire-visible
reasons, and the test's doc comment now records why. §2.7's conclusion is
unchanged and its reasoning is now cheaper to check: a second ALPN move in the
same unpublished window would be gratuitous. Use `Failed(String)` with both
prefixes, as written.

### 7. Minor — `.tmp/market-example/` will not build until it is migrated

The brief ends step 1 with *"build `.tmp/market-example/`"* as the only external
macro-hygiene check. That crate was written against pre-wave-A keylink: it has
`impl floor::Actor for Floor { fn caller(..) }`, which no longer exists, and
handler methods with no `cx`. Migrate it first (mechanical: add `_cx: &Context` /
`_cx: Caller<'_>`, delete the `caller` hook, read the peer from the argument) or
its red is a false red and tells you nothing about hygiene.

### 8. Scope — step 2 crosses this branch's stated blast radius

`docs/goals/2026-08-06-correctness-programme.md` says `keylink-persist/**` must
not be touched. RFC-06 step 2 drops `keylink-persist` from keylink's
dev-dependencies and builds `keylink-durable` on top of it. That is a deliberate
widening and it needs to be said out loud in the step-2 commit message rather
than discovered in the diff. Step 1 does not touch it.

---

## Rules that invert on this branch

- **`git add -A` is safe here and is what has been used.** The "a parallel
  session owns `README.md` and `docs/goals/`" rule is about `main`. On this
  branch the ownership is the other way round: this worktree owns `README.md`
  and `docs/goals/`, and `main` is the one you must not touch. Stage what you
  like inside the worktree; never stage anything in
  `/Users/pedrocunha/repos/keylink`.
- **The gate has been run by hand here, and commits used `--no-verify`.** That
  was because waves were verified in pieces. For RFC-06, follow the brief: let
  the hook be the gate, and do not pre-run it before a commit.

## Rules that carry over unchanged

- Opus. **Never fable**, whatever a skill or a plan says.
- Commit messages in a file, `git commit -F`. Read `git log -4` on this branch
  for the register — the four wave commits are the house style.
- Do not edit an RFC to match your code. Append.
- Break every guard with `scripts/break-once.sh` (present in this worktree).
  §9 of the RFC gives the break for every proof.
- No stand-in types in a probe. The real `Panic` holds `Box<dyn Any + Send>`, so
  it is not `Sync`, so `&Panic` is not `Send`.
- On the way out: `drop(hooks); drop(inbox); ended.send(..)`, in that order. The
  test it protects is `stop_drains_what_was_sent_before_it`, now at
  `keylink/src/local.rs:520`.
- No wall-clock assertions in an integration test.
- **There is no git remote.** Nothing has ever been pushed. Creating one
  publishes the repository and is Pedro's decision.

## Stale coordinates, in one table

Every line number in the brief and in RFC-06 §2.6–§2.8 predates this branch.

| the brief says | on this branch |
|---|---|
| `local.rs:202` (`ask_catching` caller) | `local.rs:218` |
| `local.rs:232` (oneshot retains) | `local.rs:255` |
| `local.rs:327` (`deliver`) | `local.rs:414` |
| `local.rs:441` (`stop_drains_..`) | `local.rs:520` |
| `local.rs:59-67` (guarded invariant) | unchanged, `local.rs:59-67` |
| `macros.rs:588`, `:804`, `:819`, `:826` (`LocalActor` reached publicly) | `macros.rs:656`, `:924`, `:939`, `:946` |
| `macros.rs:672` (`ask_catching`) | `macros.rs:771` (`ask_catching_before`) |
| `macros.rs:695` (panic → `Failed`) | `macros.rs:799` |
| `macros.rs:700-716` (`NoSuchActor`) | `macros.rs:825-835` |
| `macros.rs:1751` (shadowing probe) | `macros.rs:1953` |
| `protocol.rs:772` (pinned ALPN) | `protocol.rs:811`, literal `aa77e29e5e1ccdc7` |
| `actor.rs:846` (`spawn_with`) | unchanged |

Re-derive rather than trust the table: `rg -n` the symbol.

## Do step 1 only

The port in keylink, no storage anywhere. Then `mise run gate`, commit with
`git commit -F` and a message file, and stop and report. Wait to be told before
starting step 2.

Report explicitly on collision 1 — where `completed()` ended up, the test that
pins it, and the break you watched fail.
