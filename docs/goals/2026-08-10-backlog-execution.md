# Execute the keylink backlog: R9, the wasm guard, R6, R5, R7, R10, R8

Repository: `/Users/pedrocunha/repos/keylink`. Branch `main`.

Execute end to end without asking questions. You have full authority to change or
remove invariants, move contract hashes, and reshape any public API. Decide, fix,
improve, validate. Annotate assumptions inline and report them at the end.

Use `/subagent-driven-development` throughout. Read `/rust-best-practices` and
`/rust-illegal-states` before designing. Keep `/codebase-design` and `/ponytail`
active. Gate with `/brooks-review`, apply findings with `/receiving-code-review`.
The reviewer never fixes its own findings.

---

## Standing rules that override habit

**The ALPN is not a cost.** A moved contract hash costs nothing until keylink has
a stable version. Do not price a pin move, order commits around one, or let one
shape a design. `AGENTS.md` carries the ruling. The only obligation is
bookkeeping: keep the two pinned tests in `keylink/tests/protocol.rs` passing, and
**read the new literal off the failing run — never compute it**.

**Capture every long command to a file.** `cmd 2>&1 | tee /tmp/<name>.log` with
`set -o pipefail`, and put the exit status into the artifact. Never pipe a gate,
a test run, or a push through `tail` or `head` — a failed run then cannot say why
it failed and the whole run is wasted. Never read `$?` after a pipe.

**`exit 0` does not mean pushed.** Verify with `git rev-parse HEAD` against
`git ls-remote --heads origin main` and require an exact SHA match.

**Freeze the branch while a gate runs.** The hook grades the working tree. Do not
commit, amend, or write into the tree while a push is in flight.

**Do not pre-run a full gate before committing or pushing.** The hooks are the
gate. Run only the narrowest relevant target while iterating.

**Break records.** Every new guard that can be broken needs a record and a row in
`.mise/tasks/anchors`. Read `RECORD_BLOCKS` off the failing run of
`mise run anchors` — never compute it. `scripts/break-once.sh` takes a positional
substring filter, and `── selected=1` is the only valid reading: `0` means
nothing ran and `?` means it did not compile, neither of which is a caught defect.

---

## Evidence status

Everything below marked **[verified]** was read directly at `f6f5f87`. Items
marked **[reported]** come from an audit subagent and were not independently
confirmed — re-read them before relying on them. Re-anchor every line number
before use; this tree moves.

---

## Scope

Build, in this order. The order is a dependency order, not a priority order.

### 1. R9 — timing on `TurnInfo`, and it unblocks R6

An observer sees no time at all, not even the turn's own duration.

- `TurnInfo` has exactly two public fields, `outcome` and `method`,
  `keylink/src/observe.rs:211-230`. **[verified]**
- `observe.rs` declares no time type anywhere. **[reported]**
- The one place a turn runs is `observed_turn`, `keylink/src/turns.rs:935`. The
  handler span is `turns.rs:959-960`; `TurnInfo` is built at `:972`, **before**
  the observer chain at `:985-1042`. A duration field on the value as built
  therefore measures the handler only, unless the construction moves. **[reported]**
- All three paths — local, keyed/served, supervised — reach one dispatch site, so
  one measurement point covers them. **[reported]**

Constraints:

- **The clock must be `n0_future::time::Instant`**, already imported at
  `turns.rs:204`. `std::time::Instant::now()` panics on
  `wasm32-unknown-unknown`. **[reported]**
- `TurnInfo::for_test` documents that adding a field is a deliberate breaking
  signature change. **[reported]**
- Decide explicitly whether the duration covers the handler only or the handler
  plus the observer chain, and say so in the rustdoc. They are different numbers.

### 2. The wasm guard — a live hole, independent of R9

Nothing in the gate forbids `std::time::Instant`. `rg 'std::time' .mise mise.toml
.githooks` returns nothing, on a positive control that proves the search reaches
those files. **[verified]** `keylink/tests/browser.rs` holds four
`#[wasm_bindgen_test]` cases and none spawns an actor or runs a turn.
**[verified]**

So a `std::time::Instant::now()` on the turn path builds green, passes every gate
step, and panics only in a real browser. Close it. A `representability.rs`-style
probe or a gate grep are both acceptable; prefer whichever the crate already uses
for this class of refusal.

### 3. R6 — stuck-actor detection

**Narrow the claim first.** "Nothing in keylink handles a hung actor" is false:
`Timeout` at `keylink/src/middleware.rs:650` is headed *"The only bound that
answers a caller behind a turn that never returns"*, and it answers that caller.
**[verified]** The real gap is **detection and reporting**. Default to reporting,
never killing.

- `ActorStatus` carries `id`, `idle_for` (`:1228`), `busy` (`:1239`) and
  `buffered`, in `keylink/src/actor/registry.rs`. **[verified]**
- `Serving { busy, buffered }` at `keylink/src/observe.rs:774` is the shared
  reader across both sides, and it carries no clock. **[verified]**
- `LocalActor` (`turns.rs:3635`) carries no clock; a local actor has no registry
  entry to hang one on. **[reported]**

Two obstacles that are worse than the ones the backlog names:

- `idle_for` is *last routed*, not *last handled*, and the sweep predicate reads
  only it — so a wedged actor that callers keep addressing is the one actor
  eviction structurally cannot reach. **[reported]**
- `after_turn` runs after the handler returns, so **no observer fires for a turn
  that never returns**. The one event a stuck actor produces is the one nobody is
  told about. **[reported]**

The real dependency: a **turn-start timestamp**. `busy` is an `AtomicBool` on both
paths. Making it "busy since T" changes a type both paths share. That is why R9
comes first.

### 4. R5 — drain sentinel

**The backlog's own argument is wrong; use this one.** The word `uncheckable`
appears in no Rust source, at HEAD or at `d54f302` — only in the proposal.
**[verified]** But the claim is real, at `keylink/src/actor/registry.rs:2185`:
*"Nothing can check that for you: the budget is a `Duration` and the queue is a
count."* **[verified]**

The stronger argument is two lines further, and no revision has made it:
`actor/registry.rs:2187-2192` says the wait list behind the last slot is
*"unbounded and unobservable"*, because `Semaphore` does not expose its waiters.
**No count exists to size a budget against. A sentinel needs none.** That is the
case for R5.

**Delete the `Unbounded` conditional. It never applied.** The wait list is
unbounded under `Park(N)` today, and `Backpressure` has only `Park` and `Shed`
(`turns.rs:3549`), with `turns.rs:3533` refusing `Unbounded` *"rather than
deferred"* and `representability.rs:1128` guarding it. **[verified]**

Today: `drain(entry, budget: Duration)` at `actor/registry.rs:1124`, private, two
callers. On budget expiry it **aborts** the task, so `finish` never runs and the
caller is never answered. **[reported]** The local path has no drain budget at
all — `LocalActor::stop()` waits forever and `abort()` kills now — so R5 adds
machinery there rather than replacing any. **[reported]**

### 5. R7 — caller-gone signal on `Reply`

A handler can skip work nobody waits for.

- No `is_closed` exists on `Reply`. Every `is_closed` in the crate asks a mailbox
  whether the actor runs, which is a different question. **[verified]**
- `Reply<T>` is at `turns.rs:439`. Its `Answerer` has two arms: `Direct(oneshot::
  Sender<..>)` and `Wrapped(Box<dyn FnOnce(..) + Send>)`. **[verified]**

**The design constraint that must not be hidden:** the `Wrapped` arm holds no
sender to ask, so the answer is `Option<bool>`, not `bool`. Do not paper over
that with a `false` default — a handler that reads "caller still waiting" when
the truth is "cannot know" will skip nothing and trust the wrong thing. Name the
unknown in the type.

### 6. R10 — capability-narrowed handle

- Per-method authorization exists on the wire (`middleware.rs`, `Context::method`,
  `RemoteError::Unauthorized`) and **nowhere locally**: a `Handle` reaches the
  mailbox directly, so no middleware ran. **[reported]**
- The generated per-rpc methods are **inherent methods on a concrete `Handle<A>`**,
  which cannot be subsetted. **[reported]**
- The route is a trait, and there are two shipped precedents in the same
  expansion: the generated `Calls` trait at `macros.rs:3491` with **four** impls —
  `Handle<A>` at `:3504`, `ActorRef<'_>` at `:3516`, `Client` at `:3528`,
  `IdempotentClient` at `:3540` — and `compose::Reserving` for `Handle<A>`, a
  two-method trait over the same type. **[verified]**

Blockers:

- **A narrowed handle is a statement of intent, never an authorization
  boundary.** Its holder can still hold the full `Handle`, and the marker is
  forgeable. Do not document it as a capability restriction across a trust
  boundary. If the rustdoc cannot be written honestly, do not ship it.
- No trait object: `dyn Calls` is `E0038` because every method returns its future
  in return position. A narrowed surface is a generic bound or a generated
  newtype, never a `dyn`. **[reported]**
- `service!` has no grammar for declaring a subset. That is new macro syntax and
  it is the bulk of the work.

### 7. R8 — `enqueue_` / `Pending<T>`, at reduced scope

**Half of this already shipped.** `Permit` (`turns.rs:4875`) and `OwnedPermit`
(`turns.rs:4944`) already separate admission from the send: `reserve` **is** the
accepted half. **[verified]** What remains is a handle representing "accepted,
not yet answered" **after** the send. Scope it against what exists; do not
rebuild the admission half.

---

## Explicitly out of scope

- **R4 is already built. Delete the row.** `Reply::later() -> (Deferred<T>,
  Replied<T>)` at `turns.rs:503`, `Deferred<T>` at `turns.rs:734`, exported at
  `lib.rs:338`, with `parking.rs` ordering the parked send against the observer
  chain. **[verified]**
- **R11 (rate limiting)** — none exists in the crate **[verified]**, and §4 of the
  factory proposal deleted the queued mode it was meant to live in. It needs its
  own design before any code.
- **R12 (live reconfiguration)** — `Factory { members: Pool<H>, routing }`
  (`compose/factory.rs:519-527`) builds members once and shares one `Arc` cursor
  between clones **[verified]**. A resize re-shards every key under `ByKey`. Wait
  until the factory has users.
- **Priority queue in the factory** — the factory holds no queue, so this needs
  the deleted mode back. If it is ever built, carry priority on the call rather
  than deriving it from the key, and state what a flood of high priority does to
  low priority.

---

## Definition of done

- Every item above either built or explicitly descoped with its reason recorded.
- `docs/proposals/2026-08-10-compose-factory.md` §12 updated: rows for what
  shipped, and R4's row deleted.
- Every new guard carries a break record and a registered row; `mise run anchors`
  green with `RECORD_BLOCKS` read off the failing run.
- Every new compile-only guard **watched in both directions** — show the control
  green and quote the exact `error[E....]` a perturbation produces. A guard that
  has never been seen to fail is not a guard.
- `mise run gate` green, output captured to `/tmp`, with the test totals quoted.
- Pushed to `origin/main`, confirmed by exact SHA match.
- Report what you assumed, what you could not establish, and what you refused.
