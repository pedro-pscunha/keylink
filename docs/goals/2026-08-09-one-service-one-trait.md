> **SUPERSEDED 2026-08-09.** Three review passes (8 Critical, 12 Major) found this
> to be three charters in one hat. Split into:
>
> - `2026-08-09-one-failure-vocabulary.md` — executable today
> - `2026-08-09-one-trait-both-sides.md` — blocked on the first
> - `2026-08-09-peer-administration.md` — blocked on a security design
>
> Kept as the record of what was believed. The reviews are in
> `docs/audits/2026-08-09-local-vs-remote/`.

# Goal: One service, one trait, both sides

## Intent

A `service!` block declares one contract and today produces two worlds that share
no type, no trait and no error. A caller cannot write one function that works
against a local actor in a test and a remote one in production. This goal
collapses every asymmetry between the local and remote caller surfaces that is
not physics, so the same code compiles against both — and states plainly, in the
crate's own docs, the two that remain and why.

The measured starting point: **31 asymmetries — 16 essential, 8 incidental, 7
argued.** The audit is at `.tmp/surf/` (`REPORT.md`, `seam.md`, `local.md`,
`remote.md`). Copy it into `docs/` as part of this work; `.tmp/` is gitignored
and the evidence must survive.

## Context

- `keylink/src/macros.rs` generates every caller surface from one `service!`
  block: `Handle<A>`, `Client`, `IdempotentClient`, `KeyedClient` + `ActorRef`,
  and a blocking `Client` with its own `IdempotentClient` — **six types**.
- `keylink/src/turns.rs` holds `Undelivered` (7 variants) and `NotQueued` (2);
  `keylink/src/observe.rs` holds `TurnLost` (8) and `Ended` (6);
  `keylink/src/reply.rs` holds `RemoteError` (5) and the **five `Failed` prefix
  strings** that carry, as text, discriminations the local side carries as
  variants.
- The ALPN is a hash of `Contract`/`KeyedContract` (`macros.rs`, `node.rs`).
  **Associated consts are not hashed**, which is why the prefixes exist and why
  the handshake cannot protect them.
- Reuse rather than duplicate: the twin guard in `turns.rs`'s `mod tests`
  (`every_turn_lost_except_faulted_has_one_undelivered_twin_with_the_same_words`),
  `Inspect`'s `EndReason` (already flattens `Ended`'s `Arc<dyn Error>` payloads
  for the wire), `compose::Endings`, and `.mise/tasks/anchors`' registry.

## Skills (load the matching one BEFORE working in its area)

**Review gates (always):** `brooks-review` (the plan gate and the merge gate),
`brooks-sweep` (discretionary quality passes), `superpowers:requesting-code-review`
(before merge), `superpowers:receiving-code-review` (to triage every finding —
the reviewer never fixes its own).

**Domain / craft (this is a Rust crate):**
- `rust-illegal-states` — the whole goal is making a representable-but-impossible
  state unrepresentable; §1 enums, §2 newtypes, §3 typestate.
- `rust-best-practices` — borrowing, error handling (`thiserror` for a library),
  exhaustive `match`, `#[non_exhaustive]` discipline.
- `codebase-design` — deep modules behind small interfaces; the trait *is* the
  interface, and the deletion test applies to every wrapper introduced.
- `ponytail` — the ladder. This goal deletes more than it adds; keep it that way.

**Process/meta:** `superpowers:writing-plans` (the plan follows this goal),
`superpowers:subagent-driven-development` (execution), `test-driven-development`
(each guard red before green), `systematic-debugging` (root cause before any fix).

**Caveats — skills that do NOT apply here:** every `arche-*` skill is for the
arche Python monorepo and is irrelevant to this Rust crate. The write-goal
template's Python gates (`pyright`, `mise run p:full-check`, `dict[`/`Optional`
greps, cassettes/testcontainers) do **not** apply; this repo's gates are
`mise run gate`, `cargo nextest run --workspace`, `mise run anchors`,
`mise run docs`, `mise run replay-anchors`.

## Scope

**In scope**

1. **`Withheld`** — one shared enum naming what happened to a turn, carried by
   both sides, **on the wire as a typed variant** (not a string prefix).
2. **A generated trait per service**, implemented by every caller surface, with
   an associated error type so precise code stays exhaustive and generic code
   works against one failure type.
3. **Method parity 5/5** on `Handle`: local `send`/`recv` (generalise
   `FrameReader` over `AsyncRead`), and `blob` returning one type on both sides.
4. **Keyed `tell`** returning the routing acknowledgement — the same question a
   local `tell` answers.
5. **Blocking client to full coverage** (2/5 → 5/5).
6. **Client types 6 → 3**: fold `IdempotentClient` into `Client` (idempotency is
   a field, not a type); `at` becomes a method, not a free function.
7. **Reserved `__status()` and `__ended()` doors** on the service's own contract,
   so a peer stops needing a second ALPN and opt-in wiring for facts the local
   handle has for free. `Ended` crosses using `EndReason`'s existing flattening.
8. **Peer-reachable `shutdown` / `abort` / `evict_idle` — on a SEPARATE admin
   service with its own ALPN and its own `Allowlist`.** Not on the service's own
   contract: one `Allowlist` gates a whole service, so any peer allowed to call
   `place_order` would by construction be allowed to call `shutdown`, and the only
   in-tree separator is a `Middleware` the *caller* must remember to mount —
   fail-open, in a crate whose `Allowlist` deliberately has no `Default` "so it
   can never be what you get by forgetting". The tension with item 7 is
   deliberate and must be stated in the docs: **reads lose their second ALPN,
   destructive writes gain one**, because they differ in blast radius.
9. **Every `Failed` keylink itself builds becomes a typed variant** — registry
   capacity, malformed `ActorId`, the console subscription refusal, and
   `FollowRefused::{Ahead, Crowded}` (today flattened, losing the distinction).
   Establish the invariant: **`Failed(String)` means a caller's own `Middleware`,
   and nothing else.**
10. **Delete the prefix machinery** once nothing produces it: the five consts,
    their pinned-value test, `no_failure_prefix_is_a_prefix_of_another`,
    `no_other_served_failure_begins_with_one_of_the_five`, and every
    `starts_with` at every call site.
11. **`NodeStatus::lost` / `EventPage::lost`** — two counters, one name. Rename.
12. **Adjacent, cheap, already measured:** `ServableAsActor::CHECK` fires at
    three of four doors (a service is refused as an actor and accepted as a
    handler); same shape for `SpawnableUnfed`. `keyed_service` has no `serve`
    twin. Expose `ask_catching` on the generated `Handle`. Rule on
    `Undelivered::Expired`, which is public and unreachable locally.
13. **Re-argue RFC-04 §5.5** by appending. Its conclusion (two wrapper types)
    survives; its stated reason — *"A local mailbox has exactly one failure: the
    task is gone"* — is false at seven and names a type (`MailboxClosed`) that no
    longer exists.

**Out of scope**

- `with_state` remotely. The closure is a Rust `FnOnce` over `&A`; there is no
  `A` on that machine. **Physics — document, do not attempt.**
- `mount` remotely. It takes a `Mounted<M>`, a spawned local task a peer cannot
  send. Creation-by-id is already covered by `SystemBuilder::spawn_with`.
- Changing the *policy* a panicking observer produces. `fatal: false` and the
  commit decision are defended in writing and were re-confirmed this week.
- Supervision strategies / supervisor trees — RFC-07 §9.8 refuses them and this
  goal does not reopen that.
- Any performance work. If a change costs an allocation on a failure path, say
  so; do not optimise it here.

**Blast radius**

- May modify: `keylink/src/**`, `keylink/tests/**`, `keylink/examples/**`,
  `.mise/tasks/anchors`, `scripts/*.sh`, `README.md`, `docs/**` (RFCs by
  **append** only, save the identifier carve-out), `demo/**` and
  `keylink-persist/**` **only** where the trait or a renamed type forces it.
- Must not touch: `.githooks/**`, `mise.toml`'s gate/pre-commit step lists,
  `Cargo.toml` dependency versions, `rust-toolchain.toml`, `deny.toml`.

## Public interface

Designed here so review can check nothing else escaped.

```rust
/// What happened to the turn. One object, both sides, and on the wire.
/// Exhaustive on purpose: a wildcard arm is the collapse this replaces.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize,
         postcard_schema::Schema, thiserror::Error)]
pub enum Withheld {
    Panicked         { message: String },
    Refused          { name: String },
    ObserverPanicked { name: String },
    Dropped,
    Restarting       { reason: String },
    Gone,
    Expired,
}

/// Local: the shared middle, plus the one failure only a local call can have.
pub enum Undelivered { Turn(Withheld), Reentrant }

/// Remote: the shared middle, plus the failures only a wire can have.
pub enum RemoteError {
    Withheld(Withheld),
    NoSuchActor { id: String },     // kept distinct from Withheld::Gone
    Unauthorized(String),
    TimedOut { millis: u64 },
    Idempotency(IdempotencyRefusal),
    AtCapacity { capacity: usize },
    BadActorId { id: String },
    Failed(String),                 // a caller's own Middleware, and nothing else
}

// generated per service — TWO traits, sharing the associated error.
// One trait cannot serve both: `blocking::Runtime::run` starts with `guard()?`,
// which returns `Error::BlockingInAsync` whenever a tokio runtime is present. An
// impl compiles and is a lie — a trait that says "await me", implemented by a type
// that says "never be awaited", is exactly the representable-but-impossible state
// this goal's own invariants forbid.
pub trait Orders {
    type Error: Into<Failure>;
    fn place(&self, size: u64) -> impl Future<Output = Result<u64, Self::Error>> + Send;
    fn status(&self) -> impl Future<Output = Result<ActorStatus, Self::Error>> + Send;
    /// `EndReason`, not `Ended`: `Ended::Observer` carries `Arc<dyn Error>`, which
    /// has no schema. `EndReason` already exists for this and holds one
    /// `From<&Ended>` with no wildcard arm, so a new ending is a compile error on
    /// one line. Reuse it; do not duplicate it.
    fn ended(&self) -> impl Future<Output = Result<EndReason, Self::Error>> + Send;
}

pub mod blocking {
    pub trait Orders {
        type Error: Into<Failure>;
        fn place(&self, size: u64) -> Result<u64, Self::Error>;
        // same names, no futures
    }
}
```

```rust
// The prize: one function over the three ASYNC surfaces.
async fn settle(o: &impl orders::Orders) -> Result<u64, Failure> {
    o.place(10).await.map_err(Into::into)
}
settle(&handle).await?;                 // local
settle(&client).await?;                 // remote
settle(&keyed.at(id)).await?;           // keyed
// blocking::Client implements orders::blocking::Orders instead — same names,
// same error, no future. It is a different execution model, not a missing impl.
```

A local `Handle` additionally keeps a richer `ended()` answering the full `Ended`.
The **trait** method returns `EndReason` on every surface, because a trait method
whose return type varies by implementor is not one trait.

## Success criteria

Each is binary and names its proof. Proofs must be **printed in the transcript**.

- [ ] One function generic over `impl <svc>::Trait` compiles and passes against
      **`Handle`, `Client` and `ActorRef`** — proven by a new test
      `one_function_serves_every_async_surface` in `keylink/tests/`.
      **`blocking::Client` is deliberately NOT in that set** and implements
      `<svc>::blocking::Trait` instead — proven by a second test, and by a
      `compile_fail` doctest showing the blocking client rejected by the async
      trait's bound. The compile_fail is the guard that keeps the two traits from
      being quietly merged later.
- [ ] `Withheld` is byte-identical in type and value on both sides for all seven
      conditions — proven by `a_local_caller_and_a_peer_read_the_same_withheld`,
      asserting equality of the two values, not of their rendered text.
- [ ] Zero `starts_with` on a served failure anywhere in the workspace —
      `rg 'starts_with' keylink/ demo/ keylink-persist/` shows no hit on a
      `RemoteError`/`Failed` value.
- [ ] The five prefix consts, their pin test,
      `no_failure_prefix_is_a_prefix_of_another` and
      `no_other_served_failure_begins_with_one_of_the_five` **no longer exist** —
      `rg 'PANICKED|OBSERVER_PANICKED|RESTARTING|DROPPED|REFUSED' keylink/src/reply.rs`
      is empty.
- [ ] `Handle` covers 5 of 5 declared kinds, blocking covers 5 of 5, `ActorRef`
      covers 5 of 5 — proven by a compile-time test instantiating every kind on
      every surface.
- [ ] Exactly three generated client types remain (`Client`, `KeyedClient`,
      `blocking::Client`) — `rg 'pub struct IdempotentClient' keylink/src/macros.rs`
      is empty.
- [ ] A peer reads `__status()` and `__ended()` from the service's own contract,
      with no `Inspect` connection — proven by a test that stands up **one**
      server and asserts both.
- [ ] `Undelivered` keeps `Clone + PartialEq + Eq` and every existing
      `assert_eq!` on it still compiles — `cargo nextest run --workspace` green.
- [ ] `mise run gate` exits 0, all 22 steps, output printed.
- [ ] `mise run anchors` green; `RECORD_BLOCKS` **read off the failing run**, and
      the ledger entry says which run printed it.
- [ ] `mise run replay-anchors` is N of N with every row at `── selected=1`.
- [ ] `mise run docs` clean under `-D warnings`.
- [ ] RFC-04 §5.5 has an appended section re-arguing the two-type split from the
      current tree — `git diff main...HEAD -- docs/ | rg '^-' | rg -v '^---'`
      prints **only** identifier renames under the carve-out.
- [ ] The audit survives: `.tmp/surf/*.md` copied into `docs/`.

## Invariants

### Universal (this repo)

- Workspace lints: **deny** `clippy::unwrap_used` / `expect_used` outside tests,
  **deny** `unsafe_code`, **warn** `missing_docs`. `cargo clippy --workspace
  --all-targets -- -D warnings` clean.
- **Public error enums stay exhaustive** — no `#[non_exhaustive]`, no wildcard
  arm inside the crate. The crate's own position: *a wildcard arm is the collapse
  it exists to prevent.* Adding a variant SHOULD be a compile error downstream.
- **Every behavioural guard is watched red-then-green**, control green FIRST,
  with a break record whose watched sentence is **read off the run, never
  predicted**, and whose search literal resolves **exactly once** — counted
  **after `cargo fmt`**, which owns line breaks. Register the row.
- **Numbers come from runs.** `RECORD_BLOCKS` and every count in prose is read
  off the failing run, never computed. A figure written into a doc carries the
  commit it was measured at (`CONTRIBUTING.md`, `## RFCs`).
- **`docs/rfc-*.md` is append-only**, with the one carve-out: an identifier a
  break record names may be renamed in place, because that moves a pointer and
  not a claim. A count is what a row asserts — append it.
- Rustdoc states the **mechanism and the reason**, never the conclusion. A public
  doc may not link a private item — and a private link errors only if nothing
  else answers to the name, so **check what each link resolves to**, not whether
  the build is green.
- wasm: no `tokio::select!`, no `std::time::Instant` (use `n0_future::time`), no
  `tokio::task_local!`.
- **Never `--no-verify`.** The hooks are the gate; do not pre-run a full gate
  before committing.
- Fail loudly; no silent fallback; fix at the root (`systematic-debugging`), never
  around it.

### Context-dependent (decided for this goal)

- **The hashed contract MAY move, and moves once — measured `main` to branch tip,
  not commit by commit.** Five separate in-scope items each move a pin
  (`Withheld`/`RemoteError`; `FollowRefused`/`NodeStatus.lost`; the reserved
  doors; the admin verbs; the acknowledged keyed `tell`), and this goal also
  requires atomic commits. Both cannot hold under a per-commit reading, so the
  criterion is **`main`→tip**: every pin differs from `main` exactly once, and no
  peer ever sees an intermediate. A **second coordinated redeploy** is the failure
  this forbids — not a second commit. Authorised by the user, valid until this
  app is in production.
- **`Withheld` carries `String`, not `Cow<'static, str>`.** No `Cow` appears in
  any wire type today and `postcard_schema::Schema` support for it is unverified;
  every sibling `RemoteError` field is already `String`. Cost: one allocation on
  a local failure path. Accepted, and stated in the doc rather than hidden.
- **`Undelivered` drops `Copy`, keeps `Clone + PartialEq + Eq`.** Measured:
  **zero** `@`-binding sites depend on `Copy` for `Undelivered` (three exist,
  all on `NotQueued`, one in a doc comment), and **16** `assert_eq!` sites need
  `Eq`. `Ended` derives `Clone` but not `PartialEq`, which is why the reason
  crosses as rendered text and not as `Arc<Ended>`.
- **`Withheld::Gone` and `NoSuchActor` stay distinct.** *This id resolves to
  nothing* and *the actor that owed you this answer is finished* are different
  facts, and only the first carries an id. `remote.rs` currently conflates them;
  that conflation is the defect, not the separation.
- **Trait error is an associated type**, not one unified enum. A unified enum
  would force a local caller to write `SchemaMismatch` and `Transport` arms that
  can never fire — representable-but-impossible, which `rust-illegal-states`
  forbids and this crate has spent a week removing.
- **Test strategy:** in-process actors for local behaviour; a real `Node` +
  `Server` + client for served behaviour — this crate has no mocking layer and
  none is to be introduced. Cross-path claims assert **both** answers in **one**
  test.

## Execution interaction policy

**Questions during execution: not allowed.** The executor annotates every
question and assumption inline as it goes and surfaces them to the user **only
after finishing**. It never blocks mid-run. If genuinely blocked with no valid
path, it stops and reports the paths attempted, the evidence, and the blocker.

## Process

Isolated worktree via worktrunk (`wt switch -c <branch>` — never raw
`git worktree`). Atomic commits, one logical change each.

**Explore → Plan → (Review plan ⇄ Improve)×N → Execute (`subagent-driven-development`)
→ (Review execution ⇄ Apply fixes)×N → PR.**

- **Reviews are run by subagents on opus** using `brooks-review` (+ `brooks-sweep`
  where the matter is quality rather than a gate) and
  `superpowers:requesting-code-review`. **Never fable** — not for a gate, not
  under any workflow, without Pedro's explicit authorisation in his own words.
- **Fixes go through `superpowers:receiving-code-review`.** The reviewer never
  fixes its own findings.
- **A dedicated invariant-compliance wave runs on the execution review**, separate
  from the quality wave: a subagent grades the diff against `## Invariants` item
  by item and must surface machine output — `mise run gate`, `mise run anchors`,
  `mise run docs`, `cargo clippy -D warnings`, the append-only `rg '^-'` check,
  and each named success-criterion proof. **An invariant it cannot evidence is
  treated as violated.** Any miss is Critical and re-opens the loop.
- **Freeze the branch at a named SHA before opening any gate**, and land no
  commit on it until the verdict arrives. This session moved a branch under
  review four times, once flipping a verdict from PASS to FAIL.
- **Loop-break:** iterate until both the quality wave and the invariant wave
  return no Critical/Major. Report Critical-vs-Major counts and the per-invariant
  verdict each wave.

## Definition of done

- [ ] Every success criterion proven, evidence printed.
- [ ] Diff inside the blast radius; no unrequested refactors.
- [ ] `mise run gate` green (22 steps), `cargo nextest run --workspace` green,
      `mise run anchors` green, `mise run replay-anchors` N of N,
      `mise run docs` clean.
- [ ] Review waves clean — no open Critical/Major.
- [ ] **Invariant-compliance wave clean**, per-invariant verdict attached.
- [ ] **The hash moved exactly once.** Two moves is a failure.
- [ ] **The two residual asymmetries are documented, not silently dropped** —
      `with_state` and `mount`, each with the physical fact that makes it
      impossible.
- [ ] Final pass: re-read this goal end to end against real evidence, and walk
      the plan task-by-task. A half-done plan is not done.

## /goal completion condition

```text
Satisfy every success criterion and Definition-of-done item in
docs/goals/2026-08-09-one-service-one-trait.md. AS THE FINAL STEP, print the full
proof state in ONE block: `mise run gate` output (exit 0, 22 steps), `cargo nextest
run --workspace` (0 failed), `mise run anchors`, `mise run replay-anchors` (N of N),
`mise run docs` (clean under -D warnings), `git diff main...HEAD -- docs/ | rg '^-' |
rg -v '^---'` (identifier renames only), and the invariant-compliance wave's
per-invariant pass/fail verdict. The hashed contract may move ONCE and only once.
Stay inside the blast radius in that file. Freeze the branch at a named SHA before
each review gate. Stop when all pass or after 40 turns; if blocked, stop and report
the paths attempted, the evidence and the blocker — do not ask mid-run.
```

## Deliverable

Isolated worktree, PR to `main` with the session id in the body, all checks green.
