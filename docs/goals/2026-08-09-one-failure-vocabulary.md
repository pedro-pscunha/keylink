# Goal 1 of 3: One failure vocabulary, both sides

Supersedes `2026-08-09-one-service-one-trait.md`, which three review passes found
to be three charters in one hat (8 Critical, 12 Major). This is the first, and the
only one executable today. The other two are
`2026-08-09-one-trait-both-sides.md` and `2026-08-09-peer-administration.md`;
both depend on this landing first.

## Intent

Seven conditions are the same fact on both sides of keylink, and each is a typed
variant locally and a **string prefix inside `Failed(String)`** remotely. The
prefixes sit outside the hashed contract, so the handshake cannot protect them: a
drifted peer meets a moved prefix as a `Failed` it silently stops recognising —
which the crate itself calls the more dangerous kind of break.

Give those seven conditions **one Rust object, carried by both sides and encoded
on the wire**, and delete the prefix machinery. This is net **deletion**: five
consts, their pin test, two guards and every `starts_with` go away.

## Context

- `keylink/src/reply.rs` — `RemoteError` (5 variants) and the five `Failed`
  prefix consts; `reply.rs:119-124` records that associated consts are **not**
  hashed, which is the whole reason the prefixes exist.
- `keylink/src/turns.rs` — `Undelivered` (7), `NotQueued` (2), and a **private**
  `enum Withheld { Vetoed { name, fatal }, Panicked { name } }` at `turns.rs:866`
  whose `Panicked` means the **observer**.
- `keylink/src/observe.rs` — `TurnLost` (8), `Ended` (6).
- `keylink/src/console.rs:184` — `EndReason`, already `Clone + PartialEq + Eq +
  Schema`, already on the wire, holding exactly one `From<&Ended>` with no
  wildcard arm.
- The audit this rests on: `docs/audits/2026-08-09-local-vs-remote/` (copy it out
  of `.tmp/surf/`; `.tmp` is gitignored and the evidence must survive).

## Skills

**Review gates (always):** `brooks-review` (plan gate + merge gate),
`brooks-sweep` (quality passes), `superpowers:requesting-code-review`,
`superpowers:receiving-code-review` (the reviewer never fixes its own findings).

**Craft:** `rust-illegal-states` (this goal removes a representable-but-impossible
state), `rust-best-practices` (`thiserror` for a library, exhaustive matching),
`codebase-design` (the deletion test on every wrapper), `ponytail` (this change
deletes more than it adds — keep it that way).

**Process:** `superpowers:writing-plans`, `superpowers:subagent-driven-development`,
`test-driven-development`, `systematic-debugging`.

**Caveats:** every `arche-*` skill is for a Python monorepo and is irrelevant
here. The write-goal template's Python gates (`pyright`, `p:full-check`, cassettes,
testcontainers) do **not** apply; this repo's gates are `mise run gate`,
`cargo nextest run --workspace`, `mise run anchors`, `mise run docs`,
`mise run replay-anchors`.

## Scope

**In scope**

1. **Rename the private `Withheld` → `ObserverVerdict`.** It answers *what the
   observer decided*, and its `Panicked` means the observer — the public name
   below means the handler. Two meanings for one word is the confusion
   `OBSERVER_PANICKED` was added to delete. A break record quotes
   `Withheld::Panicked`: re-quote it and **re-watch it**, do not edit the
   watched sentence.
2. **Add public `Withheld`**, on the wire, replacing the five prefixes.
3. **Collapse `TurnLost` to `{ Faulted(Panic), Reentrant, Turn(Withheld) }`** and
   `Undelivered` to `{ Turn(Withheld), Reentrant }`. This deletes the third
   spelling of one fact and turns the hand-kept twin guard into a type-checked
   relationship.
4. **Reshape `RemoteError`**: add `Withheld(Withheld)`, `AtCapacity { capacity:
   u64 }` and `BadActorId { id: String }`; stop flattening
   `FollowRefused::{Ahead, Crowded}`. Establish the invariant **`Failed(String)`
   means a caller's own `Middleware`, and nothing else.**
5. **Delete the prefix machinery** once nothing produces it: the five consts,
   their pinned-value test, `no_failure_prefix_is_a_prefix_of_another`,
   `no_other_served_failure_begins_with_one_of_the_five`, and the two live
   `starts_with` sites.
6. **Rename `Undelivered`.** Its first doc line is false of four of its seven
   variants — `Dropped`, `Restarting`, `Refused` and `ObserverPanicked` all
   *ran*. The type was renamed once for exactly this and has outgrown the
   replacement. Costs no ALPN.
7. **`NodeStatus::lost` → `dropped`.** Three names for two quantities,
   cross-wired: `NodeStatus::lost` and `EventPage::dropped` are the **same**
   reading; `EventPage::lost` is a different one. **This direction only** —
   renaming the other breaks the loop the docs tell readers to write.
8. **One more arm on absence:** a peer cannot tell *the factory refused me* from
   *no factory exists*; both render as one `NoSuchActor`. Removes an inference
   rather than adding a feature.
9. **Rename `deliver_to_local`** — it is the **served** path and builds an
   `Origin::peer`. Private, free, and the cheapest true finding in the audit.
10. **Re-argue RFC-04 §5.5 by appending.** Its conclusion (two wrapper types)
    survives; its stated reason — *"A local mailbox has exactly one failure"* —
    is false at seven and names a type that no longer exists.
11. **`Withheld::Restarting` carries `EndReason`, and `EndReason` moves from
    `console.rs` to `observe.rs` beside `Ended`, in the same commit.** Decided
    2026-08-09, measured:
    - The only objection on file was that it would kill `Eq`. It derives
      `Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Schema`
      (`console.rs:184`), so it does not. That objection is withdrawn.
    - A `String` here would carry a discrimination as text **inside the type
      built to delete text-carried discriminations**. Items 2 and 5 delete five
      prefixes; a `String` payload adds a sixth.
    - The counter-cost — *"drags `EndReason` out of `console.rs` into every
      service's hashed graph"* — is a **layering** objection, and it dissolves by
      moving the type. `EndReason` is `Ended`'s wire projection and belongs beside
      `Ended`; it lives in the console only because the console needed it first.
      Measured cost: **9 references across 2 files** (`console.rs` ×8, `lib.rs` ×1).
    - **Do NOT narrow it to a `RestartCause`.** `retryable()` (`turns.rs:2752`)
      returns `false` for `Ended::Stopped`, and `GaveUp` is the give-up itself, so
      at least two of six variants are unreachable in this position. That residue
      is **documented, not designed away**: a narrower type would have to move
      whenever `retryable()`'s policy moves. The in-grain precedent is
      `TurnLost::Expired`, kept and documented as *"written to be correct if
      something reaches it"*.
    - **Verify, do not assume:** `derive_alpn` calls `hash_ty_path`, so a type's
      **module path** may be part of the hash. Establish by measurement whether
      moving `EndReason` moves `Inspect`'s pin. It rides the single authorised
      move either way, but it must be **known** or the "moves once" criterion
      fails on a technicality.

**Out of scope — belongs to goal 2 or 3**

- Any trait, any method-parity work, any client-type collapse, local
  `send`/`recv`, reserved doors, peer-reachable administration.
- ~~**`Withheld::Restarting`'s payload.**~~ **DECIDED 2026-08-09 — see item 11.**
- Changing what a panicking observer *does*. `fatal: false` and the commit
  decision are defended in writing and were re-confirmed this week.

**Blast radius**

- May modify: `keylink/src/**`, `keylink/tests/**`, `.mise/tasks/anchors`,
  `scripts/*.sh`, `README.md`, `docs/**` (RFCs by **append**, save the identifier
  carve-out), **`CONTRIBUTING.md`** (the ALPN-pin rule at `:196-210` governs the
  two literal pins and must be honoured, so it is in-radius), `demo/**` and
  `keylink-persist/**` only where a renamed type forces it.
- Must not touch: `.githooks/**`, `mise.toml` step lists, `Cargo.toml`
  dependency versions, `rust-toolchain.toml`, `deny.toml`.

## Public interface

```rust
/// One object, both sides, and on the wire.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Schema, thiserror::Error)]
#[non_exhaustive]                       // ← error enums ARE non_exhaustive here
pub enum Withheld {
    Panicked         { message: String },   // the HANDLER
    Refused          { name: String },
    ObserverPanicked { name: String },
    Dropped,
    Restarting       { reason: EndReason },  // DECIDED — item 11
    Gone,
    Expired,
}

pub enum TurnLost   { Turn(Withheld), Faulted(Panic), Reentrant }
pub enum Undelivered /* renamed */ { Turn(Withheld), Reentrant }
```

## Success criteria

- [ ] `Withheld` decodes on a peer as a **typed variant** — proven by
      `a_local_caller_and_a_peer_read_the_same_withheld`, asserting **value
      equality of the two `Withheld`s**, not equality of rendered text, for the
      six conditions reachable from both. `Panicked` is asserted through
      `ask_catching`, because `ask` resumes it and cannot yield it.
- [ ] The five prefix consts and both prefix guards **no longer exist** —
      `rg 'PANICKED|OBSERVER_PANICKED|RESTARTING|DROPPED|REFUSED' keylink/src/reply.rs`
      is empty. **Not** a `starts_with` count: only two live sites exist, both
      inside the guards being deleted, so that grep passes while proving nothing.
- [ ] **Every site that used to build a prefixed `Failed` now builds a typed
      variant** — enumerate the producers in `rendered` and `lost`, and assert
      each by name in one test.
- [ ] `Failed(String)` has **zero** producers inside `keylink/` — `rg
      'RemoteError::Failed\(' keylink/src` returns only the `Middleware`
      pass-through.
- [ ] Both ALPN pins in `keylink/tests/protocol.rs` are updated **once**, with the
      release-note sentence `CONTRIBUTING.md:196-210` requires.
- [ ] `mise run gate` exits 0 — **18 declared steps** (`mise.toml`'s gate list;
      the run prints more banners than that, which is not the same count).
- [ ] `cargo nextest run --workspace` green; `mise run anchors` green with
      `RECORD_BLOCKS` **read off the failing run**; `mise run replay-anchors`
      N of N at `── selected=1`; `mise run docs` clean under `-D warnings`.
- [ ] `git diff main...HEAD -- docs/ | rg '^-' | rg -v '^---'` prints **only**
      identifier renames under the carve-out.
- [ ] The audit is copied into `docs/audits/` and survives the branch.

## Invariants

### Universal (this repo)

- Deny `unwrap`/`expect` outside tests, deny `unsafe_code`, warn `missing_docs`;
  `cargo clippy --workspace --all-targets -- -D warnings` clean.
- **`#[non_exhaustive]` is for ERROR enums; enums matched exhaustively are not.**
  `console.rs:398-404` states this rule. The previous draft inverted it and would
  have had the compliance wave strip it from three public types and certify that
  as conformance.
- Every behavioural guard watched red-then-green, **control green first**, with a
  break record whose watched sentence is read off the run and whose search
  literal resolves exactly once — counted **after `cargo fmt`**.
- Numbers come from runs. A figure in a doc carries the commit it was measured at.
- `docs/rfc-*.md` append-only, one carve-out: an identifier a break record names
  may be renamed in place. A count is what a row asserts — append it.
- Rustdoc states mechanism and reason. Check what each intra-doc link **resolves
  to**, not whether the build is green.
- wasm: no `tokio::select!`, no `std::time::Instant`, no `tokio::task_local!`.
- Never `--no-verify`; the hooks are the gate; do not pre-run a full gate.

### Context-dependent (decided here)

- **The hashed contract moves once, measured `main`→tip**, not per commit. Five
  in-scope items move a pin and this goal also requires atomic commits; both
  cannot hold per-commit. The failure this forbids is a second **coordinated
  redeploy**, not a second commit.
- **`usize` never crosses the wire** — `AtCapacity` carries `u64`.
  `registry.rs:1228-1240` gives the reason.
- **`EndReason` may represent an ending that cannot cause a restart.** That is
  documented on the variant, naming `retryable()` as what decides reachability.
  It is not designed away — see item 11.
- **`Gone` and `NoSuchActor` stay distinct.** This overturns `reply.rs:79-81`'s
  written anti-enumeration argument, and the cost — a peer can now tell *this id
  ran and stopped* from *never existed* — must be **recorded in the append**, not
  deleted.
- Test strategy: in-process for local behaviour, a real `Node` + `Server` +
  client for served behaviour. No mocking layer exists and none is introduced.
  A cross-path claim asserts **both** answers in **one** test.

## Execution interaction policy

**Questions not allowed.** The executor annotates every question and assumption
inline and surfaces them only after finishing. If genuinely blocked with no valid
path, it stops and reports the paths attempted, the evidence and the blocker.

## Process

Worktree via `wt switch -c`. Atomic commits.
**Explore → Plan → (Review plan ⇄ Improve)×N → Execute → (Review ⇄ Fix)×N → PR.**

- Reviews run by subagents **on opus** using `brooks-review` and
  `requesting-code-review`. **Never fable** without Pedro's explicit words.
- Fixes go through `receiving-code-review`.
- **An invariant-compliance wave** runs on the execution review, separate from
  quality: it grades the diff against `## Invariants` item by item and must
  surface machine output. An invariant it cannot evidence is treated as violated.
- **Freeze the branch at a named SHA before any gate**, and land nothing on it
  until the verdict arrives.
- Loop-break when both waves return no Critical/Major.

## Definition of done

- [ ] Every success criterion proven, evidence printed.
- [ ] Diff inside the blast radius.
- [ ] Gate, suite, anchors, replay-anchors, docs all green.
- [ ] Review waves clean; invariant wave clean with a per-invariant verdict.
- [ ] The hash moved once, `main`→tip, and both pins carry their release note.
- [ ] Final pass: re-read this goal against real evidence and walk the plan
      task-by-task.

## /goal completion condition

```text
Satisfy every success criterion and Definition-of-done item in
docs/goals/2026-08-09-one-failure-vocabulary.md. AS THE FINAL STEP print the full
proof state in ONE block: `mise run gate` (exit 0), `cargo nextest run --workspace`
(0 failed), `mise run anchors`, `mise run replay-anchors` (N of N), `mise run docs`
(clean), `rg 'PANICKED|OBSERVER_PANICKED|RESTARTING|DROPPED|REFUSED'
keylink/src/reply.rs` (empty), `rg 'RemoteError::Failed\(' keylink/src` (Middleware
pass-through only), `git diff main...HEAD -- docs/ | rg '^-' | rg -v '^---'`
(identifier renames only), and the invariant wave's per-invariant verdict. The
hashed contract moves ONCE, main-to-tip. Freeze the branch at a named SHA before
each gate. Stop when all pass or after 30 turns; if blocked, stop and report the
paths attempted, the evidence and the blocker — do not ask mid-run.
```

## Deliverable

Worktree, PR to `main` with the session id in the body, all checks green.
