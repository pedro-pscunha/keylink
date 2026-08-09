# Goal: execute keylink issue #3 — the actor runtime, wave by wave

## Intent
Execute 100% of https://github.com/pedro-pscunha/keylink/issues/3 against `main`.
That issue is self-contained and **is** the spec. Do not redesign it. It carries ten
waves (0–9), per-wave acceptance criteria, a binding "Refused designs — do not
re-derive" table, and the Invariants.

## Context
`~/repos/keylink`, `main@ae0ea23` — the correctness programme, the actor/transport
split and the merge gate have landed. `keylink/src` is 14,257 lines.

**Verify first:** `keylink/src/actor/{mod,registry,remote}.rs` exist, `registry.rs:79`
has `pub type Mailbox<M = Mail>`, `keylink/src/observe.rs` exists, and `local.rs:128`
still has `type Turn<A> = Box<...>`. If any is false the tree moved — re-verify #3's
citations before planning. Optional deeper rationale:
`.tmp/actor-runtime-issue/{piece-a,piece-b,piece-c-rfc}.md`.

## Skills — load before working in each area
- `rust-best-practices` — every wave.
- `rust-illegal-states` — every new type. #3's refused table came from this lens;
  re-read it before adding any marker trait or enum.
- `rust-performance` — Wave 1's mailbox re-measurement; Wave 5's per-turn store cost.
- `improve-codebase-architecture` — Waves 1 and 3, the two that move structure.
- `ponytail` before each review. Ten waves is the shape that grows a second system.
- `subagent-driven-development` — the execution loop itself.
- `brooks-review` + `receiving-code-review` — the review gates.

## Scope
**In:** Waves 0–9 in order.
**Out:** Wave 9 phase 3 (`AfterFault`) — deferred by decision, reasoning in #3.
`Timer`, `WeakHandle`, the §16.8 registry crate.
**Blast radius:** `keylink/**`, `keylink-persist/**`, `demo/**`, `docs/rfc-*`,
`README.md`. Do not bump `iroh` or `rust-version`.

## Wave order — the dependencies are not negotiable
```
0 probe Mail-enum Send bounds   ← HARD GATE: fails ⇒ stop and report, do not start 1
1 Keyed<T> + the Mail enum      ← net deletion
2 Faulted · method name · #[defer] · Observer::finish
3 feeds — and unify the keyed and LocalActor loops
4 mailbox depth (4a) · shutdown budget (4b) · snapshot versioning (4c) · status (4d)
5 the durability adapter        ← gates 9
6 snapshot → Inspect → console
7 pubsub · broker · bus · pool · scheduler · name registry
8 reliable-delivery recipe (docs, may float after 1)
9 supervision, phases 1–2 only
```
1 before 2 and 3 (same loop). **4c before 5.** **5 gates 9.** 3 before 7. **4d before 6.**
**4a before 4d** — whether `ActorStatus` carries a buffered count depends on it.
Parallel lanes: within 2, `#[defer]` and `Observer::finish`; within 4, 4b and 4c are
independent of 4a, 4d is not.

## Process
Worktree: `wt switch -c actor-runtime -y`. Atomic commits per wave. ONE PR to `main`.
**All subagents on opus — never fable.**
- **Explore** (`Explore` type) — re-verify #3's claims against the tree before each
  wave; citations drift.
- **Plan** — one plan per wave, then ONE adversarial plan review before code.
- **Execute** — `subagent-driven-development`; parallel lanes only where #3 says so.
- **Review** — `brooks-review` at the merge gate, plus a dedicated
  invariant-compliance wave grading the diff against #3's Invariants item by item,
  evidencing each with machine output. An invariant it cannot evidence is violated.
- Apply findings via `receiving-code-review`. A reviewer never fixes its own findings.
Loop until no Critical/Major remains.

## Success criteria
Every checkbox in #3 proven with evidence. `mise run gate`, `deny`, `msrv`, `wasm`
green. Every new guard red-tested. Wave 1's deletion reported **per file**. Wave 1's
mailbox cost **re-measured** (the old figure was taken against a different shape).
Wave 5's per-turn store cost in the commit message **and** the module doc.

## Invariants
Inherit #3's Invariants section verbatim. The three that already bit: `n0_future::time`
only (`std::time::Instant::now()` panics on wasm and that shipped once);
`representability.rs` probes work only on **local** types; and **no stand-in types in a
probe** — a unit-struct `Panic` hid a `Send` fault through two revisions, which is
exactly what Wave 0 exists to catch.

## Execution interaction policy
**Questions are not allowed.** Annotate every assumption and open question inline as
you go and surface them all in the final report. Never block. If genuinely blocked
with no valid path, stop and report attempted paths, evidence, blocker and open
questions — do not ask.

## Definition of done
All criteria proven; diff inside the blast radius; review waves clean; RFC-04
§7.4/§8/§9.1 and RFC-06 §5.3/§6/§8 superseded and removed, with a new RFC recording
the model, Wave 9 phase 3's deferral plus its trigger, and 4a's depth knob — its
meaning, the measured 32-slot flat-memory fact, the depth↔shutdown-budget
relationship, `buffered` as a floor, and `unbounded` as permanently refused.
**Final pass:** re-read #3 end to end and confirm each item against real evidence,
not memory.

## /goal completion condition
See `.tmp/keylink-issue-3/goal-condition.txt` — reproduced verbatim below.
