# Goal: deliver HTTP gateway Tasks 4-9 autonomously

## Intent

Finish the HTTP gateway extension work — Tasks 4 through 9 — in one uninterrupted
run, and merge it. The executor asks Pedro nothing until it is done. Every
question and assumption is annotated inline as it goes and surfaced once, at the
end, in a single block.

This goal governs **how** the remaining work is executed and reviewed. It does
not restate **what** to build. That is already written down and stays normative.

## Context

Two documents already own the requirements, and this goal deliberately does not
duplicate them:

- `docs/goals/2026-08-13-http-gateway-extensions.md` — the product charter.
- `docs/superpowers/plans/2026-08-13-http-gateway-extensions.md` — the plan.
  **A plan already exists. Do not write another.** Its per-task test-cycle maps
  are binding, exact test names included.

State at the time of writing:

- Worktree `~/repos/keylink.goal-http-gateway`, branch `goal-http-gateway`,
  HEAD `4e5b020`, tree clean, base main `841c0959`.
- Tasks 0-3 are committed. Tasks 4-9 remain.
- `/tmp/keylink-http-gateway-handoff.md` carries the session handoff: the
  validated Task 4 blob design with every fact cited to file and line, the five
  recurring traps, the anchor rows, and the tooling traps. **Read it first.** If
  it is missing, the same content is recoverable from this branch's commit
  messages, which were written to survive without it.

## Skills (load the matching one BEFORE working in its area)

**Named by Pedro, mandatory throughout:** `rust-best-practices` (every line of
Rust — typing, error handling, type-driven design), `codebase-design` (deep
modules, seam placement — Task 4's `BlobReader` and Task 7's new crate are both
interface-design problems), `ponytail` (reach for the lowest rung that holds;
this plan has 60+ checkboxes and speculative abstraction is the failure mode),
`brooks-review` (every review wave), `receiving-code-review` (triage every
finding; the reviewer never fixes its own).

**Review and delivery:** `requesting-code-review` (before merge),
`brooks-test` (Tasks 4 and 5 are test-heavy; 33 rows between them),
`verification-before-completion` (before every completion claim),
`subagent-driven-development`, `superpowers:using-git-worktrees`.

**Process:** `systematic-debugging` (mandatory before any fix — this branch has
already lost a day to a fix applied before the diagnosis),
`test-driven-development` (the plan's micro-cycle is red → green → watched red →
anchor, one row at a time), `simple-english` (Task 8's public documentation).

**Subagent harnesses Pedro named:** `codex` (adversarial second opinion and
protocol-fact verification — it caught a real bug in the Task 4 design before a
line was written), `omp` (swarm/`case` runner when a wave is worth parallelising).

**Caveats — read before trusting a source:**

- **The `write-goal` skill's own Invariant Library does not apply here.** It is
  written for arche/Python: pyright strict, `mise run p:full-check`, Orval,
  SQLC, testcontainer Postgres, cassettes. keylink is Rust. The gates are the
  ones in `## Invariants` below. Do not import a Python invariant into this repo.
- **`AGENTS.md` and `CONTRIBUTING.md` outrank every skill here**, and both must
  be read before touching the tree. Where a skill and `AGENTS.md` disagree,
  `AGENTS.md` wins.
- **Historical goals, plans, RFCs and audits are not current capability
  evidence.** They preserve states later commits reversed.
- **Never run `fable`.** Not for a gate, not for a merge review, not under any
  skill or matrix that names it. Authorization means Pedro asking for it in his
  own words in a live conversation. Silence is no. Run `opus` or `codex`.

## Scope

**In scope**

- Tasks 4, 5, 6, 7, 8 and 9 of the tracked plan, executed in order, each ending
  in its own commit with the exact message the plan specifies.
- The reviews, the authoritative pre-push gate, the PR, the merge, and the
  worktree/branch cleanup that Task 9 defines.

**Out of scope**

- Re-litigating Tasks 0-3. They are committed and reviewed. Touch that code only
  where Tasks 4-9 genuinely require it, or where a review wave raises a
  Critical/Major finding against it.
- Rewriting either normative document. Record a deviation; do not edit the
  charter to match the code.
- Anything the extension goal's own **Non-goals** section excludes.

**Blast radius** — inherited verbatim from
`docs/goals/2026-08-13-http-gateway-extensions.md`. May modify:
`keylink/src/external.rs`, `keylink/src/macros.rs`, `keylink/src/blobs.rs` and
focused exports/tests; `keylink-http/**`; a new `keylink-openapi/**`; workspace
manifests, focused docs, examples, and `.mise/tasks/anchors`.

One widening is already taken and disclosed: `keylink/src/body.rs` carries the
13-line Task 2 reset fix. **Any further widening beyond the list above requires
stopping and reporting it** — annotate it inline and surface it at the end
rather than asking mid-run.

## Public interface

Two new public surfaces land in this run. Both must be designed before they are
written, and neither may leak an internal type.

```rust
// Task 4 — keylink::blobs. Design validated against iroh-blobs 0.103.0 source;
// see the handoff for the citations behind each decision.
#[derive(Debug, Clone)]
pub struct BlobHandle { /* private */ }

impl BlobHandle {
    pub async fn verified_len(&self) -> Result<u64>;
    pub async fn open(&self) -> Result<BlobReader>;
    pub async fn open_range(&self, range: Range<u64>) -> Result<BlobReader>;
    pub async fn copy_to<W: AsyncWrite + Unpin>(&self, sink: &mut W) -> Result<u64>;
    pub async fn fetch_into(&self, store: &Blobs) -> Result<u64>;
}

/// Owns the connection AND the get/FSM state — dropping it cancels that read.
/// Owning only the `Connection` loses cancellation.
pub struct BlobReader { /* private */ }

impl BlobReader {
    pub fn total_len(&self) -> u64;     // verified whole-blob size
    pub fn selected_len(&self) -> u64;  // bytes this reader will yield
    pub async fn next_chunk(&mut self) -> Result<Option<Bytes>>;
    pub async fn copy_to<W: AsyncWrite + Unpin>(self, sink: &mut W) -> Result<u64>;
}
```

No `iroh_blobs` type — `GetBlobItem`, `BaoContentItem`, `Stats`, `ChunkRanges`,
`Hash`, `fsm::*` — appears on any of those signatures.

Task 7's `keylink-openapi` exports a document builder whose `DocumentConfig::new`
takes a `Security` value, so a document with an undocumented authentication
state cannot be constructed.

## Success criteria

Each is binary and names its proof. A criterion whose proof was not surfaced in
the transcript is not met.

- [ ] Tasks 4-8 each land as one commit with the plan's exact message — proved by
      `git log --oneline` showing `feat: expose verified blob range readers`,
      `feat(http): proxy principal-bound blob ranges`,
      `feat(http): route keyed actor calls`,
      `feat: generate HTTP OpenAPI and JSON Schema`,
      `docs(http): document the complete gateway`.
- [ ] Every test-cycle row named in the plan for Tasks 4, 5 and 7 exists under
      that exact name — proved by `cargo nextest list` output containing each.
- [ ] Every new behavioral guard was watched red and restored — proved by the
      per-row break output (`── exit=101`, `── selected=1`, and the assertion
      text) and a registered anchor row.
- [ ] `mise run anchors` exits 0 with `RECORD_BLOCKS` matching this tree —
      proved by its output. **Read the number off the failing run; never
      compute it.**
- [ ] `cargo clippy --workspace --all-targets --all-features --locked -- -D warnings`
      exits 0 — proved by its output. The commit hook's `lint-fast` is
      `-p keylink --lib` only and never sees a test target, so this is not a
      duplicate of the hook.
- [ ] `cargo tree -p keylink` shows no HTTP or schema dependency, and
      `cargo tree -p keylink-http` shows no schema dependency — proved by output.
- [ ] The authoritative `mise run pre-push` gate passes on the exact pushed HEAD
      — proved by `/tmp/keylink-http-extensions-pre-push.log` with an `EXIT=0`
      marker, captured whole.
- [ ] `git rev-parse HEAD` equals `git ls-remote --heads origin goal-http-gateway`
      — proved by both outputs side by side. Existence is not a match.
- [ ] PR opened with a file-backed body, then merged — proved by
      `gh pr view <n> --json state,mergedAt` showing `MERGED`.
- [ ] Worktree and branch removed, local and remote — proved by `wt list`,
      `git branch --list 'goal-http-gateway*'` empty, and
      `git ls-remote --heads origin goal-http-gateway` empty.
- [ ] Every review wave closed with zero open Critical/Major — proved by the
      per-wave Critical-vs-Major counts.
- [ ] The invariant-compliance wave returned a per-invariant verdict with no
      miss — proved by that verdict, printed in full.

## Invariants

### Universal for this repo (non-negotiable)

- **A guard you have not watched fail is not a guard.** Break it, see the red,
  restore it, register it where `mise run anchors` checks it. A break that comes
  back GREEN is a finding about the test, not a pass.
- **Never run a broad async filter while any new liveness test is unproven.**
  Each new test terminates alone first.
- **Never kill a running gate. Never run a duplicate full gate immediately
  before a commit or push** — the hooks own that. Never bypass with
  `--no-verify` outside the two exceptions `CONTRIBUTING.md` lists.
- **Capture long output to `/tmp/<name>.log` with an `EXIT=` marker, whole.**
  Filter when reading, never when writing. `| tail` on a gate destroys the
  evidence that says why it failed.
- **State a SHA, a count or a literal only after reading it in the same turn.**
- **No ALPN cost accounting, scheduling, or reporting.** A pin move is free and
  is not news. The two pinned literal tests stay `#[ignore]`d.
- **Rust typing (`rust-best-practices`).** Make illegal states unrepresentable:
  enums over flag-and-option soup, validating newtypes over primitives,
  typestate for illegal operation *order*. No `unwrap`/`expect` outside tests —
  the workspace lints deny both. `#[expect(...)]` with a reason over `#[allow]`.
- **Deep modules (`codebase-design`).** A small interface over real behaviour.
  Apply the deletion test. The interface is the test surface — if a test must
  reach past it, the module is the wrong shape.
- **Reuse before creating (`ponytail`).** No interface with one implementation,
  no abstraction for a single call site, no config for a value that never
  changes. Fix at the root, never at the symptom.
- **A duplicated block can make an anchor ambiguous.** Extract shared handler
  preludes rather than copying them; this already broke `HJ9` once.
- **Fail loudly.** No silent fallback, no swallowed error, no best-effort path.
- **Subagents report by calling `SendMessage`.** Final assistant text reaches
  nobody. Every subagent prompt carries absolute paths and ends with the
  SendMessage instruction.

### Context-dependent (decided for this goal)

- **Test-data strategy: real loopback iroh endpoints.** Framing, flow control
  and cancellation are transport behaviour; a hand-written fake would encode the
  assumptions under test. The one sanctioned exception is Task 4's deterministic
  **fake iroh-blobs protocol peer**, which exists solely to send malicious size,
  corrupt Bao and truncated Bao responses that a correct peer cannot produce. It
  speaks the real `GetRequest` and drives the real client FSM.
- **Measure at the seam that owns the property, not through a socket.**
  Backpressure and cancellation rows call the handler in-process. Measured
  across a real TCP connection, a megabyte disappears into kernel buffers before
  Keylink is involved, and the test then measures the kernel.
- **Order every source failure behind a receiver-observed event.** A QUIC reset
  discards what the peer has not read, request header included, so an ungated
  failure destroys the request and the test waits forever for a report with no
  author.
- **A guard that another layer also enforces is not a guard.** If HTTP framing
  (`Content-Length`, chunked encoding) independently rejects the case, the test
  proves the framing, not the adapter. Find the case with no second checker.
- **Stack gates:** `mise run pre-commit` (hook-owned),
  `mise run pre-push` (authoritative),
  `cargo clippy --workspace --all-targets --all-features --locked -- -D warnings`,
  `cargo nextest run`, `mise run anchors`. GitHub Actions is off, held by two
  independent things; the local gate is the whole safety net.
- **New crate (Task 7):** `keylink-openapi` gets an enumerated public surface, a
  `codebase-design` pass during planning, and dependency-neutrality proven by
  `cargo tree` rather than asserted.
- **Break harness isolation:** `scripts/break-once.sh` mutates a source literal
  and restores on `trap EXIT`. Run it in the foreground, verify the restore
  before the next edit, and kill a stray by exact PID — never `pkill -f`.

## Execution interaction policy

**Questions during execution: not allowed.**

The executor annotates every question, assumption, deviation and blast-radius
widening inline as it goes, and surfaces them to Pedro **only after finishing**,
in one consolidated block. It never blocks mid-run.

If genuinely blocked with no valid path left, it stops and reports the paths
attempted, the evidence gathered, the blocker, and the open questions. It does
not ask mid-run and does not guess its way past a trust-model change.

## Process

Work in the existing worktree on `goal-http-gateway`. Atomic commits, one
logical change each, pushed when a task closes.

Per task: **Explore → Plan the task's rows → Execute → (Review ⇄ Apply fixes)×N
→ Commit.** Within a task, per row: **red → green → watched red → anchor**, one
row at a time.

- **Review is always run by a subagent, never by the author.** Quality waves use
  `brooks-review` (plus `brooks-test` for Tasks 4 and 5) on **opus**. An
  adversarial second opinion comes from `codex` — it earns its place on protocol
  and dependency facts, where it has already caught a design bug this branch
  would otherwise have shipped. `omp` may run a wave as a swarm when the work
  fans out cleanly. **Never `fable`.**
- **Fixes are applied through `receiving-code-review`.** Triage every finding;
  fix everything that carries real value; verify each fix. The reviewer never
  fixes its own findings.
- **A dedicated invariant-compliance wave runs on the execution review**, separate
  from the quality wave. Its only job is to grade the diff against this goal's
  `## Invariants` item by item and evidence each one with machine output:
  `mise run anchors`, the workspace clippy run, `cargo tree` for both packages,
  `cargo nextest list` for the plan's exact row names, and each break's
  `── exit=` / `── selected=` line. **An invariant it cannot evidence is treated
  as violated.** Any miss is Critical by definition and re-opens the loop.
- **Loop-break rule:** iterate until both the quality wave and the
  invariant-compliance wave return no Critical/Severe/Major. Minor findings do
  not re-open the loop. Report Critical-vs-Major counts per wave plus the
  per-invariant verdict.
- Task 9 runs the single mandatory merge-boundary review across the complete
  branch, then the gate, PR, merge and cleanup.

## Definition of done

- [ ] Every success criterion proven, evidence printed.
- [ ] Every plan checkbox for Tasks 4-9 walked task by task and confirmed done,
      or explicitly recorded as a deviation with its reason. **A half-done plan
      is not done.**
- [ ] Diff stays inside the blast radius; every widening reported.
- [ ] All stack gates green; the pre-push log captured whole with `EXIT=0`.
- [ ] Review waves clean — no open Critical/Major — with counts reported.
- [ ] Invariant-compliance wave clean, per-invariant verdict attached.
- [ ] Merged, and the worktree plus local and remote branch removed with proof
      of merge.
- [ ] **Final pass: re-read this goal AND
      `docs/goals/2026-08-13-http-gateway-extensions.md` end to end and confirm
      each item against real evidence, not memory.**
- [ ] One consolidated block of every inline question, assumption and deviation
      collected during the run, surfaced to Pedro at the end.

## /goal completion condition

```text
Satisfy every success criterion and Definition-of-done item in
docs/goals/2026-08-13-http-gateway-autonomous-delivery.md, which governs Tasks 4-9 of
docs/superpowers/plans/2026-08-13-http-gateway-extensions.md. Read
/tmp/keylink-http-gateway-handoff.md, AGENTS.md and CONTRIBUTING.md first. A plan already
exists; do not write another. Per row: red, green, watched red, anchor. Review with opus
subagents using brooks-review plus a separate invariant-compliance wave, adversarial
second opinion via codex; apply findings with receiving-code-review; never fable. Follow
rust-best-practices, codebase-design and ponytail throughout. AS THE FINAL STEP, print
the full proof state in ONE block: git log --oneline of the task commits; cargo nextest
list showing each plan row name; mise run anchors output; cargo clippy --workspace
--all-targets --all-features --locked -- -D warnings output; cargo tree -p keylink and -p
keylink-http; the pre-push log path with its EXIT= line; git rev-parse HEAD beside git
ls-remote --heads origin goal-http-gateway; gh pr view --json state,mergedAt; the
per-wave Critical/Major counts; the invariant-compliance per-invariant verdict; and one
consolidated list of every question, assumption and deviation annotated during the run.
Stay inside the blast radius in that goal and report any widening. Stop when all pass or
after 120 turns; if blocked, stop and report the paths attempted, evidence, blocker and
open questions - do not ask mid-run.
```

## Deliverable

Merged PR to `main` from `goal-http-gateway`, all gates green, worktree and
branch removed with proof of merge, and one end-of-run block carrying every
question, assumption and deviation.
