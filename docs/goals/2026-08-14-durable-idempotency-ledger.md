# Goal: a durable idempotency ledger

## Intent

A keyed call that has been answered must stay answered across a restart of the
serving process. Today the reply record lives only in the served service's
memory, so a restart makes every settled key read `Fresh` and the next retry
runs the work a second time. For a payments desk that is a duplicate payment.

The observable result: a caller that retries with the same `IdempotencyKey`
after the server restarted reads the first reply, byte for byte, exactly as it
would have before the restart.

## Context

`keylink/examples/payment_approval.rs` demonstrates the defect end to end and
asserts it. Its sections 10 and 11 stop the actor, drop the server, re-open the
durable state, and measure both halves: the four payments come back, the key
that read `Done` reads `Fresh`, and the same call pays again as run 5. That
example is the acceptance test this goal has to invert.

Where the ledger lives now:

- `keylink/src/rpc.rs:246-260` — `struct Keys { keyable: Keyable, ledger:
  Option<Arc<Ledger>> }`, one per **service**, held by `ServiceBuilder`.
- `keylink/src/idempotency.rs` — the `Ledger` itself, its bounds, and
  `key_state(peer, key)`. Records key on `(EndpointId, IdempotencyKey)`.
- `keylink/src/rpc.rs:1726` — `ServiceBuilder::records(Records)`, the existing
  opt-out. `Records::Memory` and `Records::None` are the two variants today.
- `keylink/examples/durable_ledger.rs:177-178` names the volatile ledger as the
  gap that `Persist` does **not** close.

What already exists and must be reused rather than duplicated:

- `keylink-persist` — the durability port. `Snapshots::claim(&StateId)` hands
  out a `Claim`, and `Claim::store(&[u8])` replaces one whole value atomically.
  `Versioned` stamps a version frame; `Files` is the one adapter.
- `keylink/src/persist.rs` — `Persist<D, S>`, the `Observer` that stores an
  actor's state inside the turn. Its ordering guarantee — the handler runs, the
  observer stores, and only then is the caller told anything — is the ordering
  this goal needs, applied to a different value.

The bounds the ledger already enforces, from `keylink/src/idempotency.rs`, all
of which the durable form must keep: `MAX_RECORDS = 1024`,
`MAX_RECORDED_BYTES = 1 << 20`, `MAX_TOMBSTONES = 8192`,
`RECORD_TTL = 600s`, `MAX_RUNNING = 4096`, `MAX_WAITING = 1024`. Every one is
settable through `Limits`.

## Skills

Load the matching skill before working in its area.

- **`rust-best-practices`** — the whole task is Rust library design. Chapter 7
  (type-driven design) governs the new port's error type and the record key
  newtype; chapter 4 governs the error hierarchy this adds to; chapter 10
  governs where the new module lives.
- **`codebase-design`** — this goal adds a **port**, which is the exact subject:
  a small interface with the storage behind it, at a seam that already has one
  adapter. Read the deletion test before adding a second trait.
- **`systematic-debugging`** — for the crash-window work in success criterion 4.
  A durability bug that appears only under a kill is the case where guessing
  costs the most.
- **`ponytail`** — the port must be the smallest thing that closes the hazard.
  Three operations, not a storage engine.

Not applicable here, and named so the executor does not go looking: there is no
`arche-*` skill in this repository, no database, no Kubernetes, and no frontend.

## Scope

**In scope**

- A new per-key durability port in `keylink-persist`, with `put`, `remove` and
  `compact` (or the minimal set that turns out to be sufficient).
- One adapter for it, alongside `Files`.
- A `Records` variant that wires a service's ledger to that port.
- Recovery at startup: a rebuilt `Ledger` that reads what is stored.
- The ordering guarantee: the record is durable **before** the reply is written
  to the stream.
- `keylink/examples/payment_approval.rs` updated so its restart act asserts the
  new behaviour, and the header rewritten to match.

**Out of scope**

- Durability across **machines**. One process's disk, exactly as `Files` is
  today. `keylink-persist/src/lib.rs:62-64` already says this about the state
  port and the same sentence applies here.
- Making the in-flight half durable. A claimed-but-unsettled key is a live
  caller on a live stream, like a parked `Deferred`, and no disk can carry one.
  A key claimed at crash time must recover as `Interrupted`, which is the state
  that already means *whether it took effect is unknown*.
- Changing `Keyable`, the wire types, or anything that moves the ALPN.
- Any change to the default. A service that does not ask for durability keeps
  `Records::Memory` and its current cost.

**Blast radius**

- May modify: `keylink-persist/src/`, `keylink/src/idempotency.rs`,
  `keylink/src/rpc.rs`, `keylink/src/persist.rs`,
  `keylink/examples/payment_approval.rs`, `keylink/tests/idempotency.rs`,
  `keylink/tests/persist.rs`, `keylink/Cargo.toml`,
  `keylink-persist/Cargo.toml`, `mise.toml`, `README.md`.
- Must not touch: `keylink/src/macros.rs` (this must not move the ALPN or change
  a generated surface), `keylink-http/`, `keylink-openapi/`, `demo/`,
  `.mise/tasks/anchors`, any existing break record.

## Public interface

Designed up front. No internal type appears here.

```rust
// keylink-persist: the new port, beside `Snapshots`.

/// One entry's address in a durable record store.
pub struct RecordKey(/* private */);

impl RecordKey {
    /// Build one from the pair a ledger keys on.
    pub fn new(peer: &[u8; 32], key: &str) -> Result<Self, InvalidRecordKey>;
}

/// A store of many small entries, each written and removed on its own.
///
/// Distinct from `Snapshots`, which replaces one whole value. A ledger writes
/// one record per settled call and would otherwise rewrite every record it
/// holds on each one.
pub trait Records: Send + Sync + 'static {
    /// Exclusive access, for the reason `Snapshots::claim` takes it.
    type Held: Held;

    fn claim(&self, id: &StateId) -> impl Future<Output = Result<Self::Held>> + Send;
}

pub trait Held: Send + 'static {
    /// Every entry currently stored, for recovery at startup.
    fn load(&self) -> impl Future<Output = Result<Vec<(RecordKey, Vec<u8>)>>> + Send;

    /// Write one entry. Durable when this resolves.
    fn put(&mut self, key: &RecordKey, bytes: &[u8]) -> impl Future<Output = Result<()>> + Send;

    /// Remove one entry. Durable when this resolves.
    fn remove(&mut self, key: &RecordKey) -> impl Future<Output = Result<()>> + Send;

    /// Reclaim space from removed entries. Never required for correctness.
    fn compact(&mut self) -> impl Future<Output = Result<()>> + Send;
}
```

```rust
// keylink: the one line a service author writes.

let service = treasury::service(handler, Allowlist::only([desk.id()]))
    .records(Records::Durable(store))     // the new variant
    .build();
```

Usage, end to end:

```rust
let store = keylink_persist::file::RecordFiles::open(&root).await?;
let _server = Server::builder(&node)
    .service(
        treasury::service(Treasury::default(), allow)
            .records(keylink::Records::Durable(store))
            .build(),
    )
    .spawn()?;
// A restart rebuilds the ledger from `root`, and a settled key stays settled.
```

## Success criteria

Each is binary and names its proof. The proof must be **printed in the
transcript**, not merely passed.

- [ ] A settled key survives a full restart of the serving process — a new node,
      a new `Server`, a new client — and the retry replays the first reply byte
      for byte. Verified by `cargo run -p keylink --example payment_approval`,
      whose restart act asserts `KeyState::Done` where it currently asserts
      `KeyState::Fresh`, and asserts the payment count does **not** grow.
- [ ] The record is durable before the reply is written. Verified by a test that
      fails a `Held::put` and asserts the caller never receives a success reply
      — named `a_reply_is_not_sent_before_its_record_is_stored` in
      `keylink/tests/idempotency.rs`.
- [ ] A key claimed but not settled at crash time recovers as `Interrupted`, not
      as `Fresh` and not as `Done`. Verified by
      `an_unsettled_key_recovers_as_interrupted` in the same file.
- [ ] Every existing bound still holds against the durable store — records,
      bytes, tombstones and TTL. Verified by the existing
      `keylink/tests/idempotency.rs` suite passing unchanged against the new
      `Records` variant, run as a parameterised fixture over both variants.
- [ ] The default is unchanged. Verified by `ServiceBuilder::new` still setting
      `Records::Memory`, asserted by a test that builds a service without
      naming `records` and reads its behaviour.
- [ ] The ALPN does not move. Verified by printing the ALPN of a fixture service
      before and after and asserting equality, or by the existing wire-contract
      pin in `CONTRIBUTING.md` staying green.
- [ ] `mise run gate` passes end to end, all 18 steps.

## Invariants

### Universal

- No `unwrap` and no `expect` outside a test module. The workspace denies both
  (`Cargo.toml:52-53`), and a durability path is the last place to panic.
- Every fallible boundary is typed. No `map_err(|e| Error::Other(e.to_string()))`
  — `keylink/src/error.rs` opens by stating that rule for this repository.
- Illegal states unrepresentable: `RecordKey` is a validating newtype with a
  private field and one constructor, in `StateId`'s shape. A decoded value
  bypasses the constructor, so the load path re-validates.
- No `#[allow]` without a named reason. Prefer `#[expect]`.
- Bounds are stated, never inherited. Anything unbounded is a defect, and
  `compose/mod.rs:69-72` is the sentence to match.

### Context-dependent, decided for this goal

- **Storage shape: a per-key port.** Not a whole-value snapshot through
  `Snapshots`. Pedro chose this on 2026-08-14 over reusing the existing port,
  which would rewrite up to `MAX_RECORDED_BYTES` on every settled call.
- **Ordering: durable before the reply.** Pedro chose this on 2026-08-14. The
  weaker "store afterwards" setting narrows the window and does not close it: a
  record lost after its reply was sent puts the caller back on a `Fresh` key and
  the work runs twice, which is the defect this goal exists to remove.
- **Opt-in, always.** The measured cost of an atomic file replace with two
  `fsync`s is ~7.6 ms against ~18 µs unobserved
  (`keylink/examples/durable_ledger.rs:165-168`). A default that pays it would
  make every keyed service slower by three orders of magnitude without anyone
  deciding to.
- **Test-data strategy: the real adapter on a temp directory.** No mock of the
  port. `keylink/tests/persist.rs` is the existing pattern; a mocked store
  cannot exhibit the crash windows criterion 3 is about.
- **New module, so:** an enumerated public surface, and a
  `codebase-design` pass before it is called done. `keylink-persist` must
  continue not to depend on `keylink`.
- **Feature gating:** the file-backed record adapter goes behind a feature the
  way `persist-file` gates `Files`, and a browser build must refuse it with a
  named `compile_error!` beside the four in `keylink/src/lib.rs:90-138`.

## Execution interaction policy

Questions during execution: **not allowed**. The executor annotates every
question and assumption inline as it goes, finishes everything that is not
blocked, and surfaces the complete list only after it has finished. It never
blocks mid-run waiting for an answer.

## Process

Work in an isolated worktree created with `wt switch -c <branch>`. Make atomic
commits. The delivery loop:

**Explore → Plan → (Review plan ⇄ Improve)×N → Execute → (Review ⇄ Fix)×N → PR.**

- Review is run by a subagent using the `brooks-*` skills and
  `requesting-code-review`. Findings are applied through
  `receiving-code-review`; the reviewer never fixes its own findings.
- A dedicated invariant-compliance wave grades the diff against the
  `## Invariants` list item by item and returns a per-invariant verdict. Any
  miss is Critical by definition.
- Loop-break: both waves return no Critical, Severe or Major.

Two repository rules override any habit here. `mise run gate` is the only gate
and CI is off by decision, so a green gate is the evidence. And a new example
command must be added to `[tasks.examples]` in `mise.toml`, because nothing else
enumerates them.

## Definition of done

- [ ] Every success criterion proven, with its output pasted.
- [ ] The diff stays inside the blast radius. `keylink/src/macros.rs` is
      untouched.
- [ ] `mise run gate` green, all 18 steps.
- [ ] Review waves clean; invariant-compliance wave clean, item by item.
- [ ] `keylink/examples/payment_approval.rs` no longer demonstrates the defect,
      and its header says what changed rather than being silently edited.
- [ ] Re-read this goal end to end and confirm each item against real evidence
      rather than memory.

## /goal completion condition

Satisfy every success criterion and Definition-of-done item in
`docs/goals/2026-08-14-durable-idempotency-ledger.md`: a keyed call that settled
before a restart replays its reply after one, through a new per-key durability
port in keylink-persist wired by an opt-in `Records` variant, with the record
durable before the reply is written. Do not touch `keylink/src/macros.rs` and do
not move the ALPN. Keep `Records::Memory` the default. AS THE FINAL STEP, print
the full proof state in ONE block: the output of `cargo run -p keylink --example
payment_approval` showing `KeyState::Done` after the restart and an unchanged
payment count, the results of
`a_reply_is_not_sent_before_its_record_is_stored` and
`an_unsettled_key_recovers_as_interrupted`, and the last line of `mise run gate`
with its exit code. If blocked or no valid path remains, stop and report the
attempted paths, the evidence, the blocker and every open question — do not ask
mid-run. Stop when all proofs pass or after 40 turns.

## Deliverable

Isolated worktree, PR to `main`, `mise run gate` green.
