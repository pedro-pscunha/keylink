# Goal: Close the twenty findings the ledger audit produced

## Intent

Two independent opus audits of PR #53 (merge `930036a`) produced 23 findings. Twenty
of them are real and must be closed. Two are correctness defects that a peer
disconnect triggers today. One is a scaling defect measured at 63.8x. One is a
restart stall measured at 9.1 seconds on a single call. The rest are interface
honesty, allocation, and documentation defects that the audits proved with compile
errors, clippy output, or a counting allocator.

The outcome is a keylink whose durable idempotency ledger no longer leaks a slot on
an abort, no longer scans every peer on every keyed call, no longer stalls for
seconds after a restart, and whose error types say what actually happened.

## Context

`keylink` is a pre-1.0 Rust workspace: typed peer-to-peer RPC over iroh/QUIC.
Workspace members: `keylink`, `keylink-persist`, `keylink-http`, `keylink-openapi`,
`demo`, `fixtures/imported-ledger`.

Every finding below lands against `main` at `930036a`, which merged PR #53 "A durable
idempotency ledger, and the port it needed".

**CI is off by decision.** `mise run gate` is the only gate. Its 18 tasks expand to 31
leaf commands, and the pre-push hook owns it. Three steps are the only place a given
thing is compiled or read: `features` is the only step that compiles a test binary at
`--no-default-features`, `docs` is the only step that resolves intra-doc links, and
`examples` is the only step that runs an example.

**This document was adversarially reviewed before you received it.** The review found
7 Critical, 11 Major and 8 Minor defects in an earlier draft, all of which are fixed
here. Four of them were proofs that passed while their item stayed open. Where an
instruction below looks pedantic about *how* something is proven, that is why.

Two audit reports are the source for every item. Their measurements and raw output
live in `/tmp/perf-*.log` and `/tmp/perf-proposed-clones-and-drain.diff`. Treat those
as evidence already gathered — but re-measure anything you change.

## Skills (load the matching one BEFORE working in its area)

**Review gates (always):**
- `brooks-review` — runs the adversarial review wave on the diff.
- `receiving-code-review` — triages every finding. The reviewer never fixes its own.
- `requesting-code-review` — before the merge.

**Domain lenses (Rust systems code, so all four apply):**
- `rust-best-practices` — ch. 1 (borrowing and allocation shape), ch. 4 (error
  hierarchies), ch. 5 (test naming and one assertion), ch. 7 (illegal states
  unrepresentable), ch. 8 (comments explain why), ch. 10 (visibility is a promise).
- `rust-performance` — items 5, 13 and 14 are measured work. Every claim needs a
  repeat count and an interleaved A/B, never one sample.
- `codebase-design` — items 11 and 12 are seam and depth findings. Apply the
  deletion test to every abstraction touched.
- `ponytail` — the counterweight. Items 12 and 18 are deliberately the *small* form
  of a larger fix. Do not let them grow.

**Process/meta:**
- `systematic-debugging` — three failed fixes on one item means the approach is
  wrong, not the hypothesis. Stop and record it.
- `test-driven-development` — every new assertion is red-tested before it counts.

**Caveats:**
- **Any instruction that treats an ALPN move as a cost is STALE for this repo.**
  `AGENTS.md:59` carries Pedro's standing ruling: a contract hash that moves costs
  nothing until keylink has a stable version. Both pinned tests are `#[ignore]`d
  (`keylink/tests/protocol.rs:1434` and `:1616`). Item 7 moves the hash. Take it. Do
  not price it, schedule around it, or report the move as a cost anywhere.
- The `write-goal` skill's own invariant library is written for Python and `arche_*`
  packages. Its universal invariants (pyright, Pydantic, `mise run p:full-check`) **do
  not apply here**. The Rust translation in `## Invariants` is authoritative.

## Scope

**In scope** — the twenty items in `## The work`, and nothing else.

**Out of scope**
- The `Held::compact` port relocation (design M3's large form). One implementor
  exists. Item 12 closes the trap without relocating the policy.
- The newtype form of item 18. `evict` reads those totals in a loop, so wrapping each
  counter carries a real runtime cost. Take only the `debug_assert`.
- A code change for item 20. Nothing measured justifies one.
- Any refactor, rename or cleanup not named in `## The work`.

**Blast radius** — this list is the one constraint the Definition of done checks
mechanically. Every file the twenty items touch is here.
- May modify: `keylink/src/idempotency.rs`, `keylink/src/durable.rs`,
  `keylink/src/rpc.rs`, `keylink/src/reply.rs`, `keylink/src/peer.rs`,
  `keylink/src/error.rs`, `keylink/tests/idempotency.rs`, `keylink/tests/durable.rs`,
  `keylink/tests/actors.rs`, `keylink/examples/payment_approval.rs`,
  `keylink-persist/src/lib.rs`, `keylink-persist/src/file.rs`,
  `keylink-persist/tests/versioned.rs`, `CONTRIBUTING.md`, `Cargo.toml` (the
  `[workspace.lints.clippy]` table only), and this goal document.
- Must not touch: `keylink/src/macros.rs`, `keylink-http`, `keylink-openapi`, `demo`,
  `fixtures/`, `.githooks/`, `.mise/`, `scripts/`, any other worktree.

## The work

Twenty items. Each names its location, its defect, its fix, and its proof.

### Correctness — a peer disconnect triggers items 1 and 2 today

**1. `Guard::record` leaks a `Running` slot on abort.**
`keylink/src/idempotency.rs:2492`. `self.settled = true` runs before the
`write_settled` await. `Drop` at `:2580` returns early when `settled` is true.
`rpc.rs` aborts every in-flight call on disconnect via `calls.shutdown().await`. A
`serve_one` aborted inside that await drops the `Guard` and settles nothing, so the
slot stays `Slot::Running` — which its own doc at `:1044` says is never evicted. The
key answers `InFlight` for the life of the process and `bucket.running` plus
`inner.running` never come back. After `MAX_RUNNING` such aborts the service refuses
every keyed call.
**Fix.** This is **not** a pure line move: the await sits inside the
`if let Err(refusal) = … .await {` head at `:2535`, so there is no position "after the
await, before the error branch". Bind the result first, then set the flag:
```rust
let stored = self.ledger.write_settled(self.peer, &self.key, &slot).await;
self.settled = true;
if let Err(refusal) = stored { /* … */ }
```
**Scope:** `Records::Durable` only. The memory path has no await point there.

**2. An abort during `write_claim` settles `Interrupted` for work that never ran.**
`keylink/src/idempotency.rs:2032`. The error path calls `guard.give_back()` because
nothing ran. The abort path drops a `Guard` with `progress == None`
(`Ledger::claim` builds it that way at `:1956`), so `Drop` at `:2587` settles
`Slot::Interrupted`. The two paths disagree about the same fact, and the abort path
inverts the module header's rule that a refusal before dispatch leaves the key
claimable.
**Fix:** build the guard in `Ledger::claim` with a fresh `Progress`, which reads
`NotDispatched`, so `Drop` releases. `serve_one` overwrites it at
`keylink/src/rpc.rs:677` and both read `NotDispatched` until dispatch.
**This changes what the in-module test helper `guard()` at `:2700` produces** — see
item 1's success criterion.

**3. `Keyable::None` with `Records::Durable` holds the store file lock forever.**
`keylink/src/rpc.rs:276`. `Keyable::needs_a_ledger()` is false for `None`, so no
`Ledger` is built and `take_recovered` never runs. The `DurableRecords` still lives in
`Keys` and still holds the exclusive claim on the log file. A correctly configured
second service on the same `StateId` then gets `Error::Busy` from a service that will
never write a record.
**Fix:** refuse at `ServiceBuilder` erase time, beside the existing
`ConfigError::UnknownMethod` check. `ConfigError` is declared at
`keylink/src/error.rs:719` and is already `#[non_exhaustive]`, so the new variant is
not a breaking change. **Put its test in `keylink/tests/durable.rs`** —
`keylink/tests/admission.rs` holds the existing `ConfigError` assertion and is outside
the blast radius.

### Stalls and scaling

**4. The first keyed call after a restart drains the whole queue.**
`keylink/src/idempotency.rs:2112`. `housekeep` takes all of `Inner::pending` with
`std::mem::take`. `Ledger::new` runs `evict` at `:1885` over the recovered set, and
every eviction pushes onto `pending`. Each queued entry is one `fsync` under the store
lock. Measured: **9102.9 ms on one call** against 1.0 ms of memory eviction, from 1500
seeded records reopened at `pool_records(64)`.
**Fix — and take this exact shape, because two weaker ones were rejected in review.**
Change `pending` (`:1153`) from `Vec` to `VecDeque` and drain a bounded prefix from the
**front**:
```rust
let batch: Vec<_> = {
    let mut guard = self.lock();
    let take = guard.pending.len().min(DRAIN_PER_CLAIM);
    guard.pending.drain(..take).collect()
};
```
`Vec::drain(..n)` shifts the whole remainder on every call, which is O(n) against a
90,000-entry backlog. `Vec::split_off(len - n)` avoids that but takes from the tail,
and `:2122` returns failures to the back — so under sustained traffic the front is
never reached and its records stay on the disk forever. `VecDeque` gives O(k) *and*
FIFO, so neither defect appears. Return failures with `push_back`.
**The doc at `keylink/src/idempotency.rs:1144-1147` becomes false** — it says `pending`
is *"bounded by the evictions between two claims"*, which a bounded drain repeals.
Rewrite it to state the new bound.
**Acceptance target, so the constant can be judged.** 16 costs about 55 ms per call at
3.5 ms per write; 4 costs about 14 ms. **The first keyed call after a restart must cost
under 60 ms at the 1500-record fixture, and the backlog must clear within 2000 keyed
calls.** Pick the largest `DRAIN_PER_CLAIM` meeting both, and print both figures in the
commit message. "Justify it with a measurement" alone is a rule no value can fail.

**5. `evict` scans every peer on every keyed call.**
`keylink/src/idempotency.rs:2413`. `largest` runs `.iter().max_by_key()` over
`inner.peers`, and both `while` loops in `evict` call it. At steady state — pools full,
which a long-running service reaches — every settle costs two full scans under the
ledger's `std::sync::Mutex`, so it serializes the whole service.
Measured, interleaved A/B, 21 samples per arm: **63.8x at 4096 peers, 16.6x at 1024,
5.1x at 256, 2.0x at 64, no change at 8 or fewer.**
**Fix:** keep two `BTreeSet<(usize, EndpointId)>` indexes on `Inner` — `by_held` and
`by_spent` — plus `filed_held`/`filed_spent` on `Bucket`. Replace both `largest` calls
with `.last()`. `max_by_key` on `(size, id)` and `BTreeSet::last` on `(size, id)`
select the same element, because peer ids are unique, so the tie-break is unchanged.
**`recover` (`keylink/src/idempotency.rs:2385-2408`) is the site no `refile` call
reaches**, because it builds `Inner::default()` and pushes straight into
`bucket.held`/`bucket.spent` without going through `claim` or `settle`. It must fill
both indexes before `Ledger::new` runs `evict` at `:1885`. **If it does not, a
recovered ledger evicts nothing, `pending` stays empty, and item 4's restart
measurement improves for the wrong reason** — the exact failure shape this repository
has already been burned by once.
**Do not count the call sites off this sentence.** Every site that changes a bucket's
queue length or removes a peer needs one, adjacent pairs inside a loop can share one,
and there are 14 such sites in `idempotency.rs` outside the test module today. Count
them off the code. Item 18 is the enforcement, so **items 5 and 18 land in the same
commit.**

**6. `housekeep` runs on the attach path.**
`keylink/src/idempotency.rs:2026`, reached from `keylink/src/rpc.rs:412`, `:464` and
`:471` — the only three call sites of `claim_durably` in the workspace.
`claim_durably`'s doc says housekeeping runs "before the claim … so that the one place
a caller is already waiting to start work is the place that pays". Two of the three
sites break that sentence: an attaching caller never receives `Claim::Fresh`. Worse,
`housekeep` takes the same store lock `Guard::record` needs, so an attacher's
housekeeping can block the settle it is parked waiting for.
**Fix:** remove the call from `claim_durably` and call `ledger.housekeep().await` once
at the top of `claim_key`, after the `validate()` check.
`housekeep` is private at `:2106` and **must widen to `pub(crate)`** or the move does
not compile. Its own doc at `:2096-2100` says the claim is written "two lines later, on
this same call", which stops being true — rewrite it. Move `claim_durably`'s paragraph
at `:2021-2025` to the new call site rather than deleting it.
**Not a correctness defect.** `notified.as_mut().enable()` at `rpc.rs:462` runs before
the re-claim, so no wakeup is lost. Do not change that ordering.

### Interface honesty — the ALPN moves here, and that is free

**7. Mint the refusal that says "nothing ran, this key is safe to send again".**
A claim-write failure is delivered as `RemoteError::Failed`, whose own doc says it is
not a discriminator. "Nothing ran, retry is safe" is the single fact a caller most
wants to act on, and it arrives as prose. `keylink/src/reply.rs:153-159` justifies
this on the grounds that minting a variant "would move the ALPN and break every
handshake", and `keylink/src/idempotency.rs:2049-2052` repeats the argument in
`write_claim`'s `# Errors` block.
**That justification is void.** `AGENTS.md:59` rules the hash move free and both pin
tests are `#[ignore]`d.
**Mint it on `IdempotencyRefusal`, NOT on `RemoteError`.** `reply.rs:95-96` and
`idempotency.rs:312-314` both rule that this vocabulary belongs to one enum — *"one
variant rather than four, because this type belongs to every service keylink will ever
carry and that vocabulary belongs to one feature"*. `IdempotencyRefusal` is already
`#[non_exhaustive]` at `:318` and already reaches the wire through
`RemoteError::Idempotency`. Name it **`NotClaimed`**. **Append it** — `reply.rs:60-66`
rules that an insert renumbers every later variant and turns a refusal into a
misdecode.
**Change the producer.** `keylink/src/idempotency.rs:2057-2060` builds
`RemoteError::Failed(format!(…))`. A variant that is declared and never returned closes
nothing.
**Delete the repealed justification in BOTH places** — `reply.rs:153-159` and
`idempotency.rs:2049-2052`. Leaving either tells the next reader a repealed rule is
live.

**8. `IdempotencyRefusal::Interrupted` is returned where the handler completed.**
`keylink/src/idempotency.rs:2085`, against the variant whose doc is at `:337` and whose
declaration is at `:360`. The variant reads "entered the handler and never produced a
reply". At this producer the handler did produce a reply and the reply is discarded.
`Unrecordable` (`:362`) is the sentence this condition needs.
The code's unstated reason is real: the disk holds `TAG_CLAIMED`, which recovers as
`Interrupted`, so answering `Unrecordable` in memory alone would give a live retry and
a post-restart retry two different answers for one event.
**Fix — the behavioral form.** Settle memory as `Slot::Unrecordable` (the settle is at
`:2538`, inside `Guard::record`), push `(peer, key)` onto `inner.pending`, and let the
next `housekeep` write `TAG_UNRECORDABLE`. The disk converges. If the process dies
first the disk still says `Interrupted`, which is the conservative direction.
`TAG_UNRECORDABLE` already exists (`durable.rs:75`, encoded at `:194`, decoded at
`:209`), so **no on-disk layout changes.**
**This item does NOT depend on item 7.** `Unrecordable` already exists and no hash
moves. An earlier draft claimed a dependency that is not real.
**It DOES change two assertions.** `keylink/tests/idempotency.rs:2252` and `:2271`
assert `IdempotencyRefusal::Interrupted` in
`a_reply_is_not_sent_before_its_record_is_stored`. Both become `Unrecordable`. The
comment at `:2253` paraphrases `Unrecordable` while asserting `Interrupted` — fix it in
the same edit.

**9. `RecordFault::KeyRefused` names three causes and one cannot happen.**
`keylink/src/durable.rs:141`, used at `:162`, `:173` and `:175`. The doc says "the bytes
after the peer id are text this build would not accept as a key". At `:173`
`EndpointId::from_bytes` refused 32 stored bytes, which is a bad peer id. At `:162`
`encode_key` failed on the **write** path, while `RecordFault`'s type doc says it names
which check refused a **stored** record — and the `const _` at `:83` proves that case
unreachable.
**Fix:** add a `PeerNotAnId` variant. `RecordFault` is `#[non_exhaustive]` at `:126` and
is not a wire type. Leave `encode_key`'s `Result` alone — the crate denies
`clippy::unwrap_used`, so no total spelling exists — and say in its doc that the
`const _` makes the error unreachable.

**10. `Records` is an exhaustive public enum with a feature-gated variant.**
`keylink/src/idempotency.rs:719`, variant at `:757` (the `#[cfg]` is at `:756`). Cargo
unifies features, so a downstream crate that matches `Records` exhaustively compiles
without `persist` and fails to compile when an unrelated crate in the same build turns
it on. The downstream author has no control over that.
**Fix:** add `#[non_exhaustive]`. It blocks exhaustive matching outside the crate and
does **not** block constructing the existing variants, so the doc's argument that a
caller constructs this is unaffected.
**The rule it excepts lives in three places**, all of which must name the exception:
`CONTRIBUTING.md:242-243`, and the in-file restatements at
`keylink/src/idempotency.rs:496-497` and `:810`.

### Shape

**11. Delete `EntryFiles`; rename `Entries::claim` to `hold`.**
`keylink-persist/src/file.rs:418`. `EntryFiles(Files)` is a large interface over a thin
implementation — `open` and `limit` both delegate, and a third setter on `Files` needs
a third delegate. It exists only because `Snapshots` and `Entries` both name a method
`claim`, which made concrete-`Files` calls ambiguous (E0034). The collision does not
bite generic callers: `keylink/src/persist.rs:336` and `keylink/src/durable.rs:306`
resolve through their trait bound.
Renaming `Entries::claim` is far cheaper than renaming `Snapshots::claim`. The two
ports do different things — one replaces a blob wholesale, the other keys many entries
— so they must not share a verb.
**Fix:** rename to `hold`, implement both ports on `Files`, delete the newtype.
**Three call sites live outside `keylink-persist`** and all three change to `Files`:
`keylink/examples/payment_approval.rs:65` and `:237`, and
`keylink/tests/idempotency.rs:2130`. The example's comment at `:233-236` explains the
two types and becomes false — rewrite it to say one type carries two ports. **The
`examples` gate step runs this example, so a missed edit fails the gate.**
Do not report a site count from this document; find them with `rg`. `claim(` is
ambiguous across `Ledger::claim`, `Snapshots::claim` and `Entries::claim`.

**12. Close the `fetch_add` trap in `DurableRecords`.**
`keylink/src/durable.rs:357`, `:371` and `:412`. Three write paths must each remember to
increment `since_compaction`. A fourth added without one silently disables compaction,
and no test can see it.
**Fix — and note the shape, because the obvious one deadlocks.** `reconcile` takes
`self.store.held` at `:396` and holds it across its whole loop, so a helper that
acquires the lock itself deadlocks there. `:404` is a `remove`, which a method named
`put` cannot carry. Use a shape that wraps the *outcome* instead:
```rust
/// The only place `since_compaction` is incremented.
async fn counted<T, E>(&self, outcome: Result<T, E>) -> Result<T, E>
```
so all three sites funnel through it and `:412` — the site where a fourth path is most
likely to be added — is covered.
**Explicitly NOT the port change.** Do not add a parameter to `Held::compact`, do not
move `COMPACTION_SLACK` into `keylink-persist`. One `Held` implementor exists;
generalizing for a second is speculative.

### Small, unambiguous, each already evidenced

**13. Two removable key clones on every keyed call.**
`keylink/src/idempotency.rs:2271` — `expire` clones the front key to reach a `break`
seven lines later at `:2278`. And `:2028` — `claim_durably` clones a key the `Fresh`
guard already holds. Measured with a counting global allocator: **8.00 → 6.00
allocations per call**, exact and identical across three runs. The timing effect was
about 100 ns against a within-arm spread of about 100 ns, so **the timing is
inconclusive and must be reported as such.** Take the allocation count as the result.
The second clone needs a restructure, not a deletion: `key` is used again at `:2032` in
`write_claim(peer, &key)`, so the `Claim::Fresh` match has to move above the write and
borrow the key off the guard.
Starting diff: `/tmp/perf-proposed-clones-and-drain.diff`.

**14. `reconcile` clones the remainder it is about to return.**
`keylink/src/durable.rs:410`. `batch[index..].to_vec()` clones out of an owned `Vec`
about to drop. Index the loop instead of `batch.iter()` and return
`batch.split_off(index)`, which moves.

**15. `redundant_clone` on every `connect()`.**
`keylink/src/peer.rs:96`. `slot()` at `:167` already returns an owned `Option<Arc<C>>`.
Delete the second `.clone()`.
**`clippy::redundant_clone` is in the `nursery` group, which is allow-by-default.**
`Cargo.toml:50-53` enables `clippy::all` plus `expect_used` and `unwrap_used`, and no
member manifest enables `nursery` or `pedantic`. **`mise run gate` therefore never sees
this lint**, so the gate is not a proof for this item. See its success criterion.

**16. `Records::stores_durably`'s doc gives one reason and omits the binding one.**
`keylink/src/idempotency.rs:789-793`. The doc says a `&DurableRecords` under the
ledger's lock is *"a borrow it has no use for"*. **That sentence is true** — nothing can
await there — and an earlier draft of this goal wrongly told you to delete it as false.
It is not the reason the split is *forced*. That reason is the
`#[cfg(feature = "persist")]` on `store` at `:781`: `evict`, `release` and `expire` are
not gated, so they cannot call it at all. The audit proved this with an `E0599` at
`--no-default-features`.
**Fix:** keep the existing sentence, add the cfg reason beside it. No code change.

**17. `durable.rs:34` prints 7.6 ms with no platform.**
`std::fs::File::sync_data` on macOS issues `F_FULLFSYNC`, measured at about **12x a
plain `libc::fsync`** (5498 µs against 462 µs). On Linux it maps to `fdatasync`. An
operator sizing a Linux deployment reads 7.6 ms as pessimistic by roughly an order of
magnitude.
**Fix:** name the platform beside the figure and state the Linux direction. `sync_data`
remains correct for the promise the ledger makes — do not change it.

**18. Add a `debug_assert` that `Inner`'s derived state agrees with `peers`.**
`keylink/src/idempotency.rs:1124-1154`.
**It must cover six facts, not five:** `held`, `bytes`, `spent` and `running` as totals
over `peers`; `waiting` as a total over `waiters`; **and `by_held`/`by_spent` as exactly
the non-empty `(len, id)` pairs `peers` holds.** The sixth is the one that makes item
5's forgotten `refile` visible — the five counters stay correct while the two indexes
rot, so a five-fact check cannot detect the drift item 5 introduces.
**Fix:** one `#[cfg(debug_assertions)]` check that `Ledger::lock` runs.
**Bound its cost.** The check is O(peers × slots) because `bytes` sums `held_bytes` over
every slot, and `Ledger::lock` is on every keyed path while item 5's fixtures reach 4096
peers. Time `mise run test` before and after, print both, and if the suite grows by more
than 20%, gate the scan behind a peer-count ceiling and say so in the code.
**Land with item 5. Do NOT take the newtype form.**

**19. Pin `self_named_module_files`.**
`Cargo.toml`, `[workspace.lints.clippy]`. `keylink/src/actor/mod.rs` and
`keylink/src/compose/mod.rs` are the only directory modules in the workspace, so the
layout is consistent today and held by habit rather than by the gate. Add
`self_named_module_files = "deny"`. The lint names read backwards from what they sound
like: `self_named_module_files` denies `foo.rs` beside `foo/`, which is a no-op on the
current tree. All six members carry `[lints] workspace = true`, so the deny reaches
every crate. This lint is `restriction`, which is allow-by-default, so `clippy::all`
never sees it.

**20. Record the compaction cost, and state the small-ledger ratio.**
`keylink/src/durable.rs:433`. `FileLog::compact` costs **10 to 20 ms** and does not grow
with the live count over a 144x range, so two syncs and a rename dominate. With
`COMPACTION_SLACK = 64` (`:102`) it fires once every `live + 64` writes: about one call
in 4640 at a full default ledger (9216 live), but about **one call in 37 at a ledger
with 10 live entries**.
**Fix:** record the measured cost in `compact_if_worthwhile`'s rustdoc and state the
small-ledger ratio as an open question. **No code change.**

## Public interface changes

Four items change the exported surface. Nothing else does.

```rust
// Item 7 — keylink/src/idempotency.rs. APPENDED: reply.rs:60-66 rules that an
// insert renumbers every later variant and turns a refusal into a misdecode.
pub enum IdempotencyRefusal {
    // … existing variants unchanged …
    /// The service could not write the claim, so the handler was never entered.
    #[error("this service could not record the key, so nothing ran - the same key is safe to send again")]
    NotClaimed,
}

// Item 9 — keylink/src/durable.rs. `RecordFault` is `#[non_exhaustive]`, not a wire type.
pub enum RecordFault {
    // … existing variants unchanged …
    /// The first 32 bytes of a record key are not a peer id.
    PeerNotAnId,
}

// Item 10 — keylink/src/idempotency.rs.
#[non_exhaustive]
pub enum Records { /* unchanged variants */ }

// Item 3 — keylink/src/error.rs. `ConfigError` is already `#[non_exhaustive]`.
pub enum ConfigError {
    // … existing variants unchanged …
    /// `Records::Durable` was configured on a service that accepts no idempotency
    /// key, so the store would be held and never written.
    DurableWithoutKeys,
}

// Item 11 — keylink-persist/src/lib.rs. `EntryFiles` is deleted; `Files` serves both ports.
pub trait Entries: Send + Sync + 'static {
    type Held: Held;
    /// Take exclusive hold of this state's entry log.
    fn hold(&self, id: &StateId) -> impl Future<Output = Result<Self::Held>> + Send;
}
```

`Slot`, `Bucket`, `Inner`, `by_held`, `by_spent`, `filed_held`, `filed_spent`,
`DRAIN_PER_CLAIM` and `DurableRecords::counted` are crate-private and stay so.

## Success criteria

Each is binary and names a proof that **cannot pass while its item is open**. Every
proof must be **printed in the transcript**.

- [ ] **Item 1.** A test drives `record` with a `Held` whose `put` never resolves, polls
      once to `Poll::Pending`, drops the future, and asserts
      `key_state == KeyState::Interrupted`. **The guard must carry a dispatched
      `Progress` via `Guard::watching`** — a guard straight out of `Ledger::claim`
      reads `NotDispatched` once item 2 lands and releases instead, so a test built on
      the `guard()` helper at `:2700` turns red for the wrong reason. Named
      `an_abort_during_the_settle_does_not_leak_a_running_slot` or equally descriptive.
      **Red run printed, showing `Running`.**
- [ ] **Item 2.** A test asserts a key is claimable after an abort during `write_claim`.
      Red run printed.
- [ ] **Item 3.** `Keyable::None` with `Records::Durable` is refused at build time,
      asserted by a test in `keylink/tests/durable.rs` naming the `ConfigError`.
- [ ] **Item 4.** Restart figures before and after printed, with the chosen
      `DRAIN_PER_CLAIM` and both acceptance figures (first call under 60 ms; backlog
      clear within 2000 calls). **Also print `pending.len()` after `Ledger::new` in the
      seeded fixture — it must be non-zero before the drain, or the measurement is of an
      empty queue and proves nothing.**
- [ ] **Item 5.** Interleaved A/B re-run and printed with its repeat count, at 64, 256,
      1024 and 4096 peers. **Red run printed**: reading the index from the wrong end
      must make `a_flooding_peer_cannot_evict_a_quiet_peers_record` (`:3473`) fail on
      its own assertion.
- [ ] **Items 6, 14 closed** — `mise run gate` passes and the diff shows the change.
- [ ] **Items 16, 17, 20 closed** — the replaced paragraph is quoted in full in the
      transcript, old text beside new. No gate step reads a comment for truth.
- [ ] **Item 15 closed** — `cargo clippy -p keylink --lib -- -W clippy::redundant_clone`
      printed **before** the change showing the warning at `peer.rs:96`, and after
      showing none.
- [ ] **Item 12 closed** — `rg 'since_compaction' keylink/src/durable.rs` returns exactly
      the declaration and one increment.
- [ ] **Item 7 closed** — `rg 'RemoteError::Failed' keylink/src/idempotency.rs` returns
      nothing; a test drives a store that refuses `claimed` and asserts the caller
      receives `IdempotencyRefusal::NotClaimed` (red run printed); and
      `rg 'move the ALPN' keylink/src/reply.rs keylink/src/idempotency.rs` returns
      nothing.
- [ ] **Items 8, 9, 10, 11 closed** — `rg 'EntryFiles'` over the whole workspace returns
      nothing; `Records` and `RecordFault` carry their new attributes and variants; the
      three `CONTRIBUTING.md`/in-file restatements name the item 10 exception.
- [ ] **Item 13 closed.** Allocation count re-measured and printed: **8.00 → 6.00 per
      call.** The timing is reported as **inconclusive**, not as a win.
- [ ] **Item 18 closed.** The check covers all six facts, and **a red run is printed**
      showing it fires when one `refile` call site is deleted. `mise run test` timing
      printed before and after.
- [ ] **Item 19 closed** — `self_named_module_files = "deny"` is in `Cargo.toml` and the
      gate passes.
- [ ] **Every new or changed assertion is red-tested**, with the red run and the
      assertion that fired printed. A green run is not evidence.
- [ ] `mise run gate` exits 0. **Last line and exit code printed.**
- [ ] `cargo run -p keylink --example payment_approval` still prints `KeyState::Done`
      after the restart and an unchanged payment count. Output printed.
- [ ] `a_reply_is_not_sent_before_its_record_is_stored` passes with its two assertions
      (`keylink/tests/idempotency.rs:2252`, `:2271`) reading `Unrecordable`, and
      `an_unsettled_key_recovers_as_interrupted` (`:2284`) passes unchanged. Printed.

## Invariants

### Universal (Rust translation — authoritative for this goal)

- **No `unwrap`/`expect` outside tests.** The workspace denies both. Do not add an
  `#[allow]`.
- **Illegal states unrepresentable** (ch. 7). Prefer a type that cannot be forgotten
  over a call that must be remembered. Items 1, 2, 3, 12 and 18 are instances.
- **Errors say what happened** (ch. 4). A variant whose doc contradicts its producer is
  a defect. A variant nobody returns closes nothing.
- **No catch-all `_ =>` inside this crate.** It discards the compiler's list of sites to
  update when a variant is added — and items 3, 7 and 9 add variants.
- **Borrow at interfaces; do not clone what is owned** (ch. 1).
- **Fail loudly.** No silent fallback, no swallowed error, no `saturating_sub` hiding a
  drift that should be reported.
- **No speculative abstraction** (`ponytail`). Items 12 and 18 are deliberately small.
  Keep them small.
- **Test quality** (ch. 5): descriptive names, one assertion where possible, and **every
  load-bearing assertion red-tested**. A test that cannot fail proves nothing. A test
  whose setup and recovery path share a rule proves nothing either — drive it with
  something recovery cannot recompute. Item 4's `pending.len()` control and item 1's
  `Progress` requirement both exist for this reason.
- **Fix the root cause** (`systematic-debugging`). Three failed fixes on one item means
  the approach is wrong; stop and record it.
- **Comments explain why** (ch. 8). A comment stating a reason that stopped being true is
  a defect — items 4, 6 and 16 each correct one.
- **Atomic commits.** One logical change per commit. Items 5+18 are one commit.

### Context-dependent (decided for this goal)

- **Gate:** `mise run gate`, 18 tasks expanding to 31 leaf commands, owned by the
  pre-push hook. **Do not pre-run it before committing or pushing** — the hooks are the
  gate and running it by hand duplicates many minutes for zero signal. While iterating,
  run the narrowest target. Before pushing a change that adds tests or doc links, run
  the three exclusive steps directly:
  `cargo check -p keylink --all-targets --no-default-features`, `mise run docs`, and the
  example command.
- **ALPN:** a hash move is expected and free. Both pin tests are `#[ignore]`d. Do not
  price it or report it as a cost.
- **`Records::Memory` stays the default.** `Durable` stays opt-in behind `persist`.
- **Persist file format unchanged.** No item alters the on-disk layout.
- **Performance evidence:** ratios from interleaved runs with a stated repeat count. A
  single absolute figure is an order of magnitude only — this machine has measured 1.7x
  variance between runs minutes apart. **If the variance exceeds the effect, report the
  measurement as inconclusive rather than reporting a midpoint.**
- **Long output captured whole** to `/tmp/<name>.log` with `tee` and an `EXIT=` marker
  from `${pipestatus[N]}`. Never `| tail` a gated command. Never read `$?` after a pipe.
- **Exit 0 does not mean pushed.** Confirm by comparing `git rev-parse HEAD` against
  `git ls-remote --heads origin <branch>` for an exact SHA match.
- **If the gate's `anchors` step fails on a break-record count**, read the new number off
  the failing run. Never compute it.

## Execution interaction policy

**Questions during execution: not allowed.** The executor annotates every question and
assumption inline as it goes and surfaces them to Pedro only after finishing. It never
blocks mid-run. If genuinely blocked with no valid path left, it stops and reports the
attempted paths, the evidence, the blocker, and every open question.

## Process

Work in an isolated worktree created with worktrunk (`wt switch -c <branch> -y`, never
raw `git worktree`), branched from `main` at `930036a` or later. Atomic commits. One PR.

**Commit this goal document as the first commit** at
`docs/goals/2026-08-14-ledger-audit-remediation.md`. It does not exist on `main` — copy
it from
`/Users/pedrocunha/repos/keylink.design-audit/docs/goals/2026-08-14-ledger-audit-remediation.md`.

**No plan document is written** — decided with Pedro. `## The work` names every file,
line, fix and proof, so a plan would restate it. The goal is the executable spec.

Delivery loop: **Execute → ( Review execution ⇄ Apply fixes )×N → merge gate → PR.**

- **Review** is run by an **opus** subagent using `brooks-review`, plus
  `requesting-code-review` before the merge.
- **Apply fixes** goes through `receiving-code-review`. The reviewer never fixes its own
  findings.
- **Loop-break:** iterate until the wave returns no Critical, Severe or Major. Minor
  findings do not re-open the loop. Report Critical-vs-Major counts each wave.
- **Never run the fable model.** Every review and every subagent runs on opus. A gate
  being a gate does not authorize fable.

## Definition of done

- [ ] All twenty items closed, each traceable to a commit.
- [ ] Every success criterion proven, with the evidence printed in the transcript.
- [ ] Every new or changed assertion red-tested, with the red run printed.
- [ ] Diff stays inside the blast radius. No unrequested refactor.
- [ ] `mise run gate` exits 0 on the final HEAD.
- [ ] Review wave clean — no open Critical or Major. Counts reported.
- [ ] The three out-of-scope items (the `Held::compact` port relocation, item 18's
      newtype form, item 20's code change) are confirmed **not** done.
- [ ] Pushed, and the push confirmed by exact SHA match against `origin`.
- [ ] Merged to `main`. Worktree and local branch removed only on proof of merge.
- [ ] **Final pass — re-read this goal end to end** and check each item against real
      evidence rather than memory.

## /goal completion condition

```text
Satisfy every success criterion and Definition-of-done item in
docs/goals/2026-08-14-ledger-audit-remediation.md — all twenty audit findings closed,
each red-tested where it carries an assertion. Copy that file in as your first commit
from /Users/pedrocunha/repos/keylink.design-audit/docs/goals/2026-08-14-ledger-audit-remediation.md;
it is not on main. Do not touch keylink/src/macros.rs. Keep Records::Memory the
default. An ALPN move is FREE and expected (AGENTS.md:59, both pin tests are
#[ignore]d) - do not price it. Do NOT do the three items that file lists as out of
scope. AS THE FINAL STEP, print the full proof state in ONE block: the item 5
interleaved A/B with its repeat count, the item 4 restart figures before and after
plus pending.len() after Ledger::new and the chosen DRAIN_PER_CLAIM against its 60ms
and 2000-call targets, the item 13 allocation count (8.00 -> 6.00) with the timing
reported as inconclusive, the red run for items 1, 5 and 18 showing which assertion
fired, the item 15 clippy output before and after, the output of
`cargo run -p keylink --example payment_approval`, the results of
`a_reply_is_not_sent_before_its_record_is_stored` and
`an_unsettled_key_recovers_as_interrupted`, and the last line of `mise run gate` with
its exit code. Run every review on opus, never fable. Stop when all proofs pass or
after 60 turns. If blocked or no valid path remains, stop and report the attempted
paths, the evidence, the blocker and every open question - do not ask mid-run.
```

## Deliverable

Isolated worktree, atomic commits, one PR to `main`, `mise run gate` green, merged.
