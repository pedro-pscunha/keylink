# Wave 1 — one failure vocabulary, both sides

Plan for `docs/goals/2026-08-09-one-failure-vocabulary.md`, revision 2, written
against `unify-surfaces` at `b1f4422`.

Revision 1 went through a three-lens plan gate and came back with **7 Critical
and 16 Major**. This revision is a rewrite, not a patch. §11 records what
changed and why, so nobody re-derives the refuted version.

Read the goal's appended corrections first. Five of ITS claims are false against
the tree.

## 1. What this is

Seven conditions are one fact on both sides of keylink. Locally each is a typed
variant; remotely each is a **string prefix inside `Failed(String)`**. Prefixes
are associated consts, which `postcard_schema` does not hash
(`reply.rs:119-124`), so the handshake cannot protect them. A drifted peer meets
a moved prefix as a `Failed` it silently stops recognising.

Give those conditions one Rust object carried by both sides and encoded on the
wire, and delete the prefix machinery.

**It is not a small deletion.** The crate loses five consts and three tests.
Against that, all of these have to move: **20 test consumer sites** (enumerated
in §7, and the only figure here I assert exactly), **on the order of 122
intra-doc link occurrences**, **five RFC files**, five README lines, and a
break-record registry.

The wave is dominated by the consumers of the channel being deleted, not by the
channel. Every figure above except the 20 is a **measurement at `b1f4422` for
sizing**, not a claim to verify against — the verifications are `mise run docs`,
`mise run anchors` and the suite.

## 2. Six rulings

### 2.1 `Withheld` is NOT `#[non_exhaustive]` — and the reason is the caller test, not the wire

Revision 1 argued this from `console.rs:394-404` ("a new arm moves the ALPN, so
an old console is refused at the handshake"). **That argument is void.** The
sentence opens with *"Either way"*: it holds for both choices and discriminates
nothing.

The rule's real discriminator is `console.rs:396-400` — does every caller handle
every variant the same way, or must a caller be **made** to look at a new one.
`FollowRefused` is sealed because "every caller handles all of them the same two
ways". `EndReason` is open because "a console written against five endings must
be *made* to look at a sixth".

`Withheld` is plainly the second kind. `Restarting` means retry, `Gone` means
stop calling, `Refused` means an observer vetoed. `turns.rs:3162-3165` and
`observe.rs:640-645` already say this about the types being collapsed — the
latter reading *"Sealing it would let a downstream `_` swallow a fifth answer
exactly the way `Gone` swallowed this fourth one — the defect, not the fix."*

**Ruling: no `#[non_exhaustive]` on `Withheld`.** `RemoteError` **keeps** it, on
the other half of the same rule: it is handled, not exhausted.

Two things the RFC append must carry, or the recorded rule becomes a hazard:

- `#[non_exhaustive]` governs **source** compatibility for a downstream crate
  that recompiles. The ALPN governs **runtime wire** compatibility. Neither
  substitutes for the other.
- The ALPN removes the wire hazard for **both** choices, exactly as
  `console.rs:394-404` says. It is not evidence for either.

Written the revision-1 way, the next author applies "on the wire, therefore
open" and strips `#[non_exhaustive]` from `RemoteError`, which is also on the
wire and must stay sealed.

### 2.2 `Withheld` lives in `observe.rs`, with `EndReason` and `TurnLost`

`observe.rs` becomes the failure vocabulary. Three modules must reach `Withheld`
— `observe.rs` (`TurnLost`), `turns.rs` (`Unanswered`), `reply.rs`
(`RemoteError`) — and it joins the `pub use` list in `lib.rs` beside
`EndReason` (`:253`), `TurnLost` (`:322`) and `NotQueued`/`Undelivered`
(`:334-335`).

`EndReason` moves here too (goal item 11), and the move is **wire-free**: type
names and module paths are not hashed. The move is one retarget of the re-export
at `lib.rs:252-255`, because `tests/inspect.rs:22-26` imports from the crate
root. **Move only — rename nothing.** `EndReason`'s 6 variant names and 8 field
names ARE hashed.

The move makes `observe.rs` wire-aware for the first time: its use block
(`observe.rs:75-81`) names no serde and no `postcard_schema`. State it — a
module acquiring a wire dependency is a design fact, not an import.

**Publication hazard:** `CONTRIBUTING.md:234-240` — making a type `pub` promotes
its whole rustdoc, so any link inherited from the private `Withheld`
(`turns.rs:866`) that pointed at a private neighbour becomes an error at once.

### 2.3 Four conditions are reachable from both sides. Not six.

Revision 1 claimed six. Measured, four have a producer on each side:

| Variant | Local | Served | Both |
|---|---|---|---|
| `Refused` | `turns.rs:3951` | `turns.rs:4554` | **yes** |
| `ObserverPanicked` | `turns.rs:3952` | `turns.rs:4562` | **yes** |
| `Dropped` | `turns.rs:4018` | `turns.rs:4639` | **yes** |
| `Restarting` | `turns.rs:4019` | `turns.rs:4618` | **yes** |
| `Panicked` | none | `turns.rs:4547` | served only |
| `Gone` | `turns.rs:3907` | see below | local, then both |
| `Expired` | none | none | neither |

- **`Panicked` is served-only, deliberately.** `ask_catching` yields
  `TurnLost::Faulted(Panic)` (`turns.rs:3950`), and §3.1 keeps `Faulted` outside
  `Turn(..)`. That is not an oversight: a panic that unwinds a local caller is a
  live `Panic` with a payload; a panic relayed to a peer is a `String`. They are
  different objects. **`Faulted` does NOT also produce a `Withheld::Panicked`.**
- **`Gone` gains a served producer.** `lost()`'s `Standing::Gone` arm
  (`turns.rs:4644`) becomes `Withheld::Gone`. This is what the goal's "`Gone` and
  `NoSuchActor` stay distinct" invariant requires: the singleton path then says
  *this ran and stopped*, the registry path keeps *never existed*.
  **It overturns a second written argument the goal never named** —
  `turns.rs:4643` reads *"NOT a `Failed`: this is the one of the three where
  there is nothing here, stop asking is true."* Rewrite that comment in the same
  commit; a comment left contradicting the code beneath it is silent rot.
- **`Expired` has no producer on either side.** Locally documented unreachable
  (`turns.rs:3265-3271`); on the served side `rendered` maps `Answered::Expired`
  to `RemoteError::TimedOut { millis }` (`turns.rs:4566-4570`), which carries a
  duration `Withheld::Expired` cannot. **Keep the variant, exempt it by name**,
  on the ground `turns.rs:3265-3271` already uses: written to be correct if
  something reaches it. `rendered`'s arm stays `TimedOut`.

**The success criterion asserts FOUR**, plus `Panicked` served-side only.

### 2.4 Re-entrancy keeps its `Failed`, carved out by name

Revision 1 contradicted itself: §3.1 ruled `Reentrant` stays outside `Withheld`
"principled rather than incidental", and §3.3 then routed `turns.rs:4775` to a
`Withheld` variant that ruling refuses to create.

§3.1's principle stands — `Reentrant` is a caller fault, not a turn outcome.
Inventing `RemoteError::Reentrant` would put a fourth spelling of one fact on
the wire. `turns.rs:4744-4772` already argues the arm is unreachable from a peer
and untested.

**Ruling: `turns.rs:4775` keeps its `Failed`.** The invariant becomes:
**`Failed(String)` means a caller's own `Middleware`, or the one carved-out
re-entrancy arm — and nothing else.**

### 2.5 `Unanswered` loses `Copy`, and loses fidelity too

`Undelivered` is `Copy` today (`turns.rs:3166`). `Copy` is precisely why
`TurnLost::Restarting(Arc<Ended>)` discards its reason (`turns.rs:3909-3912`) —
the defect that left a local caller worse-informed than a peer. Carrying the
reason means carrying data, so `Copy` cannot survive, and neither can
`&'static str` for the two `name` fields.

**Revision 1 stopped there and was wrong to.** The collapse also *loses*
information the local caller has today: `TurnLost::Restarting(Arc<Ended>)` hands
over the whole `Ended`, and `EndReason` flattens both `Arc<dyn Error>` arms to
`String` (`console.rs:239-251`). The `source()` chain goes.

**Ruling: accept the loss in the shared type; point at the unflattened object.**
`Handle::ended()` (`macros.rs:2378`) still returns the real `Ended`, so the error
chain stays reachable locally — it stops riding the failure value, which is the
right place for it to stop. Record it in the RFC append.

Two costs, stated:

- Every restart now builds an `EndReason`, running the full `Display` chain of a
  `dyn Error` and allocating up to two `String`s, where today it is one refcount
  bump. Refusals and restarts are already the slow path.
- Do **not** reach for `Cow<'static, str>`: no `Schema` impl, and buying one
  costs more than the allocation it saves.

`TurnLost` gains nothing: `Faulted(Panic)` keeps it `Debug + thiserror::Error`
only, because `Panic` (`panic.rs:38`) has no derives at all and holds
`Box<dyn Any + Send>`. This is why the success criterion compares `Withheld`
rather than `TurnLost`.

### 2.6 The docs carve-out is widened, not reinterpreted

Twelve RFC lines sit inside break-record rows and quote **search literals**.
`CONTRIBUTING.md:337-344` scopes the carve-out to *"the test it runs, the file it
edits, the script that drives it"* — a search literal is none of those three, so
editing them in place fails the docs success criterion under a literal reading.

**Ruling: widen the carve-out in `CONTRIBUTING.md`, with the reason stated.** The
carve-out exists to keep a record runnable, and a stale search literal is exactly
what makes one unrunnable. Do not argue a search literal was a pointer all along.
`CONTRIBUTING.md` is in-radius per the goal.

## 3. The shape

```rust
// keylink/src/observe.rs — the failure vocabulary
pub enum EndReason { /* unchanged, 6 variants, moved from console.rs:184 */ }

/// One object, both sides, and on the wire.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Schema, thiserror::Error)]
pub enum Withheld {                       // NO #[non_exhaustive] — see §2.1
    Panicked         { message: String },  // the HANDLER; served-side only
    Refused          { name: String },
    ObserverPanicked { name: String },
    Dropped,
    Restarting       { reason: EndReason },
    Gone,
    Expired,                               // representable, unreachable, exempt
}

pub enum TurnLost   { Turn(Withheld), Faulted(Panic), Reentrant }
pub enum Unanswered { Turn(Withheld), Reentrant }   // was `Undelivered`
```

`Reentrant` stays **outside** `Withheld` in both: every `Withheld` variant
describes what happened to a turn; `Reentrant` describes what was wrong with the
*caller* — the turn never existed.

### `RemoteError` after the wave

```rust
#[non_exhaustive]                       // stays — handled, not exhausted
pub enum RemoteError {
    Unauthorized(String),
    TimedOut { millis: u64 },
    Idempotency(IdempotencyRefusal),
    NoSuchActor { id: String },
    Failed(String),                     // Middleware, or the carved-out reentrancy arm
    Withheld(Withheld),                 // NEW — APPEND, do not insert
    AtCapacity { capacity: u64 },       // NEW — u64, never usize
    BadActorId { id: String },          // NEW — a request that addressed nothing
    FollowRefused(FollowRefused),       // NEW — stop flattening Ahead/Crowded
}
```

**Append; never insert.** Both move the hash equally, but only insertion
renumbers later variants and risks the misdecode `reply.rs:48-53` describes.

`Failed` producers that stop: `remote.rs:353` (`AtCapacity` — note it is spelled
`Self::Failed(`), `remote.rs:434` (`BadActorId`), `console.rs:1031`
(`FollowRefused`), and the five prefixed producers. `turns.rs:4775` stays, per
§2.4.

## 4. The work, in commits

`lint-fast` is `cargo clippy -p keylink --lib` (`mise.toml:258`) — **library
target only**. A green `pre-commit` is NOT proof a commit compiles. Every commit
below must be verified with `cargo check --workspace --all-targets` **and**
`mise run docs` before it lands.

**Commits 1–4 are NOT parallelisable.** `turns.rs` carries three of them,
`lib.rs` three, `macros.rs` and `actor/remote.rs` two each, `tests/observe.rs`
two. Only the pair (1, 2) is disjoint. They are mechanical renames; serial costs
minutes and parallel buys conflicts in the largest file in the crate. **Run
1→2→3→4 serially.**

| # | Commit | ALPN pin | anchors registry | rustdoc links |
|---|---|---|---|---|
| 1 | private `Withheld` → `ObserverVerdict` | no | **row `O1`** | few |
| 2 | `EndReason` → `observe.rs` | no | no | 5 |
| 3 | `Undelivered` → `Unanswered` | no | **row `E4`** | **75** |
| 4 | rename `deliver_to_local` | no | no | 7 |
| 5 | **the type change** (see below) | **BOTH** | rows `E1,E2,E3,D11,O4` | 26 |
| 6 | delete the prefix machinery | no | rows `O2` + records | 14 |
| 7 | RFC/README/CONTRIBUTING appends | no | no | no |

**Commit 1 is not a pure rename.** It re-quotes AND re-watches the record at
`tests/observe.rs:446,448` (row `anchors:1720`). `anchors` is step 2 of
`pre-commit`, so the commit cannot land while row and record disagree. Count the
new literal's uniqueness **after `cargo fmt`**.

**Commit 5 is the big one, and it is deliberately not split.** It carries: public
`Withheld`; the `TurnLost`/`Unanswered` collapse; the `RemoteError` reshape; the
`NodeStatus::lost` → `dropped` field rename; the twin-guard replacement (§5); and
every test site that stops compiling. Revision 1 split these across commits 5, 7
and 8 and left the tree red for three commits — which `pre-commit` cannot see.
An atomic commit leaves the tree green.

Both pins move here. Read both new literals **off one failing run** of:

```
cargo test -p keylink --test protocol contract_hash_has_not_moved
```

Verified: both test names contain that substring (`protocol.rs:1200`,
`protocol.rs:1304`), so one positional filter reaches both. Use `cargo test`, not
a nextest `-E 'test(...)'` filterset — a filterset matching nothing exits
non-zero having run zero tests, which reads exactly like a caught defect. Never
compute a hash.

## 5. The twin guard — six properties, not four

The old guard (`turns.rs:5101`) asserts six things over two flat enums. Its
premise dies with the collapse; `slot()`, `EVERY` and `twin()`
(`turns.rs:4909-4946`) are invalidated together. `const EVERY: [Undelivered; 7]`
(`turns.rs:4908`) cannot even stay a `const` once variants carry `String`.

**Do not delete it and call the type system a replacement.** The type system
gives non-drift on the shared arm. It does not give the rest:

1. `Unanswered::Reentrant` and `TurnLost::Reentrant` render the same sentence —
   two hand-written `#[error]` strings.
2. `NotQueued`'s two variants render the same sentences as their `Unanswered`
   counterparts, through the production `From` impl.
3. No two sentences collide **across the whole `Unanswered` surface** — not just
   within `Withheld`. A copy-paste giving `Withheld::Gone` the re-entrancy
   sentence passes every narrower check.
4. No `#[error]` text is empty — refuses a vacuous pass.
5. **Every `Withheld` variant has a named producer**, `Expired` exempted by name
   per §2.3. The type system gives reachability nowhere, which is exactly how
   `Expired` reached zero producers unnoticed.
6. The `Faulted` exception is exactly one variant wide.

Watch the replacement red-then-green with a break record. That record moves
`RECORD_BLOCKS` (`anchors:3239`, currently 177) — **read the new value off the
failing run, never compute it.**

## 6. What is measured, and by what

| Claim | Proof |
|---|---|
| One object both sides | `a_local_caller_and_a_peer_read_the_same_withheld`, asserting **value equality of two `Withheld`s** for the **four** conditions of §2.3, plus `Panicked` served-side only |
| The consts are gone | `rg 'PANICKED\|OBSERVER_PANICKED\|RESTARTING\|DROPPED\|REFUSED' keylink/src` — scoped to `keylink/src`, NOT `reply.rs`: the five exist **twice**, at `reply.rs:154/161/181/189/206` and as aliases at `turns.rs:4501-4505`. Expect residue at `admission.rs:77` (`const REFUSED: u32 = 1;`, unrelated — do not touch) |
| Every former producer is typed | enumerate `turns.rs:4547,4554,4562,4618,4639` and assert each by name |
| `Failed` has no in-crate producer | `rg 'RemoteError::Failed\(\|Self::Failed\(' keylink/src` returns only the carved-out `turns.rs:4775` and any break-record doc line. **There is no `Middleware` producer to expect** — it lives in user code (`rpc.rs:1438`) |
| The hash moved once | both pins updated in commit 5 from one failing run, release note written naming **both** tests — `CONTRIBUTING.md:197-210` names only the first |
| Nothing regressed | `mise run gate` (18 steps), `cargo nextest run --workspace`, `mise run anchors`, `mise run replay-anchors`, `mise run docs` under `-D warnings` |

**Already discharged at `b1f4422`:** the audit is tracked under
`docs/audits/2026-08-09-local-vs-remote/` (7 files). Do not re-do it.

**Not at risk:** all 9 `REDNESS` rows (`L3 L4 L5 A1 A2 A2b A3 A4 A5`) are
untouched, so `replay-anchors` is safe. Stated so nobody re-checks it.

## 7. The consumers — the actual bulk of this wave

**Test sites: 20.** Three use the const and break at compile time
(`defer.rs:1199`, `singleton.rs:1117`, `:1121`). Nine hard-code the literal and
fail at run time (`defer.rs:718,920`, `supervision.rs:1155,1166,1380,1503,1514`,
`actors.rs:735`, `singleton.rs:992`). Four compare whole rendered strings
(`singleton.rs:1018,1113,1267`, `keylink/tests/persist.rs:507` — **the keylink
crate, not `keylink-persist`**). Four are match arms and table drivers one line
above the ones already listed (`supervision.rs:1130,1165,1467,1513`).

**Broken by the new `RemoteError` variants, not by the prefixes:**
`actor/remote.rs:726-737` (`the_capacity_refusal_reads_the_same_by_hand_and_lazily`
— decide whether `AtCapacity`'s `#[error]` keeps the sentence *"the actor system
is at its capacity of 10000 live actors"*), `tests/actors.rs:836`
(`contains("capacity")`), `tests/actors.rs:468` and `:3144`
(`contains("actor id")`). Also `representability.rs:105,855`, which is
`#[cfg(test)]` so `cargo check` alone will not report it.

**Intra-doc links: ~122 OCCURRENCES, ~75 doc LINES, measured at `b1f4422`.**
State the unit, because the two figures differ and both are true: a doc line can
carry several links. Independent counts at `b1f4422` were 75 occurrences versus
61 lines for `Undelivered`, and 7 for `deliver_to_local` on both methods.
Breakdown by occurrence: `Undelivered` 75, the `TurnLost` collapse 26, the consts
14, `deliver_to_local` 7; `EndReason` adds 5. By file: `turns.rs` 78,
`macros.rs` 21, `observe.rs` 7, `reply.rs` 5, `parking.rs` 3, `persist.rs` 2, and
one each in `blocking.rs`, `compose/names.rs`, `compose/pool.rs`,
`actor/remote.rs`, `admission.rs`, `error.rs`.

**Do not carry these forward as claims.** They size the work; they do not verify
it. The verification is `mise run docs` exiting 0, which is the only gate that
sees a broken link at all. Budget for
`rustdoc::redundant_explicit_links`, which is on and not allowed: a mechanical
rename preserving `[`X`](crate::X)` trips it where the shorthand now suffices.

**Three tests die, not two:** `no_failure_prefix_is_a_prefix_of_another`
(`turns.rs:4855`), `no_other_served_failure_begins_with_one_of_the_five`
(`actor/remote.rs:776`), `the_five_prefixes_are_the_text_a_peer_matches_on`
(`reply.rs:332-337`). Two break records die with them (`reply.rs:310-330`,
`turns.rs:4834-4853`).

**`README.md:143,184,185,187,194.** Line 187 states the wire rendering this wave
deletes and needs a rewrite, not a rename. `CONTRIBUTING.md` carries one affected
line (`:282`) that the collapse does not change.

**`NodeStatus::lost` → `dropped` is exactly three code sites**: `console.rs:317`
(declaration), `console.rs:860` (construction), `tests/inspect.rs:286`. Eight
must NOT move, including the byte-identical `EventPage::lost` at
`console.rs:352`, 35 lines away. **Forbid a file-wide substitution in
`console.rs`.** Three doc passages carry the old wording and no compiler reads
them: `console.rs:95-96`, `:101`, `:814`. `lost` is also a free function
(`turns.rs:4616`), an `Events` field and method (`console.rs:507,558`), a
parameter (`turns.rs:4938`) and a local (`turns.rs:5102,5424`,
`console.rs:818`) — **item 7 renames the `NodeStatus` field and nothing else.**

## 8. Break records

**Carry the identifier list, never a total.** Two passes counted 32 and 42, and
at least one false positive is confirmed: `D6`/`D9` (`cycles.rs:479-567`) is an
A→B→A deadlock record matching that file's own unrelated
`const REFUSED: u64 = u64::MAX;` (`cycles.rs:136`). The number comes off
`mise run anchors`.

**Registered rows that break loudly**, with the commit that rots each:
`O1` (commit 1), `E4` (commit 3), `D11` and `O4` (commit 5), `E1`, `E2`, `E3`
(commit 5), `O2` (commit 6).

**Re-watch, because the collapse destroys the break:** `E1`/`E2`, `E3`, `E4`,
the twin guard record (`turns.rs:4951-5099`, highest cost in the tree),
`turns.rs:6080-6120`, and the five that break **on** the consts
(`reply.rs:310-330`, `turns.rs:4834-4853`, `turns.rs:4867-4893`,
`defer.rs:1156-1179`, `actor/remote.rs:739-774`) — deleting the consts deletes
those breaks outright; they are retired, not re-quoted.

**Re-quote only:** `O1` (pure rename; the arm, substitution and watched output
all survive). `D2`, `D3`, `D10`, `D11`, `G13` and `persist.rs:892-910` are
re-watch-lite — re-run to **read** the new word, never guess it.

**Excluded false positives, so nobody re-adds them:** `turns.rs:6268-6326`,
`:6471-6491`, `:6593-6629`, `tests/feeds.rs:352-368` (all `Ended::Panicked`);
`tests/persist.rs:363-386` (`TurnOutcome::Panicked`); `tests/cycles.rs:479-567`
(that file's own `REFUSED`); `scripts/hook-guard.sh` (the word in test names).

**Three traps:**

1. A row and its record must move in **one commit**. Row alone gives `literal is
   NOT QUOTED by its own record`; record alone gives an unresolvable anchor.
   `CONTRIBUTING.md:317-323` forbids deleting a row to reach green.
2. `break-once.sh:219` — `── selected=1` is the only reading a record supports.
   `0` means nothing ran. **`?` means no totals line: an abort or a tree that did
   not compile, which `:204-206` warns is NOT a zero.** A rename leaving a
   substitution non-compiling produces `?`, which reads like a caught defect.
3. Count anchor uniqueness **after `cargo fmt`** — the formatter owns the line
   breaks a record quotes.

**`EndReason` appears in zero break records.** The move rots nothing.

## 9. Docs

**Five RFC files are affected. The line count depends on the identifier set and
is not a claim to carry.** Two independent measurements at `b1f4422`: 80 lines
on a narrow set (`rfc-07` 61, `rfc-06-observers` 8, `rfc-04` 7, `rfc-08` 3,
`rfc-06-implementation-brief` 1) and 105 lines on a set that also matched the
prefix TEXTS (`rfc-07` 67, `rfc-06-observers` 26, `rfc-04` 5,
`rfc-06-implementation-brief` 4, `rfc-08` 3). Both are correct about what they
counted. **What matters is the file list, which agrees at five** — revision 1
named four appends and missed three files.

**Append-only means the ~70 ordinary prose lines are NOT edited.** One
rename-ledger append per affected RFC file, saying the names moved and at which
commit. An implementer reading "80 lines affected" and rewriting history is the
failure this paragraph exists to prevent.

**Twelve lines sit inside break-record rows** (`rfc-04:3706`; `rfc-07:1090`,
`:1104`, `:1112`, `:1113`, `:1122`, `:1123`, `:2900`, `:3746`, `:3754`) and are
edited in place **only after** the carve-out is widened per §2.6.

**Counts and watched output are appended, never edited**: `rfc-07:4345-4351`,
`:4479-4493`, `:4511`, `:4530`, `:4554`, `:4580-4587`, `rfc-08:886`.

**Leave alone:** `rfc-01-blobs.md:732,742,758,794,797,808,809` — blob-fetch
console transcripts that merely contain the word.

**The appends commit 7 must write:** RFC-04 §5.5 re-argued (its *"A local
mailbox has exactly one failure"* reason is false at seven and names a type that
no longer exists); the `#[non_exhaustive]` ruling per §2.1 **with the
source-versus-wire distinction**; the `Gone`/`NoSuchActor` cost per the goal's
invariant, plus the second overturned argument at `turns.rs:4643`; the
`EndReason` widening from one service's graph to every service's; and the
local-fidelity loss per §2.5.

## 10. wasm

Four of the eighteen gate steps are `wasm`, `wasm-guards`, `wasm-web`,
`wasm-browser`. The dimension that matters is **`--no-default-features`
compilation of `keylink` AND `keylink-persist`**. `mod console` (`lib.rs:212`)
and `mod observe` (`lib.rs:219`) are unconditional, so the `EndReason` move stays
inside the wasm build with no `#[cfg]` change — verified, not assumed. None of
the forbidden APIs (`tokio::select!`, `std::time::Instant`,
`tokio::task_local!`) is involved. Stated so a red step 13 is read as a link
problem rather than a design one.

**`keylink-persist/` and `demo/` name ZERO affected identifiers.** The `Copy`
removal forces no downstream edit, and the goal's blast radius over-declares.

## 11. What changed from revision 1

Refuted and replaced: the `#[non_exhaustive]` argument (§2.1); "six conditions
reachable from both", now four (§2.3); the re-entrancy contradiction (§2.4); the
claim that the collapse only adds data (§2.5); the parallel-execution
instruction (§4); the commit split that left the tree red for three commits
(§4); "two guards", now three tests (§7); 16 test sites, now 20 (§7); four RFC
appends, now five files (§9). Added entirely: rustdoc's 122 links, the README,
`RECORD_BLOCKS`, wasm, the break-record schedule, and the ruling that a
`Self::Failed(` spelling made the old success criterion blind to a site it was
written to prove had changed.

## 12. Refused, so nobody re-derives them

- **Distinguishing "factory refused" from "no factory".** Descoped to goal 3 —
  the two converge at the `return` (`registry.rs:778` vs `:813`) and `Factory`
  refuses through `Option::None` (`registry.rs:533`).
- **Making `TurnLost` `Clone`/`Eq`.** `Faulted(Panic)` forbids it.
- **`Cow<'static, str>` to dodge the allocation.** No `Schema` impl.
- **Routing local calls through a fake `Dispatch`.** `middleware.rs:389-396`
  argues against it — through the `Context` argument, not through `Dispatch` by
  name.
- **Touching what a panicking observer does.** Defended in writing, re-confirmed
  this week.
