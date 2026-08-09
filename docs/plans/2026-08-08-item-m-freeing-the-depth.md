# Item M — freeing the local mailbox depth

Design of record. The measurement in §1 is the reason this item exists in this
shape, and it refutes the sentence that has held the constant at 1.

## 1. The measurement, which overturns the stated justification

`local.rs:239-241` claims a deeper local mailbox "turns a documented
always-deadlock into an intermittent one. An intermittent deadlock is not a
slower program; it is a different program in production than in the tests."

**Tested directly.** Control first, required green: `cargo nextest run --test
cycles`, 8 of 8 passed in 3.020s. Then `break-once.sh` substituting
`const LOCAL_MAILBOX_DEPTH: usize = 1;` with `= 32;` — thirty-two and not two, so
that "the mailbox happens to be full" is unreachable in these tests.

**Result: 6 passed, 2 failed. Every deadlock guard still fired at depth 32.**

The two failures are the only two assertions that are *about* depth-1 permit
arithmetic: `a_running_turn_leaves_exactly_one_free_permit_behind_it`, and the
"A's mailbox is full" line of `a_two_actor_cycle_hangs_and_wedges_both_actors`.
That second one settles the question — assertions run in order, and it panicked
at the later line, so **the ring still hung** at the earlier one, with no
contention anywhere.

**The hazard is on the ANSWER, not the permit.** `macros.rs:1756-1788` names four
self-call cases. `rpc`, `stop` and `with_state` all send into the free slot and
then wait forever on an answer only the blocked turn could produce; depth moves
where the hang happens and nothing else. All three are deterministic at every
depth, and the depth-32 run confirms two of them directly.

**And the one load-dependent case runs the other way.** `tell` is *already*
intermittent at depth 1 — the crate's own words at `macros.rs:1767-1772`: it
"succeeds *only if nobody else took the permit first*", and "a served actor
always has other senders". A deeper mailbox makes that block **less** often. So
depth 1 is not the deterministic choice for the one case that varies; it is the
choice that maximises the chance of the block.

`cycles.rs:18-22` already states the honest, much smaller claim: "any depth >= 1
is silent for one queued message... What 1 buys is that **exactly one** message
fits."

**So `local.rs:222-250` is rewritten, not relaxed.** It states a hazard the tree
measures as absent, and it states the direction of the one real hazard backwards.

## 2. The rule, in two halves

A first draft of this design said: refuse a send that would BLOCK and targets an
actor on the current call chain. **That is wrong, and it catches only `tell`.**

- **`ask`, `ask_catching`, `read`, `stop` to a chain member: refuse ALWAYS**,
  whether or not the send would block. The hang is on the answer, so a free slot
  does not save it.
- **`tell` to a chain member: refuse only when the send would BLOCK.** A
  self-`tell` into a free slot is legitimate and safe — the message runs after
  the turn returns. Refusing it would delete a working pattern.

## 3. Identity, chain, propagation, check

**Identity.** Local actors have none: `LocalActor` holds `mail`, `ended`, `fate`,
`busy`, `task` and nothing else. Add a private `ActorId(NonZeroU64)` minted from
a `static AtomicU64` with `fetch_add(.., Relaxed)` — `Relaxed` because the
counter orders nothing, matching `Watched::mark_busy`. `NonZeroU64` so that
`Option<ActorId>` stays 8 bytes. Cost per turn: zero.

**The chain is a `thread_local!`, not a task-local**, and the reason binds:
`tokio::task_local!` sits behind tokio's `rt` feature, which this crate enables
on no target. `parking.rs:66-71` records exactly this and `parking.rs:142-150` is
the template — a `Cell`, exchanged in and out by `poll`, restored by a `Drop`
guard so an unwinding poll cannot leave one turn's state visible to the next.

Three variants, and the reason is allocation: `Empty`, `One(ActorId)`, and
`Deep(Arc<[ActorId]>)`. Every top-level turn installs `One`, which allocates
nothing. `Deep` allocates once, only for a turn reached from inside another turn,
and an `Arc<[..]>` clones without allocating.

**Propagation travels in the message, because a thread-local cannot cross a
task.** `Origin` is the vehicle: `#[doc(hidden)] pub`, `pub(crate)` constructors,
no public one, so a new field is invisible downstream. **It carries no
`Serialize` and is not a wire type, so neither ALPN hash moves.** Built at four
live sites; the two peer origins pass an empty chain, because a connection task
is not a turn.

`run_loop` takes the id as a new private parameter, so **no macro changes**.
`run_keyed` and `run_keyed_supervised` mint their own and keep their signatures.

**The check goes in `LocalActor::send`**, documented as "the one place a message
enters the mailbox", with five callers. One site covers every door. The fast path
is one thread-local read that also yields the ancestors for the outgoing
`Origin`, so a send pays for one read rather than two. Off-turn, the read is a
unit discriminant and the check is a match arm returning false.

## 4. The error, and why unrepresentable is refused

The bad state is "a handle to actor X is reachable from inside X's turn".
Identity here is per *instance*, not per type: `cycles.rs:210-223` builds two
actors of one type, and `macros.rs:1783-1784` already says "no type can tell one
actor's handle from another's". So the type-state lens cannot reach it and the
failure must be reported.

**`MailboxClosed` must not be stretched to carry it.** It means gone; a refused
re-entrant call leaves the actor alive, and reporting it would be the same lie
`local.rs:3080-3086` names — reporting a live actor as gone through a public API,
silently.

A panic was considered and rejected, and the trade is worth keeping visible: it
would cost zero API change, `guarded` would catch it and `ask` would resume it in
the caller. It was rejected because `tell` into a full mailbox is a **load**
condition rather than a bug, and one mechanism cannot honestly carry both facts.
**If a zero-migration version is ever wanted, this is the trade to reopen.**

## 5. The knob

`DEFAULT_LOCAL_MAILBOX_DEPTH: NonZeroUsize = NonZeroUsize::MIN`, matching
`SystemBuilder::mailbox_depth`'s type exactly — and `representability.rs:1061`
pins that signature as the probe refusing an unbounded mailbox, so a second knob
needs its own twin probe, watched failing.

**Default stays 1, for three reasons.** The backpressure argument at
`local.rs:216-220` survives this design untouched: a caller cannot get more than
one message ahead, so a caller measuring its own latency sees the actor's. Five
sites pin depth-1 behaviour today. And a caller who wants more pays nothing while
idle — `actor_memory.rs:33-56` measures an idle actor at 1,424 bytes at depths 1,
32 and 1,024, identical.

Carry `registry.rs:1974-1981`'s warning to the local door verbatim: raising the
depth raises the shutdown budget owed, by up to N turns, and nothing can check
that for you.

## 6. The limits, stated rather than discovered

**No case converts a deterministic deadlock into an intermittent one.** Ten were
enumerated. The one new intermittency is a `tell` refused because the mailbox is
full — which replaces an intermittent **hang** with an intermittent **error** a
caller can see.

Uncaught, all deterministic: a ring closed through `tokio::spawn` or another
thread; a ring closed through a non-actor primitive such as a `oneshot`; a ring
through a keyed or served hop (catchable in a later wave, still moving no wire
type); a `Topic` cycle under `BlockPublisher`, which is not a `LocalActor` at
all; and `ended()` awaited from inside its own turn, which is not a send and has
no error position.

**The `spawn` limit is load-bearing and cannot be fixed.** The chain follows
messages, so a spawned task starts empty. That is precisely what makes the
crate's documented park-and-spawn remedy work. You cannot have both.

## 7. Open decisions

1. `stop()` returns `()` today and must gain a `Result`, or the refusal has
   nowhere to go and becomes a silent no-op — the fallback this crate refuses
   everywhere else. This is the largest migration item.
2. The new error's name is a placeholder.
3. The ~40-byte per-actor estimate is an **estimate**. `actor_memory.rs` has 275
   bytes of slack and must be re-run before the figure is written down.
4. `.mise/tasks/anchors` registers the literal `const LOCAL_MAILBOX_DEPTH: usize
   = 1;`. Changing that line reddens the gate before anything compiles. Re-quote
   the row and the record, and read the new count off the failing run.
