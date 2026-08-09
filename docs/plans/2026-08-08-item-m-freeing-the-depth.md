# Item M — freeing the local mailbox depth

Design of record. The measurement in §1 is the reason this item exists in this
shape, and it refutes the sentence that has held the constant at 1.

## Review status: PROCEED-AMENDED

Adversarially reviewed at `f74a508` on a separate worktree, with §1's experiment
re-run by the reviewer rather than taken on trust. **It reproduced.** The verdict
is PROCEED-AMENDED, and every amendment is applied below.

**Two of them were CRITICAL, and both were internal contradictions rather than
objections to the goal.** The first draft said the chain would ride on `Origin`
AND that no macro would change — and those two cannot both hold, because `Origin`
is consumed into the opaque `M` before a `Message` exists, so `run_loop` never
sees one. That version would have detected self-calls and **silently missed
`A → B → A`**, which is the exact hazard §1 measures. The second said one check
in `LocalActor::send` covers both halves of §2's rule, but the refuse-always
caller and the refuse-if-blocking caller hand `send` byte-identical arguments.

**Both fixes make the design smaller.** The chain moves onto the private
`Message` enum, which deletes the whole `Origin` paragraph and every ALPN
question with it. `send` gains one private-enum parameter, which is also the
Level-1 encoding the design was missing.

One Major was a silent hole: **`Lent` is a fourth `Inbox` impl and it is the
PRODUCTION supervised path**, so the `None` default would have disabled the check
for every supervised actor with no test failing.

One Major was a false count: **"five sites pin depth-1" counted prose, not
behaviour**, and the `defer`/`persist` timing argument did not survive its own
experiment. §5 now carries the measured figures.

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
at `cycles.rs:383` (`A's mailbox is full: B's call took the one permit A
released`), while the ring assertion sits earlier at `cycles.rs:367-370`
(`A -> B -> A must hang`). So **the ring still hung** at depth 32, with no
contention anywhere.

**Reproduced independently at `f74a508` by a second agent**, on a separate
worktree: control green 8 of 8 in 3.024s, depth 32 giving 6 passed and 2 failed
with `── exit=100`, the same two test names, and the same two panic lines. Every
other deadlock guard also passed at depth 32 —
`a_read_from_inside_a_turn_is_a_self_deadlock`,
`publishing_into_your_own_topic_hangs_under_block_publisher`, and
`a_parked_reply_that_stays_in_the_turn_does_not_break_the_cycle`.

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

**Identity costs zero new fields, and it is the `Arc` and not its address.**
`LocalActor` holds `mail`, `ended`, `fate`, `busy`, `task` and nothing else.
`spawn_fed` makes one `Arc<AtomicBool>` at `local.rs:2837`, clones it at `:2838`,
and hands one to `run_loop` inside `Watched`; `Clone` shares it at `:3466`. So
that `Arc` already identifies the actor, uniquely and across every clone.

**A first version of this used `Arc::as_ptr` as a `usize`, and that is a false
positive waiting to happen.** The argument for it was: the chain only holds
actors whose turns are on the stack, and such an actor's loop is alive, so the
allocation cannot be freed. **That is true of the innermost identity and false of
the ancestors**, which travel in the message and name actors that were alive when
it was sent. The reachable sequence: A calls B; A is aborted and every handle
dropped, freeing the allocation; C spawns onto the same address; B calls C, and C
is refused as if it were A. A live, unrelated actor is refused — worse than a
missed detection, because it breaks a working program with a named error.

Address reuse here is the default rather than a corner: a probe allocating an
`Arc<AtomicBool>`, recording the pointer, dropping and reallocating hit the same
address **1000 times out of 1000**. (The reachability of the abort inside the
window was reasoned, not measured. It does not change the decision, because the
fix is free.)

**So `Chain` holds `Arc<AtomicBool>` clones and compares with `Arc::ptr_eq`.** A
strong reference keeps the allocation alive for exactly the window the chain uses
it, so the address cannot be reused inside it. An `Arc` is a non-null pointer, so
the entry is the same 8 bytes a `usize` costs and `Chain` keeps its niche. One
atomic increment per chain entry, paid only on the nested path.

Two guards, both required. **A private newtype from the start** —
`struct Actor(Arc<AtomicBool>)`, private to `local.rs`, never public and never
`#[doc(hidden)] pub`; `local.rs:294-297` is the scar this avoids. And **chain
membership and nothing else**, written where the type is defined: it is not a
handle, not a key, and must never enter a map that outlives a turn.

**Reaching it from `run_loop`**, which is generic over `I: Inbox<A, M>` where
`Keyed` and the bare receiver have no busy cell: add
`fn identity(&self) -> Option<Actor> { None }` to the private `Inbox` trait.
`Watched` returns its `busy`; the bare receiver keeps the default; a keyed
residual is stated rather than hidden. This is `Inbox::mark_busy`'s idiom
verbatim (`local.rs:1364-1373`, "Written by whoever has somewhere to put it").

**There are FOUR `Inbox` impls, not three, and the fourth is the one that
matters.** `mpsc::Receiver` (`local.rs:1449`), `Watched` (`:1501`), `Keyed`
(`:1548`), and **`Lent<'_, I>` (`:2364`), a delegating wrapper**. `Lent` is the
inbox the PRODUCTION supervised path uses: `spawn_supervised` builds a real
`Watched { rx, busy }` with a real busy cell (`:2928`, `:2945`), and `supervise`
then wraps it in a `Lent` and calls `run_loop` (`:2591-2596`).

**So without an override, every supervised local actor loses the check —
silently, with no test failing.** `Lent` must forward it:
`fn identity(&self) -> Option<Actor> { self.inbox.identity() }`, exactly as it
already forwards `mark_busy` at `local.rs:2372-2374`. One line.

An earlier draft of this section attributed the `None` default to "the
supervision tests", which made supervision read as a test-only concern. It is
not: the production path has a busy cell and would still have received `None`.
**A `None` default on a private trait is acceptable here — `mark_busy` sets the
idiom — but `Lent` is the proof that the risk is real.** A delegating wrapper
that forgets to forward turns a safety check off and nothing complains. So the
acceptance bar gains a test: a supervised actor must still refuse a self-call.

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
task. The vehicle is the private `Message` enum, NOT `Origin`.**

**A first version of this design put the chain on `Origin`, and that version
cannot work.** The refutation is structural, not a matter of taste. `Origin` is
consumed INTO the opaque `M` before a `Message` exists: every send builds its
payload as `mail(Origin::local(), reply)` through a closure typed
`impl FnOnce(Origin, Reply<T>) -> M` (`local.rs:3128`, `:3173`, `:3678`), and
`Message::Mail(Box<M>)` (`local.rs:1320-1341`) carries nothing beside it.
`run_loop` receives `Message::Mail(mail)` at `local.rs:1766` and hands it to
`dispatch`. **`run_loop` never sees an `Origin` at all**, and `trait Turn<A>`
(`local.rs:275-282`) declares `fn run(self, state, hooks)` with no `Origin`
parameter and no accessor.

**The consequence is that the `Origin` version detects self-calls only.**
`run_loop` could install `One(self)` from a new parameter, but never
`Deep(ancestors)`, because the ancestors would be sealed inside `M`. A two-actor
ring is precisely the case that needs `Deep` — and §1 proves `A → B → A` is the
deterministic hazard this whole item exists to close. The design would have
shipped detecting the easy half and silently missing the half that motivated it.
Reaching the `Origin` at dispatch time means putting it on `Turn::run`, which is
`#[doc(hidden)] pub` and implemented by generated code — a macro change, which
this section promises to avoid.

**So the chain rides on `Message`: `Mail(Box<M>, Chain)`.** `Message` is private
to `local.rs` (`local.rs:1320`), so this costs no public API, no generated line,
and raises no ALPN question at all. `run_loop` reads it at `local.rs:1766`
directly. This is SIMPLER than the version it replaces, not larger.

The keyed path does not gain this, and that is consistent rather than an
oversight: `local.rs:1553` shows `Keyed` mapping a bare `M` into `Message::Mail`,
so a keyed mailbox carries no `Chain`. §6 already states that residual.

`run_loop` takes the id as a new private parameter, so **no macro changes** —
verified: `run_loop` has exactly three call sites, all inside `local.rs`
(`:1896`, `:2596`, `:2850`). The macros call `$crate::run_keyed` and
`$crate::run_keyed_supervised`, which are the public wrappers at `local.rs:1881`
and `:2690`. Those mint their own and keep their signatures.

**The check goes in `LocalActor::send`**, documented as "the one place a message
enters the mailbox", with five callers. Verified by two independent methods — a
sweep for `.send(`/`try_send(`/`reserve(` across `keylink/src`, and a sweep for
every `Message::(Mail|Read|Stop)` construction. Both return the same five
non-test callers of `local.rs:3447-3449`: `answered` (`:3128`, serving both `ask`
and `ask_catching`), `notify` (`:3173`), `read` (`:3220`), `stop` (`:3306`) and
`deliver_to_local` (`:3678`). **There is no sixth door.** The only other
`try_send` calls are `console.rs:543` and `compose/topic.rs:300`, and neither is
a `LocalActor` mailbox.

**`send` must be told WHICH HALF of §2's rule applies, because it cannot tell.**
This is the second contradiction in the first draft, and it is worth stating
plainly. Compare the refuse-always caller with the refuse-only-if-blocking one:

    answered, local.rs:3128:  self.send(Message::Mail(Box::new(mail(Origin::local(), reply))))
    notify,   local.rs:3173:  self.send(Message::Mail(Box::new(mail(Origin::local(), reply))))

**They are byte-identical in shape.** `notify` differs only by `drop(answer)` at
`:3172` and by not awaiting. So `send` receives the same `Message::Mail` from
both halves and **cannot implement §2 from its argument**. An implementer forced
to choose one behaviour for both either breaks the legitimate self-`tell` into a
free slot, which §2 promises to keep, or reverts to the first draft's rule, which
§2 already calls wrong.

So `send` gains a second parameter, a private enum naming the half. **That is
also the Level-1 encoding this design was missing**: the caller cannot forget to
say which half it is, because the type will not let it compile. It composes with
the `Message` amendment above — both facts ride on the same private plumbing.

The fast path stays one thread-local read. Off-turn, the read is a unit
discriminant and the check is a match arm returning false.

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

**Default stays 1 — but for ONE surviving reason, not three.** The backpressure
argument at `local.rs:216-220` is untouched by this design: a caller cannot get
more than one message ahead, so a caller measuring its own latency sees the
actor's. That reason stands on its own and is enough.

**The other two did not survive measurement, and both are deleted rather than
softened.**

The first claimed "five sites pin depth-1 behaviour today". **That is a count of
PROSE presented as a count of behaviour pins.** Measured at depth 2 across 524
tests: exactly **TWO** fail, and both are the permit-arithmetic assertions in
`cycles.rs`. The five-to-seven mentions — `feeds.rs:448`, `actors.rs:961`,
`actors.rs:2845`, `cycles.rs:18`, `cycles.rs:249-265`, `compose/mod.rs:77` — are
doc comments, and three of them explain why a test is SHAPED as it is while that
test passes at depth 2. So the real cost of moving the default is two test edits
and four doc comments going stale unchecked. That is a docs cost, not a
flakiness cost, and saying "five pins" hid the difference.

The second claimed the `#[defer]` and `persist` suites become timing dependent
above depth 1, because `tests/defer.rs`'s `parked()` helper is a synchronisation
point. **Measured and false.** At depth 2, `--test defer --test persist` runs 31
tests and passes 31. `defer.rs:144-151` shows why: `parked()` is a 500-iteration
retry loop with `yield_now()` between attempts. The permit makes one probe
meaningful; the LOOP is what makes the suite deterministic, and the loop does not
depend on depth.

**The user's ask is a configurable depth >= 1, and a default of 1 plus a knob
satisfies it.** Moving the default buys nothing the knob does not already buy,
and costs the two test edits and four doc rewrites above.

Carry `registry.rs:2032-2043`'s warning to the local door verbatim: raising the
depth raises the shutdown budget owed, by up to N turns, and nothing can check
that for you. **The citation was `:1974-1981` and that is the wrong paragraph** —
those lines say "Nothing here recommends a value, deliberately" under the heading
"A full mailbox BLOCKS the sender". The warning wanted here sits under
`:2032`, "The shutdown budget's meaning moves with this, and under-draining is a
defect".

## 6. The limits, stated rather than discovered

**No case converts a deterministic deadlock into an intermittent one.** The one
new intermittency is a `tell` refused because the mailbox is full — which
replaces an intermittent **hang** with an intermittent **error** a caller can
see.

**Six residuals, and the count is six because they are listed.** An earlier draft
said "ten were enumerated" and then showed five. A design of record that claims
an enumeration it does not publish is asking to be trusted on the part nobody can
check, so the number now equals the list.

Uncaught, all deterministic:

1. A ring closed through `tokio::spawn`.
2. A ring closed through another thread.
3. A ring closed through a non-actor primitive such as a `oneshot`.
4. A ring through a keyed or served hop — catchable in a later wave, still
   moving no wire type. The keyed mailbox carries no `Chain`, because
   `local.rs:1553` maps a bare `M` into `Message::Mail`.
5. A `Topic` cycle under `BlockPublisher`, which is not a `LocalActor` at all.
6. `ended()` awaited from inside its own turn, which is not a send and so has no
   error position.

**The `spawn` limit is load-bearing and cannot be fixed.** The chain follows
messages, so a spawned task starts empty. That is precisely what makes the
crate's documented park-and-spawn remedy work. You cannot have both.

## 7. Open decisions

1. `stop()` returns `()` today and must gain a `Result`, or the refusal has
   nowhere to go and becomes a silent no-op — the fallback this crate refuses
   everywhere else. The premise holds: `local.rs:3306` really is
   `let _ = self.send(Message::Stop).await;`.

   **It is NOT the largest migration item, and that claim is withdrawn.**
   Measured: about 27 `.stop()` sites — 5 in `keylink/src`, 26 in
   `keylink/tests`, 1 in `docs`, none in `keylink-persist` or `demo`. Five of the
   test sites are `Group::stop` (`compose.rs:1426`, `:1919`, `:1952`, `:2055`,
   `:2362`), a different method entirely — synchronous, sets a flag, sends
   nothing — so this migration does not touch them. That leaves **exactly ONE
   production call site**: the generated client's two-line `stop` at
   `macros.rs:2108-2109`. Everything else is tests and docs, and there are no
   external users. **The largest migration item is the `Message` and `send`
   plumbing in §3**, not this.
2. The new error's name is a placeholder.
3. The ~40-byte per-actor estimate is an **estimate** and must be re-run.

   **And the figure it would be compared against measures the WRONG PATH.** An
   earlier draft cited `actor_memory.rs:33-56` for "an idle actor at 1,424 bytes
   at depths 1, 32 and 1,024, identical". The citation is exact and the numbers
   are right; the noun is wrong. `actor_memory.rs:37` says "at two thousand idle
   **keyed** actors of the `ledger` service", and phase 5 drives
   `SystemBuilder::mailbox_depth` over `ledger::Handle<Empty>` — the registry
   path, not `LocalActor`. The mechanism does transfer, because `:47-48` explains
   it is `tokio::sync::mpsc` allocating its first block of 32 slots eagerly and
   both paths use that channel. **But it is not measured on the path this section
   applies it to.** Either measure the local path, or say plainly that the figure
   is the keyed path's and name the shared channel type as the reason it carries.
4. `.mise/tasks/anchors` registers the literal `const LOCAL_MAILBOX_DEPTH: usize
   = 1;`. Changing that line reddens the gate before anything compiles. Re-quote
   the row and the record, and read the new count off the failing run.
