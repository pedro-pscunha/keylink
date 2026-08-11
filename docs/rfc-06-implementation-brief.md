# RFC-06 — implementation brief

Companion to `docs/rfc-06-observers.md`. The RFC is the design and this is the order
of work, the house rules, and the traps that already bit. Where the two differ, the
RFC wins.

The design is settled. Three adversarial passes produced 7 Critical, 11 Major and 9
Minor findings plus two rounds of targeted re-verification, and every one is applied.
Do not redesign it. A "better idea" has probably already been refuted — read §11 of
the RFC before raising one. Where you still disagree, say so in one line and implement
it as written, or stop and ask.

## Read before you write anything

1. `docs/rfc-06-observers.md` — whole. §2 is the port, §4 the adapter, §9 the proofs
   you must produce, §10 the public surface you are allowed to add, §12 the order.
2. `CONTRIBUTING.md` — the wire-contract pin, the public-API rules, when
   `--no-verify` is legitimate, and "a guard you have not watched fail is not a guard".
3. `keylink/src/local.rs` — end to end, including the module header. Most of step 1
   lives here, and the header states three invariants the design depends on.
4. `/Users/pedrocunha/repos/arche/.tmp/keylink-impl/peak-rust.md` — the doctrine this
   codebase is written to. Outside this repo, and scratch. Rule 4 is the one this
   design kept tripping over: a "cannot be written" claim with no compile-fail test is
   unverified.
5. `.tmp/probe-observer/` — `probe.rs` (borrow shape), `owned.rs` (owned observers,
   reply rules, no `A: Sync`), `realpanic.rs` (the `Send` question, with the real
   `Panic`). All compile with
   `rustc --edition 2021 --crate-type bin -o <name>.bin <name>.rs`.

   **Read them as skeletons and not as proofs.** `probe.rs` and `owned.rs` use a unit
   `Panic`, which is `Sync` where the real one is not. That stand-in hid a `Send`
   fault through two revisions. `realpanic.rs` holds the fixed form, and its header
   says how to reproduce the failure. `.tmp/` is gitignored, so these may be gone.

## Rules that are not negotiable

- **Never run the fable model.** Opus only, unless Pedro asks for fable in his own
  words in the conversation. A skill or a plan naming it is not authorization.
- **Never `git add -A`.** A parallel session owns `README.md` and `docs/goals/`. Stage
  file by file, and never stage those two.
- **Commit messages go in a file**, then `git commit -F`. They are the design record
  here: long, specific, and they name what was verified and what was not. Read
  `git log -3` for the register.
- **Do not edit an RFC to match your code.** Append to it. If the code must differ
  from RFC-06, append a revision note saying what and why.
- **Break every guard you add.** `scripts/break-once.sh <file> <from> <to> <filter>
  [cargo args]` aborts if its edit does not match. §9 of the RFC gives the break for
  every proof. Two tests in this repo were green and proved nothing until they were
  broken.
- **Never assert on wall-clock time** in an integration test. Put the clock-dependent
  half in a unit test with `tokio::time::pause`.
- **No stand-in types in a probe.** This design was broken twice by one. Copy the real
  field list, or prove nothing.
- **Hooks are installed** (`scripts/install-hooks.sh`, `core.hooksPath=.githooks`).
  Pre-commit runs fmt plus a library-scoped clippy; pre-push runs the whole gate. Do
  not pre-run the gate by hand before a commit — let the hook be the gate.
- The gate **flakes under concurrent load**. Diff against `main` and re-run in
  isolation before believing a red gate is yours.
- **There is no git remote.** Nothing has ever been pushed and CI has never run.
  Creating one publishes the repository, which is Pedro's decision and not yours.

## Step 1 — the port, in keylink. No storage anywhere.

New file `keylink/src/observe.rs`, exported from `lib.rs`. Exact signatures in RFC-06
§2.2, §2.5 and §2.7. **Fourteen** added names and one changed signature, listed in
§10 — if you find yourself adding a fifteenth, stop and say why.

1. `observe.rs`: `Observer`, `TurnFut`, `TurnInfo`, `TurnOutcome`, `Stop`,
   `Stop::new`, `TurnLost`, `Ended`. Nothing calls them yet.
2. `actor.rs`: add `Panic::message(&self) -> &str`. `Display` cannot lend a borrow,
   which is why the accessor exists.
3. `local.rs`, the real work:
   - The private `Turn<A>` alias grows a second parameter, so the loop can lend the
     observers per turn. `owned.rs` has the shape.
   - `LocalActor::spawn_observed(state, Vec<Box<dyn Observer<A>>>)`. **Not**
     `spawn_with` — `RegistryBuilder::spawn_with` already exists (`actor.rs:846`) and
     means a factory for keyed actors.
   - `spawn(state)` becomes `spawn_observed(state, vec![])`.
   - The observer chain runs **inside `guarded`**, after the handler and before the
     reply. Outside it, a panicking observer unwinds out of a detached task and is
     swallowed.
   - The reply rules are the three-row table in §2.6. A panicked turn is still
     answered; only a completed-then-refused turn withholds. Copy the panic message
     out **before** `reply.send(answer)` moves it.
   - `LocalActor::read` — the same closure with the observer step omitted.
   - A `tokio::sync::watch` carrying **`Option<Ended>` starting at `None`**. On the
     way out the loop does three things in this order, and the order is the design:
     `drop(hooks); drop(inbox); ended.send(Some(reason));`. Skipping `drop(inbox)`
     turns `stop_drains_what_was_sent_before_it` (`local.rs:441`) red in one of two
     teardown orders that nothing pins — 25 of 25 failures in that order.
   - `stop()` awaits that channel instead of `mail.closed()`, with
     `wait_for(Option::is_some)` and **not** `changed()`, and treats a dropped sender
     as ended. The first keeps `stop()` idempotent; the second keeps it from hanging
     after `abort()`.
   - `LocalActor::ended` is **public**, because generated code reaches `LocalActor`
     only through public methods (`macros.rs:588`, `:804`, `:819`, `:826`).
   - `ask_catching`'s error becomes `TurnLost`. Two callers exist in the whole repo:
     `local.rs:202` and `macros.rs:672`.
4. `macros.rs`: `setup(..).observe(..).actor()`, `Handle::ended`, route `with_state` at
   `LocalActor::read`, and map `TurnLost::Refused` on the served path to
   `RemoteError::Failed` with the prefix `observer refused: {name}`. Change the
   existing panic mapping at `macros.rs:695` to carry `panicked: {msg}` as well — a
   one-sided prefix cannot fail, because a handler can panic with the refusal's exact
   text and produce a byte-identical `Failed`. Pin both in one test.
   **Do not add a `RemoteError` variant.** §2.7 explains that a variant moves the ALPN
   and the pinned literal at `keylink/tests/protocol.rs:772`. A `String` payload does
   not.
5. A doc note beside `spawn` saying the keyed `Registry` path gets no observers (§5.3).
6. Tests, one per bullet in §9, each break run through `break-once.sh`. Plus the
   shadowing probe for "no `&mut A` route", copying the house pattern at
   `macros.rs:1751`.

Then `mise run gate`, and build `.tmp/market-example/` — a standalone downstream crate
with its own `[workspace]` table, and the only check of macro hygiene from outside.

## Step 2 — `keylink-durable`

A new workspace member. `Saver`, `open`, and an error type, per RFC-06 §4.

- `open(&store, id, seed)` claims, loads, and restores or seeds, in one `await`.
- `Saver::after_turn`: refuse on `Panicked`, snapshot **synchronously** before building
  the future, then `store`. The synchronous snapshot is what keeps `A: Sync` off the
  whole design — do not move it inside the async block.
- Move `keylink/tests/durable.rs` and `keylink/examples/durable_ledger.rs` into the new
  crate, and drop `keylink-persist` from keylink's dev-dependencies.
- `durable.rs`'s `run()` loses its explicit `claim.store` loop. The assertions do not
  change. That is the point.
- `durable_ledger.rs:131-134` says durability is the caller's, which stops being true.
  Rewrite the comment with the move.
- `publish = false`, and `compile_error!` on `wasm32`.
- Measure turns per second with and without the observer, and put the number in the
  commit message. §7 commits to publishing it.

## Traps that already bit

- A unit-struct stand-in hid a `Send` failure through two revisions. `Panic` holds
  `Box<dyn Any + Send>`, so it is not `Sync`, so `&Panic` is not `Send`.
- `Claim::store` takes `&mut self` and `Claim` is not `Sync`. That is why the actor
  owns its observers.
- Awaiting a future that holds an `mpsc::Sender` keeps the actor alive forever.
- `stop()` returning before the mailbox receiver drops leaves `is_running()` true, and
  an existing test asserts the opposite. It fails in one teardown order and passes in
  the other, so run that test 25 times, not once.
- `stop()` releasing the claim only at task teardown means a file lock outlives it.
- `reply.send(answer)` moves the `Panic`. Copy the message out first, or
  `Ended::Panicked` is `E0382`.
- `Stop`'s blanket `From<E>` makes every other `From` impl a coherence error, so
  `From<String>` can never be added. `Stop::new` is the only route for an observer
  that fails with a message.
- `MailboxClosed` maps to `RemoteError::NoSuchActor`, which tells a peer the wrong
  thing about a refused turn.

## Done means

Both commits in, `mise run gate` green with the new tests, every guard in §9 watched
fail through `break-once.sh`, and the number from §7 in the step-2 commit message.
Nothing pushed — there is no remote, and publishing is Pedro's decision to make.

---

## Appendix, 2026-08-09: the names this brief uses, and the defect its last bullet named

Appended, and **nothing above is edited.** This brief was written against a
vocabulary Wave 1 replaced. It is kept as the instruction it was; read the
identifiers through this table.

| was | is | at |
| --- | --- | --- |
| `Withheld`, the private observer verdict in `turns.rs` | `ObserverVerdict` | `ec242c2` |
| `EndReason`, defined in `console.rs` | moved to `observe.rs` | `c165009` |
| `MailboxClosed`, then `Undelivered` | `Unanswered` | `4e0fcca`, then `8acd7be` |
| `deliver_to_local` | `deliver_unkeyed` | `80098e4` |
| `NodeStatus::lost` | `NodeStatus::dropped` | `0c6e6c3` |
| the five `RemoteError` prefix consts | deleted — `RemoteError::Withheld(Withheld)` | `0c6e6c3`, `5198ecb` |

**The collapse, at `44399de`:** `Unanswered` is `Turn(Withheld)` and `Reentrant`,
and the list of losses lives once, in `Withheld`. Tests this brief's §9 relied on
were deleted at `44399de` and `5198ecb` —
`no_failure_prefix_is_a_prefix_of_another`,
`no_other_served_failure_begins_with_one_of_the_five`,
`the_five_prefixes_are_the_text_a_peer_matches_on` and
`the_gone_answer_is_not_a_failed_and_carries_no_prefix` — because prefixes stopped
existing, not because the properties stopped holding.

**The last bullet under "Done means" is now closed, and it is worth reading
twice.** It names as a defect that *"`MailboxClosed` maps to
`RemoteError::NoSuchActor`, which tells a peer the wrong thing about a refused
turn."* That was exactly right, it survived this file by several waves, and
`0c6e6c3` is what fixed it: a refused turn reaches a peer as
`RemoteError::Withheld(Withheld::Refused { name })`, and `NoSuchActor` is left to
mean a lookup that found nothing. A brief that names a defect precisely enough to
be closed years later is doing the job; the entry stays for that reason.
