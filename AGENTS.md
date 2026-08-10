# Working on keylink as an agent

`CONTRIBUTING.md` is the contract for the code. This file is the contract for
how an agent works here. Read both.

Every rule below has an incident behind it. The incident is stated, because a
rule without its reason is a rule somebody edits away.

## The gate is the only gate

**GitHub Actions is disabled, and TWO independent things hold that.** The
repository's Actions setting is off, and `main` carries no workflow file — deleted,
not paused. Either alone would hold. Both is the state. There is no second checker.

Both are named here rather than one, because this is the file an agent reads first
and a single reason reads as the whole reason. `CONTRIBUTING.md` carries the
measurement of each, with the date and the command that read it back, and it is the
only place that does. Do not restate the figures here — go and read them before you
argue anything about CI.

`mise run pre-commit` and `mise run pre-push` are therefore the whole safety
net. `deny`, `msrv` and `wasm-guards` used to run only in CI and now run in
`gate`, because disabling a gate and dropping what only it covered are two
separate acts.

- **Never kill a running gate.** Twelve gate runs in one session ended by kill
  or timeout, and each one threw away every minute it had spent. If the machine
  is busy, wait.
- **Never bypass with `--no-verify`** except for the two reasons
  `CONTRIBUTING.md` lists. "The gate is slow" is not one of them.
- A `mise` gate's grandchildren survive a process-group kill, so a kill is
  unreliable as well as wasteful.

## Reporting, if you are a subagent

**Your final text reaches nobody.** It is not delivered, and it does not error.

Report by calling `SendMessage(to: "main")`. A file on disk is not a report
either — say where the file is, in a message.

In one 18-hour session, 32 of 34 subagent transcripts never called
`SendMessage`. Fourteen reports had to be requested a second time, and one was
never recovered. This is the single largest source of wasted time in this repo.

Send a message before going idle even when the news is "blocked" or "nothing
found". An idle notification carries no findings, so silence is indistinguishable
from a crash.

## Waiting

- **Wait on events, never on `sleep`.** No `sleep`, no `for i in $(seq …)`, no
  `while true`. Seventy-one polling loops in one session blocked for 67 minutes.
- **Make every long command write its own terminator**, as one unit:
  `cmd; echo "EXIT=$?" >> log`. One loop waited 36 minutes for a marker its
  watched script never wrote, while the work it waited on had already succeeded.
- If a loop is unavoidable, bound it, and make the timeout name the marker it
  wanted.

## The ALPN is not a cost, and will not be until there is a stable version

**Decided by Pedro on 2026-08-10. Do not price an ALPN move. Do not schedule
around one. Do not let one shape a design.** A contract hash that moves costs
nothing here, and it will keep costing nothing until keylink has a stable
version. When that day comes, this section is what gets deleted, deliberately.

What that repeals:

- **No commit is ordered so that pin moves land together.** Take the change in
  its correct form and let the hash fall where it falls.
- **No design is weighed by whether it pays a pin.** A field rename, a new
  variant and a reshaped struct are all free. Choose the right shape, not the
  cheap one.
- **No proposal reports an ALPN column, and no summary reports a pin move as a
  cost.** It is not news.

**The two pinned tests are `#[ignore]`d**, so nothing blocks a hash move at all,
not even the bookkeeping. They are `the_contract_hash_has_not_moved` and
`the_inspect_contract_hash_has_not_moved` in `keylink/tests/protocol.rs`. They
come back on the day a stable version is cut, which is the day this section goes.
Read the new literals off the failing run then, as the ALPN section of
`CONTRIBUTING.md` says. Never compute one.

**The property tests stay on, and they are a different claim**: that two type
sets derive two different contracts, and that a peer built from other types is
refused at the handshake. Those are about the mechanism and they still hold.
Only the literals are parked.

The incident: the factory proposal built a four-commit schedule with **`ALPN
pin` as a column of the table**, grouped the work so that one commit paid it,
and carried that reasoning through its rulings and its open questions. Every bit
of that accounting bought nothing, on a repository whose own `README.md` already
said an ALPN move costs nothing.

## Claims

- **State a SHA, a count or a literal only after reading it in the same turn.**
  Twenty-seven claims were retracted in one session.
- **Verify an infrastructure claim against the infrastructure, never against the
  prose describing it.** `mise.toml` said "CI runs these same tasks". Thirty
  statements repeated it. Nobody queried a CI run, and the trigger named a
  branch that does not exist, so CI had never run on a merge and every one of
  its 15 pull-request runs had failed.
- A statement that was true when written does not stay true. "There is no git
  remote" was correct on 2026-08-06 and wrong two days later.

## Worktrees

Use `wt`, never raw `git worktree`.

- `wt switch -c` branches from the **local** default branch. Fetch and
  fast-forward first, or you will build on a stale base. This has happened.
- **The `X` plus `X-gate` pairing is deliberate. Do not collapse it.**
  `scripts/break-once.sh` edits a source literal in place and restores it on
  `trap EXIT`, while the feature agent keeps committing in the branch worktree.
  A mutating harness cannot share that checkout.
- Never run a break against a sibling worktree that may be live.
- An orphaned `break-once.sh` fires its `trap EXIT` later and overwrites a live
  file with a stale backup. Kill it by exact PID, never by `pkill -f`.
- Removing a worktree needs proof of merge, not a clean tree. Check
  `git merge-base --is-ancestor` against `origin/main`, and check that no
  process holds a cwd inside it with `lsof -a -d cwd <path>`.

## Builds

- Two cargo runs in **one** target directory contend. Two worktrees have two
  target directories and do not contend, so there is no reason to hold agents
  idle for the compiler. A hand-run mutex over chat cost about 140 minutes of
  idle agent time in one session.
- `target/debug/incremental` reaches 45 % of all build bytes, and sccache cannot
  cache an incremental build at all. So `mise.toml` sets `CARGO_INCREMENTAL=0`
  in `[env]`, which applies to **every** cargo command run under mise, not only
  to the gate. Override it per shell with `CARGO_INCREMENTAL=1` when you are
  iterating by hand.
- `ls` on this machine is aliased to a long-format tool. Use `find` or a glob
  when a script consumes filenames. An audit reported "zero shared artifacts"
  because it was comparing size and date columns.

## Tests

`CONTRIBUTING.md` holds the rules. The two that agents break most:

- **Do not assert on wall-clock time in a direction load can break.** A lower
  bound that load only strengthens is fine. An *upper* bound is the one that
  reddens on a loaded machine, and a ceiling that survives must be derived from
  the constant it is bounding rather than written as its own number — otherwise
  it rots silently when that constant moves. Put the clock-dependent half in a
  unit test with `tokio::time::pause`.
- **A guard you have not watched fail is not a guard.** Break it, see the red,
  restore it, and record the break where `mise run anchors` can check it.
