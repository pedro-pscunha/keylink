#!/usr/bin/env bash
# Replay every break recorded by the `Fate` wave (RFC-07 §12), each on a UNIQUE
# two-line anchor.
#
# A record that cannot be replayed is not a record, and a record that replays for
# the WRONG REASON is worse: it reads as evidence and is not. So each break here
# carries the **exact substring its guard's doc comment records as watched**, and
# this asserts that substring appears in the run.
#
# # Why the substring and not the exit code
#
# Counting exits was the first design and it cannot see the failure this harness
# exists for. Three times in this tree a break has stopped reddening because a
# *second writer* masked it — `Mailbox::drop` masking `System::abort`'s break, the
# same masking `drain`'s, and a whole-function `Floor` masking `Lent::ending`'s
# give-up arm. A fourth time a break stayed red at a **different** assertion,
# because the record's stated reason ("`supervise` reads the same cell") had been
# deleted three commits later. `exit=101` is identical in every one of those
# cases. The recorded sentence is not.
#
# It also catches the two cheaper failures for free: a break that goes green
# prints `exit=0`, one character from `exit=101` in a wall of output, and an
# aborted anchor prints no `exit=` line at all.
#
# # It fails, and it reports every failure
#
# `run` accumulates rather than `set -e`-ing out, so one rotten record does not
# hide the state of the nineteen after it. The exit code used to be the status of
# the **last** `run` only — red-then-green exited 0 — which is worse than always
# zero, because a non-zero looked like it meant something.
#
# Not part of any gate: it edits the tree. **Run it only on a clean worktree** —
# `break-once.sh` restores a backup on EXIT, so a run racing an unsaved edit
# restores over it.
set -uo pipefail
cd "$(git rev-parse --show-toplevel)"

if [ -n "$(git status --porcelain)" ]; then
    echo "refusing to run against a dirty tree: break-once.sh restores a backup" >&2
    echo "on EXIT, which would overwrite whatever is uncommitted here." >&2
    git status --porcelain >&2
    exit 2
fi

failed=0
total=0

# run <label> <expected-substring> <file> <from> <to> <filter> [cargo args...]
run() {
    local label="$1" expected="$2"; shift 2
    total=$((total + 1))
    echo "════ $label"
    local out
    out="$(./scripts/break-once.sh "$@" 2>&1)" || true
    # EVERY LINE `break-once.sh` PRINTS FOR ITSELF, and none of the runner's. That
    # is the rule now, and it replaces a list of three patterns that had to be
    # extended by hand each time the harness grew a line — the count below was
    # invisible here for exactly that reason on its first draft. The harness tags
    # its own lines `── `; the runner's output carries no such prefix. `ABORT` and
    # `expected exactly one` stay because the abort is printed on stderr WITHOUT
    # that prefix.
    echo "$out" | rg "ABORT|expected exactly one|^── " || true
    # THE FILTER MUST SELECT EXACTLY ONE TEST, and this is checked BEFORE the
    # recorded sentence, because a filter that selects the wrong number fails the
    # check below for a reason that has nothing to do with the tree.
    #
    # `cargo test` with a filter naming no test prints `0 passed` and exits **0**.
    # That reached the arm below as "the break was not reproduced" — the same report
    # a genuinely rotted anchor gives, from the opposite cause, and the remedy for
    # one is useless for the other. Break 16 sat in that state: it named a test that
    # existed nowhere, so it could never redden and was never able to say so.
    #
    # **Zero was the first failure this caught and TWO OR MORE is the mirror of it,
    # which the equality-to-zero form could not see.** `cargo test` matches a filter
    # as a SUBSTRING, so a test whose name is a strict PREFIX of a sibling's selects
    # both — `a_remote_caller_can_tell_restarting_from_gone` against
    # `..._on_a_keyed_actor`, which is the shape a break record's own anchors hit one
    # level down. Two tests under one break means a run where only the OTHER one
    # reddens exits non-zero, so a zero-check never fires and the `expected`
    # substring below can be satisfied by output from a test this stanza does not
    # name. A break that reddened the wrong test, reported as success.
    #
    # **THE COUNT ITSELF MOVED INTO `break-once.sh`, and that is a correctness fix
    # rather than tidying.** It used to be a `[0-9]+ passed; [0-9]+ failed` regex
    # here. That is libtest's wording; `cargo nextest run` prints
    # `Summary [ 0.109s] 2 tests run: …`, so under the runner the gate itself uses,
    # and which `break-once.sh`'s header calls required, the regex matched nothing,
    # BOTH arms below fell through, and a stanza passed on the sentence alone —
    # precisely the state the count was added to end. The harness now counts under
    # either runner, prints one `── selected=` line, and prints the remedy, so the
    # `replay-anchors` task and every hand-run are covered by the same reading.
    #
    # `?` means the harness found NO totals line: an abort, or a tree that did not
    # compile. That is not zero and it is not two. It is left to the `expected`
    # check below, which already reports it, rather than given a third failure class
    # that would say the same thing twice.
    local selected
    selected="$(echo "$out" | rg -o '^── selected=(\S+)' -r '$1' | head -1)"
    if [ "$selected" = 0 ]; then
        echo "  ✗ BROKEN STANZA: the filter selected NO test, so this break cannot"
        echo "    redden and never could. This is not a tree change — fix the stanza,"
        echo "    with the remedy the harness printed above."
        failed=$((failed + 1))
        return
    fi
    if [ -n "$selected" ] && [ "$selected" != "?" ] && [ "$selected" -gt 1 ]; then
        echo "  ✗ BROKEN STANZA: the filter selected $selected tests, and a break"
        echo "    that reddens any of them satisfies this stanza. Fix the stanza, not"
        echo "    the tree, with the remedy the harness printed above."
        failed=$((failed + 1))
        return
    fi
    if ! echo "$out" | rg -qF -- "$expected"; then
        echo "  ✗ the recorded output is NOT in this run. Expected to find:"
        echo "      $expected"
        echo "    A break that is green, aborted, or red for a different reason all"
        echo "    land here. The guard's doc comment and the tree disagree."
        failed=$((failed + 1))
    fi
}

run "1 keyed live drop" \
'a live keyed actor must not be reported as absent: Remote(NoSuchActor { id: "obra/4471" })' \
keylink/src/actor/remote.rs \
'        .map(|answered| rendered(answered, budget.map_or(0, Queueing::millis)))
        .map_err(|_| lost(fate.standing(), actor.as_str()))' \
'        .map(|answered| rendered(answered, budget.map_or(0, Queueing::millis)))
        .map_err(|_| no_such_actor(&actor))' \
  a_live_keyed_actor_does_not_tell_a_peer_no_such_actor -p keylink --test defer

run "2 keyed restart" \
'round 1: expected restarting: , got Remote(NoSuchActor { id: "desks/4471" })' \
keylink/src/turns.rs \
'        if retryable(reason) && self.budget.spend(self.started.elapsed()) {
            return Standing::Restarting(Arc::new(reason.clone()));' \
'        if retryable(reason) && self.budget.spend(self.started.elapsed()) {
            return Standing::Gone;' \
  a_remote_caller_can_tell_restarting_from_gone_on_a_keyed_actor -p keylink --test supervision

run "3 singleton give-up (multi_thread)" \
'round 2: expected gone, got Remote(Failed("dropped: the actor is serving and this reply was never answered"))' \
keylink/src/turns.rs \
'        self.close();
        Standing::Gone' \
'        self.close();
        Standing::Running' \
  a_remote_caller_can_tell_restarting_from_gone -p keylink --test supervision -- --exact

run "4 verdict settled after drop(state)" \
'an incarnation'"'"'s verdict must be settled before its state dies: [(Running, false), (Running, true)]' \
keylink/src/turns.rs \
'    let verdict = inbox.ending(&reason);
    fate.settle(verdict.clone());' \
"    let verdict = inbox.ending(&reason);
    struct Late<'a>(&'a Fate, Option<Standing>);
    impl Drop for Late<'_> {
        fn drop(&mut self) {
            if let Some(late) = self.1.take() {
                self.0.settle(late);
            }
        }
    }
    let _late = Late(fate, Some(verdict.clone()));" \
  an_ending_incarnation_settles_its_verdict_and_its_mailbox_before_its_state -p keylink --lib

run "5 give-up does not close the mailbox" \
'a give-up must close the mailbox and a restart must not: [(Restarting(Panicked { message: "this incarnation is over" }), false), (Gone, false)]' \
keylink/src/turns.rs \
'        self.close();
        Standing::Gone' \
'        Standing::Gone' \
  an_ending_incarnation_settles_its_verdict_and_its_mailbox_before_its_state -p keylink --lib

run "6 verdict never published" \
'an incarnation'"'"'s verdict must be settled before its state dies: [(Running, false), (Running, true)]' \
keylink/src/turns.rs \
'    let verdict = inbox.ending(&reason);
    fate.settle(verdict.clone());' \
'    let verdict = inbox.ending(&reason);' \
  an_ending_incarnation_settles_its_verdict_and_its_mailbox_before_its_state -p keylink --lib

run "7 refusal arm answers running" \
'a spent budget is not a live actor: Running' \
keylink/src/turns.rs \
'        self.close();
        Standing::Gone' \
'        self.close();
        Standing::Running' \
  a_supervisor_that_refuses_a_restart_answers_gone_rather_than_running -p keylink --lib

run "8 System::abort settles running" \
'an aborted actor is gone before it is destroyed: [Running]' \
keylink/src/actor/registry.rs \
'        entry.fate.settle(Standing::Gone);
        entry.task.abort();' \
'        entry.fate.settle(Standing::Running);
        entry.task.abort();' \
  abort_settles_the_served_answer_before_it_destroys_the_actor -p keylink --lib

run "9 drain settles running" \
'an actor destroyed by an expired budget is gone before it is destroyed: [Running]' \
keylink/src/actor/registry.rs \
'            fate.settle(Standing::Gone);
            // Before the second await' \
'            fate.settle(Standing::Running);
            // Before the second await' \
  an_expired_shutdown_budget_settles_the_served_answer_before_the_abort -p keylink --lib

run "10 a RouteError that begins like a prefix" \
'a served Failed that is not one of the four must not begin like one: "dropped: " starts "dropped: the actor system is at its capacity of 10 live actors"' \
keylink/src/actor/registry.rs \
'`the_capacity_refusal_reads_the_same_by_hand_and_lazily` pins the words.
    #[error("the actor system is at its capacity of {capacity} live actors")]' \
'`the_capacity_refusal_reads_the_same_by_hand_and_lazily` pins the words.
    #[error("dropped: the actor system is at its capacity of {capacity} live actors")]' \
  no_other_served_failure_begins_with_one_of_the_four -p keylink --lib

run "11 lost live arm -> NoSuchActor" \
'a live actor must not be reported as absent: Remote(NoSuchActor { id: "Orders" })' \
keylink/src/turns.rs \
'        Standing::Running => RemoteError::Failed(format!(
            "{DROPPED}the actor is serving and this reply was never answered"
        )),' \
'        Standing::Running => RemoteError::NoSuchActor { id: id.to_owned() },' \
  a_local_caller_and_a_peer_agree_about_an_abandoned_deferred -p keylink --test defer

run "12 lost live arm -> panicked" \
'a panicking deferred handler loses the reply, and that is what the peer must read: "panicked: bad"' \
keylink/src/turns.rs \
'        Standing::Running => RemoteError::Failed(format!(
            "{DROPPED}the actor is serving and this reply was never answered"
        )),' \
'        Standing::Running => RemoteError::Failed(format!("{PANICKED}bad")),' \
  a_peer_whose_deferred_handler_panicked_reads_the_same_dropped_answer -p keylink --test defer

run "13 default Inbox::ending -> running" \
'a stopped actor is gone, not merely unanswering: Remote(Failed("dropped: the actor is serving and this reply was never answered"))' \
keylink/src/turns.rs \
'    fn ending(&mut self, _reason: &Ended) -> Standing {
        Standing::Gone
    }' \
'    fn ending(&mut self, _reason: &Ended) -> Standing {
        Standing::Running
    }' \
  a_stopped_actor_still_tells_a_peer_no_such_actor -p keylink --test defer

run "14 the Floor writes running" \
'an aborted actor is gone, and its own loop will never say so: Remote(Failed("dropped: the actor is serving and this reply was never answered"))' \
keylink/src/turns.rs \
'    fn drop(&mut self) {
        if self.armed {
            self.fate.settle(Standing::Gone);' \
'    fn drop(&mut self) {
        if self.armed {
            self.fate.settle(Standing::Running);' \
  an_aborted_actor_tells_a_peer_no_such_actor -p keylink --test defer

run "15 Mailbox floor running (aborted keyed)" \
'an aborted keyed actor is gone, and its own loop will never say so: Remote(Failed("dropped: the actor is serving and this reply was never answered"))' \
keylink/src/actor/registry.rs \
'    fn drop(&mut self) {
        self.fate.settle(Standing::Gone);
    }' \
'    fn drop(&mut self) {
        self.fate.settle(Standing::Running);
    }' \
  an_aborted_keyed_actor_tells_a_peer_no_such_actor -p keylink --test defer

# 16 named a test that DOES NOT EXIST, so it could never redden and exited 0 for it.
# `cargo test` with a filter matching nothing prints `0 passed; N filtered out` and
# exits 0, which this script read as "the break was not reproduced" - the same
# outcome a genuinely rotted anchor gives, from the opposite cause. Audited: 22 of
# the 23 names here resolve to a real `fn`, and this was the only one that did not.
#
# It was wrong four ways at once, and each hid the next. The ANCHOR took
# `Mailbox::drop` at `registry.rs:247`, while the record it replays
# (`registry.rs:3315-3318`) is about **`drain`'s** line at `:1125` - `registry.rs`
# has three distinct `Standing::Gone` settle sites and this took the wrong one. The
# TARGET was `--test defer`, which cannot reach a `#[cfg(test)]` unit test in `src`
# at all. The TEST NAME existed nowhere in the tree. And the EXPECTED sentence was
# not the one the assertion prints.
#
# Re-derived from the record rather than patched, and watched: the corrected break
# reddens BY VALUE at `registry.rs:3339` with the recorded sentence, off a control
# that runs `1 passed` rather than `0 filtered out`. Requiring the control to RUN is
# the check that would have caught the original.
run "16 expired shutdown budget settles before the abort" \
'an actor destroyed by an expired budget is gone before it is destroyed: [Running]' \
keylink/src/actor/registry.rs \
'            fate.settle(Standing::Gone);' \
'            fate.settle(Standing::Running);' \
  an_expired_shutdown_budget_settles_the_served_answer_before_the_abort -p keylink --lib

run "17 the DROPPED prefix moves" \
'left: "lost: "' \
keylink/src/reply.rs \
'/// the actor.
    pub const DROPPED: &'"'"'static str = "dropped: ";' \
'/// the actor.
    pub const DROPPED: &'"'"'static str = "lost: ";' \
  the_four_prefixes_are_the_text_a_peer_matches_on -p keylink --lib

run "18 supervise has no floor (factory panic)" \
'a peer whose actor died in its own factory must not be told it is serving: Remote(Failed("dropped: the actor is serving and this reply was never answered"))' \
keylink/src/turns.rs \
'    let mut floor = Floor::armed(fate);
    let mut budget = Budget::new(limit);' \
'    let mut floor = Floor::armed(fate);
    floor.disarm();
    let mut budget = Budget::new(limit);' \
  a_peer_whose_factory_panicked_is_told_the_actor_is_gone -p keylink --test supervision

run "19 run_loop has no floor (feed panic)" \
'a peer whose actor died in its own feed must not be told it is serving: Remote(Failed("dropped: the actor is serving and this reply was never answered"))' \
keylink/src/turns.rs \
'    let mut floor = Floor::armed(fate);
    let mut reason = Ended::Stopped;' \
'    let mut floor = Floor::armed(fate);
    floor.disarm();
    let mut reason = Ended::Stopped;' \
  a_peer_whose_feed_panicked_is_told_the_actor_is_gone -p keylink --test supervision

run "20 gone becomes a Failed" \
'left: Failed("dropped: id")' \
keylink/src/turns.rs \
'        Standing::Gone => RemoteError::NoSuchActor { id: id.to_owned() },' \
'        Standing::Gone => RemoteError::Failed(format!("{DROPPED}{id}")),' \
  the_gone_answer_is_not_a_failed_and_carries_no_prefix -p keylink --lib

run "21 no floor between incarnations (N1)" \
'an actor destroyed between incarnations is gone, not restarting: Restarting(Panicked { message: "this incarnation is over" })' \
keylink/src/turns.rs \
'    let mut floor = Floor::armed(fate);
    let mut budget = Budget::new(limit);' \
'    let mut floor = Floor::armed(fate);
    floor.disarm();
    let mut budget = Budget::new(limit);' \
  a_supervisor_between_incarnations_is_gone_when_it_is_destroyed -p keylink --lib

run "22 a per-iteration floor writes Gone on continue (N2)" \
'a supervisor that is still rebuilding must not read as gone: Gone' \
keylink/src/turns.rs \
'    let mut floor = Floor::armed(fate);
    let mut budget = Budget::new(limit);
    loop {' \
'    let mut budget = Budget::new(limit);
    loop {
        let mut floor = Floor::armed(fate);' \
  a_transient_factory_failure_does_not_overwrite_a_pending_restart -p keylink --lib

run "23 the third disarm around run_loop (load-bearing floor)" \
'a state that panicked on the way out is gone, not restarting: Restarting(Panicked { message: "this incarnation is over" })' \
keylink/src/turns.rs \
'        let (reason, verdict) = run_loop(state, hooks, feeds, lent, fate).await;' \
'        floor.disarm();
        let (reason, verdict) = run_loop(state, hooks, feeds, lent, fate).await;
        floor = Floor::armed(fate);' \
  a_state_that_panics_on_drop_mid_restart_is_gone_not_restarting -p keylink --lib

if [ "$failed" -ne 0 ]; then
    echo "── $failed of $total recorded outputs were not reproduced"
    exit 1
fi
echo "── all $total recorded outputs reproduced exactly"
