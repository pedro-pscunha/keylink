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
    echo "$out" | rg "ABORT|expected exactly one|exit=" || true
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
        .map_err(|_| lost(route.fate.standing(), actor.as_str()))' \
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
  a_remote_caller_can_tell_restarting_from_gone -p keylink --test supervision

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
  a_live_actor_does_not_tell_a_peer_no_such_actor -p keylink --test defer

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

run "16 Mailbox floor running (expired budget)" \
'an actor destroyed by an expired budget is gone: Remote(Failed("dropped: the actor is serving and this reply was never answered"))' \
keylink/src/actor/registry.rs \
'    fn drop(&mut self) {
        self.fate.settle(Standing::Gone);
    }' \
'    fn drop(&mut self) {
        self.fate.settle(Standing::Running);
    }' \
  a_keyed_actor_destroyed_by_an_expired_budget_tells_a_peer_no_such_actor -p keylink --test defer

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
