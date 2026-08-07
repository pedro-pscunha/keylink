#!/usr/bin/env bash
# Replay every break recorded by the `Fate` wave (RFC-07 §12), each on a UNIQUE
# two-line anchor.
#
# A record that cannot be replayed is not a record. `break-once.sh` aborts on an
# ambiguous anchor rather than passing falsely, so a `BREAK HARNESS ABORT` or an
# `expected exactly one occurrence` here is the defect this script exists to
# catch. Every line should report `exit=101`: the test caught its own break.
#
# It is not part of any gate — it edits the tree — and it takes a few minutes.
# It does, however, **fail**: a break that stops reddening its guard, or an
# anchor that stops being unique, exits non-zero here. It hard-coded one
# developer's absolute path and swallowed every exit code before, which made it
# unable to check the tree it ships in and unable to report anything at all.
set -euo pipefail
cd "$(git rev-parse --show-toplevel)"

failed=0

# `break-once.sh` reports the *test's* exit on its last line as `exit=N`, and
# always exits 0 itself — so the status has to be read out of that line rather
# than from the pipeline. A break that leaves its guard green is `exit=0`, which
# is precisely the failure this script exists to report.
run() {
    local label="$1"; shift
    echo "════ $label"
    local out
    out="$(./scripts/break-once.sh "$@" 2>&1)" || true
    echo "$out" | rg "ABORT|expected exactly one|^a |^an |^the |^round |^left:|^right:|  left:| right:|exit=" || true
    if ! echo "$out" | rg -q "exit=[1-9]"; then
        echo "  ✗ NOT RED — this break no longer reddens its guard, or the anchor aborted"
        failed=$((failed + 1))
    fi
}

run "1 keyed live drop" keylink/src/actor/remote.rs \
'        .map(|answered| rendered(answered, budget.map_or(0, Queueing::millis)))
        .map_err(|_| lost(route.fate.standing(), actor.as_str()))' \
'        .map(|answered| rendered(answered, budget.map_or(0, Queueing::millis)))
        .map_err(|_| no_such_actor(&actor))' \
  a_live_keyed_actor_does_not_tell_a_peer_no_such_actor -p keylink --test defer

run "2 keyed restart" keylink/src/local.rs \
'        if retryable(reason) && self.budget.spend(self.started.elapsed()) {
            return Standing::Restarting(Arc::new(reason.clone()));' \
'        if retryable(reason) && self.budget.spend(self.started.elapsed()) {
            return Standing::Gone;' \
  a_remote_caller_can_tell_restarting_from_gone_on_a_keyed_actor -p keylink --test supervision

run "3 singleton give-up (multi_thread)" keylink/src/local.rs \
'        self.close();
        Standing::Gone' \
'        self.close();
        Standing::Running' \
  a_remote_caller_can_tell_restarting_from_gone -p keylink --test supervision

run "4 verdict settled after drop(state)" keylink/src/local.rs \
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

run "5 give-up does not close the mailbox" keylink/src/local.rs \
'        self.close();
        Standing::Gone' \
'        Standing::Gone' \
  an_ending_incarnation_settles_its_verdict_and_its_mailbox_before_its_state -p keylink --lib

run "6 verdict never published" keylink/src/local.rs \
'    let verdict = inbox.ending(&reason);
    fate.settle(verdict.clone());' \
'    let verdict = inbox.ending(&reason);' \
  an_ending_incarnation_settles_its_verdict_and_its_mailbox_before_its_state -p keylink --lib

run "7 refusal arm answers running" keylink/src/local.rs \
'        self.close();
        Standing::Gone' \
'        self.close();
        Standing::Running' \
  a_supervisor_that_refuses_a_restart_answers_gone_rather_than_running -p keylink --lib

run "8 System::abort settles running" keylink/src/actor/registry.rs \
'        entry.fate.settle(Standing::Gone);
        entry.task.abort();' \
'        entry.fate.settle(Standing::Running);
        entry.task.abort();' \
  abort_settles_the_served_answer_before_it_destroys_the_actor -p keylink --lib

run "9 drain settles running" keylink/src/actor/registry.rs \
'            fate.settle(Standing::Gone);
            // Before the second await' \
'            fate.settle(Standing::Running);
            // Before the second await' \
  an_expired_shutdown_budget_settles_the_served_answer_before_the_abort -p keylink --lib

run "10 a RouteError that begins like a prefix" keylink/src/actor/registry.rs \
'`the_capacity_refusal_reads_the_same_by_hand_and_lazily` pins the words.
    #[error("the actor system is at its capacity of {capacity} live actors")]' \
'`the_capacity_refusal_reads_the_same_by_hand_and_lazily` pins the words.
    #[error("dropped: the actor system is at its capacity of {capacity} live actors")]' \
  no_other_served_failure_begins_with_one_of_the_four -p keylink --lib

run "11 lost live arm -> NoSuchActor" keylink/src/local.rs \
'        Standing::Running => RemoteError::Failed(format!(
            "{DROPPED}the actor is serving and this reply was never answered"
        )),' \
'        Standing::Running => RemoteError::NoSuchActor { id: id.to_owned() },' \
  a_live_actor_does_not_tell_a_peer_no_such_actor -p keylink --test defer

run "12 lost live arm -> panicked" keylink/src/local.rs \
'        Standing::Running => RemoteError::Failed(format!(
            "{DROPPED}the actor is serving and this reply was never answered"
        )),' \
'        Standing::Running => RemoteError::Failed(format!("{PANICKED}bad")),' \
  a_peer_whose_deferred_handler_panicked_reads_the_same_dropped_answer -p keylink --test defer

run "13 default Inbox::ending -> running" keylink/src/local.rs \
'    fn ending(&mut self, _reason: &Ended) -> Standing {
        Standing::Gone
    }' \
'    fn ending(&mut self, _reason: &Ended) -> Standing {
        Standing::Running
    }' \
  a_stopped_actor_still_tells_a_peer_no_such_actor -p keylink --test defer

run "14 the Floor writes running" keylink/src/local.rs \
'    fn drop(&mut self) {
        if self.armed {
            self.fate.settle(Standing::Gone);' \
'    fn drop(&mut self) {
        if self.armed {
            self.fate.settle(Standing::Running);' \
  an_aborted_actor_tells_a_peer_no_such_actor -p keylink --test defer

run "15 Mailbox floor running (aborted keyed)" keylink/src/actor/registry.rs \
'    fn drop(&mut self) {
        self.fate.settle(Standing::Gone);
    }' \
'    fn drop(&mut self) {
        self.fate.settle(Standing::Running);
    }' \
  an_aborted_keyed_actor_tells_a_peer_no_such_actor -p keylink --test defer

run "16 Mailbox floor running (expired budget)" keylink/src/actor/registry.rs \
'    fn drop(&mut self) {
        self.fate.settle(Standing::Gone);
    }' \
'    fn drop(&mut self) {
        self.fate.settle(Standing::Running);
    }' \
  a_keyed_actor_destroyed_by_an_expired_budget_tells_a_peer_no_such_actor -p keylink --test defer

run "17 the DROPPED prefix moves" keylink/src/reply.rs \
'/// the actor.
    pub const DROPPED: &'"'"'static str = "dropped: ";' \
'/// the actor.
    pub const DROPPED: &'"'"'static str = "lost: ";' \
  the_four_prefixes_are_the_text_a_peer_matches_on -p keylink --lib

run "18 supervise has no floor (factory panic)" keylink/src/local.rs \
'    let mut floor = Floor::armed(fate);
    let mut budget = Budget::new(limit);' \
'    let mut floor = Floor::armed(fate);
    floor.disarm();
    let mut budget = Budget::new(limit);' \
  a_peer_whose_factory_panicked_is_told_the_actor_is_gone -p keylink --test supervision

run "19 run_loop has no floor (feed panic)" keylink/src/local.rs \
'    let mut floor = Floor::armed(fate);
    let mut reason = Ended::Stopped;' \
'    let mut floor = Floor::armed(fate);
    floor.disarm();
    let mut reason = Ended::Stopped;' \
  a_peer_whose_feed_panicked_is_told_the_actor_is_gone -p keylink --test supervision

run "20 gone becomes a Failed" keylink/src/local.rs \
'        Standing::Gone => RemoteError::NoSuchActor { id: id.to_owned() },' \
'        Standing::Gone => RemoteError::Failed(format!("{DROPPED}{id}")),' \
  the_gone_answer_is_not_a_failed_and_carries_no_prefix -p keylink --lib

run "21 no floor between incarnations (N1)" keylink/src/local.rs \
'    let mut floor = Floor::armed(fate);
    let mut budget = Budget::new(limit);' \
'    let mut floor = Floor::armed(fate);
    floor.disarm();
    let mut budget = Budget::new(limit);' \
  a_supervisor_between_incarnations_is_gone_when_it_is_destroyed -p keylink --lib

run "22 a per-iteration floor writes Gone on continue (N2)" keylink/src/local.rs \
'    let mut floor = Floor::armed(fate);
    let mut budget = Budget::new(limit);
    loop {' \
'    let mut budget = Budget::new(limit);
    loop {
        let mut floor = Floor::armed(fate);' \
  a_transient_factory_failure_does_not_overwrite_a_pending_restart -p keylink --lib

if [ "$failed" -ne 0 ]; then
    echo "── $failed break(s) did not redden their guard"
    exit 1
fi
echo "── every recorded break replays and reddens its guard"
