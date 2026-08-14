//! A controller that is rebuilt after it faults, and the answer its callers get.
//!
//! ```sh
//! cargo run -p keylink --example substation
//! ```
//!
//! # The problem
//!
//! A battery site takes dispatch instructions from a grid operator. Each one is
//! parked until the operator confirms it. The controller talks to hardware that
//! sometimes reports something it cannot represent, and a controller that stays
//! down after one bad reading is worse than one that comes back.
//!
//! So the actor is supervised. What this example is really about is the half
//! that a supervisor cannot fix: a caller who was waiting when the incarnation
//! died.
//!
//! # Three answers, three different instructions
//!
//! [`Withheld`](keylink::Withheld) tells a caller what to do next, and the
//! three that matter here are not interchangeable.
//!
//! | answer | what happened | what the caller does |
//! |---|---|---|
//! | `Restarting { reason }` | the incarnation that owed the answer is gone, another is coming | retry |
//! | `Gone` | there is no next incarnation | stop calling |
//! | `Dropped` | the actor is alive and threw this one reply away | ask again, it will answer |
//!
//! Telling `Restarting` from `Gone` is the whole reason they are separate
//! variants, and both rounds below assert the exact payload rather than a
//! wildcard.
//!
//! # A panic alone does not restart anything
//!
//! The loop catches a panicking turn and runs the next message against the
//! state as it stands. **An observer is what turns a fault into an ending, and
//! therefore into a restart.** `RefuseFaults` below is that observer, and
//! without it this example would demonstrate nothing.
//!
//! # Nothing reports a restart, so the factory does
//!
//! There is no restart callback, no restart counter, and `ended()` does not
//! resolve on a restart — a supervised actor's incarnations share one task, and
//! only that task's exit publishes an ending. `Lifecycle` has `mounted` and
//! `ended`, and neither fires. So the one thing that can say a restart happened
//! is the factory that performed it, and it announces on a channel here.
//!
//! # The panic notices in the output are this example working
//!
//! Two turns really do panic, and Rust prints each one. A run with no panic
//! notice would mean the faults never happened, and every assertion below would
//! have passed for the wrong reason.

use std::error::Error as StdError;
use std::num::NonZeroU32;
use std::time::Duration;

use keylink::prelude::*;
use keylink::{
    Allowlist, Caller, Deferred, EndReason, Ended, Intensity, Node, Observer, RemoteError, Replied,
    Reply, Stop, TurnFut, TurnInfo, TurnOutcome, Unanswered, Withheld,
};
use postcard_schema::Schema;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;

/// A confirmed dispatch instruction.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Schema)]
pub struct Ticket {
    /// How much was dispatched.
    pub megawatts: u32,
}

keylink::service! {
    service Site;

    /// Take an instruction. Parked until the operator confirms it.
    #[defer]
    rpc Dispatch(megawatts: u32) -> Ticket;

    /// Confirm every parked instruction, and say how many.
    rpc Confirm() -> u32;

    /// The hardware reported something this controller cannot represent.
    ///
    /// A `tell` and not an `rpc`, and the reason is mechanical rather than
    /// stylistic: a local `rpc` resumes the handler's panic in its caller, so
    /// an `rpc` here would unwind this example instead of faulting the actor. A
    /// `tell` drops its reply receiver before sending, so the panic stays where
    /// it belongs.
    tell Fault() -> ();
}

/// One site controller.
struct Site {
    parked: Vec<Deferred<Ticket>>,
    /// Fires from inside the turn, after the reply is parked.
    ///
    /// This is what replaces a sleep. The signal is sent inside the handler, so
    /// receiving it proves the park is committed to the state — and a fault
    /// that overtook the park would otherwise land the parked reply in the next
    /// incarnation, where it would wait for ever.
    parked_tx: mpsc::UnboundedSender<()>,
}

impl site::Actor for Site {
    async fn dispatch(
        &mut self,
        _cx: Caller<'_>,
        reply: Reply<Ticket>,
        megawatts: u32,
    ) -> Replied<Ticket> {
        let (deferred, replied) = reply.later();
        self.parked.push(deferred);
        let _ = self.parked_tx.send(());
        let _ = megawatts;
        replied
    }

    async fn confirm(&mut self, _cx: Caller<'_>) -> u32 {
        let mut confirmed = 0;
        for deferred in std::mem::take(&mut self.parked) {
            deferred.send(Ticket { megawatts: 50 });
            confirmed += 1;
        }
        confirmed
    }

    async fn fault(&mut self, _cx: Caller<'_>) {
        panic!("the inverter reported an impossible setpoint");
    }
}

/// Turn a faulted turn into an ending, which is what makes it a restart.
///
/// Without this the actor keeps its state and answers the next message, and
/// nothing restarts at all.
struct RefuseFaults;

impl Observer<Site> for RefuseFaults {
    fn name(&self) -> &'static str {
        "refuse-faults"
    }

    fn after_turn<'a>(&'a mut self, _state: &'a Site, turn: TurnInfo<'a>) -> TurnFut<'a> {
        let faulted = matches!(turn.outcome, TurnOutcome::Panicked { .. });
        Box::pin(async move {
            if faulted {
                return Err(Stop::new("a faulted turn is not committed"));
            }
            Ok(())
        })
    }
}

/// One restart, allowed once, with a backoff small enough to run in a gate.
///
/// The backoff is `recovered_after / max` and it is a real sleep in a real
/// process. Every test of this machinery hides it under a paused clock, and an
/// example has no such thing, so the span is kept small on purpose.
fn budget() -> Option<Intensity> {
    Intensity::new(NonZeroU32::new(1)?, Duration::from_millis(50))
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn StdError>> {
    let (announce, mut incarnations) = mpsc::unbounded_channel::<u32>();
    let (parked_tx, mut parked) = mpsc::unbounded_channel::<()>();

    // ── 1. The supervised actor.
    //
    //  The factory runs once per incarnation and rebuilds everything: the
    //  state, the observers, and the feeds. State is NOT carried across — a
    //  durable actor re-reads its store here, which is what
    //  `durable_ledger.rs` does with `Persist`.
    let mut built = 0;
    let site = site::restarting(
        move || {
            let announce = announce.clone();
            let parked_tx = parked_tx.clone();
            let at = built;
            built += 1;
            async move {
                let _ = announce.send(at);
                let observers: Vec<Box<dyn Observer<Site>>> = vec![Box::new(RefuseFaults)];
                Ok((
                    Site {
                        parked: Vec::new(),
                        parked_tx,
                    },
                    observers,
                    (),
                ))
            }
        },
        budget().ok_or("a recovery span whose backoff is not zero")?,
    )
    .actor();

    let node = Node::bind_local().await?;
    let operator_node = Node::bind_local().await?;
    let _serving = site.serve(&node, Allowlist::anyone())?;
    let operator = site::Client::connect(&operator_node, node.endpoint().addr()).await?;

    assert_eq!(
        incarnations.recv().await,
        Some(0),
        "the first incarnation announces itself"
    );

    // ── 2. Two parked callers, one local and one over QUIC.
    //
    //  Both are blocked on one ordinary call. Awaiting the park signal is what
    //  makes the fault below land after them rather than racing them.
    let local = {
        let site = site.clone();
        tokio::spawn(async move { site.dispatch(50).await })
    };
    parked.recv().await.ok_or("the local caller never parked")?;

    let remote = {
        let operator = operator.clone();
        tokio::spawn(async move { operator.dispatch(30).await })
    };
    parked.recv().await.ok_or("the peer never parked")?;

    // ── 3. The fault. The observer refuses the turn, the incarnation ends, and
    //       the supervisor builds another.
    let _ = site.fault().await?;
    assert_eq!(
        incarnations.recv().await,
        Some(1),
        "the restart is reported by the thing that performed it"
    );

    // ── 4. What each parked caller reads.
    //
    //  The exact payload, not a wildcard. A caller that matched only the
    //  variant could not tell a fault from a dead feed, and the reason is what
    //  an operator pages on.
    let lost_local = match local.await? {
        Ok(ticket) => {
            return Err(format!("a dead incarnation must answer nobody: {ticket:?}").into())
        }
        Err(withheld) => withheld,
    };
    assert_eq!(
        lost_local,
        Unanswered::Turn(Withheld::Restarting {
            reason: EndReason::Panicked {
                message: "the actor panicked: the inverter reported an impossible setpoint"
                    .to_owned(),
            },
        }),
    );
    println!("the local caller was told: {lost_local}");

    let lost_remote = match remote.await? {
        Ok(ticket) => return Err(format!("the peer must not be answered: {ticket:?}").into()),
        Err(error) => error,
    };
    assert!(
        matches!(
            &lost_remote,
            keylink::Error::Remote(RemoteError::Withheld(Withheld::Restarting { .. }))
        ),
        "a peer must read the same instruction, got {lost_remote:?}"
    );
    println!("the peer was told:          {lost_remote}");

    // ── 5. "Retry" is real advice, so retrying has to work.
    //
    //  A new incarnation is serving on the same handle and the same ALPN. The
    //  caller does not reconnect and does not re-address anything.
    let retried = {
        let operator = operator.clone();
        tokio::spawn(async move { operator.dispatch(30).await })
    };
    parked.recv().await.ok_or("the retry never parked")?;
    assert_eq!(site.confirm().await?, 1, "the new incarnation answers");
    assert_eq!(retried.await??.megawatts, 50);
    println!("the retry was confirmed by the second incarnation");

    // ── 6. The second fault, with nothing left in the budget.
    //
    //  One restart was allowed and one was spent. This fault ends the actor for
    //  good, and the answer a caller gets changes from "retry" to "stop".
    let doomed = {
        let site = site.clone();
        tokio::spawn(async move { site.dispatch(10).await })
    };
    parked.recv().await.ok_or("the last caller never parked")?;
    let _ = site.fault().await?;

    let gone = match doomed.await? {
        Ok(ticket) => {
            return Err(format!("an actor that gave up answers nobody: {ticket:?}").into())
        }
        Err(withheld) => withheld,
    };
    assert_eq!(
        gone,
        Unanswered::Turn(Withheld::Gone),
        "no next incarnation is coming, and the answer has to say so"
    );
    println!("after the budget ran out:   {gone}");

    // ── 7. The ending itself names how many restarts were spent.
    //
    //  `after` is a number the supervisor counted. There is no counter on the
    //  actor, and `LocalActor::incarnations()` deliberately does not exist.
    match site.ended().await {
        Ended::GaveUp { after, error } => {
            assert_eq!(after, 1, "one restart was budgeted and one was spent");
            println!("the actor gave up after {after} restart: {error}");
        }
        other => return Err(format!("a spent budget must end the actor, got {other:?}").into()),
    }
    Ok(())
}
