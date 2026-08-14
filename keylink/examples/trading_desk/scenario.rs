//! The scenario, written once against `impl robot::Calls`.
//!
//! Nothing in this file names a `Handle`, an `ActorRef`, a node or a
//! connection. That is the point: `main` runs it against a robot on this task
//! and against the same robot behind QUIC, and every deterministic number has
//! to match.

use std::error::Error as StdError;
use std::time::Duration;

use tokio::sync::Semaphore;

use crate::venue::{Desk, Market};
use crate::{desk, robot, Position, Print, Quote};

/// Quotes published at the pace of the fast robot.
pub const QUOTES: u64 = 16;
/// Quotes published after both robots are halted.
pub const AFTER_HALT: u64 = 4;
/// Prints published on the feed that drops nothing.
pub const PRINTS: u64 = 4;

/// What one run of the scenario measured.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Books {
    /// The robot the publisher paced itself to.
    pub fast: Position,
    /// The robot that was stalled while the market ran on.
    pub slow: Position,
    /// The desk's own total.
    pub filled: u64,
}

/// Poll one surface until `ready` accepts its position, then hand it back.
async fn until<C: robot::Calls>(
    surface: &C,
    what: &str,
    ready: impl Fn(&Position) -> bool,
) -> Result<Position, Box<dyn StdError>> {
    for _ in 0..600 {
        let position = surface.position().await?;
        if ready(&position) {
            return Ok(position);
        }
        n0_future::time::sleep(Duration::from_millis(10)).await;
    }
    Err(format!("the robot never {what}").into())
}

/// Run the whole scenario against one pair of surfaces.
///
/// # Errors
///
/// Returns an error when a surface stops answering, or when a wait does not
/// reach its condition inside the bound.
pub async fn run<C: robot::Calls>(
    market: &Market,
    fast: &C,
    slow: &C,
    held: &Semaphore,
    desk: &desk::Handle<Desk>,
) -> Result<Books, Box<dyn StdError>> {
    // ── 1. Quotes into a ring that holds four, paced to the fast robot.
    //
    //  Each publish waits for `fast` to have processed it, so `fast` cannot lag
    //  by construction. That is the control: without it, a skip count of zero
    //  would be luck rather than evidence.
    for tick in 1..=QUOTES {
        market.quotes.publish(Quote { cents: tick * 100 });
        until(fast, "processed a quote", |p| p.seen == tick).await?;
    }
    let paced = fast.position().await?;
    assert_eq!(paced.seen, QUOTES);
    assert_eq!(paced.skipped, 0, "the paced consumer must miss nothing");

    // ── 2. Release the stalled robot, and wait for it to account for the
    //       arrears.
    //
    //  The wait condition is the invariant: every published quote is either
    //  processed or reported skipped, exactly once. Which of the two it was is
    //  a timing outcome, and it is deliberately not asserted.
    held.add_permits(Semaphore::MAX_PERMITS);
    let drained = until(slow, "accounted for every quote", |p| {
        p.seen + p.skipped == QUOTES
    })
    .await?;
    assert!(
        drained.skipped > 0,
        "a stalled subscriber under DropOldest must report what it missed"
    );

    // ── 3. The other policy, on the same pair.
    //
    //  `publish` returns when every subscriber's channel has accepted the
    //  item, so a slow robot slows the tape rather than losing it. Both counts
    //  reach the same number, and neither can report a gap, because this feed
    //  has none to report.
    for _ in 0..PRINTS {
        market.prints.publish(Print { qty: 1 }).await;
    }
    for (surface, who) in [(fast, "fast"), (slow, "slow")] {
        let read = until(surface, "read the tape", |p| p.prints == PRINTS).await?;
        assert_eq!(read.prints, PRINTS, "{who} lost a print");
    }

    // ── 4. The accounting identity, which is the real assertion.
    //
    //  Every order went through one serial desk, so the desk's total is the sum
    //  of the two books. Neither robot can hold something the desk did not
    //  execute.
    let before_halt = (fast.position().await?, slow.position().await?);
    let filled = desk.filled().await?;
    assert_eq!(
        filled,
        before_halt.0.filled + before_halt.1.filled,
        "the desk filled what the robots hold, and nothing else"
    );
    assert!(filled > 0, "control: the robots actually traded");

    // ── 5. Halt, and keep the market running.
    //
    //  A halted robot keeps reading and stops acting. A halted robot that
    //  stopped consuming would fill its channel and take the policy decision
    //  away from whoever halted it.
    fast.halt().await?;
    slow.halt().await?;
    for tick in 1..=AFTER_HALT {
        market.quotes.publish(Quote { cents: tick * 100 });
        until(fast, "kept reading the market", |p| p.seen == QUOTES + tick).await?;
    }
    let after = (
        until(fast, "settled", |p| p.seen == QUOTES + AFTER_HALT).await?,
        until(slow, "accounted for every quote", |p| {
            p.seen + p.skipped == QUOTES + AFTER_HALT
        })
        .await?,
    );

    assert!(after.0.halted && after.1.halted);
    assert_eq!(
        after.0.filled, before_halt.0.filled,
        "a halted robot reads the market and does not trade on it"
    );
    assert_eq!(
        desk.filled().await?,
        filled,
        "and the desk saw nothing new from either of them"
    );

    Ok(Books {
        fast: after.0,
        slow: after.1,
        filled,
    })
}
