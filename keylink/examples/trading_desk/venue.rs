//! The execution desk, and the two market feeds.

use std::error::Error as StdError;

use keylink::compose::{BroadcastDepth, Lossy, LossyTopic, Overflow, Topic};
use keylink::{depth, Caller};

use crate::{desk, Fill, Print, Quote};

/// The one venue session.
///
/// Serial turns are the point rather than an implementation detail: two writers
/// on one session is the failure this shape removes.
#[derive(Default)]
pub struct Desk {
    sequence: u64,
    filled: u64,
}

impl desk::Actor for Desk {
    async fn execute(&mut self, _cx: Caller<'_>, _robot: String, qty: u64) -> Fill {
        self.sequence += 1;
        self.filled += qty;
        Fill {
            sequence: self.sequence,
            qty,
        }
    }

    async fn filled(&mut self, _cx: Caller<'_>) -> u64 {
        self.filled
    }
}

/// Two feeds with two backpressure policies, and a reason for each.
///
/// | feed | policy | why |
/// |---|---|---|
/// | quotes | `Lossy::DropOldest` | A stale price is worse than no price. A slow robot gets the newest quote and an exact count of what it missed. |
/// | prints | `Overflow::BlockPublisher` | A print is a fact about the tape. A robot that skips one has a wrong picture, so the publisher waits instead. |
///
/// Neither is published from inside a turn. A topic is not an actor, so no call
/// chain reaches it, and a publish made from inside a turn would wait for a
/// subscriber that cannot run until that turn returns.
pub struct Market {
    /// Quotes, which may be dropped and are always counted.
    pub quotes: LossyTopic<Quote>,
    /// Prints, which are never dropped.
    pub prints: Topic<Print>,
}

impl Market {
    /// Build the pair.
    ///
    /// # Errors
    ///
    /// Returns an error when the broadcast depth is not a power of two. The
    /// ring underneath is one, and the type refuses anything else rather than
    /// rounding it silently.
    pub fn new() -> Result<Self, Box<dyn StdError>> {
        Ok(Self {
            quotes: LossyTopic::new(Lossy::DropOldest(BroadcastDepth::try_new(depth!(4))?)),
            prints: Topic::new(Overflow::BlockPublisher(depth!(8))),
        })
    }
}
