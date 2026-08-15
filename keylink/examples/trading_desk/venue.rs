//! The execution desk, and the two market feeds.

use std::error::Error as StdError;

use keylink::compose::{
    BroadcastDepth, Fanout, Lossy, LossyTopic, Overflow, Patience, PatienceError, Subscription,
    Topic,
};
use keylink::{depth, Caller};
//  `Subscription` is a `Stream`, and `next` comes from the extension trait.
//  keylink re-exports `Stream` itself, so a caller needs no stream crate of its
//  own — this is the runtime shim the rest of the crate uses.
use n0_future::StreamExt;

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

/// Park a subscriber past its patience, and report what the publisher did.
///
/// The third `Overflow` policy, which neither feed above uses.
/// [`Overflow::BlockPublisher`] waits for ever and
/// [`Lossy::DropOldest`] never waits at all; this one waits a stated amount and
/// then **unsubscribes the laggard**, which is the choice for a feed where a
/// slow consumer must not hold the publisher and must not silently miss items
/// either.
///
/// # Deterministic without a clock, and that is by construction
///
/// The parked subscriber below is never polled, so its depth-1 channel cannot
/// free and the publisher always exhausts its patience. Wall-clock time changes
/// how long this takes. It never changes the outcome.
///
/// # Errors
///
/// Returns [`PatienceError`](keylink::compose::PatienceError) when the span is
/// not a non-zero whole number of milliseconds inside `i32::MAX`. The bound is
/// portability: the value has to fit a browser's `setTimeout`.
pub async fn patience() -> Result<(), Box<dyn StdError>> {
    //  Fractional milliseconds are refused rather than rounded, so a caller
    //  cannot ask for a span the browser cannot express.
    let fractional = Patience::try_after(std::time::Duration::from_micros(1_500))
        .err()
        .ok_or("a patience must be whole milliseconds")?;
    //  Named rather than merely counted as an error: a `PatienceError` is the
    //  only thing this constructor can refuse with, so matching it is a caller
    //  stating which of the two constructors it used.
    let _: PatienceError = fractional;
    let patience = Patience::try_after(std::time::Duration::from_millis(50))?;

    let topic: Topic<u64> = Topic::new(Overflow::BlockThenDrop {
        depth: depth!(1),
        patience,
    });
    //  Subscribed and never polled. That is the laggard.
    let mut parked: Subscription<u64> = topic.subscribe();

    let accepted: Fanout = topic.publish(1).await;
    assert_eq!(accepted.delivered, 1, "the one slot took the first item");

    //  The channel is full and nothing is draining it, so the publisher waits
    //  out its patience and then removes the subscriber. `dropped` is the count
    //  of subscribers unsubscribed for being too slow, and it is always zero
    //  under `BlockPublisher`.
    let gave_up = topic.publish(2).await;
    assert_eq!(gave_up.delivered, 0);
    assert_eq!(
        gave_up.dropped, 1,
        "the laggard was unsubscribed, not waited on"
    );
    assert_eq!(topic.subscribers(), 0);

    //  What it accepted before it was dropped still arrives. Then the stream
    //  ends, which is what makes the loss loud rather than silent: a subscriber
    //  actor reads it as `Ended::Feed`.
    assert_eq!(parked.next().await, Some(1), "it keeps what it accepted");
    assert_eq!(parked.next().await, None, "and then its stream ends");

    //  The third field. A subscriber whose `Subscription` was dropped is `gone`
    //  rather than `dropped`: nobody was too slow, the receiver simply left.
    let live = topic.subscribe();
    drop(topic.subscribe());
    let mixed = topic.publish(3).await;
    assert_eq!(
        (mixed.delivered, mixed.dropped, mixed.gone),
        (1, 0, 1),
        "one live subscriber, one abandoned"
    );
    drop(live);

    println!(
        "patience: the publisher gave up on the laggard ({gave_up:?}), and a departed subscriber is `gone` ({mixed:?})"
    );
    Ok(())
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
