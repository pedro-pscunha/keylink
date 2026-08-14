//! One trading scenario, run twice: in process, and over QUIC.
//!
//! ```sh
//! cargo run -p keylink --example trading_desk
//! ```
//!
//! # The claim
//!
//! The serving shape is a deployment decision, not a code decision. The
//! scenario in `scenario.rs` is written once, against `impl robot::Calls`, and
//! `main` runs it against two different deployments of the same robots:
//!
//! | deployment | robots | surface the scenario holds |
//! |---|---|---|
//! | local | mounted in a `Registry` on this task | `robot::LocalRef<'_>` |
//! | served | the same shape, dialled over QUIC | `robot::ActorRef<'_>` |
//!
//! Both runs must agree on every deterministic number. That is the assertion,
//! and it is what "one function, every surface" means when a program rather
//! than a test says it.
//!
//! The two deployments differ in **one** line each — `robot::at_local(..)`
//! against `client.at(..)`. Same registry type, same mount, same actor, same
//! scenario. Nothing else moves, which is the claim.
//!
//! # Why the desk is not the thing being compared
//!
//! Each robot calls one execution desk from inside its own turn. The desk is a
//! singleton actor in both deployments, because "the venue accepts one session"
//! does not change when a robot moves. Only the robots' surface changes.
//!
//! # What is deterministic here, and what is not
//!
//! The market feed is lossy on purpose, so how a stalled robot's arrears split
//! between processed and skipped is a timing outcome. Two things are not
//! timing outcomes, and both are asserted in both runs:
//!
//! - every published quote is either processed or reported skipped, exactly
//!   once, so the two counts sum to the number published;
//! - the desk's total equals the sum of the two books, because one serial desk
//!   executed every order.

use std::error::Error as StdError;
use std::sync::Arc;
use std::time::Duration;

use keylink::compose::Lagged;
use keylink::prelude::*;
use keylink::{ActorId, Allowlist, Eviction, Node, Server};
use postcard_schema::Schema;
use serde::{Deserialize, Serialize};
use tokio::sync::Semaphore;

mod scenario;
mod strategy;
mod venue;

use scenario::Books;
use strategy::Robot;
use venue::{Desk, Market};

/// One quote off the market feed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Schema)]
pub struct Quote {
    /// The price, in cents.
    pub cents: u64,
}

/// One public trade print off the tape.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Schema)]
pub struct Print {
    /// How much traded.
    pub qty: u64,
}

/// What the desk did with one order.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Schema)]
pub struct Fill {
    /// The sequence the desk gave it. Serial, because one session is serial.
    pub sequence: u64,
    /// How much was filled.
    pub qty: u64,
}

/// Everything a caller can read off a robot without stopping it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Schema)]
pub struct Position {
    /// Quotes this robot processed.
    pub seen: u64,
    /// Quotes the market feed dropped before this robot could read them.
    pub skipped: u64,
    /// Prints this robot processed.
    pub prints: u64,
    /// Quantity the desk filled for this robot.
    pub filled: u64,
    /// Whether a caller has stopped it.
    pub halted: bool,
}

keylink::service! {
    service Desk;

    /// Execute one order. Serial, because the desk is one actor.
    rpc Execute(robot: String, qty: u64) -> Fill;

    /// Everything the desk has filled, for the accounting identity.
    rpc Filled() -> u64;
}

keylink::service! {
    service Robot;

    /// The market. `Lagged` and not `Quote`, so a gap is a value.
    feed Quotes(quote: Lagged<Quote>);

    /// The tape. No `Lagged`, because this feed drops nothing.
    feed Prints(print: Print);

    /// What this robot holds. On `Calls`, so the scenario reads it through any
    /// surface without naming one.
    rpc Position() -> Position;

    /// Stop trading. An `rpc` returning `()` and not a `tell`, because an
    /// `ActorRef` carries no `tell` at all.
    rpc Halt() -> ();
}

/// Mount `alpha` and `bravo` into one registry.
///
/// Used by both deployments, unchanged. A registry does not know whether the
/// callers reaching it are in this process or on a connection.
fn mount_pair(
    market: &Market,
    desk: &desk::Handle<Desk>,
    open: &Arc<Semaphore>,
    held: &Arc<Semaphore>,
) -> Result<(robot::Registry, ActorId, ActorId), Box<dyn StdError>> {
    let system: robot::Registry = robot::Registry::builder(Eviction::Never)
        .capacity(64)
        .build();
    let alpha = ActorId::new("robot/alpha")?;
    let bravo = ActorId::new("robot/bravo")?;
    for (id, gate) in [(&alpha, open), (&bravo, held)] {
        system.mount(
            id.clone(),
            robot::setup(Robot::new(&id.to_string(), desk.clone(), Arc::clone(gate)))
                .feeds(robot::Feeds::new(
                    market.quotes.subscribe(),
                    market.prints.subscribe(),
                ))
                .mountable(),
        )?;
    }
    Ok((system, alpha, bravo))
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn StdError>> {
    // ── 1. The local deployment. A keyed registry, and no node, no ALPN and no
    //       connection anywhere near it.
    let local_market = Market::new()?;
    let local_desk = desk::actor(Desk::default());
    let local_held = Arc::new(Semaphore::new(0));
    let local_open = Arc::new(Semaphore::new(Semaphore::MAX_PERMITS));
    let (local, local_alpha, local_bravo) =
        mount_pair(&local_market, &local_desk, &local_open, &local_held)?;

    let in_process: Books = scenario::run(
        &local_market,
        &robot::at_local(&local, local_alpha.clone()),
        &robot::at_local(&local, local_bravo.clone()),
        &local_held,
        &local_desk,
    )
    .await?;
    println!(
        "in process: fast filled {}, slow filled {}, desk {}",
        in_process.fast.filled, in_process.slow.filled, in_process.filled
    );

    // ── 2. The served deployment. Same robots, same desk, same scenario, and
    //       every call in it now crosses a real QUIC connection.
    let venue_node = Node::bind_local().await?;
    let operator_node = Node::bind_local().await?;

    let served_market = Market::new()?;
    let served_desk = desk::actor(Desk::default());
    let served_held = Arc::new(Semaphore::new(0));
    let served_open = Arc::new(Semaphore::new(Semaphore::MAX_PERMITS));

    //  The same function that built the local pair, called again. If the two
    //  deployments differed in how the actors are built, the comparison below
    //  would be comparing two programs rather than two surfaces.
    let (system, alpha, bravo) =
        mount_pair(&served_market, &served_desk, &served_open, &served_held)?;

    //  One node serving a keyed registry and a singleton at once. The two
    //  ALPNs differ, so neither knows about the other.
    let _server = Server::builder(&venue_node)
        .service(system.service(Allowlist::anyone()).build())
        .service(served_desk.service(Allowlist::anyone()).build())
        .spawn()?;
    let operator =
        robot::RegistryClient::connect(&operator_node, venue_node.endpoint().addr()).await?;
    let books = desk::Client::connect(&operator_node, venue_node.endpoint().addr()).await?;

    let over_quic: Books = scenario::run(
        &served_market,
        &operator.at(alpha.clone()),
        &operator.at(bravo.clone()),
        &served_held,
        &served_desk,
    )
    .await?;
    println!(
        "over QUIC:  fast filled {}, slow filled {}, desk {}",
        over_quic.fast.filled, over_quic.slow.filled, over_quic.filled
    );

    // ── 3. The comparison, which is what this example exists for.
    //
    //  The paced robot is fully determined: it processed every quote, skipped
    //  none, and filled the sum of their sizes. Both deployments must agree on
    //  all of it, because the robot did not change — only how it is addressed.
    assert_eq!(
        in_process.fast, over_quic.fast,
        "the paced robot must behave identically in process and over QUIC"
    );
    println!(
        "identical on both surfaces: {:?}",
        (in_process.fast.seen, in_process.fast.filled)
    );

    //  The stalled robot's arrears split by timing, so the split is NOT
    //  compared. What is compared is the invariant underneath it, which both
    //  runs already asserted: nothing was lost without being counted.
    let published = scenario::QUOTES + scenario::AFTER_HALT;
    assert_eq!(in_process.slow.seen + in_process.slow.skipped, published);
    assert_eq!(over_quic.slow.seen + over_quic.slow.skipped, published);
    println!(
        "the stalled robot's split differs by timing ({} against {} processed) and its total does not",
        in_process.slow.seen, over_quic.slow.seen
    );

    // ── 4. One extra assertion the served deployment can make and the local
    //       one cannot: the desk's own total, read over the wire by a third
    //       party rather than through the handle this process holds.
    assert_eq!(
        books.filled().await?,
        over_quic.filled,
        "the served desk agrees with the handle"
    );

    let _ = system.stop(&alpha, Duration::from_secs(1)).await;
    let _ = system.stop(&bravo, Duration::from_secs(1)).await;
    served_desk.stop().await?;
    let _ = local.stop(&local_alpha, Duration::from_secs(1)).await;
    let _ = local.stop(&local_bravo, Duration::from_secs(1)).await;
    local_desk.stop().await?;
    Ok(())
}
