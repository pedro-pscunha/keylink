//! Many matches on one node, each answering two players who wait together.
//!
//! ```sh
//! cargo run -p keylink --example match_room
//! ```
//!
//! Four nodes on the loopback interface: a server and three players.
//! `Node::bind_local` binds with no relay and no discovery. A real program uses
//! [`Node::bind`].
//!
//! # What a keyed actor buys here
//!
//! A match is a small piece of state with a lifetime of its own, and a server
//! runs many at once. That is a [`Registry`](keylink::Registry): one
//! [`ActorId`] per match, one task per match, serial turns inside each, and
//! full concurrency between them. Players address a match by id over one
//! connection.
//!
//! # Two callers, one event
//!
//! A round resolves only when both seats have chosen, so neither `play` call
//! can answer inside its own turn. Each handler parks its reply with
//! [`Reply::later`], and the turn that completes the pair answers both. This is
//! the case a single-reply model cannot express: the caller sees one ordinary
//! `rpc`, and the actor decides when it returns.
//!
//! # The clock is a feed, and a feed item is a turn
//!
//! A stalled round must not park a player forever, so the room takes a second
//! input: a local [`Stream`](keylink::Stream) of ticks, selected against the
//! mailbox inside the actor's own loop. A tick is a turn like any other — same
//! panic guard, same observer chain — and it is not on the wire, so adding one
//! does not move the ALPN.
//!
//! The ticks here are sent by hand rather than by a timer. A test that waits
//! for real time is a test that goes red on a loaded machine.
//!
//! # What this example does not show
//!
//! Supervision. A room that panics keeps its state and loses that one message,
//! which is the unsupervised behaviour. [`restarting`](match_room::restarting)
//! rebuilds an actor instead, and it changes what a parked reply means — the
//! incarnation that owed the answer is gone, and the caller reads
//! [`Withheld::Restarting`](keylink::Withheld::Restarting). That deserves its
//! own example rather than a corner of this one.

use std::error::Error as StdError;
use std::time::Duration;

use keylink::prelude::*;
use keylink::{ActorId, Allowlist, Caller, Deferred, Eviction, Node, Replied, Reply};
use postcard_schema::Schema;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;

/// Which side of the board a player sits on.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Schema)]
pub enum Seat {
    /// The player who moves first in this example.
    North,
    /// The player who completes the pair.
    South,
}

/// What a player chose this round.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Schema)]
pub enum Choice {
    /// Beats `Scissors`.
    Rock,
    /// Beats `Rock`.
    Paper,
    /// Beats `Paper`.
    Scissors,
}

impl Choice {
    /// Did this choice beat the other one?
    fn beats(self, other: Self) -> bool {
        matches!(
            (self, other),
            (Self::Rock, Self::Scissors)
                | (Self::Paper, Self::Rock)
                | (Self::Scissors, Self::Paper)
        )
    }
}

/// One resolved round, sent to both seats at the same moment.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Schema)]
pub struct Round {
    /// Which round this was.
    pub number: u32,
    /// The winner, or `None` for a draw.
    pub winner: Option<Seat>,
    /// True when the clock ended the round rather than the players.
    pub forfeit: bool,
}

/// What an observer can read without playing.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Schema)]
pub struct Board {
    /// How many rounds have resolved.
    pub played: u32,
    /// How many seats are parked, waiting for the other.
    pub parked: u32,
}

keylink::service! {
    service MatchRoom;

    /// Choose. Parked until the other seat chooses, or until the clock ends it.
    ///
    /// `Play` and not `Move`: a declaration becomes a `snake_case` method, and
    /// `move` is a Rust keyword, which the macro rejects at the declaration.
    #[defer]
    rpc Play(seat: Seat, choice: Choice) -> Round;

    /// The board, for an operator or for a player that reconnected.
    rpc Board() -> Board;

    /// The round clock. Two ticks with a seat still parked ends the round.
    feed Clock(tick: u64);
}

/// One match. Nothing here is shared, because nothing else touches it.
#[derive(Default)]
struct Room {
    played: u32,
    waiting: Vec<(Seat, Choice, Deferred<Round>)>,
    idle_ticks: u64,
}

impl Room {
    /// Answer both seats and clear the round.
    fn resolve(&mut self, forfeit: bool) {
        self.played += 1;
        self.idle_ticks = 0;
        let winner = match self.waiting.as_slice() {
            [(seat, _, _)] if forfeit => Some(opposite(*seat)),
            [(north, first, _), (south, second, _)] => {
                if first.beats(*second) {
                    Some(*north)
                } else if second.beats(*first) {
                    Some(*south)
                } else {
                    None
                }
            }
            _ => None,
        };
        let round = Round {
            number: self.played,
            winner,
            forfeit,
        };
        for (_, _, deferred) in std::mem::take(&mut self.waiting) {
            deferred.send(round.clone());
        }
    }
}

/// The other seat.
fn opposite(seat: Seat) -> Seat {
    match seat {
        Seat::North => Seat::South,
        Seat::South => Seat::North,
    }
}

impl match_room::Actor for Room {
    async fn play(
        &mut self,
        _cx: Caller<'_>,
        reply: Reply<Round>,
        seat: Seat,
        choice: Choice,
    ) -> Replied<Round> {
        let (deferred, replied) = reply.later();
        self.waiting.push((seat, choice, deferred));
        if self.waiting.len() == 2 {
            self.resolve(false);
        }
        replied
    }

    async fn board(&mut self, _cx: Caller<'_>) -> Board {
        Board {
            played: self.played,
            parked: self.waiting.len() as u32,
        }
    }

    async fn on_clock(&mut self, _tick: u64) {
        if self.waiting.is_empty() {
            return;
        }
        self.idle_ticks += 1;
        if self.idle_ticks >= 2 {
            self.resolve(true);
        }
    }
}

/// Wait for the board to report `parked` seats, by asking rather than sleeping.
async fn parked(
    client: &match_room::RegistryClient,
    room: &ActorId,
    want: u32,
) -> Result<(), Box<dyn StdError>> {
    for _ in 0..400 {
        if client.at(room.clone()).board().await?.parked == want {
            return Ok(());
        }
        n0_future::time::sleep(Duration::from_millis(10)).await;
    }
    Err(format!("the board never reported {want} parked seat(s)").into())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn StdError>> {
    let server_node = Node::bind_local().await?;
    let north_node = Node::bind_local().await?;
    let south_node = Node::bind_local().await?;

    // ── 1. A registry with an idle policy, and two matches in it.
    //
    //  `Eviction` has no default, so this is a decision the builder makes you
    //  state. `after` is a staleness threshold and `shutdown_budget` is the
    //  time a stopping actor gets to drain — unrelated quantities that a single
    //  number would conflate.
    let system: match_room::Registry = match_room::Registry::builder(Eviction::Idle {
        after: Duration::from_millis(200),
        shutdown_budget: Duration::from_secs(1),
    })
    .capacity(64)
    .build();

    let derby = ActorId::new("match/derby")?;
    let abandoned = ActorId::new("match/abandoned")?;
    let (ticks, tick_rx) = mpsc::channel::<u64>(4);

    system.mount(
        derby.clone(),
        match_room::setup(Room::default())
            .feeds(match_room::Feeds::new(clock(tick_rx)))
            .mountable(),
    )?;
    //  A second feed channel, because a feed that ENDS ends its actor. Holding
    //  the sender is what makes this room merely quiet rather than finished.
    let (_never, quiet_rx) = mpsc::channel::<u64>(1);
    system.mount(
        abandoned.clone(),
        match_room::setup(Room::default())
            .feeds(match_room::Feeds::new(clock(quiet_rx)))
            .mountable(),
    )?;

    let _server = system.serve(&server_node, Allowlist::anyone())?;
    let north =
        match_room::RegistryClient::connect(&north_node, server_node.endpoint().addr()).await?;
    let south =
        match_room::RegistryClient::connect(&south_node, server_node.endpoint().addr()).await?;
    println!("two matches mounted, {} live", system.len());

    // ── 2. One round, two callers, one answer.
    //
    //  North's call parks. It is an ordinary `rpc` to north, so it blocks, and
    //  it runs on its own task while this one plays south.
    let northern = {
        let north = north.clone();
        let derby = derby.clone();
        tokio::spawn(async move { north.at(derby).play(Seat::North, Choice::Rock).await })
    };
    parked(&south, &derby, 1).await?;

    let southern = south
        .at(derby.clone())
        .play(Seat::South, Choice::Paper)
        .await?;
    let northern = northern.await??;
    assert_eq!(
        northern, southern,
        "one round, one result, delivered to both seats"
    );
    assert_eq!(northern.winner, Some(Seat::South), "paper covers rock");
    println!(
        "round {} went to {:?}, and both players read the same answer",
        northern.number, northern.winner
    );

    // ── 3. The clock, which is the actor's second input.
    //
    //  North plays alone. Two ticks later the room ends the round rather than
    //  leaving a player parked on a call that nobody will complete.
    let stalled = {
        let north = north.clone();
        let derby = derby.clone();
        tokio::spawn(async move { north.at(derby).play(Seat::North, Choice::Scissors).await })
    };
    parked(&south, &derby, 1).await?;
    ticks.send(1).await?;
    ticks.send(2).await?;

    let forfeited = stalled.await??;
    assert!(forfeited.forfeit, "the clock ended this one");
    assert_eq!(
        forfeited.winner,
        Some(Seat::South),
        "the seat that did not answer loses it"
    );
    assert_eq!(south.at(derby.clone()).board().await?.played, 2);
    println!("round {} was ended by the clock", forfeited.number);

    // ── 4. Eviction, which is what keeps ten thousand of these affordable.
    //
    //  The sweeper stops an actor nothing has addressed. Touching `derby` in
    //  this loop is the control: it proves the sweeper removes the idle match
    //  and not simply every match.
    let mut swept = false;
    for _ in 0..200 {
        if !system.contains(&abandoned) {
            swept = true;
            break;
        }
        south.at(derby.clone()).board().await?;
        n0_future::time::sleep(Duration::from_millis(10)).await;
    }
    assert!(swept, "an idle match must not be held forever");
    assert!(
        system.contains(&derby),
        "an addressed match must survive the same sweep"
    );
    println!("the idle match was evicted; {} live", system.len());

    let _ = system.stop(&derby, Duration::from_secs(1)).await;
    Ok(())
}

/// A feed is any `Stream`, and this repository adds no stream crate to build
/// one. `poll_fn` over a receiver is the whole adapter.
fn clock(mut ticks: mpsc::Receiver<u64>) -> impl keylink::Stream<Item = u64> + Send + 'static {
    n0_future::stream::poll_fn(move |cx| ticks.poll_recv(cx))
}
