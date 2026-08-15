//! What a running node will tell an operator, and how it is wired.
//!
//! ```sh
//! cargo run -p keylink --example control_room
//! ```
//!
//! Two nodes on the loopback interface. `Node::bind_local` binds with no relay
//! and no discovery, so this runs offline.
//!
//! # Nothing observes itself
//!
//! keylink records no events unless a program asks it to, and asking is three
//! wirings rather than a flag. Each answers a different question, and leaving
//! one out loses exactly that question's answers.
//!
//! | wiring | what it reports |
//! |---|---|
//! | [`Registry::builder(..).lifecycle(..)`](keylink::RegistryBuilder) | mounts and endings |
//! | [`Events::observer`] on each actor | turns and faults |
//! | [`Inspector`] served beside the registry | the two above, to a peer |
//!
//! The registry sees mounts and endings and never sees a turn. Only the actor
//! sees what it ran. That is why the observer is per actor and the lifecycle is
//! per registry.
//!
//! # The console is an ordinary client of an ordinary service
//!
//! [`Inspect`](keylink::inspect) is declared with `service!` like anything else,
//! so it has its own ALPN and its own allowlist. A peer admitted to the presses
//! is not thereby admitted to the console, and act 1 gives the two different
//! lists to say so.
//!
//! # Two loss counters, and they count different losses
//!
//! [`NodeStatus::dropped`] and [`EventPage::dropped`] are one quantity: events
//! that never reached the log, because the channel behind [`Events::new`] was
//! full and recording will not block a mount. [`EventPage::lost`] is the other:
//! events that did reach the log and were pushed out of the ring before this
//! console read them. A console that watches only one is blind to the likelier
//! of the two.

use std::error::Error as StdError;
use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Duration;

use keylink::prelude::*;
use keylink::{
    ActorId, ActorStatus, Allowlist, Caller, Console, EndReason, Ended, EventPage, EventStream,
    Events, Eviction, FollowRefused, Lifecycle, LifecycleEvent, Node, NodeKey, NodeStatus, Server,
};

keylink::service! {
    service Press;

    /// One printed job. Reported as a `Turn` event named `"Print"`.
    rpc Print(pages: u32) -> u32;

    /// How many pages this press has printed.
    rpc Total() -> u32;

    /// The head crashed.
    ///
    /// An `rpc`, and safe to declare as one because every caller below is a
    /// peer. A panicking turn reaches a peer as
    /// [`Withheld::Panicked`](keylink::Withheld) rather than unwinding it; it is
    /// a *local* `rpc` through a `Handle` that resumes the panic in its caller,
    /// which is why `substation.rs` uses a `tell` for the same job. An
    /// `ActorRef` carries no `tell` at all.
    rpc Jam() -> ();
}

#[derive(Default)]
struct Press {
    pages: u32,
}

impl press::Actor for Press {
    async fn print(&mut self, _cx: Caller<'_>, pages: u32) -> u32 {
        self.pages += pages;
        self.pages
    }

    async fn total(&mut self, _cx: Caller<'_>) -> u32 {
        self.pages
    }

    async fn jam(&mut self, _cx: Caller<'_>) {
        panic!("the print head hit the carriage");
    }
}

/// A [`Lifecycle`] of this program's own, wrapped around the one the console
/// reads.
///
/// Written out because the registry takes exactly one, so a program that wants
/// its own counters *and* a console needs the two in one value. Forwarding is
/// not optional: an implementation that swallows these calls hands back a
/// console that reports no mounts and no endings.
struct Floor {
    events: Events,
    mounted: Arc<AtomicU32>,
    ended: Arc<AtomicU32>,
}

impl Lifecycle for Floor {
    fn mounted(&self, id: &ActorId) {
        self.mounted.fetch_add(1, Ordering::SeqCst);
        self.events.mounted(id);
    }

    fn ended(&self, id: &ActorId, why: &Ended) {
        self.ended.fetch_add(1, Ordering::SeqCst);
        self.events.ended(id, why);
    }
}

/// A depth, without an `unwrap` this workspace denies.
fn depth(n: usize) -> Result<NonZeroUsize, Box<dyn StdError>> {
    NonZeroUsize::new(n).ok_or_else(|| "a depth is not zero".into())
}

/// Read pages until `enough` accepts what has been seen.
///
/// A console advances its own cursor, so this accumulates rather than replacing.
/// Bounded by a clock rather than by a retry count, because every call here
/// parks until the node records something.
async fn until(
    console: &mut Console,
    what: &str,
    enough: impl Fn(&[LifecycleEvent]) -> bool,
) -> Result<Vec<LifecycleEvent>, Box<dyn StdError>> {
    let mut seen: Vec<LifecycleEvent> = Vec::new();
    let read = tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            match console.events().await {
                Ok(page) => {
                    seen.extend(page.events);
                    if enough(&seen) {
                        return Ok(());
                    }
                }
                Err(error) => return Err(error),
            }
        }
    })
    .await;
    match read {
        Ok(Ok(())) => Ok(seen),
        Ok(Err(error)) => Err(error.into()),
        Err(_) => Err(format!("the console never saw {what}").into()),
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn StdError>> {
    let floor_node = Node::bind_local().await?;
    let operator_node = Node::bind_local().await?;

    // ── 1. The three wirings, in one place.
    //  `Events` is the sender half and the `EventStream` is the receiver.
    //  Dropping the stream is not inert: it makes every later record a
    //  `dropped` count rather than a silent no-op, which is why the two are
    //  handed out together and neither is optional.
    let (events, stream): (Events, EventStream) = Events::new(depth(256)?);
    let mounted = Arc::new(AtomicU32::new(0));
    let ended = Arc::new(AtomicU32::new(0));
    let system: press::Registry = press::Registry::builder(Eviction::Never)
        .lifecycle(Floor {
            events: events.clone(),
            mounted: Arc::clone(&mounted),
            ended: Arc::clone(&ended),
        })
        .capacity(16)
        .build();

    let inspector = keylink::inspect::setup(keylink::Inspector::new(
        &system,
        &floor_node,
        &events,
        depth(64)?,
    ))
    .feeds(keylink::inspect::Feeds::new(stream))
    .actor();

    //  Two services, two ALPNs, two lists. The console peer is named on the
    //  inspect list and the presses admit anyone, so the two admissions are
    //  visibly separate decisions.
    let _server = Server::builder(&floor_node)
        .service(system.service(Allowlist::anyone()).build())
        .service(
            inspector
                .service(Allowlist::only([operator_node.id()]))
                .build(),
        )
        .spawn()?;

    let east = ActorId::new("press/east")?;
    let west = ActorId::new("press/west")?;
    for id in [&east, &west] {
        system.mount(
            id.clone(),
            press::setup(Press::default())
                //  Per actor, because only the actor sees its own turns.
                .observe(events.observer(id.clone()))
                .mountable(),
        )?;
    }

    let mut console = Console::connect(&operator_node, floor_node.endpoint().addr()).await?;

    // ── 2. What the node says about itself.
    //
    //  Three fields and nothing inferred: no uptime, no connection count, no
    //  message rate. Every number here is one this crate actually holds.
    let status: NodeStatus = console.node().await?;
    println!(
        "the node reports {} actors and {} dropped events",
        status.actors, status.dropped
    );
    assert_eq!(status.actors, 2);
    assert_eq!(
        status.dropped, 0,
        "a non-zero drop count means the channel is too shallow"
    );

    // ── 3. Turns, which the registry cannot see and the observer can.
    let printer =
        press::RegistryClient::connect(&operator_node, floor_node.endpoint().addr()).await?;
    printer.at(east.clone()).print(12).await?;
    printer.at(west.clone()).print(30).await?;

    let seen = until(&mut console, "both presses print", |seen| {
        seen.iter()
            .filter(
                |event| matches!(event, LifecycleEvent::Turn { method, .. } if method == "Print"),
            )
            .count()
            == 2
    })
    .await?;
    println!(
        "the console read {} event(s), including two turns",
        seen.len()
    );
    assert!(
        seen.iter()
            .any(|event| matches!(event, LifecycleEvent::Mountable { id } if id == &east)),
        "the registry's own mount must reach the console"
    );

    // ── 4. A fault, which is its own event rather than a field on a turn.
    //
    //  The actor is unsupervised, so it keeps its state and loses that one
    //  message. The event says what panicked and where; the total below says
    //  the press is still the same press.
    let _ = printer.at(east.clone()).jam().await;
    let faults = until(&mut console, "the fault", |seen| {
        seen.iter()
            .any(|event| matches!(event, LifecycleEvent::Faulted { .. }))
    })
    .await?;
    for event in &faults {
        if let LifecycleEvent::Faulted {
            id,
            method,
            message,
        } = event
        {
            println!("fault on {id}, in {method}: {message}");
        }
    }
    assert_eq!(
        printer.at(east.clone()).total().await?,
        12,
        "an unsupervised actor keeps its state across a faulted turn"
    );

    // ── 5. An ending, and the one call that answers for an actor that is gone.
    //
    //  `actors()` lists what is mounted, so a stopped actor is simply absent
    //  from it. `why_ended` is what a console asks instead, and the registry
    //  keeps the answer after the actor itself has gone.
    let _ = system.stop(&west, Duration::from_secs(1)).await;
    let endings = until(&mut console, "the ending", |seen| {
        seen.iter()
            .any(|event| matches!(event, LifecycleEvent::Ended { id, .. } if id == &west))
    })
    .await?;
    //  A DIFFERENT predicate from the one `until` waited on. Asserting the one
    //  it waited on could not fail, because `until` returns `Ok` only when that
    //  predicate accepted.
    assert!(
        !endings
            .iter()
            .any(|event| matches!(event, LifecycleEvent::Ended { id, .. } if id == &east)),
        "the press that is still mounted must not be reported as ended"
    );

    let why: Option<EndReason> = console.why_ended(west.clone()).await?;
    assert_eq!(
        why,
        Some(EndReason::Stopped),
        "a stopped actor's reason outlives it, and says which reason"
    );
    println!("the console asked why press/west ended: {why:?}");

    let live: Vec<ActorStatus> = console.actors().await?;
    println!(
        "one press is left: {:?}, idle for {:?}, running a turn: {}",
        live.first().map(|actor| actor.id.to_string()),
        live.first().map(|actor| actor.idle_for),
        live.first().is_some_and(|actor| actor.busy),
    );
    assert_eq!(live.len(), 1, "a stopped actor is not listed");

    assert_eq!(
        mounted.load(Ordering::SeqCst),
        2,
        "this program counted too"
    );
    assert_eq!(ended.load(Ordering::SeqCst), 1);
    println!(
        "and the program's own Lifecycle counted {} mount(s) and {} ending(s)",
        mounted.load(Ordering::SeqCst),
        ended.load(Ordering::SeqCst)
    );

    // ── 6. The node's own key, as it crosses the wire.
    //
    //  Bytes rather than text, so a console rebuilds an `EndpointId` without
    //  parsing. `TryFrom` and not `From` on the way back, because not every
    //  thirty-two bytes is a point on the curve — and which of them are is
    //  iroh's to say rather than this type's.
    let key = NodeKey::from(&floor_node.id());
    assert_eq!(
        keylink::EndpointId::try_from(key)?,
        floor_node.id(),
        "the round trip is lossless"
    );
    println!("the node key round-trips to {}", floor_node.id());

    // ── 7. The two loss counters, made non-zero on purpose.
    //
    //  Everything above ran on a healthy wiring, and act 2 asserted `dropped`
    //  is zero there. This is the other case, and it needs a SECOND node —
    //  a shallow channel and a shallow ring — because the point is what a
    //  console reads when its operator under-sized them.
    let strained_node = Node::bind_local().await?;
    let reader_node = Node::bind_local().await?;

    //  Depth 2 accepts two events. Nothing drains the stream yet, so the other
    //  three never reach the log at all — that is the CHANNEL loss.
    let (strained, feed) = Events::new(depth(2)?);
    for n in 0..5u32 {
        strained.record(LifecycleEvent::Mountable {
            id: ActorId::new(format!("press/{n}"))?,
        });
    }
    assert_eq!(strained.lost(), 3, "three events never reached the log");

    //  A history of one makes the RING lose: what is sequenced is kept, and the
    //  ring holds one of it.
    let quiet: press::Registry = press::Registry::builder(Eviction::Never).build();
    let strained_inspector = keylink::inspect::setup(keylink::Inspector::new(
        &quiet,
        &strained_node,
        &strained,
        depth(1)?,
    ))
    .feeds(keylink::inspect::Feeds::new(feed))
    .actor();
    let _strained_server = Server::builder(&strained_node)
        .service(
            strained_inspector
                .service(Allowlist::only([reader_node.id()]))
                .build(),
        )
        .spawn()?;

    //  The raw client rather than a `Console`, because a console advances its
    //  own cursor and this loop needs to re-ask. `follow(0)` advances nothing
    //  on the node, so asking again is free — which is what replaces a sleep.
    let raw =
        keylink::inspect::Client::connect(&reader_node, strained_node.endpoint().addr()).await?;
    let page: EventPage = tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            match raw.follow(0).await {
                Ok(Ok(page)) if page.next == 2 => return Ok(page),
                Ok(Ok(_)) => {}
                Ok(Err(refused)) => return Err(format!("zero is behind any log: {refused:?}")),
                Err(error) => return Err(error.to_string()),
            }
        }
    })
    .await
    .map_err(|_| "the strained node never sequenced its two events")??;

    println!(
        "the strained console read {} event(s), lost {} to the ring and {} to the channel",
        page.events.len(),
        page.lost,
        page.dropped
    );
    assert_eq!(page.events.len(), 1, "the ring holds its newest one");
    assert_eq!(page.lost, 1, "and pushed one out from under this reader");
    assert_eq!(
        page.dropped, 3,
        "while three never got a sequence number at all"
    );

    // ── 8. A cursor the node has not reached.
    //
    //  Not an error to page past the end — it is a CLIENT error, and it names
    //  where to resume. A sequence counter starts at zero in every process, so
    //  this is also how a restart reaches a console that was following.
    let ahead = raw
        .follow(page.next + 1_000)
        .await?
        .err()
        .ok_or("a cursor past the log must be refused")?;
    assert_eq!(
        ahead,
        FollowRefused::Ahead {
            after: page.next + 1_000,
            seq: page.next,
        },
        "the refusal names both the cursor asked for and where the log is"
    );
    println!("a cursor past the log was refused and said where to resume: {ahead}");

    let _ = system.stop(&east, Duration::from_secs(1)).await;
    Ok(())
}
