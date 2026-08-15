//! One condition, several doors, and the sentence each door says about it.
//!
//! ```sh
//! cargo run -p keylink --example fault_lines
//! ```
//!
//! Two nodes on the loopback interface. `Node::bind_local` binds with no relay
//! and no discovery, so this runs offline.
//!
//! # Why there is more than one error type
//!
//! A door that can meet three conditions and answers an enum with nine arms is
//! a door that makes every caller handle six impossibilities. keylink instead
//! gives each door the type that names *what that door can actually observe*:
//!
//! | door | success | failure |
//! |---|---|---|
//! | local `tell` on an [`ActorRef`](keylink::Servable) | [`Queued`] | — it cannot fail |
//! | local `tell` on a `Handle` | [`Queued`] | [`NotQueued`] |
//! | keyed local `tell` | [`Queued`] | [`LocalTellError`] |
//! | keyed local `rpc` | the value | [`LocalError`] |
//! | remote `tell` | [`Transmitted`] | [`TellError`] |
//! | remote `rpc` | the value | [`Error`] |
//! | mounting an actor | `()` | [`MountError`] |
//! | any `rpc`, through `Calls` | the value | [`Failure`] |
//!
//! The cost is that there are eight of them. The benefit is that no caller ever
//! matches an arm its door cannot produce, and every arm it *can* produce is one
//! it has to think about.
//!
//! # And why there is exactly one on top
//!
//! [`Failure`] is the shared vocabulary. Business logic written against the
//! generated `Calls` trait runs on a local handle or over a connection —
//! `trading_desk.rs` is the example of that — and it must not need a different
//! match per deployment. So the surfaces converge: the same condition becomes
//! the same [`Failure`] arm whichever side produced it. Act 4 measures that with
//! one function and two surfaces.
//!
//! # What a success proof does and does not claim
//!
//! [`Queued`] and [`Transmitted`] are both `#[must_use]` types with no public
//! constructor, so they cannot be fabricated — and both claim much less than a
//! reader assumes. [`Queued`] says the message entered a mailbox. [`Transmitted`]
//! says the bytes reached the *local* transport. Neither says a handler ran, and
//! `firmware_depot.rs` measures how far short of that a `tell` stops.
//!
//! # What this example cannot reach
//!
//! [`TurnLost`](keylink::TurnLost) and [`Panic`](keylink::Panic) are the
//! answers of [`LocalActor::ask_catching`](keylink::LocalActor), which is the
//! layer *beneath* `service!`. A generated `Handle` holds its `LocalActor` in a
//! private field and exposes no accessor, and building one by hand needs
//! `keylink::turn`, which is `#[doc(hidden)]`. So the panic in act 5 is measured
//! through the two doors an application actually has.

use std::error::Error as StdError;

use keylink::prelude::*;
use keylink::{
    ActorId, Allowlist, Caller, Error, Eviction, Failure, LocalError, LocalTellError, MountError,
    Node, NotQueued, Queued, RemoteError, TellError, Transmitted, Withheld,
};

keylink::service! {
    service Furnace;

    /// Raise the temperature and answer the new reading.
    rpc Heat(by: u32) -> u32;

    /// The current reading.
    rpc Reading() -> u32;

    /// A line for the log. Answers admission and nothing else.
    tell Note(line: String) -> ();

    /// Always panics. Act 5's subject.
    rpc Fault() -> u32;
}

#[derive(Default)]
struct Furnace {
    celsius: u32,
    notes: u32,
}

impl furnace::Actor for Furnace {
    async fn heat(&mut self, _cx: Caller<'_>, by: u32) -> u32 {
        self.celsius = self.celsius.saturating_add(by);
        self.celsius
    }

    async fn reading(&mut self, _cx: Caller<'_>) -> u32 {
        self.celsius
    }

    async fn note(&mut self, _cx: Caller<'_>, _line: String) {
        self.notes += 1;
    }

    async fn fault(&mut self, _cx: Caller<'_>) -> u32 {
        panic!("the burner is out");
    }
}

/// Business logic, written once, against whatever surface it is handed.
///
/// The bound is the generated `Calls` trait and the error is [`Failure`]. That
/// pair is the whole point of the shared vocabulary: this function is the same
/// text whether the furnace is in this process or on another machine.
async fn warm_up<C: furnace::Calls>(furnace: &C) -> Result<u32, Failure> {
    furnace.heat(300).await?;
    furnace.heat(200).await?;
    furnace.reading().await
}

/// What a failure was, in one word, so two surfaces can be compared.
fn arm(failure: &Failure) -> &'static str {
    match failure {
        Failure::Turn(Withheld::Gone) => "Turn(Gone)",
        Failure::Turn(Withheld::Panicked { .. }) => "Turn(Panicked)",
        Failure::Turn(_) => "Turn(other)",
        Failure::Reentrant => "Reentrant",
        Failure::Full => "Full",
        Failure::Other(_) => "Other",
        //  `Failure` is `#[non_exhaustive]`, so this arm is not dead code: it is
        //  what lets keylink name a condition this crate has not met yet without
        //  breaking a caller that compiled against today's list.
        _ => "unknown",
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn StdError>> {
    // ── 1. The two local `tell` doors, and why they answer different types.
    //
    //  An `ActorRef` is reached from inside a turn and its `tell` cannot fail:
    //  the actor it addresses is the one running, so "gone" is not a state it
    //  can observe. A `Handle` is held from outside, where the actor may have
    //  stopped, so the same declaration answers a `Result`.
    let furnace = furnace::actor(Furnace::default());

    let queued: Queued = furnace.note("lit at 06:00".to_string()).await?;
    println!("a local tell: {queued:?} — it entered the mailbox, and no more");

    //  Stopping the actor is what makes the second door's failure arm reachable.
    furnace.heat(50).await?;
    furnace.stop().await?;

    let refused: NotQueued = furnace
        .note("too late".to_string())
        .await
        .err()
        .ok_or("a stopped actor cannot take a message")?;
    //  The KIND, not merely an error: a full mailbox also refuses, and it means
    //  the opposite — back off and retry, rather than stop calling.
    assert!(
        matches!(refused, NotQueued::Closed),
        "a stopped actor closes its mailbox, got {refused:?}"
    );
    println!("after stop:   {refused:?} — {refused}");

    // ── 2. The remote `tell` door, whose failures are all local.
    //
    //  There is deliberately no remote-error arm on `TellError`. A `tell` has no
    //  response stream, so authorization, routing and the handler's outcome are
    //  unobservable — an error type with an arm for them would be inventing
    //  evidence.
    let depot = Node::bind_local().await?;
    let office = Node::bind_local().await?;
    let live = furnace::actor(Furnace::default());
    let _serving = keylink::Server::builder(&depot)
        .service(live.service(Allowlist::anyone()).build())
        .spawn()?;
    let client = furnace::Client::connect(&office, depot.endpoint().addr()).await?;

    let sent: Transmitted = client.note("shift change".to_string()).await?;
    println!("\na remote tell: {sent:?} — the bytes reached the local transport");

    //  One failure a caller can produce on purpose. A `tell` puts its arguments
    //  in the header frame, and the header frame is capped — see
    //  `wire_contract.rs` for `MAX_HEADER` itself. Announcing more is refused
    //  here, by the sender, before anything is written.
    let huge = "x".repeat(keylink::MAX_HEADER as usize + 1);
    let too_large = client
        .note(huge)
        .await
        .err()
        .ok_or("a header frame past the cap must be refused")?;
    match too_large {
        TellError::FrameTooLarge { len, cap } => {
            println!("a tell past the cap: {len} bytes announced against a {cap} byte cap");
            assert_eq!(cap, keylink::MAX_HEADER);
            assert!(len > u64::from(cap));
        }
        other => return Err(format!("expected the cap to refuse it, got {other:?}").into()),
    }

    // ── 3. The keyed registry, whose three doors have three error types.
    //
    //  A mount can meet conditions a message cannot, and the reverse: an id that
    //  is taken cannot stop a message, and an id that is empty cannot stop a
    //  mount. One enum over both would give each path an arm the other can never
    //  produce.
    let system: furnace::Registry = furnace::Registry::builder(Eviction::Never)
        .capacity(1)
        .build();
    let bay = ActorId::new("furnace/bay-1")?;
    let spare = ActorId::new("furnace/bay-2")?;
    system.mount(bay.clone(), furnace::setup(Furnace::default()).mountable())?;

    //  Occupied: refused rather than replaced, because replacing would drop live
    //  state somebody is still addressing.
    let taken = system
        .mount(bay.clone(), furnace::setup(Furnace::default()).mountable())
        .err()
        .ok_or("mounting onto a live id must be refused")?;
    assert!(matches!(taken, MountError::Occupied { .. }), "{taken:?}");
    println!("\nmounting twice:   {taken}");

    //  AtCapacity: a different condition, on a free id, at the same cap of one.
    let full = system
        .mount(
            spare.clone(),
            furnace::setup(Furnace::default()).mountable(),
        )
        .err()
        .ok_or("a registry at capacity must refuse a mount")?;
    assert!(
        matches!(full, MountError::AtCapacity { capacity: 1 }),
        "{full:?}"
    );
    println!("mounting past it: {full}");

    //  And the two message doors, on an id nothing is mounted under. Same
    //  condition, two types, because a `tell` has no turn outcome to report.
    let missing: LocalError = furnace::at_local(&system, spare.clone())
        .heat(10)
        .await
        .err()
        .ok_or("calling an empty id must fail")?;
    assert!(
        matches!(missing, LocalError::NoSuchActor { .. }),
        "{missing:?}"
    );

    let unsent: LocalTellError = furnace::at_local(&system, spare.clone())
        .note("nobody".to_string())
        .await
        .err()
        .ok_or("telling an empty id must fail")?;
    assert!(
        matches!(unsent, LocalTellError::NoSuchActor { .. }),
        "{unsent:?}"
    );
    println!("an empty id:      {missing}");
    println!("                  and the same sentence from the tell door");
    //  The sentence is shared on purpose: a reader who learns it once should not
    //  have to learn it again per door.
    assert_eq!(missing.to_string(), unsent.to_string());

    // ── 4. `Failure`: one vocabulary over all of it.
    //
    //  `warm_up` is written once. It runs against a local handle and against a
    //  client, and neither call site adapts an error.
    let local = furnace::actor(Furnace::default());
    assert_eq!(warm_up(&local).await?, 500);
    assert_eq!(warm_up(&client).await?, 500);
    println!("\none function, both surfaces: 500 degrees each");

    //  Now the same *condition* on both, and the assertion is that they produce
    //  the same arm. A local handle observes its actor stop; a client observes a
    //  `RemoteError` come back off a connection. Two entirely different paths.
    local.stop().await?;
    let local_loss = warm_up(&local).await.err().ok_or("a stopped actor fails")?;

    live.stop().await?;
    let served_loss = warm_up(&client)
        .await
        .err()
        .ok_or("a stopped actor fails")?;

    println!("a stopped actor, locally: {}", arm(&local_loss));
    println!("a stopped actor, served:  {}", arm(&served_loss));
    assert_eq!(
        arm(&local_loss),
        arm(&served_loss),
        "one condition must be one arm, whichever side saw it"
    );
    assert_eq!(local_loss.withheld(), Some(&Withheld::Gone));
    assert_eq!(served_loss.withheld(), Some(&Withheld::Gone));

    // ── 5. A panic, which is the one thing the surfaces genuinely disagree on.
    //
    //  Against a served surface the fault arrives as a value: `Box<dyn Any>` has
    //  no encoding, so a peer is told `Withheld::Panicked` instead. Against a
    //  local handle the payload is resumed into the calling task, which unwinds
    //  — the generated trait's own rustdoc states this rather than hiding it.
    //
    //  The divergence is real either way. Routing the local door through a
    //  catching one would give a single method name two behaviours on one type,
    //  because an inherent method outranks a trait method.
    //  Act 4 stopped the served furnace, so this needs a live one of its own.
    println!("\na handler that panics:");
    let kiln_node = Node::bind_local().await?;
    let fresh = furnace::actor(Furnace::default());
    let _serving_fresh = keylink::Server::builder(&kiln_node)
        .service(fresh.service(Allowlist::anyone()).build())
        .spawn()?;
    let watcher = furnace::Client::connect(&office, kiln_node.endpoint().addr()).await?;

    let remote = watcher.fault().await.err().ok_or("a panicking rpc fails")?;
    match &remote {
        Error::Remote(RemoteError::Withheld(Withheld::Panicked { .. })) => {
            println!("  served: {remote}");
        }
        other => return Err(format!("a peer reads a fault as a value, got {other}").into()),
    }

    //  The local half, observed from a task so the unwind is caught rather than
    //  ending this program. `is_panic` is the measurement: the call did not
    //  return an error, it unwound.
    let unwinding = furnace::actor(Furnace::default());
    let joined = tokio::spawn(async move { unwinding.fault().await }).await;
    let panicked = joined
        .err()
        .ok_or("a local fault must unwind, not return")?;
    assert!(
        panicked.is_panic(),
        "the payload is resumed into the caller"
    );
    println!("  local:  the calling task unwound instead of returning an error");

    println!("\neight doors, eight types, and one vocabulary above them.");
    Ok(())
}
