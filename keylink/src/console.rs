//! keylink, serving its own introspection over keylink.
//!
//! A process that runs actors could answer `contains`, `len` and `is_empty`
//! about them, and nothing else, and only from inside itself. This module is the
//! other side of both limits: [`System::status`] is the whole list, and
//! [`Inspect`](inspect) is that list plus a live event log, served on its **own**
//! ALPN behind its **own** [`Allowlist`], beside whatever else the node serves.
//!
//! ```ignore
//! let (events, stream) = Events::new(EVENT_DEPTH);
//!
//! let system: journal::System = journal::System::builder(Eviction::Never)
//!     .lifecycle(events.clone())          // mounts and endings
//!     .build();
//!
//! // one observer per actor, which is where turns come from
//! system.mount(id.clone(), journal::spawn_with(state, vec![events.observer(id)]))?;
//!
//! let inspector = inspect::actor_wired(
//!     Inspector::new(&system, &node, &events, HISTORY),
//!     vec![],
//!     inspect::Feeds::new(stream),
//! );
//!
//! Server::builder(&node)
//!     .service(journal::keyed_service(&system, Allowlist::anyone()).build())
//!     .service(inspector.service(Allowlist::only([operator])).build())   // its own list
//!     .spawn()?;
//! ```
//!
//! The console is then an ordinary peer: [`Console::connect`] dials by public
//! key, is admitted or refused by that second allowlist, and reads. There is no
//! HTTP server, no port, no session and no bearer token, because there is
//! nothing here that the transport does not already do — and because every one
//! of those would have been a second authorization surface on a node that
//! already has one.
//!
//! # THE DECISION THIS MODULE OWNS — a subscriber is a parked reply, not a held handler
//!
//! A live console wants a push. An actor cannot push, and the obvious way to
//! fake one is a handler that loops on a `recv` and writes as it goes. On an
//! actor that is fatal: a turn holds `&mut A` for as long as it runs, so a
//! handler parked on a subscription is an actor that answers nothing else — not
//! the next console, not [`Inspector::actors`](inspect::Actor::actors), not the
//! local handle. The console would be the one client that stops the thing it is
//! there to watch.
//!
//! So [`Follow`](inspect::Client::follow) is `#[defer]`. A caller with nothing
//! new to read has its [`Reply`] **parked** and the turn **ends**; the actor goes
//! straight back to its loop. A [`LifecycleEvent`] arriving on the `Events` feed
//! is a turn like any other, and that turn answers everybody parked. Two waves
//! pay for themselves at once: `#[defer]` is what lets the reply outlive the
//! turn, and the `feed` is what wakes it without a caller.
//!
//! What a reader gets from that is a property they can check rather than trust:
//! **an actor with N subscribers attached is idle**, `busy == false`, and
//! answers an ordinary method at once. `tests/inspect.rs` asserts exactly that.
//!
//! # Three things this does not promise
//!
//! - **No event is guaranteed to arrive.** [`Events`] is a bounded channel and
//!   [`Events::record`] never blocks, because a diagnostic that can stall a
//!   mount or an eviction is worse than a diagnostic that skips one. What is
//!   dropped is counted: [`NodeStatus::lost`] for the channel,
//!   [`EventPage::lost`] for the ring behind a slow console.
//! - **No ordering against anything but itself.** Events are sequenced in the
//!   order the log accepted them. A turn on actor A and a mount of actor B are
//!   two tasks, and their order here is the order they reached the channel.
//! - **No history before the [`Inspector`] existed.** The log starts empty and
//!   holds its last `history` events. A console that connects late sees a `lost`
//!   count, not a replay.

use std::collections::VecDeque;
use std::num::NonZeroUsize;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::task::{Context as TaskContext, Poll};

use postcard_schema::Schema;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;

use crate::{
    ActorId, ActorStatus, Caller, Deferred, Ended, EndpointAddr, Lifecycle, Node, Observer,
    Replied, Reply, Result, Stream, System, TurnFut, TurnInfo, TurnOutcome,
};

// ────────────────────────────────────────────────────────── what a console reads

/// One thing that happened to one actor, in the form that crosses the wire.
///
/// Exhaustive, for [`Ended`]'s reason and with [`Ended`]'s bill: a console that
/// matches on this gets a compile error on the day a new kind of event exists,
/// which is the day it has a decision to make. A `_ =>` arm forced by
/// `#[non_exhaustive]` would swallow that silently, and swallowing a lifecycle
/// signal is the failure this whole module exists against.
///
/// A fault is its own variant rather than a field on [`Turn`](Self::Turn), which
/// is Wave 2a's rule applied one layer out: a faulted turn is an explicit answer
/// and never something a reader reconstructs by finding a field set.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Schema)]
pub enum LifecycleEvent {
    /// An actor was mounted, by hand or by a lazy factory.
    Mounted {
        /// Which actor.
        id: ActorId,
    },
    /// An actor completed a turn.
    Turn {
        /// Which actor.
        id: ActorId,
        /// The wire spelling of the declaration that ran — `"Reserve"`, not
        /// `"reserve"`, and a `feed`'s own name for a feed turn. See
        /// [`TurnInfo::method`].
        method: String,
    },
    /// An actor's handler panicked.
    Faulted {
        /// Which actor.
        id: ActorId,
        /// The declaration that panicked, spelled as in [`Self::Turn`].
        method: String,
        /// What it panicked with. The **message**, never the payload — see
        /// [`TurnOutcome::Panicked`].
        message: String,
    },
    /// An actor is gone.
    Ended {
        /// Which actor.
        id: ActorId,
        /// Why. See [`Lifecycle::ended`] for exactly what each reason claims.
        why: EndReason,
    },
}

/// Why an actor ended, in the form that crosses the wire.
///
/// # A second vocabulary, and the one place it could not be avoided
///
/// This crate refuses parallel types for one concept, and this is a parallel
/// type for [`Ended`]. It exists because [`Ended::Observer`] carries an
/// `Arc<dyn Error + Send + Sync>`, which has no encoding and no schema — a
/// trait object is not data. The choice was this, or dropping the one arm that
/// names *which* observer refused, which is the arm an operator most needs.
///
/// What keeps the two from drifting is that there is exactly one conversion,
/// [`From<&Ended>`](#impl-From<%26Ended>-for-EndReason), it is written with no
/// wildcard arm, and `Ended` is exhaustive. So a new ending is a compile error
/// **here**, on the line that would otherwise have quietly mapped it to
/// something else.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Schema)]
pub enum EndReason {
    /// [`Ended::Stopped`].
    Stopped,
    /// [`Ended::Vanished`].
    Vanished,
    /// [`Ended::Panicked`].
    Panicked {
        /// What the handler panicked with.
        message: String,
    },
    /// [`Ended::Observer`], with the error rendered through `Display`, because
    /// that is the whole of what survives an `Arc<dyn Error>` crossing a wire.
    Observer {
        /// [`Observer::name`].
        name: String,
        /// Why it gave up.
        error: String,
    },
    /// [`Ended::Feed`].
    Feed {
        /// The feed's declared name.
        name: String,
    },
}

impl From<&Ended> for EndReason {
    /// No wildcard arm, deliberately: see this type's own documentation.
    fn from(ended: &Ended) -> Self {
        match ended {
            Ended::Stopped => Self::Stopped,
            Ended::Vanished => Self::Vanished,
            Ended::Panicked { message } => Self::Panicked {
                message: message.clone(),
            },
            Ended::Observer { name, error } => Self::Observer {
                name: (*name).to_owned(),
                error: error.to_string(),
            },
            Ended::Feed { name } => Self::Feed {
                name: (*name).to_owned(),
            },
        }
    }
}

/// One node, as much of it as can be measured.
///
/// Three fields, and the discipline is [`ActorStatus`]': nothing here is
/// inferred, averaged or estimated. There is no uptime (the crate has no epoch a
/// peer could compare against), no connection count (iroh owns that and does not
/// publish it), and no message rate (nothing counts messages).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Schema)]
#[non_exhaustive]
pub struct NodeStatus {
    /// This node's public key — the address peers dial.
    ///
    /// The 32 bytes rather than the text, so a console can rebuild an
    /// [`EndpointId`](crate::EndpointId) from it without parsing.
    pub id: [u8; 32],
    /// How many actors are mounted right now, which is [`System::len`].
    ///
    /// `u64` for [`ActorStatus::buffered`]'s reason: `usize` has no wire schema,
    /// because its width is the compiler's choice and a browser's is not the
    /// server's.
    pub actors: u64,
    /// Lifecycle events this node produced and never delivered.
    ///
    /// [`Events`] is a bounded channel and [`Events::record`] never blocks, so a
    /// burst of mounts deeper than the channel loses its tail rather than
    /// stalling the mount. This is that count, for the life of the process.
    ///
    /// Non-zero means the channel is too shallow for what this node does, and
    /// the fix is a bigger `depth` at [`Events::new`]. It is **not** the same
    /// number as [`EventPage::lost`], which is about one console falling behind
    /// a log that did receive the events.
    pub lost: u64,
}

/// Events after the sequence number a console asked from.
///
/// The reply to [`Follow`](inspect::Client::follow), and the unit a console
/// advances its cursor by.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Schema)]
#[non_exhaustive]
pub struct EventPage {
    /// Pass this as the next `after`. [`Console`] does it for you.
    pub next: u64,
    /// Events between the cursor and the oldest one still held, which this
    /// caller will never see.
    ///
    /// Exact, not estimated: the log is sequenced, so this is arithmetic between
    /// the cursor asked for and the oldest sequence still in the ring. Non-zero
    /// means either this console was away longer than `history` events, or it
    /// connected after the node had already been running.
    pub lost: u64,
    /// The events themselves, oldest first.
    ///
    /// Never empty. A call with nothing to report parks instead of answering, so
    /// an empty page is not a thing a console has to distinguish from a quiet
    /// one.
    pub events: Vec<LifecycleEvent>,
}

// ────────────────────────────────────────────────────────── where events come from

/// Where a system and its actors report what they are doing.
///
/// One sink, three producers, and that is the whole reason it is a type rather
/// than a channel at a call site: a [`System`] reports mounts and endings through
/// [`Lifecycle`], an [`Observer`] reports turns, and both have to arrive in one
/// sequence or a console is reading two unrelated logs.
///
/// Cheap to clone: every clone names the same sink.
///
/// # It never blocks, and it says what that cost
///
/// [`record`](Self::record) is a `try_send` on a bounded channel. A full channel
/// drops the event and increments [`lost`](Self::lost). That is deliberate and it
/// is the opposite of what this crate does with a mailbox — [`SystemBuilder::mailbox_depth`]
/// blocks the sender, on the argument that an unbounded queue turns a latency
/// problem into an OOM. Here the sender is a **mount** or an **eviction**, and
/// blocking one of those on a console would let a diagnostic stall the runtime it
/// describes. Losing a diagnostic loudly is the lesser failure, so the count is
/// published at [`NodeStatus::lost`] rather than kept.
///
/// [`SystemBuilder::mailbox_depth`]: crate::SystemBuilder::mailbox_depth
#[derive(Debug, Clone)]
pub struct Events {
    to: mpsc::Sender<LifecycleEvent>,
    lost: Arc<AtomicU64>,
}

impl Events {
    /// A sink and the stream that carries what it records.
    ///
    /// `depth` is how many events may wait for the [`Inspector`] to take them.
    /// Past it, [`record`](Self::record) drops and counts — so this is a
    /// statement about how large a burst of mounts survives, not about how much
    /// history is kept. History is [`Inspector::new`]'s parameter.
    ///
    /// `NonZeroUsize` because `mpsc::channel(0)` panics, for
    /// [`SystemBuilder::mailbox_depth`](crate::SystemBuilder::mailbox_depth)'s
    /// reason: a zero would panic at the first event rather than at the call that
    /// asked for it.
    ///
    /// Hand the [`EventStream`] to `inspect::Feeds::new`. Dropping it makes every
    /// later `record` a drop — the actor that was reading is gone — which is
    /// visible as a rising `lost` rather than as silence.
    #[must_use]
    pub fn new(depth: NonZeroUsize) -> (Self, EventStream) {
        let (to, from) = mpsc::channel(depth.get());
        (
            Self {
                to,
                lost: Arc::new(AtomicU64::new(0)),
            },
            EventStream { from },
        )
    }

    /// Record one event, without waiting for anybody.
    ///
    /// Never blocks and never fails. An event that does not fit is dropped and
    /// counted at [`lost`](Self::lost).
    pub fn record(&self, event: LifecycleEvent) {
        if self.to.try_send(event).is_err() {
            // Both `try_send` failures are the same fact for a diagnostic: this
            // event will not be seen. `Full` is a console behind, `Closed` is an
            // `Inspector` that has gone away. Counting them apart would be two
            // numbers to explain one absence.
            self.lost.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// How many events have been dropped, for the life of this sink.
    ///
    /// `Relaxed`, like every other counter this crate reads for a diagnostic:
    /// it orders nothing but itself, and a reading one event stale costs a
    /// console nothing.
    #[must_use]
    pub fn lost(&self) -> u64 {
        self.lost.load(Ordering::Relaxed)
    }

    /// An [`Observer`] that reports every turn `id` takes into this sink.
    ///
    /// One per actor, given to the actor at spawn:
    /// `journal::spawn_with(state, vec![events.observer(id)])`. It has to be
    /// per-actor because an observer is owned by the actor it watches and
    /// [`TurnInfo`] carries no id — nothing in a turn knows which registry entry
    /// it belongs to, so the id is bound here, once, where it is known.
    ///
    /// It never refuses a turn: `after_turn` always returns `Ok`. An observer
    /// that could end an actor because a console was watching would make
    /// observability change behaviour, which is the one thing it must not do.
    #[must_use]
    pub fn observer<A>(&self, id: ActorId) -> Box<dyn Observer<A>> {
        Box::new(Turns {
            id,
            to: self.clone(),
        })
    }
}

/// Mounts and endings, which is the half a registry knows.
impl Lifecycle for Events {
    fn mounted(&self, id: &ActorId) {
        self.record(LifecycleEvent::Mounted { id: id.clone() });
    }

    fn ended(&self, id: &ActorId, why: &Ended) {
        self.record(LifecycleEvent::Ended {
            id: id.clone(),
            why: why.into(),
        });
    }
}

/// Turns, which is the half only the actor knows.
struct Turns {
    id: ActorId,
    to: Events,
}

impl<A> Observer<A> for Turns {
    fn name(&self) -> &'static str {
        "keylink::console"
    }

    fn after_turn<'a>(&'a mut self, _state: &'a A, turn: TurnInfo<'a>) -> TurnFut<'a> {
        let event = match turn.outcome {
            TurnOutcome::Completed => LifecycleEvent::Turn {
                id: self.id.clone(),
                method: turn.method.to_owned(),
            },
            TurnOutcome::Panicked { message } => LifecycleEvent::Faulted {
                id: self.id.clone(),
                method: turn.method.to_owned(),
                message: message.to_owned(),
            },
        };
        self.to.record(event);
        // Recorded synchronously, before the future is built, so the report is
        // not something a cancelled turn can lose. `Ok` always: see
        // `Events::observer`.
        Box::pin(async { Ok(()) })
    }
}

/// The stream half of [`Events::new`], for `inspect::Feeds::new`.
///
/// A [`Stream`] and nothing else. Its `None` — every [`Events`] clone dropped —
/// ends the [`Inspector`] as
/// [`Ended::Feed { name: "Events" }`](crate::Ended::Feed), which is the loud
/// answer this crate gives to a subscription that quietly died.
#[derive(Debug)]
pub struct EventStream {
    from: mpsc::Receiver<LifecycleEvent>,
}

/// `Unpin` inherited from `Receiver`, never declared — `compose::topic`'s
/// `Subscription` carries the full argument for why that distinction matters.
impl Stream for EventStream {
    type Item = LifecycleEvent;

    fn poll_next(self: Pin<&mut Self>, cx: &mut TaskContext<'_>) -> Poll<Option<LifecycleEvent>> {
        self.get_mut().from.poll_recv(cx)
    }
}

// ─────────────────────────────────────────────────────────────────── the service

crate::service! {
    service Inspect;

    /// Every actor mounted on this node, with the four fields
    /// [`ActorStatus`](keylink::ActorStatus) measures.
    ///
    /// Stale on arrival, and the reason is
    /// [`System::status`](keylink::System::status)' — an id read here may have
    /// been evicted before the reply reached you.
    rpc Actors() -> Vec<ActorStatus>;

    /// This node.
    rpc Node() -> NodeStatus;

    /// The events after `after`, waiting until there is one.
    ///
    /// `#[defer]`, which is the whole design: a call with nothing to report is
    /// parked and its turn ends, so a console attached to an actor costs that
    /// actor nothing between events. Start at `0` for everything still held.
    #[defer]
    rpc Follow(after: u64) -> EventPage;

    /// Where the events come from. Local, wired at construction, never on the
    /// wire — see [`Events`](keylink::Events).
    feed Events(event: LifecycleEvent);
}

/// The state behind [`inspect`]: what is running, and what has happened.
///
/// # It holds a closure and not a `System<M>`, and that is the seam
///
/// A [`System`] is generic in its service's mailbox type and an `Inspect`
/// service is not generic in anything — a `service!` block generates concrete
/// types. Erasing the system to `Fn() -> Vec<ActorStatus>` is what lets one
/// console answer for a system of any service, and it is a narrow erasure: the
/// only thing this can do to that system is ask it what is running. It cannot
/// mount, shut down, abort or address anything, and no `Arc<dyn Any>` downcast
/// gets those back.
pub struct Inspector {
    /// The one thing this may ask a system.
    actors: Arc<dyn Fn() -> Vec<ActorStatus> + Send + Sync>,
    id: [u8; 32],
    lost: Events,
    /// The last `history` events, oldest first, each with its sequence number.
    log: VecDeque<(u64, LifecycleEvent)>,
    history: usize,
    /// The sequence number of the newest event ever recorded — including the
    /// ones the ring has since dropped, which is what makes
    /// [`EventPage::lost`] arithmetic rather than a counter.
    seq: u64,
    /// Callers parked with nothing to read, each with the cursor it asked from.
    ///
    /// Bounded by what a peer can have in flight: every entry is one open QUIC
    /// stream, and iroh caps those at `max_concurrent_bidi_streams` (100) per
    /// connection, times the peers this service's [`Allowlist`](crate::Allowlist)
    /// admits. A local caller through the generated handle is bounded by its own
    /// discipline, exactly as `#[defer]`'s own tests are.
    waiting: Vec<(u64, Deferred<EventPage>)>,
}

impl Inspector {
    /// Watch `system` on `node`, keeping the last `history` events.
    ///
    /// `events` is the sink the system and its observers were built with — the
    /// same value, so that [`NodeStatus::lost`] reports the drops of the channel
    /// this inspector is reading and not of some other one.
    ///
    /// `history` is a count of events, not of bytes and not of time. Past it the
    /// oldest is dropped and a console that was behind is told how many it missed
    /// at [`EventPage::lost`].
    #[must_use]
    pub fn new<M>(system: &System<M>, node: &Node, events: &Events, history: NonZeroUsize) -> Self
    where
        M: Send + 'static,
    {
        let system = system.clone();
        Self {
            actors: Arc::new(move || system.status()),
            id: *node.id().as_bytes(),
            lost: events.clone(),
            log: VecDeque::with_capacity(history.get()),
            history: history.get(),
            seq: 0,
            waiting: Vec::new(),
        }
    }

    /// Append one event, dropping the oldest if the ring is full.
    fn record(&mut self, event: LifecycleEvent) {
        self.seq += 1;
        if self.log.len() == self.history {
            self.log.pop_front();
        }
        self.log.push_back((self.seq, event));
    }

    /// Everything after `after`, or `None` when there is nothing — which is what
    /// makes a call park rather than answer with an empty list.
    fn page(&self, after: u64) -> Option<EventPage> {
        let events: Vec<LifecycleEvent> = self
            .log
            .iter()
            .filter(|(seq, _)| *seq > after)
            .map(|(_, event)| event.clone())
            .collect();
        if events.is_empty() {
            return None;
        }
        // The oldest sequence still held, against the first one this caller had
        // not seen. `saturating_sub` is the caller who is not behind at all.
        let oldest = self.log.front().map_or(0, |(seq, _)| *seq);
        Some(EventPage {
            next: self.seq,
            lost: oldest.saturating_sub(after + 1),
            events,
        })
    }
}

impl inspect::Actor for Inspector {
    async fn actors(&mut self, _cx: Caller<'_>) -> Vec<ActorStatus> {
        (self.actors)()
    }

    async fn node(&mut self, _cx: Caller<'_>) -> NodeStatus {
        NodeStatus {
            id: self.id,
            // The same reading `actors()` returns, counted rather than a second
            // call to `System::len` — one source, so the two answers a console
            // gets from one node cannot disagree by a mount.
            actors: u64::try_from((self.actors)().len()).unwrap_or(u64::MAX),
            lost: self.lost.lost(),
        }
    }

    async fn follow(
        &mut self,
        _cx: Caller<'_>,
        reply: Reply<EventPage>,
        after: u64,
    ) -> Replied<EventPage> {
        if let Some(page) = self.page(after) {
            return reply.send(page);
        }
        // Nothing to say, so the reply outlives the turn and the actor goes back
        // to its loop. This is the line the whole module is arranged around.
        let (deferred, replied) = reply.later();
        self.waiting.push((after, deferred));
        replied
    }

    async fn on_events(&mut self, event: LifecycleEvent) {
        self.record(event);
        // Taken out and put back rather than drained in place, because a caller
        // whose cursor is ahead of the log — a console that sent a sequence
        // number this node has not reached — has nothing to read even now, and
        // must stay parked instead of being answered with an empty page.
        for (after, deferred) in std::mem::take(&mut self.waiting) {
            match self.page(after) {
                Some(page) => deferred.send(page),
                None => self.waiting.push((after, deferred)),
            }
        }
    }
}

// ─────────────────────────────────────────────────────────────────── the console

/// A console: an ordinary peer that reads a node's [`Inspect`](inspect) service.
///
/// It owns one thing the raw [`Client`](inspect::Client) does not, and it is the
/// only part of this protocol that is easy to get wrong: **the cursor.** A
/// console that forgets to advance it re-reads the same page forever, and one
/// that advances it past `next` parks until the node produces an event it will
/// then be told it missed. [`events`](Self::events) is that loop, written once.
///
/// ```ignore
/// let mut console = Console::connect(&node, serving_addr).await?;
/// println!("{:#?}", console.actors().await?);
/// loop {
///     let page = console.events().await?;
///     if page.lost > 0 { eprintln!("missed {}", page.lost); }
///     for event in page.events { render(&event); }
/// }
/// ```
///
/// # It compiles for a browser, and that is not decoration
///
/// Every call here is a client call. There is no accept loop, no reverse dial
/// and no service the console has to serve, so a console is exactly as much of a
/// node as a tab can be. The alternative — the accept side dialling back, which
/// is what [`Subscribers`](crate::Subscribers) supports — would have made the
/// console a server and taken wasm off the table for it.
pub struct Console {
    client: inspect::Client,
    at: u64,
}

/// By hand, because a generated `Client` has no `Debug` — and the cursor is the
/// one field worth printing anyway.
impl std::fmt::Debug for Console {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Console").field("at", &self.at).finish()
    }
}

impl Console {
    /// Dial a node's inspect service.
    ///
    /// Refused at the handshake if that service's [`Allowlist`](crate::Allowlist)
    /// does not name you — an ALPN of its own means this is a separate decision
    /// from whatever else the node serves.
    ///
    /// # Errors
    ///
    /// [`Error::Transport`](crate::Error::Transport) when the dial fails, which
    /// is what a refusal by the allowlist arrives as.
    pub async fn connect(node: &Node, peer: impl Into<EndpointAddr>) -> Result<Self> {
        Ok(Self {
            client: inspect::Client::connect(node, peer).await?,
            at: 0,
        })
    }

    /// Every actor mounted on that node.
    ///
    /// # Errors
    ///
    /// Whatever the call failed with.
    pub async fn actors(&self) -> Result<Vec<ActorStatus>> {
        self.client.actors().await
    }

    /// That node.
    ///
    /// # Errors
    ///
    /// Whatever the call failed with.
    pub async fn node(&self) -> Result<NodeStatus> {
        self.client.node().await
    }

    /// The next events, waiting until there are some.
    ///
    /// Advances the cursor, so a loop over this sees every event the node's log
    /// still held — and reports the ones it did not at
    /// [`EventPage::lost`](EventPage::lost).
    ///
    /// This does not time out. It resolves when the node records an event, and
    /// on a quiet node that is a long time; the call is one QUIC stream and
    /// dropping the future closes it. Wrap it in a timeout if a console needs to
    /// do something else meanwhile.
    ///
    /// # Errors
    ///
    /// Whatever the call failed with.
    pub async fn events(&mut self) -> Result<EventPage> {
        let page = self.client.follow(self.at).await?;
        self.at = page.next;
        Ok(page)
    }
}
