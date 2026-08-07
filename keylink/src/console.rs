//! keylink, serving its own introspection over keylink.
//!
//! A process that runs actors could answer `contains`, `len` and `is_empty`
//! about them, and nothing else, and only from inside itself. This module is the
//! other side of both limits: [`System::status`] is the whole list, and
//! [`Inspect`](crate::inspect) is that list plus a live event log, served on its **own**
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
//! # That allowlist is a secrets boundary, and nothing else says so
//!
//! It is the only thing between a peer and **whatever this node's panics say**.
//! [`LifecycleEvent::Faulted`] carries a panic message, [`EndReason::Panicked`]
//! carries a panic message, and [`EndReason::Observer`] carries the `Display` of
//! an arbitrary `dyn Error` — the errors a durability adapter or an
//! authorization observer produces, which is where a path, a key name or a row
//! id most often lives.
//!
//! The reassurance elsewhere in this module is that a fault crosses as *the
//! message, never the payload*. That is true and it is not the same as safe: the
//! message is exactly the half that carries interpolated state, because
//! `panic!("bad token {token}")` puts the token in it. So this list governs
//! whatever the operators of this node have written into their panics and their
//! errors, and it should be named the way a credential is —
//! [`Allowlist::only`](crate::Allowlist::only) with the ids that may read them,
//! not [`anyone`](crate::Allowlist::anyone).
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
//! **an actor with a subscriber attached goes on answering ordinary calls at
//! once.** `tests/inspect.rs::a_parked_subscriber_does_not_stop_the_actor_serving_turns`
//! attaches one, serves twenty `Actors()` calls while it is parked, and only
//! then wakes it.
//!
//! This paragraph used to claim a stronger property — that such an actor reads
//! `busy == false` — and named a test for it that does not exist. **It cannot
//! exist.** `busy` is a *registry* fact, kept beside a keyed entry and read by
//! [`System::status`](crate::System::status), and the [`Inspector`] is a
//! [`LocalActor`](crate::LocalActor) with no registry entry anywhere. There is
//! nothing to read the flag from, and inventing a second one for actors with no
//! registry would be exactly the parallel fact this crate refuses. The twenty
//! answered calls are the observable form of the same claim and a sharper
//! instrument besides: a handler parked inside a turn fails them, while a flag
//! can be wrong without anybody being kept waiting.
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
        /// Whether the observer said another incarnation cannot help.
        ///
        /// Carried rather than dropped because it is the difference between "this
        /// turn was refused" and "this actor is not coming back", and a console
        /// that showed the first for the second would send an operator looking for
        /// a restart that was never going to happen.
        fatal: bool,
    },
    /// [`Ended::Feed`].
    Feed {
        /// The feed's declared name.
        name: String,
    },
    /// [`Ended::GaveUp`] — a supervisor stopped re-incarnating this actor.
    ///
    /// The one ending a console most needs to distinguish, and the reason
    /// [`Ended`] is worth putting on a wire at all: every other variant means
    /// something asked the actor to stop, and this one means it could not be kept
    /// alive.
    GaveUp {
        /// Restarts spent before giving up. `0` means the first incarnation was
        /// the last — see [`Ended::GaveUp::after`], whose sense this copies.
        after: u32,
        /// The last incarnation's reason, or the factory's reason for refusing to
        /// build one, through `Display`.
        error: String,
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
            Ended::Observer { name, error, fatal } => Self::Observer {
                name: (*name).to_owned(),
                error: error.to_string(),
                fatal: *fatal,
            },
            Ended::Feed { name } => Self::Feed {
                name: (*name).to_owned(),
            },
            Ended::GaveUp { after, error } => Self::GaveUp {
                after: *after,
                error: error.to_string(),
            },
        }
    }
}

/// One node's public key, as it crosses the wire.
///
/// A newtype and not a bare `[u8; 32]`, which is what this was: **domain scalars
/// are newtypes** in this crate, and thirty-two bytes is the one shape where the
/// rule earns most — a hash, a secret key, a blob id and an endpoint id are all
/// `[u8; 32]`, so the bare array is assignable between four things a caller must
/// never confuse.
///
/// The encoding is identical to the array's, so this costs the wire nothing; it
/// moves [`Inspect`](crate::inspect)'s ALPN because the *schema* names a struct where
/// it named an array, which is the correct outcome — a peer built against the
/// old shape is refused at the handshake rather than reading one kind of
/// thirty-two bytes as another.
///
/// The bytes rather than the text, so a console rebuilds an
/// [`EndpointId`](crate::EndpointId) without parsing. [`TryFrom`] rather than
/// [`From`] because not every thirty-two bytes is a point on the curve, and
/// which of them are is `iroh`'s to say rather than this type's.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Schema)]
pub struct NodeKey(pub [u8; 32]);

impl From<&crate::EndpointId> for NodeKey {
    fn from(id: &crate::EndpointId) -> Self {
        Self(*id.as_bytes())
    }
}

impl TryFrom<NodeKey> for crate::EndpointId {
    type Error = iroh::KeyParsingError;

    fn try_from(key: NodeKey) -> std::result::Result<Self, Self::Error> {
        Self::from_bytes(&key.0)
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
    /// This node's public key — the address peers dial. See [`NodeKey`].
    pub id: NodeKey,
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
///
/// # Two loss counters, because there are two places an event is lost
///
/// [`lost`](Self::lost) is the **ring**: events that reached the log and were
/// pushed out of it by newer ones before this console read them.
/// [`dropped`](Self::dropped) is the **channel**: events that never reached the
/// log at all, because [`Events::record`] found it full and would not block a
/// mount to wait.
///
/// Both are here, and that is the fix for a hole this type had: only the first
/// one was, so the loop [`Console`]'s own documentation tells a reader to write
/// — `if page.lost > 0 { .. }` — was blind to the likelier of the two. A burst
/// of mounts deeper than [`Events::new`]'s `depth` is dropped *before* it is
/// sequenced, so it can never appear in a `lost` computed from sequence numbers,
/// and the only place it used to surface was [`NodeStatus::lost`] — a different
/// call, on a different RPC, that a following console has no reason to make.
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
    /// Events this node produced and never sequenced, for the life of the
    /// process.
    ///
    /// The same reading [`NodeStatus::lost`] returns, carried here so that one
    /// call answers "did I see everything". It is a **running total and not a
    /// delta**, exactly as `NodeStatus::lost` is: a console compares it against
    /// the previous page's to learn what it missed since, and a first page whose
    /// `dropped` is non-zero says the node was already losing events before this
    /// console arrived.
    ///
    /// Non-zero means the channel behind [`Events::new`] is too shallow for what
    /// this node does, and the fix is a bigger `depth` there. It is **not**
    /// [`lost`](Self::lost): these events have no sequence number, so no cursor
    /// can name them.
    pub dropped: u64,
    /// The events themselves, oldest first.
    ///
    /// Never empty, and structurally so: the only constructor is
    /// `Inspector::page_after`, which takes a cursor the log has already passed
    /// and therefore has at least the newest event to hand back. A call with
    /// nothing to report parks instead of answering, so an empty page is not a
    /// thing a console has to distinguish from a quiet one.
    pub events: Vec<LifecycleEvent>,
}

/// Why a [`Follow`](inspect::Client::follow) was answered rather than parked.
///
/// Both arms are the serving side saying **no** to a cursor rather than waiting
/// on it, and both exist because parking on them is what an unbounded
/// `Inspector::waiting` looks like from the outside.
///
/// # `#[non_exhaustive]`, where [`EndReason`] beside it is not
///
/// Two rules meet here and they point opposite ways, so this is the reasoning
/// rather than the attribute alone. **Error enums are `#[non_exhaustive]`** in
/// this crate — [`RemoteError`](crate::RemoteError),
/// [`MountError`](crate::MountError), [`Error`](crate::Error) — and this is one.
/// **Enums a caller matches exhaustively are not**, which is why [`EndReason`]
/// and [`LifecycleEvent`] are open: a `_ =>` arm forced on a console would
/// swallow a lifecycle signal, and swallowing one is the failure this module
/// exists against.
///
/// A refusal is not a signal to swallow; it is one to **handle**, and every
/// caller handles all of them the same two ways — report it, or resume from the
/// number in it. A console written against `Ahead` and `Crowded` should keep
/// working on the day a third reason exists, whereas a console written against
/// five endings must be *made* to look at a sixth. That is the difference, and
/// it is why the rules disagree without either being wrong.
///
/// Either way the wire is not the lax half: a new arm moves
/// [`Inspect`](crate::inspect)'s ALPN, so an old console is refused at the
/// handshake rather than handed a variant it cannot decode.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Schema, thiserror::Error)]
#[non_exhaustive]
pub enum FollowRefused {
    /// The cursor names a sequence number this node has not reached.
    ///
    /// **A client error, not a wait**, and the difference is the whole reason
    /// this arm exists. A cursor at `seq` is a console that has caught up and is
    /// parked correctly; a cursor *past* `seq` is a console asking about events
    /// that do not exist, and `follow(u64::MAX - 1)` asks about events that
    /// never will. Parking those was an entry in [`Inspector`]'s waiting list
    /// that no event could ever release.
    ///
    /// # It is also how a **restart** reaches a console
    ///
    /// The sequence counter starts at zero in every process, so a node that
    /// restarted has a log *behind* the cursor a console was holding — which for
    /// a counter that only goes up means it is not the same log. A console that
    /// asked `follow(50)` of a restarted node used to park until fifty fresh
    /// events existed and then be handed a page computed against the new ring,
    /// with the first fifty events of the new incarnation gone and unreported.
    /// It is told at once now, with both numbers, and `seq` is where to resume.
    #[error(
        "cursor {after} is ahead of this node's log, which is at {seq}: \
             either the cursor is not this node's, or this node has restarted"
    )]
    Ahead {
        /// The cursor that was asked from.
        after: u64,
        /// Where this node's log actually is. Resume from here.
        seq: u64,
    },
    /// Too many callers are already parked on this inspector.
    ///
    /// [`MAX_FOLLOWERS`] is the limit and carries the argument for the number.
    /// Retryable: one event releases every parked caller at once, so a node that
    /// is doing anything at all clears this by itself.
    #[error("{parked} callers are already following this node, which is its limit")]
    Crowded {
        /// How many were parked when this call arrived.
        parked: u64,
    },
}

/// Callers one [`Inspector`] will hold parked at once.
///
/// # The bound this replaces was false, and it was written down as a refusal
///
/// `Inspector::waiting` carried the claim that it needed no cap because *"every
/// entry is one open QUIC stream, and iroh caps those at
/// `max_concurrent_bidi_streams` (100) per connection"*. **The entry outlives the
/// stream.** Nothing removed a parked caller except an event arriving for it, so
/// a peer that opened a stream, called [`Follow`](inspect::Client::follow) and
/// went away left its entry behind until the node recorded something — and on a
/// quiet node, that is forever. A peer admitted by the allowlist and merely buggy
/// could grow that list without bound, one call at a time, which is the refused
/// designs' own argument about an unbounded mailbox turned inward on the first
/// service this crate puts on the wire.
///
/// # Why a hundred and twenty-eight
///
/// [`MAX_WAITING_PER_PEER`](crate::MAX_WAITING_PER_PEER)'s number, for its
/// reason: it is the ceiling a *legitimate* client reaches, and an inspect
/// service's legitimate client count is smaller than an idempotency ledger's by
/// construction — a console is an operator's, admitted by an
/// [`Allowlist`](crate::Allowlist) that names them. One console needs one parked
/// call, so a hundred and twenty-eight is a hundred and twenty-eight consoles,
/// which is more of them than a node has operators.
///
/// It is a constant and not a builder knob because there is nothing to trade: a
/// cap this far above the legitimate ceiling costs a correct caller nothing, and
/// a caller past it is refused with [`FollowRefused::Crowded`] rather than
/// dropped. The [`Eviction`](crate::Eviction)/`Retention`/`Allowlist` rule —
/// type it at the call site — is for decisions with no safe default, and this
/// one has one.
pub const MAX_FOLLOWERS: usize = 128;

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
///
/// **That ending was unreachable, and the cause was in [`Inspector`].** It held
/// an [`Events`] clone for the drop counter, which is to say a `Sender` for this
/// very channel, held inside the actor reading it — so "every clone dropped"
/// could not happen while the actor existed, and the actor could not end without
/// it happening. The inspector keeps only the counter now; see
/// `Inspector::dropped`.
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
    ///
    /// Not every call parks. A cursor **ahead** of this node's log is refused
    /// rather than waited on, and so is a call that arrives with
    /// [`MAX_FOLLOWERS`](keylink::MAX_FOLLOWERS) already parked — see
    /// [`FollowRefused`](keylink::FollowRefused), which carries why each of those
    /// is a refusal and not a wait.
    ///
    /// `core::result::Result` spelled in full because `Result` inside this crate
    /// is [`crate::Result`], which takes one parameter — the same collision a
    /// caller never meets, since a `service!` block in their crate resolves the
    /// prelude's.
    #[defer]
    rpc Follow(after: u64) -> ::core::result::Result<EventPage, FollowRefused>;

    /// Where the events come from. Local, wired at construction, never on the
    /// wire — see [`Events`](keylink::Events).
    feed Events(event: LifecycleEvent);
}

/// What one `Follow` resolves to: a page, or the serving side's refusal.
///
/// An alias because the type is written at six sites — the `service!` block, the
/// handler's two parameters, the waiting list, and both halves of the answer —
/// and a nested `Result` spelled six times is a place for six spellings to
/// disagree.
type Followed = std::result::Result<EventPage, FollowRefused>;

/// The state behind [`inspect`](crate::inspect): what is running, and what has happened.
///
/// # It holds a closure and not a `System<M>`, and that is the seam
///
/// A [`System`] is generic in its service's mailbox type and an `Inspect`
/// service is not generic in anything — a `service!` block generates concrete
/// types. Erasing the system to `Fn() -> Option<Vec<ActorStatus>>` is what lets
/// one console answer for a system of any service, and it is a narrow erasure:
/// the only thing this can do to that system is ask it what is running. It
/// cannot mount, shut down, abort or address anything, and no `Arc<dyn Any>`
/// downcast gets those back.
///
/// **It is also a `Weak` erasure, and that was a hole.** The closure held a
/// `System` *clone*, so an inspector kept the registry — and therefore every
/// actor in it — alive after the program had let go of the system. `Option` is
/// what that costs: `None` is the system being gone, which is not the same fact
/// as a system with nothing in it.
pub struct Inspector {
    /// The one thing this may ask a system, and never enough to keep it.
    actors: Arc<dyn Fn() -> Option<Vec<ActorStatus>> + Send + Sync>,
    id: NodeKey,
    /// The sink's drop counter, and **not** the sink.
    ///
    /// An [`Events`] clone would hold a `Sender` for the very channel this actor
    /// reads, so the feed's every sender could never be gone and
    /// [`Ended::Feed { name: "Events" }`](crate::Ended::Feed) would be
    /// unreachable — an ending this module documents as the loud answer to a
    /// subscription that quietly died. The counter is the only half the
    /// inspector needs, so the counter is the only half it takes.
    dropped: Arc<AtomicU64>,
    /// The last `history` events, oldest first, each with its sequence number.
    log: VecDeque<(u64, LifecycleEvent)>,
    history: usize,
    /// The sequence number of the newest event ever recorded — including the
    /// ones the ring has since dropped, which is what makes
    /// [`EventPage::lost`] arithmetic rather than a counter.
    seq: u64,
    /// Callers parked with nothing to read, each with the cursor it asked from.
    ///
    /// **Bounded twice, and the second bound is what makes the first one true.**
    /// [`MAX_FOLLOWERS`] caps the length outright. And every cursor in here is
    /// `<= seq` as of its park — [`Self::follow`] refuses the rest — so the very
    /// next recorded event has a sequence number past all of them and
    /// [`Self::on_events`] empties the whole list. Nothing accumulates across
    /// events, which is what the previous version's stated bound *claimed* and
    /// could not deliver: an entry survived the QUIC stream it was said to be
    /// one of.
    waiting: Vec<(u64, Deferred<Followed>)>,
}

impl Inspector {
    /// Watch `system` on `node`, keeping the last `history` events.
    ///
    /// `events` is the sink the system and its observers were built with — the
    /// same value, so that [`NodeStatus::lost`] and [`EventPage::dropped`] report
    /// the drops of the channel this inspector is reading and not of some other
    /// one. Only its **counter** is kept; see `Inspector::dropped`.
    ///
    /// `history` is a count of events, not of bytes and not of time. Past it the
    /// oldest is dropped and a console that was behind is told how many it missed
    /// at [`EventPage::lost`].
    #[must_use]
    pub fn new<M>(system: &System<M>, node: &Node, events: &Events, history: NonZeroUsize) -> Self
    where
        M: Send + 'static,
    {
        let actors = system.weak_status();
        Self {
            actors: Arc::new(actors),
            id: NodeKey::from(&node.id()),
            dropped: Arc::clone(&events.lost),
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

    /// The page for a cursor the log has already passed.
    ///
    /// **Total, and never empty**, on one precondition: `after < self.seq`. The
    /// log holds the newest event at exactly `self.seq`, so a cursor below that
    /// has at least one event to be handed. That is why the emptiness
    /// [`EventPage::events`] rules out is ruled out *structurally* rather than by
    /// a check — the two callers are [`Self::page`], which applies the
    /// precondition, and [`Self::on_events`], where the precondition is the
    /// invariant [`Self::waiting`] is kept under.
    fn page_after(&self, after: u64) -> EventPage {
        let events: Vec<LifecycleEvent> = self
            .log
            .iter()
            .filter(|(seq, _)| *seq > after)
            .map(|(_, event)| event.clone())
            .collect();
        // The oldest sequence still held, against the first one this caller had
        // not seen. `saturating_sub` is the caller who is not behind at all.
        let oldest = self.log.front().map_or(0, |(seq, _)| *seq);
        EventPage {
            next: self.seq,
            lost: oldest.saturating_sub(after + 1),
            dropped: self.dropped.load(Ordering::Relaxed),
            events,
        }
    }

    /// Everything after `after`, or `None` when there is nothing — which is what
    /// makes a call park rather than answer with an empty list.
    ///
    /// `after >= self.seq` is exactly the `None`, and writing the condition
    /// rather than collecting and testing for empty is what lets
    /// [`Self::page_after`] be total.
    fn page(&self, after: u64) -> Option<EventPage> {
        (after < self.seq).then(|| self.page_after(after))
    }
}

impl inspect::Actor for Inspector {
    async fn actors(&mut self, _cx: Caller<'_>) -> Vec<ActorStatus> {
        // A system that is gone has no actors, which is what an empty list says.
        // The two are distinguishable through `node()`, and only just — see
        // `NodeStatus::actors`.
        (self.actors)().unwrap_or_default()
    }

    async fn node(&mut self, _cx: Caller<'_>) -> NodeStatus {
        NodeStatus {
            id: self.id,
            // The same reading `actors()` returns, counted rather than a second
            // call to `System::len` — one source, so the two answers a console
            // gets from one node cannot disagree by a mount.
            actors: (self.actors)().map_or(0, |rows| u64::try_from(rows.len()).unwrap_or(u64::MAX)),
            lost: self.dropped.load(Ordering::Relaxed),
        }
    }

    async fn follow(
        &mut self,
        _cx: Caller<'_>,
        reply: Reply<Followed>,
        after: u64,
    ) -> Replied<Followed> {
        if let Some(page) = self.page(after) {
            return reply.send(Ok(page));
        }
        // Refused rather than parked, in the two cases where waiting is the
        // wrong answer. `FollowRefused` carries both arguments; what they have in
        // common is that parking on either is how the waiting list stopped being
        // bounded by anything.
        if after > self.seq {
            return reply.send(Err(FollowRefused::Ahead {
                after,
                seq: self.seq,
            }));
        }
        if self.waiting.len() >= MAX_FOLLOWERS {
            return reply.send(Err(FollowRefused::Crowded {
                parked: u64::try_from(self.waiting.len()).unwrap_or(u64::MAX),
            }));
        }
        // Nothing to say, so the reply outlives the turn and the actor goes back
        // to its loop. This is the line the whole module is arranged around.
        let (deferred, replied) = reply.later();
        self.waiting.push((after, deferred));
        replied
    }

    async fn on_events(&mut self, event: LifecycleEvent) {
        self.record(event);
        // Drained, never re-parked: `record` has just advanced `seq`, and every
        // cursor in here was `<= seq` before it did, so `page_after`'s
        // precondition holds for all of them and each one has a page. The version
        // that took the list out and put unsatisfiable entries back is what made
        // the list unbounded — and after `follow`'s refusal there is no such
        // entry to put back.
        for (after, deferred) in std::mem::take(&mut self.waiting) {
            deferred.send(Ok(self.page_after(after)));
        }
    }
}

// ─────────────────────────────────────────────────────────────────── the console

/// A console: an ordinary peer that reads a node's [`Inspect`](crate::inspect) service.
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
///
/// // Both loss counters, because there are two places an event is lost and one
/// // of them has no sequence number to be missed by. See `EventPage`.
/// let mut dropped = 0;
/// loop {
///     let page = console.events().await?;
///     if page.lost > 0 { eprintln!("{} fell out of the node's ring", page.lost); }
///     if page.dropped > dropped {
///         eprintln!("{} never reached it at all", page.dropped - dropped);
///         dropped = page.dropped;
///     }
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
    /// [`EventPage::lost`](EventPage::lost) and
    /// [`EventPage::dropped`](EventPage::dropped).
    ///
    /// This does not time out. It resolves when the node records an event, and
    /// on a quiet node that is a long time; the call is one QUIC stream and
    /// dropping the future closes it. Wrap it in a timeout if a console needs to
    /// do something else meanwhile.
    ///
    /// # A refusal here means the node restarted
    ///
    /// A [`Console`] starts its cursor at zero and only ever advances it to a
    /// `next` the node itself sent, so it cannot get **ahead** of that node's log
    /// by its own doing. It can be overtaken from the other side: the sequence
    /// counter starts at zero in every process, so a node that restarted is
    /// behind a cursor that was correct a moment ago.
    ///
    /// That arrives as [`FollowRefused::Ahead`], rendered into
    /// [`Error::Remote`](crate::Error::Remote) — its message carries both
    /// numbers, and the `seq` in it is where to resume. It is deliberately **not**
    /// caught and retried here: resuming silently would hide a restart, and every
    /// event of the node's new incarnation before that point would be gone with
    /// nothing said. A console that wants to act on the distinction rather than
    /// read it calls [`inspect::Client::follow`] directly, where the refusal is
    /// still a [`FollowRefused`].
    ///
    /// # Errors
    ///
    /// Whatever the call failed with, plus
    /// [`Error::Remote`](crate::Error::Remote) for a [`FollowRefused`].
    pub async fn events(&mut self) -> Result<EventPage> {
        let page = self
            .client
            .follow(self.at)
            .await?
            .map_err(|refused| crate::RemoteError::Failed(refused.to_string()))?;
        self.at = page.next;
        Ok(page)
    }
}
