//! What comes back from a call.
//!
//! A handler returns a value. Anything *around* the handler — a timeout, a
//! per-method authorization check, a rate limiter — has to be able to refuse
//! the call instead, and it cannot do that by synthesising a return value it
//! knows nothing about. So every reply carries a second channel: an [`Outcome`]
//! is the handler's value, or a [`RemoteError`] the caller sees as
//! [`crate::Error::Remote`].
//!
//! [`WireRequest`] and [`WireResponse`] name what the two directions actually
//! carry, and the hashed contract names the same two types. That is what makes
//! adding a [`RemoteError`] variant move the ALPN, so a drifted peer is refused
//! at the handshake rather than misreading one refusal as another.
//!
//! # Not everything on the wire is a call
//!
//! A caller can also ask what the serving side's ledger knows about one of its
//! idempotency keys. That question has no handler, no body and no layer to run
//! through — it is a read of keylink's own bookkeeping — so it is a second arm
//! of the request type rather than a method on the user's service. Keeping it
//! here means a service's `Request` enum stays exactly the methods its author
//! wrote, and the query is peer-scoped by construction: it is answered from the
//! ledger of the connection it arrived on.

use serde::{Deserialize, Serialize};

use crate::{FollowRefused, IdempotencyKey, KeyState, Withheld};

/// Why the serving side refused a call.
///
/// Constructed by handlers and by [`Middleware`](crate::Middleware). The caller
/// receives it as [`crate::Error::Remote`].
///
/// # One variant was removed, and it moved every ALPN in the fleet
///
/// `ActorSchemaMismatch { ours, theirs }` sat fifth of six, and it said "the
/// actor exists, and you built its message against different types". It had to
/// exist while one system-wide ALPN carried every actor on a node: the handshake
/// could only prove agreement about an envelope, so per-actor drift was caught
/// inside it.
///
/// A keyed system is per-service now. Its ALPN hashes
/// `Envelope<WireRequest<Addressed<Request>>>`, so drift is refused at the
/// handshake exactly as it is for a singleton, and a `System` of one service
/// physically cannot hold an actor of another. There is nothing left for the
/// variant to report.
///
/// Removing it renumbered [`Failed`](Self::Failed) from index 5 to 4. This enum
/// is inside every service's hashed contract — `Envelope<WireResponse<Response>>`
/// reaches `Outcome::Err(RemoteError)` — so that renumbering is **wire-visible
/// for every service, singletons included**, and it is meant to be: a peer built
/// against the old numbering would decode `Failed("disk full")` as a variant
/// that no longer exists. Refusing it at the handshake is the correct outcome.
///
/// # Five variants were appended, and the append is the point
///
/// [`Withheld`](Self::Withheld), [`AtCapacity`](Self::AtCapacity),
/// [`BadActorId`](Self::BadActorId), [`FollowRefused`](Self::FollowRefused) and
/// [`MailboxFull`](Self::MailboxFull)
/// went on the **end**. An append and an insert move the hash equally — this
/// enum is in every service's contract, for the reason above — so the handshake
/// refuses a drifted peer either way and that is not what separates them. What
/// separates them is that an insert renumbers every variant after it, so the
/// wire meaning of code the two sides already agreed on changes underneath them,
/// and the failure is a misdecode rather than a refusal. An append leaves those
/// indices exactly where they were.
///
/// **All five have a producer.** This paragraph said *nothing produces the four
/// yet* long after that stopped being true, which is what a sentence about
/// somebody else's future does: [`Withheld`](Self::Withheld) is answered on both
/// served paths, [`AtCapacity`](Self::AtCapacity) by the keyed door's
/// `RouteError` conversion, [`BadActorId`](Self::BadActorId) by both `validate`
/// calls, [`FollowRefused`](Self::FollowRefused) by `console.rs`'s follow, and
/// [`MailboxFull`](Self::MailboxFull) by the served singleton door. Each move was
/// a separate commit from the one that paid the ALPN for it, which is what the
/// sentence was really recording.
#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, postcard_schema::Schema, thiserror::Error,
)]
#[non_exhaustive]
pub enum RemoteError {
    /// The peer is known, but not for this method.
    #[error("unauthorized: {0}")]
    Unauthorized(String),

    /// The handler did not finish inside its budget.
    #[error("timed out after {millis} ms")]
    TimedOut {
        /// The budget that expired.
        millis: u64,
    },

    /// The call carried an idempotency key, and the key is why it did not run.
    ///
    /// One variant rather than four, because this type belongs to every service
    /// keylink will ever carry and that vocabulary belongs to one feature.
    #[error("idempotency: {0}")]
    Idempotency(crate::IdempotencyRefusal),

    /// No actor is mounted under this id.
    ///
    /// Deliberately does NOT distinguish "never existed" from "was stopped".
    /// The caller's next action is the same either way, and telling them apart
    /// would leak that an id was once real.
    ///
    /// **Both of those are about the id, and that is the whole of what this
    /// variant claims.** A call that reached a live actor and lost its answer is
    /// not one of them: a handler may take a `#[defer]`red reply out of its turn
    /// and drop it, which answers nobody while the actor goes on serving the
    /// same connection. That reads as
    /// [`Withheld(Withheld::Dropped)`](Self::Withheld) instead, because a peer
    /// told `NoSuchActor` is being told to stop asking and it would be false.
    /// It read [`Failed`](Self::Failed) under a `"dropped: "` prefix until
    /// `5198ecb` replaced the five prefixes with the variant.
    ///
    /// **On both served paths**, which this sentence could not say for one wave:
    /// `turns.rs`'s `deliver_to_singleton` separated the causes and
    /// `actor/remote.rs`'s `deliver_to_actor` rendered all of them as this
    /// variant, so the paragraph above was a promise a keyed actor did not keep.
    /// The two render through `turns.rs`'s `lost` now, off a verdict each actor's
    /// own loop settles before it drops anything.
    #[error("no actor is mounted as {id}")]
    NoSuchActor {
        /// The id that was addressed.
        id: String,
    },

    /// Anything the serving side wants to report that this enum has no variant
    /// for.
    ///
    /// # It means a layer somebody wrote, and keylink builds exactly one
    ///
    /// Every refusal keylink itself produces has a variant here — a withheld
    /// turn, a timeout, an idempotency refusal, a capacity refusal, a malformed
    /// id. So a peer reading a `Failed` is reading a sentence written **in its own
    /// deployment**, by a layer that answered `Outcome::Err(RemoteError::Failed(..))`
    /// or by a handler that returned one. That is what makes the `String` useful
    /// to that peer and useless to this crate: it is not a discriminator, and
    /// nothing in keylink matches on it.
    ///
    /// **The one in-crate exception is named here rather than left in a comment
    /// four thousand lines away**, which is where it lived and where no reader of
    /// this variant would meet it: `turns.rs`'s `deliver_to_singleton` answers a
    /// `Failed` on its `NotQueued::Reentrant` arm, carrying
    /// [`Unanswered`](crate::Unanswered)'s own sentence. A peer cannot produce a
    /// re-entrant call — its request arrives on a connection's task, which has run
    /// no turn — so that arm is written to be right rather than to fire.
    /// `turns.rs`'s `neither_served_door_answers_a_failed` holds the rest of the
    /// boundary: nothing `rendered` or `lost` answers is one of these.
    ///
    /// The `String` **was** a discriminator that keylink wrote and a peer read off
    /// the front with `starts_with`, against five prefix constants on this type.
    /// The losses it discriminated are [`Withheld`](Self::Withheld) variants now,
    /// and the constants are deleted.
    #[error("{0}")]
    Failed(String),

    /// The call reached an actor and got no value back, and this is which loss
    /// it was.
    ///
    /// [`Withheld`] is the same vocabulary
    /// [`Unanswered`](crate::Unanswered) and [`TurnLost`](crate::TurnLost) give a
    /// local caller. It is a variant here so that a peer **matches** the loss
    /// instead of reading it off the front of a [`Failed`](Self::Failed) string
    /// with `starts_with`, which is what the five prefix constants this type used
    /// to carry asked a peer to do.
    #[error("{0}")]
    Withheld(Withheld),

    /// The system is at its cap of live actors and would not build another.
    ///
    /// Retryable in the only sense that matters: the cap is a *live* count, so
    /// an eviction or a shutdown clears it. `SystemBuilder::capacity` sets it.
    #[error("the actor system is at its capacity of {capacity} live actors")]
    AtCapacity {
        /// The cap that was met.
        ///
        /// `u64` and not `usize` for [`NodeStatus::actors`](crate::NodeStatus)'
        /// reason: `usize` has no wire schema at all, because its width is the
        /// compiler's choice and a browser's is not the server's.
        capacity: u64,
    },

    /// The peer addressed something that is not a valid
    /// [`ActorId`](crate::ActorId).
    ///
    /// Refused before any lookup, so it says nothing about what is mounted —
    /// unlike [`NoSuchActor`](Self::NoSuchActor), which is an answer about a
    /// well-formed id.
    ///
    /// # Why `reason` is a field rather than a sentence
    ///
    /// The `Failed` this replaces rendered `ActorId::validate`'s own error,
    /// which is `Error::Invalid { kind, reason }` — so a peer learned *why* the
    /// id was refused, not merely that it was. Carrying only the id would have
    /// told a peer to fix something without saying what, which is the loss this
    /// whole vocabulary exists to stop. It is a separate field rather than
    /// baked into the id so a caller can match on the id and log the reason.
    #[error("{id} is not a valid actor id: {reason}")]
    BadActorId {
        /// The id as the peer sent it.
        id: String,
        /// Why `validate` refused it — length, character set, or shape.
        ///
        /// Rendered from [`ActorId::validate`](crate::ActorId::validate)'s
        /// error, so the sentence a peer reads is the one the crate uses
        /// locally rather than a second wording of the same rule.
        reason: String,
    },

    /// A console's [`Follow`](crate::inspect::Client::follow) was answered
    /// rather than parked.
    ///
    /// [`FollowRefused`] carries which of the two reasons
    /// it was and the number to resume from.
    #[error("{0}")]
    FollowRefused(FollowRefused),

    /// This node serves the service some other way than as an actor, so there
    /// is no turn to report.
    ///
    /// The answer to the generated `__status` method from a service built with
    /// the concurrent `service(handler, allow)` door. **One ALPN carries both
    /// ways of serving a singleton** — the same `Request` reaches a handler and
    /// an actor — so which of the two a node chose is a fact about that node
    /// that a peer can learn only from the answer.
    ///
    /// A refusal rather than a [`Serving`](crate::Serving) with both fields at
    /// rest, which is what a concurrent service would truthfully fold: every
    /// call there takes its own task, so nothing is ever inside a serial turn
    /// and nothing is ever queued behind one. A peer reading that pair could
    /// not tell it from an idle actor, and a diagnostic whose "nothing is
    /// happening" is indistinguishable from "this question does not apply here"
    /// is the silent fallback this crate refuses.
    ///
    /// It is not about an id and says nothing about one, which is what
    /// separates it from [`NoSuchActor`](Self::NoSuchActor): the keyed door
    /// answers that one for an id nothing is mounted under, and this one is
    /// answered before any actor is looked for.
    #[error("this service is not served as an actor here, so it has no turn to report")]
    NotAnActor,

    /// The actor's mailbox was full and it is served under
    /// [`Backpressure::Shed`](crate::Backpressure::Shed), so the call was refused
    /// instead of queued.
    ///
    /// **It asks for backoff, not for a retry**, and that is the whole reason it
    /// is not [`TimedOut`](Self::TimedOut). A retry policy reads the two
    /// differently: a timeout invites the same call again, an overload says the
    /// far side is behind and a second call now makes it worse. Rendering a shed
    /// as a timeout would hide a load signal inside a latency one.
    ///
    /// **Not [`AtCapacity`](Self::AtCapacity), which already exists and means
    /// something else.** That one is the *registry* — too many live actors, and
    /// `SystemBuilder::capacity` sets it. This one is one actor's own mailbox.
    /// Two bounds, two facts, and a peer that folded them would back off from a
    /// whole node when one actor is hot.
    ///
    /// **Not a [`Withheld`](Self::Withheld) variant.** `Withheld` is why a call
    /// produced no value on every path that *has* one — a turn that ran and lost.
    /// A shed happens before the turn, which is the ruling `Withheld` already
    /// records for `Reentrant`: no turn ran, so nothing was withheld.
    ///
    /// The local half of the same condition is
    /// [`NotQueued::Full`](crate::NotQueued::Full), and it carries the argument
    /// for why the arguments are gone. A peer has no permit to take — that door
    /// is local — so a peer that meets this has lost the call and rebuilds it.
    #[error("the actor's mailbox is full: back off rather than retry")]
    MailboxFull,
}

/// The reply as it appears on the wire: a value, or a refusal.
#[derive(Debug, Clone, Serialize, Deserialize, postcard_schema::Schema)]
pub enum Outcome<T> {
    /// The handler's return value.
    Ok(T),
    /// The call was refused, by the handler or by a layer above it.
    Err(RemoteError),
}

/// The exact type the request direction carries on the wire.
///
/// One name, used by both the wire and the hashed contract, so the two cannot
/// drift. See [`WireResponse`] for what that drift cost last time.
#[derive(Debug, Clone, Serialize, Deserialize, postcard_schema::Schema)]
pub enum WireRequest<Req> {
    /// An ordinary call: the service's own request, headed for its handler
    /// through whatever layers are installed.
    Call(Req),
    /// A question about this peer's idempotency ledger. Answered without
    /// entering the handler and without running a layer, because there is
    /// nothing to authorize past the connection's own admission: a peer can
    /// only ever ask about its own keys.
    KeyState(IdempotencyKey),
}

/// The exact type the response direction carries on the wire.
///
/// Named once, and named by BOTH the wire and the hashed contract, because they
/// drifted apart once already: the contract hashed the bare response while the
/// wire carried `Outcome<Response>`, so adding a `RemoteError` variant did not
/// move the ALPN. Inserting a variant then decoded one refusal as another —
/// `Failed("disk full")` arriving as a different variant carrying that string.
///
/// Change what the wire carries by changing this type, and the contract
/// follows. Do not spell `Outcome<T>` at either site.
#[derive(Debug, Clone, Serialize, Deserialize, postcard_schema::Schema)]
pub enum WireResponse<Resp> {
    /// The answer to a [`WireRequest::Call`].
    Call(Outcome<Resp>),
    /// The answer to a [`WireRequest::KeyState`].
    KeyState(KeyState),
}

impl<Resp> WireResponse<Resp> {
    /// Which arm this is, for an error that has to name what came back.
    pub(crate) fn arm(&self) -> &'static str {
        match self {
            Self::Call(_) => "a call reply",
            Self::KeyState(_) => "a key-state reply",
        }
    }
}

impl<T> Outcome<T> {
    /// Whether this refuses the call. Lets logging and metrics layers report an
    /// outcome without knowing the success type.
    pub fn is_err(&self) -> bool {
        matches!(self, Self::Err(_))
    }
}

impl<T> From<RemoteError> for Outcome<T> {
    fn from(err: RemoteError) -> Self {
        Self::Err(err)
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::expect_used)]

    use super::*;

    /// A layer refuses a call without knowing the method's return type. That is
    /// only sound because the `Err` arm carries no `T`, so its encoding is
    /// identical for every instantiation. If postcard ever changed that, a
    /// refusal written for one method would misdecode on another.
    #[test]
    fn the_error_arm_encodes_identically_for_every_return_type() {
        let err = RemoteError::TimedOut { millis: 250 };
        let as_u64 = postcard::to_stdvec(&Outcome::<u64>::Err(err.clone())).expect("encodes");
        let as_string = postcard::to_stdvec(&Outcome::<String>::Err(err)).expect("encodes");
        assert_eq!(as_u64, as_string);
    }

    #[test]
    fn a_value_round_trips_through_the_envelope() {
        let bytes = postcard::to_stdvec(&Outcome::Ok("muda de ipê".to_string())).expect("encodes");
        let decoded: Outcome<String> = postcard::from_bytes(&bytes).expect("decodes");
        let Outcome::Ok(text) = decoded else {
            panic!("expected the ok arm");
        };
        assert_eq!(text, "muda de ipê");
    }

    #[test]
    fn a_refusal_reports_itself_as_an_error() {
        let refused: Outcome<u64> = RemoteError::Unauthorized("write".into()).into();
        assert!(refused.is_err());
    }
}
