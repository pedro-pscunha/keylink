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
/// # Four variants were appended, and the append is the point
///
/// [`Withheld`](Self::Withheld), [`AtCapacity`](Self::AtCapacity),
/// [`BadActorId`](Self::BadActorId) and [`FollowRefused`](Self::FollowRefused)
/// went on the **end**. An append and an insert move the hash equally — this
/// enum is in every service's contract, for the reason above — so the handshake
/// refuses a drifted peer either way and that is not what separates them. What
/// separates them is that an insert renumbers every variant after it, so the
/// wire meaning of code the two sides already agreed on changes underneath them,
/// and the failure is a misdecode rather than a refusal. An append leaves those
/// indices exactly where they were.
///
/// Nothing produces the four yet: they are the vocabulary the served paths move
/// onto next, and the move is a separate commit from the one that pays the ALPN
/// for it.
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
    /// same connection. That reads as [`Failed`](Self::Failed) under a
    /// `"dropped: "` prefix instead, because a peer told `NoSuchActor` is being
    /// told to stop asking and it would be false.
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

    /// Anything else the serving side wants to report.
    ///
    /// The `String` is a **discriminator** and not only a message on the served
    /// paths keylink writes: see the prefix constants on this type.
    #[error("{0}")]
    Failed(String),

    /// The call reached an actor and got no value back, and this is which loss
    /// it was.
    ///
    /// [`Withheld`] is the same vocabulary
    /// [`Unanswered`](crate::Unanswered) and [`TurnLost`](crate::TurnLost) give a
    /// local caller. It is a variant here so that a peer **matches** the loss
    /// instead of reading it off the front of a [`Failed`](Self::Failed) string
    /// with `starts_with`, which is what the prefix constants on this type ask a
    /// peer to do today.
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
}

/// The prefixes [`RemoteError::Failed`] carries on keylink's own served paths.
///
/// **Public because a peer has to match on them, and could not name them.** The
/// four were `const`s private to `turns.rs` for four waves, documented as a
/// contract in four rustdocs, while every caller in and out of this repository
/// hard-coded `why.starts_with("dropped: ")`. A contract nobody can name by
/// symbol is a string literal with a paragraph next to it.
///
/// Exporting them costs no ALPN move, and that is worth being exact about: they
/// are **not** part of the hashed contract. `Envelope<WireResponse<Response>>`
/// reaches `Outcome::Err(RemoteError)`, and `postcard_schema` hashes the enum's
/// variants and fields. Associated constants are not either, so
/// `keylink/tests/protocol.rs`'s two pins are unmoved by this — checked, not
/// assumed.
///
/// # The property, and exactly what it is a property OF
///
/// **No one of these five is a prefix of another**, in either direction, so a
/// peer matching with `starts_with` is never ambiguous.
/// `turns::tests::no_failure_prefix_is_a_prefix_of_another` holds it, and a sixth
/// has to be added to that list by hand.
///
/// [`OBSERVER_PANICKED`](Self::OBSERVER_PANICKED) is the fifth and it is the one
/// worth checking a new prefix against: it neither begins with nor is begun by
/// [`PANICKED`](Self::PANICKED), which shares its verb, and the guard is what says
/// so rather than the eye.
///
/// The claim used to read *"every prefix a `RemoteError::Failed` carries on the
/// served path"*, and that was false. keylink builds a `Failed` from at least
/// three other places whose text is a whole sentence rather than one of these:
/// the registry's capacity refusal, an [`ActorId`](crate::ActorId) a peer sent
/// that fails validation, and a console subscription refusal. None of them
/// collides today, and the same guard checks that too — but the claim these
/// constants can carry is the narrow one: **every prefix `rendered` and `lost`
/// build.** A `Failed` whose text came from a caller's own
/// [`Middleware`](crate::Middleware) is outside any guard here and always will
/// be.
impl RemoteError {
    /// Your message exploded and the actor kept its state.
    ///
    /// The turn panicked, the panic was caught rather than unwound into a
    /// connection task, and the actor is still serving. Followed by the panic's
    /// own message, which is usually caller-derived.
    pub const PANICKED: &'static str = "panicked: ";

    /// The turn ran and was not committed.
    ///
    /// An [`Observer`](crate::Observer) refused it — a durability adapter that
    /// could not write, most often — so the actor is ending for a reason worth
    /// telling the peer apart from a fault. Followed by the observer's name.
    pub const REFUSED: &'static str = "observer refused: ";

    /// An observer **panicked** watching the turn, which ran and was not
    /// committed.
    ///
    /// Distinct from [`REFUSED`](Self::REFUSED) because a veto is a decision an
    /// author wrote down and a panic is the absence of one, and distinct from
    /// [`PANICKED`](Self::PANICKED) because that one is the **handler**: your
    /// message exploded, and the actor kept its state. Followed by the observer's
    /// name.
    ///
    /// **A prefix and not a fifth [`RemoteError`] variant.** A variant sits inside
    /// the hashed contract and would move every service's ALPN; a prefix is
    /// content inside `Failed(String)` and moves nothing. RFC-06 §6 refuses the
    /// variant route on exactly this ground.
    ///
    /// A peer has no [`ended`](crate::LocalActor::ended) to fall back on, so this
    /// prefix is the whole of what it learns. Before it existed, a peer read
    /// [`REFUSED`](Self::REFUSED) for both — the worst-informed audience, given
    /// the same false word.
    pub const OBSERVER_PANICKED: &'static str = "observer panicked: ";

    /// The incarnation that owed you an answer is gone, and another is coming.
    ///
    /// **Retry.** A supervised actor's state is dropped when an incarnation
    /// ends, and a reply parked in that state dies with it; the supervisor had
    /// already granted the restart when this was written. Followed by the ending
    /// that caused it.
    pub const RESTARTING: &'static str = "restarting: ";

    /// The actor is serving, and this one reply was never answered.
    ///
    /// **Do not retry blindly, and do not give up on the actor**: it is mounted,
    /// it took the message, and it will answer the next one. A handler took
    /// `reply.later()` and dropped the `Deferred` — sanctioned, and what that
    /// type's `#[must_use]` asks for — or panicked after taking it.
    ///
    /// The sentence after it deliberately does **not** name a cause, and it took
    /// two attempts to stop naming one. Three producers reach it: the sanctioned
    /// drop above, a handler that panicked *after* `reply.later()`, and a handler
    /// that panicked **before** consuming the channel — where what died is the
    /// `Reply` itself, inside the turn, never a `Deferred`. "The reply was taken
    /// out of the turn" described only the first; "the reply left its turn" was
    /// the same claim reworded, and false of the third. What all three share is
    /// the actor.
    pub const DROPPED: &'static str = "dropped: ";
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

    /// **The five prefixes are bytes on a wire, so their values are pinned like
    /// bytes on a wire.**
    ///
    /// They are public now, which is what makes this necessary rather than
    /// pedantic: every other assertion in this repository can name the constant
    /// and would go on passing if the constant's *value* changed, and so would
    /// every downstream `starts_with`. Nothing else anywhere pins the text a peer
    /// actually matches on. This does, in the register `tests/protocol.rs` uses
    /// for an ALPN: a literal that must not move quietly.
    ///
    /// It does **not** claim the five may never change. It claims a change is a
    /// deliberate act with a compatibility argument attached, exactly like a
    /// hash pin — and unlike an ALPN, moving one of these refuses nothing at the
    /// handshake, so an old peer meets it as a `Failed` it silently stops
    /// recognising. That is the more dangerous kind of break and the reason this
    /// is here.
    ///
    /// Made red by `scripts/break-once.sh keylink/src/reply.rs` on
    /// `pub const DROPPED: &'static str = "dropped: ";` →
    /// `pub const DROPPED: &'static str = "lost: ";`. Watched: `left: "lost: "`,
    /// `right: "dropped: "`.
    #[test]
    fn the_five_prefixes_are_the_text_a_peer_matches_on() {
        assert_eq!(RemoteError::PANICKED, "panicked: ");
        assert_eq!(RemoteError::REFUSED, "observer refused: ");
        assert_eq!(RemoteError::OBSERVER_PANICKED, "observer panicked: ");
        assert_eq!(RemoteError::RESTARTING, "restarting: ");
        assert_eq!(RemoteError::DROPPED, "dropped: ");
    }
}
