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

use crate::{IdempotencyKey, KeyState};

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
    /// told to stop asking and it would be false. `local.rs`'s `deliver_to_local`
    /// is where the three causes are separated.
    #[error("no actor is mounted as {id}")]
    NoSuchActor {
        /// The id that was addressed.
        id: String,
    },

    /// Anything else the serving side wants to report.
    #[error("{0}")]
    Failed(String),
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
