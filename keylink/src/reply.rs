//! What comes back from a call.
//!
//! A handler returns a value. Anything *around* the handler — a timeout, a
//! per-method authorization check, a rate limiter — has to be able to refuse
//! the call instead, and it cannot do that by synthesising a return value it
//! knows nothing about. So every reply carries a second channel: an [`Outcome`]
//! is the handler's value, or a [`RemoteError`] the caller sees as
//! [`crate::Error::Remote`].
//!
//! [`WireResponse`] names what the response direction actually carries, and the
//! hashed contract names the same alias. That is what makes adding a
//! [`RemoteError`] variant move the ALPN, so a drifted peer is refused at the
//! handshake rather than misreading one refusal as another.

use serde::{Deserialize, Serialize};

/// Why the serving side refused a call.
///
/// Constructed by handlers and by [`Middleware`](crate::Middleware). The caller
/// receives it as [`crate::Error::Remote`].
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
    #[error("no actor is mounted as {id}")]
    NoSuchActor {
        /// The id that was addressed.
        id: String,
    },

    /// The actor exists, and the caller built its message against different
    /// types.
    ///
    /// The payload was dropped without being decoded. This is the in-envelope
    /// counterpart of [`crate::Error::SchemaMismatch`], one layer down: an
    /// actor system carries every actor's messages behind one ALPN, so the
    /// handshake cannot see per-actor drift and this variant is what does.
    #[error("actor message schema mismatch: ours is {ours:016x}, theirs is {theirs:016x}")]
    ActorSchemaMismatch {
        /// The hash of the type the actor is mounted with.
        ours: u64,
        /// The hash the caller declared.
        theirs: u64,
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

/// The exact type the response direction carries on the wire.
///
/// Named once, and named by BOTH the wire and the hashed contract, because they
/// drifted apart once already: the contract hashed the bare response while the
/// wire carried `Outcome<Response>`, so adding a `RemoteError` variant did not
/// move the ALPN. Inserting a variant then decoded one refusal as another —
/// `Failed("disk full")` arriving as a different variant carrying that string.
///
/// Change what the wire carries by changing this alias, and the contract
/// follows. Do not spell `Outcome<T>` at either site.
pub type WireResponse<Resp> = Outcome<Resp>;

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
