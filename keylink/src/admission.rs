//! Who may talk to a served protocol, as a port with live-policy adapters.
//!
//! [`Allowlist`] answers the question once, at startup, from a value in
//! memory. That is the right shape for a fleet you deploy and the wrong one for
//! a fleet you operate: revoking a key means a restart, and a restart drops
//! every connection including the ones that were fine.
//!
//! # The hexagon
//!
//! The domain need does not mention a file, an environment variable or an
//! identity provider: **decide about a proven peer, and be able to change your
//! mind while connections are live.**
//!
//! That sentence is [`Admission`] and [`Revocable`]. Everything else is an
//! adapter.
//!
//! ```text
//!            ┌──────────────────────────────┐
//!            │  RpcService::accept          │
//!            └───────────────┬──────────────┘
//!                            │  admits / admitted
//!            ┌───────────────▼──────────────┐
//!            │  PORT   Admission · Revocable│
//!            └───┬─────────────┬────────────┘
//!                │             │
//!        ┌───────▼──────────┐  ┌▼─────────────────┐
//!        │ DynamicAdmission │  │ trust::Trust     │
//!        │  (built)         │  │ (signed records) │
//!        └──────────────────┘  └──────────────────┘
//! ```
//!
//! `Allowlist` is the **value** [`DynamicAdmission`] holds, not a second
//! adapter: a static allowlist is a `DynamicAdmission` nobody replaces. The
//! signed-record adapter is [`crate::trust::Trust`]. Applications can add their own policy systems.
//!
//! # Why revocation is in the port and not in an adapter
//!
//! An [`Admission`] is consulted at accept and never again, so a policy that
//! changes its mind changes nothing about the connections already open. Every
//! adapter *could* close them, and one that quietly did not would be
//! indistinguishable from one that did. So the capability is [`Revocable`], a
//! trait a caller can ask for by name, rather than a promise in a doc comment —
//! the argument `keylink-persist` makes about its own `Claim`, applied here.
//!
//! # Three answers, not two
//!
//! [`Admittance`] has an `Unavailable` arm because a `bool` cannot say "the
//! policy could not be consulted". A failing backend would answer `false`,
//! byte-identical to "not on the list", and an operator who cannot tell a dead
//! authorizer from an unlisted peer reaches for [`Allowlist::anyone`] during an
//! incident. That is a fail-open arrived at through a diagnostic hole.
//! `authz.rs` makes this exact criticism of RFC-01's `Option`-keyed admission.
//!
//! Both refusing arms refuse. They differ in the close code and in the log,
//! which is where the difference has to be visible.
//!
//! # What this does not promise
//!
//! - **Nothing about a peer already inside a stream.** LiveConnection closes the
//!   connection; a call already dispatched runs to completion.
//! - **Each node has its own view.** Feeders must handle explicit capacity refusals;
//!   revocation latency is the feeder's delivery latency.
//! - **No history.** There is no audit trail of past decisions; that is a
//!   different port with different operations.
//! - **Both built-in revocable adapters track every connection.** Each removed
//!   peer's sessions are closed across ALPNs. The returned peer IDs are unique;
//!   `live()` counts connections, not unique identities.

use std::collections::BTreeSet;
use std::sync::{Arc, Mutex, PoisonError};

use iroh::endpoint::Connection;
use iroh::EndpointId;

use crate::{AdmittedPeer, Allowlist, ProvenPeer, Watch};

/// The close code for a peer a policy refused.
pub(crate) const REFUSED: u32 = 1;

/// The close code for a peer whose policy could not be reached.
///
/// Distinct from [`REFUSED`] on purpose: a peer that is told "you are not on the
/// list" retries never, and one that is told "ask again" retries later, and
/// giving them the same code makes an outage look like a revocation to every
/// peer at once.
pub(crate) const UNAVAILABLE: u32 = 2;

/// Why the far side's admission policy closed a connection.
///
/// Admission runs one layer above the handshake, so a refused peer *connects*
/// and then fails on its first call. Both outcomes reach the caller as
/// [`Error::Transport`](crate::Error::Transport) and read exactly like a lost
/// link. [`Error::refusal`](crate::Error::refusal) is what tells them apart.
///
/// The two variants are separate for the reason the `UNAVAILABLE` close code is
/// separate from `REFUSED`: one means stop, and the other means try later.
/// Collapsing them makes an outage look like a revocation to every peer at once.
///
/// Exhaustive, and for [`Admittance`]'s reason rather than by omission: these
/// are the two answers that policy produces, a caller has to act differently on
/// each, and a wildcard arm is exactly the collapse the type exists to prevent.
/// The `Option` around it already carries "not an admission decision".
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Refusal {
    /// The policy said no. **Stop.** Retrying changes nothing until the policy
    /// does, and the caller needs access rather than a backoff.
    Denied,
    /// The policy could not be reached. **Try later.** Nothing was decided
    /// about this caller.
    Unavailable,
}

impl Refusal {
    /// Read one back off a QUIC application-close code.
    pub(crate) const fn from_code(code: u64) -> Option<Self> {
        match code {
            c if c == REFUSED as u64 => Some(Self::Denied),
            c if c == UNAVAILABLE as u64 => Some(Self::Unavailable),
            _ => None,
        }
    }
}

/// What a policy says about a proven peer.
///
/// Exhaustive: a caller writing a policy has to confront all three, and a
/// wildcard arm is exactly the collapse this type exists to prevent.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Admittance {
    /// A policy said yes.
    ///
    /// Carries no value, which is what keeps this trait implementable outside
    /// the crate: [`AdmittedPeer`] is a newtype whose only public constructor is
    /// [`Allowlist::admit`], and that privacy is the invariant. keylink mints
    /// the `AdmittedPeer` itself, at the one call site that has just been told
    /// yes.
    Admitted,
    /// A policy said no.
    Refused,
    /// The policy could not be consulted. Refuses, and says why.
    Unavailable(String),
}

/// A live connection, and the one thing a policy is allowed to do with it.
///
/// Deliberately opaque. It wraps a connection and exposes closing it and
/// nothing else, because a policy that could read or write on it would be a
/// second, unaudited path into the service it is guarding.
///
/// Carries the [`ProvenPeer`] it was admitted for, so a policy re-deciding
/// later has the proof rather than having to re-invent one from an id — which
/// it could not do, since the only constructor takes a completed handshake.
#[derive(Debug, Clone)]
pub struct LiveConnection {
    connection: Connection,
    peer: ProvenPeer,
}

impl LiveConnection {
    pub(crate) fn id(&self) -> usize {
        self.connection.stable_id()
    }
    pub(crate) fn new(connection: Connection, peer: ProvenPeer) -> Self {
        Self { connection, peer }
    }

    /// The identity this connection proved.
    #[must_use]
    pub fn peer(&self) -> ProvenPeer {
        self.peer
    }

    /// End this connection now.
    ///
    /// Not async: iroh's `Connection::close` does not await, and the awaiting
    /// variant is the separate `closed()`. Calls already dispatched on this
    /// connection run to completion; see the module documentation.
    pub fn close(&self) {
        self.connection.close(REFUSED.into(), b"revoked");
    }

    /// Resolve when this connection ends, however it ends.
    // No `#[must_use]`: `Watch` already carries one, and repeating it here is
    // `clippy::double_must_use`.
    pub fn watch(&self) -> Watch {
        Watch::on(&self.connection)
    }
}

/// Who may talk to a served protocol.
///
/// Held as `Arc<dyn Admission>`, so `admits` is sync, non-generic and takes
/// `&self`. Deliberately **unlike** `keylink-persist`'s `Snapshots`, which is
/// RPITIT because its one operation does I/O at one monomorphized call site.
/// `admits` runs once per *connection* and does no I/O; a backend that needs
/// I/O keeps its own snapshot and feeds it in. A generic parameter would not
/// reach [`Server`](crate::Server) — a service erases to a boxed handler — but
/// it WOULD reach the generated `service(handler, allow)`, putting it into
/// every `service!` user's type errors for a decision made once per connection.
pub trait Admission: Send + Sync + 'static {
    /// Decide about a peer iroh proved at the handshake.
    #[must_use = "an admission decision that is computed and dropped is the \
                  failure this type exists to prevent"]
    fn admits(&self, peer: &ProvenPeer) -> Admittance;

    /// Take the connection of a peer this policy just admitted.
    ///
    /// Called once per admitted connection, so a policy that can revoke has
    /// something to close. The default drops it, which is correct for a policy
    /// that never changes its mind: a static allowlist has nothing to revoke.
    fn admitted(&self, peer: &AdmittedPeer, live: LiveConnection) {
        let _ = (peer, live);
    }
}

/// An admission whose answer can change while connections are live.
pub trait Revocable: Admission {
    /// The change this adapter applies before rechecking live connections.
    type Change;
    /// A typed refusal to apply the change. An infallible adapter uses `Infallible`.
    type Error;

    /// Apply the change before closing connections no longer admitted.
    #[must_use = "a revocation that closed nothing is usually a mistake"]
    fn apply(&self, next: Self::Change) -> Result<Vec<EndpointId>, Self::Error>;
}

/// A mutable allowlist snapshot behind a lock.
///
/// There is deliberately no `Default`, for the same reason [`Allowlist`] has
/// none: admitting everyone is a decision you have to type out, so it can never
/// be what you get by forgetting.
#[derive(Debug)]
pub struct DynamicAdmission {
    allow: Mutex<Allowlist>,
    /// Every connection is tracked separately, including simultaneous sessions
    /// from one identity and connections on different service ALPNs.
    live: crate::live::Live,
}

impl DynamicAdmission {
    /// Start from a policy, which you have to name.
    #[must_use]
    pub fn new(initial: Allowlist) -> Self {
        Self {
            allow: Mutex::new(initial),
            live: crate::live::Live::default(),
        }
    }

    /// How many admitted connections this policy is holding.
    ///
    /// Advisory, like every observation of a live system: a peer can leave
    /// between this answer and the next call.
    #[must_use]
    pub fn live(&self) -> usize {
        self.live.len()
    }

    /// Poison recovery rather than propagation: one bad request costs one
    /// request. Nothing here can panic while the lock is held.
    fn policy(&self) -> std::sync::MutexGuard<'_, Allowlist> {
        self.allow.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

impl Admission for DynamicAdmission {
    fn admits(&self, peer: &ProvenPeer) -> Admittance {
        if self.policy().admit(*peer).is_some() {
            Admittance::Admitted
        } else {
            Admittance::Refused
        }
    }

    fn admitted(&self, _peer: &AdmittedPeer, live: LiveConnection) {
        let registration = self.live.insert(live.clone());

        // Re-read the policy AFTER the insert, and the order is the whole fix.
        //
        // `admits` released the policy lock before this ran, so an `apply` can
        // land entirely inside that gap: it swaps the policy, takes its
        // snapshot of `live`, and finds nothing — and then this connection is
        // inserted, admitted under a policy that no longer exists, never closed
        // and never reported in the returned list. Silent, and worst for the
        // peer most likely to be reconnecting in a loop.
        //
        // Re-reading here cannot miss, because the two orders are exhaustive: a
        // `apply` that takes its snapshot AFTER the insert sees this entry
        // and closes it, and one that takes it BEFORE is visible to this read.
        if self.policy().admit(live.peer()).is_none() {
            live.close();
            self.live.remove(registration);
        }
    }
}

impl Revocable for DynamicAdmission {
    type Change = Allowlist;
    type Error = std::convert::Infallible;

    fn apply(&self, next: Self::Change) -> Result<Vec<EndpointId>, Self::Error> {
        // The swap happens FIRST, and under the lock, so a peer that reconnects
        // during the sweep below meets the new policy rather than the old one.
        // Closing first would leave exactly that window open.
        *self.policy() = next;

        let policy = self.policy().clone();
        let mut closed = BTreeSet::new();
        for (registration, live) in self.live.snapshot() {
            let id = live.peer().id();
            if policy.admit(live.peer()).is_none() {
                live.close();
                self.live.remove(registration);
                closed.insert(id);
            }
        }
        Ok(closed.into_iter().collect())
    }
}

/// The canonical [`Admission`]: the value every other policy is compared to.
///
/// Named `admits` rather than `admit` because [`Allowlist`] already has an
/// inherent `admit` that returns the *proof* — and those are two different
/// questions with two different answers, so they get two different names rather
/// than one name resolved by whichever happens to be inherent.
impl Admission for Allowlist {
    fn admits(&self, peer: &ProvenPeer) -> Admittance {
        if self.admit(*peer).is_some() {
            Admittance::Admitted
        } else {
            Admittance::Refused
        }
    }
}

/// What the accept path does about one answer.
///
/// Kept here rather than inline in `rpc.rs` so the close codes and the reasons
/// live beside the enum they are reporting, and so a fourth arm is a compile
/// error in one place.
pub(crate) fn settle(
    admission: &dyn Admission,
    required: &[Arc<dyn Admission>],
    connection: &Connection,
) -> Option<(AdmittedPeer, LiveConnection)> {
    let proven = ProvenPeer::on(connection);
    for policy in required {
        require_admitted(policy.admits(&proven), connection)?;
    }
    require_admitted(admission.admits(&proven), connection)?;

    // keylink mints the proof, here, one line after every policy said yes.
    // `AdmittedPeer`'s only *public* constructor is still
    // `Allowlist::admit`, which is what keeps a foreign policy from
    // manufacturing one.
    let admitted = AdmittedPeer::mint(proven);
    let live = LiveConnection::new(connection.clone(), proven);
    for policy in required {
        policy.admitted(&admitted, live.clone());
    }
    admission.admitted(&admitted, live.clone());
    Some((admitted, live))
}

fn require_admitted(admittance: Admittance, connection: &Connection) -> Option<()> {
    match admittance {
        Admittance::Admitted => Some(()),
        Admittance::Refused => {
            connection.close(REFUSED.into(), b"unauthorized");
            None
        }
        // A different code and a different reason, because an operator has to
        // be able to tell a peer that is not on the list from a list that could
        // not be read. Same outcome, and never the same diagnosis.
        Admittance::Unavailable(why) => {
            let _ = why;
            connection.close(UNAVAILABLE.into(), b"admission unavailable");
            None
        }
    }
}
