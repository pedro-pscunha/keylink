//! Who may talk to a served protocol, as a port with one adapter.
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
//!        │ DynamicAdmission │  │ your policy      │
//!        │  (built)         │  │ system, not built│
//!        └──────────────────┘  └──────────────────┘
//! ```
//!
//! `Allowlist` is the **value** [`DynamicAdmission`] holds, not a second
//! adapter: a static allowlist is a `DynamicAdmission` nobody replaces. The
//! reason to write a second adapter is an external policy system, and that is
//! the only reason.
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
//! - **Nothing about a peer already inside a stream.** Revocation closes the
//!   connection; a call already dispatched runs to completion.
//! - **Nothing across nodes.** [`DynamicAdmission`] is one process's view.
//! - **No history.** There is no audit trail of past decisions; that is a
//!   different port with different operations.
//! - **One connection per peer.** The registry is keyed by [`EndpointId`], so a
//!   peer holding several has its most recent remembered and closed. Two live
//!   connections from one identity are not two peers.

use std::sync::{Mutex, PoisonError};

use iroh::endpoint::Connection;
use iroh::EndpointId;

use crate::{AdmittedPeer, Allowlist, ProvenPeer, Subscribers, Watch};

/// The close code for a peer a policy refused.
const REFUSED: u32 = 1;

/// The close code for a peer whose policy could not be reached.
///
/// Distinct from [`REFUSED`] on purpose: a peer that is told "you are not on the
/// list" retries never, and one that is told "ask again" retries later, and
/// giving them the same code makes an outage look like a revocation to every
/// peer at once.
const UNAVAILABLE: u32 = 2;

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
pub struct Revocation {
    connection: Connection,
    peer: ProvenPeer,
}

impl Revocation {
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
    fn admitted(&self, peer: &AdmittedPeer, live: Revocation) {
        let _ = (peer, live);
    }
}

/// An admission whose answer can change while connections are live.
pub trait Revocable: Admission {
    /// Swap the policy, **then** close every live connection the new one no
    /// longer admits, and report the peers closed.
    ///
    /// The order is the correctness property, not an implementation detail:
    /// closing first leaves a window in which a revoked peer reconnects and is
    /// admitted by the policy that is on its way out.
    #[must_use = "a revocation that closed nothing is usually a mistake"]
    fn replace(&self, next: Allowlist) -> Vec<EndpointId>;
}

/// The one adapter keylink ships: a snapshot behind a lock.
///
/// There is deliberately no `Default`, for the same reason [`Allowlist`] has
/// none: admitting everyone is a decision you have to type out, so it can never
/// be what you get by forgetting.
#[derive(Debug)]
pub struct DynamicAdmission {
    allow: Mutex<Allowlist>,
    /// Wave C's registry, instantiated at the one other thing worth keying by
    /// peer. One type, two uses: fan out to the peers that dialled in, and
    /// close the ones a new policy no longer admits.
    live: Subscribers<Revocation>,
}

impl DynamicAdmission {
    /// Start from a policy, which you have to name.
    #[must_use]
    pub fn new(initial: Allowlist) -> Self {
        Self {
            allow: Mutex::new(initial),
            live: Subscribers::new(),
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

    fn admitted(&self, peer: &AdmittedPeer, live: Revocation) {
        let watch = live.watch();
        self.live.insert(peer.id(), live, watch);
    }
}

impl Revocable for DynamicAdmission {
    fn replace(&self, next: Allowlist) -> Vec<EndpointId> {
        // The swap happens FIRST, and under the lock, so a peer that reconnects
        // during the sweep below meets the new policy rather than the old one.
        // Closing first would leave exactly that window open.
        *self.policy() = next;

        let policy = self.policy().clone();
        let mut closed = Vec::new();
        for (id, live) in self.live.snapshot() {
            if policy.admit(live.peer()).is_none() {
                live.close();
                self.live.remove(id);
                closed.push(id);
            }
        }
        closed
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
    connection: &Connection,
) -> Option<(AdmittedPeer, Revocation)> {
    let proven = ProvenPeer::on(connection);
    match admission.admits(&proven) {
        Admittance::Admitted => {
            // keylink mints the proof, here, one line after being told yes.
            // `AdmittedPeer`'s only *public* constructor is still
            // `Allowlist::admit`, which is what keeps a foreign policy from
            // manufacturing one.
            let admitted = AdmittedPeer::mint(proven);
            let live = Revocation::new(connection.clone(), proven);
            admission.admitted(&admitted, live.clone());
            Some((admitted, live))
        }
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
