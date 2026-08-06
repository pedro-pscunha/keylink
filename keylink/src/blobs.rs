//! Content-addressed transfer, with an admission ledger keylink owns.
//!
//! keylink RPC carries a content address. [iroh-blobs] moves the bytes. The
//! caller gains verified resume, deduplication and content addressing, and
//! names no transport concept to get them.
//!
//! # Why keylink owns the mount
//!
//! `BlobsProtocol::new(&store, None)` — the mount six of iroh-blobs' eight call
//! sites teach, its own module documentation included — serves every hash to
//! every peer **and accepts writes**. `EventMask::DEFAULT` sets
//! `get: RequestMode::None`, and `EventSender::request` reads `mask.get` for
//! every request type, so the whole admission callback is switched off.
//! Mounting that beside an allowlisted keylink service puts an unauthenticated
//! store on an otherwise closed node.
//!
//! So a user never mounts it. `iroh_blobs::ALPN` is in
//! [`RESERVED_ALPNS`](crate::Service::foreign), keylink builds the mount from
//! the [`Blobs`] its services carry, and the policy is attached where it cannot
//! be left off.
//!
//! # What a grant means
//!
//! Admission is a ledger, not an [`Allowlist`](crate::Allowlist). An allowlist
//! is the wrong input twice over: a store is one flat namespace, so an
//! allowlisted peer could fetch any hash it ever learned; and an allowlist is
//! per service while blobs is per node, so several services give one node
//! several answers to "who may connect".
//!
//! Instead: returning a [`BlobId`] to a peer records a grant for that exact
//! `(peer, hash)` pair, and a blobs request is admitted only against a grant.
//! Every entry was therefore created by an RPC that already passed that
//! service's allowlist and its layers, so the ledger composes across services
//! whose policies disagree.
//!
//! # One node has one content store
//!
//! Two tenants on one node share a **deduplication domain**: identical content
//! is stored once, and a grant from either tenant authorizes those bytes.
//! Distinct content stays isolated — the ledger keys on the hash, and a hash
//! the node holds but never granted is refused exactly like a hash it does not
//! have. If two tenants must not share a store, give them separate nodes.
//!
//! That is a statement about deduplication, not about confidentiality. The only
//! content a grant reaches across a scope boundary is byte-identical content,
//! and content addressing makes that one object rather than two secrets.
//!
//! [iroh-blobs]: https://docs.rs/iroh-blobs

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use bao_tree::io::BaoContentItem;
use bytes::Bytes;
use iroh::endpoint::{ConnectError, Connection, ConnectionError};
use iroh::protocol::{AcceptError, ProtocolHandler};
use iroh::{Endpoint, EndpointAddr};
use iroh_blobs::api::Store;
use iroh_blobs::get::request::{get_blob, GetBlobItem};
use iroh_blobs::get::GetError;
use iroh_blobs::protocol::{GetRequest, ERR_PERMISSION};
use iroh_blobs::provider::events::{
    AbortReason, ClientConnected, ConnectMode, EventMask, EventSender, ObserveMode,
    ProviderMessage, RequestMode, RequestReceived, ThrottleMode,
};
use iroh_blobs::store::mem::MemStore;
use iroh_blobs::{BlobsProtocol, Hash};
use n0_future::StreamExt;
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncWrite, AsyncWriteExt};

use crate::{AdmittedPeer, Error, Node, ProvenPeer, Result};

/// Grants one node holds before it starts evicting the oldest.
///
/// RFC-01 §9 Risk 3: grants are never revoked, so a long-lived server that
/// hands out content to many peers accumulates `(peer, hash)` pairs forever.
/// Judgement, not measurement, exactly like
/// [`MAX_RECORDS`](crate::MAX_RECORDS): large enough that an ordinary catalog
/// fits, small enough that the ledger is a rounding error beside one blob.
///
/// **An evicted grant takes its capability with it.** A peer whose grant fell
/// off the end is refused until the handler hands it the [`BlobId`] again. That
/// is the same trade the idempotency ledger makes, and the same reason: a bound
/// somebody can exhaust is better than a leak nobody can see.
pub const MAX_GRANTS: usize = 4096;

// ────────────────────────────────────────────────────────────── the address

/// A content address: the BLAKE3 hash of a payload.
///
/// keylink owns this type because `iroh_blobs::Hash` does not implement
/// `postcard_schema::Schema`, and the `service!` macro derives `Schema` on
/// every generated wire type. The postcard encoding is the same 32 raw bytes,
/// so the two cross-decode — pinned by
/// `the_blob_id_encodes_identically_to_an_iroh_blobs_hash`.
///
/// # This is a capability, and the type does not enforce that
///
/// **Level 2, honestly labelled.** The field is private and there is no public
/// constructor, so a `BlobId` a caller holds came from content this node
/// actually stored. That is provenance, and it is worth having.
///
/// It is *not* an invariant, because every 32-byte string is a well-formed
/// content address. `derive(Deserialize)` therefore builds one from whatever a
/// peer sent, and no validator could object. So no guard is deleted here and
/// none is claimed: authorization lives in the grant ledger, which asks whether
/// *this peer* was handed *this address*, and a `BlobId` conjured on the wire
/// answers that question with "no".
#[derive(Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, postcard_schema::Schema)]
pub struct BlobId([u8; 32]);

impl BlobId {
    /// The wire bytes, for a log line or a comparison.
    #[must_use]
    pub fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }

    /// The iroh-blobs type this addresses.
    fn hash(self) -> Hash {
        Hash::from(self.0)
    }

    /// Inherent and crate-private, deliberately not a `From` impl.
    ///
    /// A public `From<iroh_blobs::Hash>` would put iroh-blobs in keylink's
    /// public API — every caller who holds a hash could mint an address, and a
    /// minor version of iroh-blobs could move keylink's own surface. Addresses
    /// come out of [`Blobs::add_bytes`], or off the wire from a peer that was
    /// handed one.
    fn from_hash(hash: Hash) -> Self {
        Self(*hash.as_bytes())
    }
}

/// Hex, like every other content address a reader has met. The derived form
/// prints 32 decimal numbers, which is unreadable in a log and impossible to
/// paste into a search.
impl std::fmt::Display for BlobId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for byte in self.0 {
            write!(f, "{byte:02x}")?;
        }
        Ok(())
    }
}

impl std::fmt::Debug for BlobId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "BlobId({self})")
    }
}

// ──────────────────────────────────────── what the ledger is keyed on, and why

// `ProvenPeer` and `AdmittedPeer` live in `authz.rs`, because admission is an
// authorization concept rather than a blobs one. This module is their most
// demanding consumer: the blobs mount is a SECOND accept path onto a node whose
// other protocols are allowlisted, so if it could write an authorization record
// from a bare `connection.remote_id()` it would be a way around every policy on
// the node, with the compiler silent.
//
// The two rungs do different jobs here, and the asymmetry is the design:
//
// - WRITING a grant demands an `AdmittedPeer`. `Blobs::grant` cannot be called
//   for a peer no `Allowlist` said yes to, so a grant is always downstream of a
//   policy decision that already happened on the keylink connection.
// - READING one takes a `ProvenPeer`, because a blobs connection is not
//   allowlisted by anybody. There is one blobs ALPN and several services with
//   several allowlists (RFC-01 §5.1), so there is no allowlist for the mount to
//   consult and picking one would be arbitrary. What authorizes the fetch is
//   the grant itself, and the grant already carries the admission.
//
// So the mount deliberately does NOT mint an `AdmittedPeer`. It proves who is
// calling and asks the ledger whether that peer was handed this address.

// ────────────────────────────────────────────────────────────── the ledger

/// Which peer may fetch which content.
///
/// Written when a handler returns a [`BlobId`], read by blobs admission. Keyed
/// by [`ProvenPeer`] rather than by `EndpointId`, which is what stops an
/// unidentified or merely claimed peer from reaching a decision.
#[derive(Debug)]
struct Grants {
    inner: Mutex<Book>,
    max: usize,
}

/// The grants, plus the order to drop them in.
#[derive(Debug, Default)]
struct Book {
    by_peer: HashMap<ProvenPeer, HashSet<BlobId>>,
    /// Oldest first. Every entry here has a matching entry in `by_peer`, which
    /// is what makes the eviction bookkeeping a single pass.
    order: VecDeque<(ProvenPeer, BlobId)>,
}

impl Grants {
    fn new() -> Arc<Self> {
        Self::with_limit(MAX_GRANTS)
    }

    fn with_limit(max: usize) -> Arc<Self> {
        Arc::new(Self {
            inner: Mutex::new(Book::default()),
            max,
        })
    }

    /// Poison recovery rather than propagation, the same call
    /// [`Ledger`](crate::MAX_RECORDS) makes: a handler that panicked must not
    /// wedge admission for every later peer. Nothing here can panic while the
    /// lock is held, so this arm is unreachable in practice and must still be
    /// typed — `.unwrap()` and `.expect()` are denied by the workspace lints.
    fn lock(&self) -> MutexGuard<'_, Book> {
        self.inner.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Takes an [`AdmittedPeer`], not a [`ProvenPeer`]: a grant is an
    /// authorization record, so writing one must be downstream of an
    /// allowlist saying yes. The map keys on the identity underneath,
    /// because that is all a later blobs connection can prove.
    fn grant(&self, peer: AdmittedPeer, id: BlobId) {
        let peer = peer.proven();
        let mut book = self.lock();
        // A repeat grant must not take a second slot in the eviction queue, or
        // a handler that answers one peer in a loop evicts every other peer.
        if !book.by_peer.entry(peer).or_default().insert(id) {
            return;
        }
        book.order.push_back((peer, id));
        while book.order.len() > self.max {
            let Some((stale_peer, stale_id)) = book.order.pop_front() else {
                break;
            };
            let Some(held) = book.by_peer.get_mut(&stale_peer) else {
                continue;
            };
            held.remove(&stale_id);
            if held.is_empty() {
                // Not tidiness: `knows` reads this map, so a peer left behind
                // with an empty set would keep passing connection admission
                // while holding no grant at all.
                book.by_peer.remove(&stale_peer);
            }
        }
    }

    fn permits(&self, peer: &ProvenPeer, id: &BlobId) -> bool {
        self.lock()
            .by_peer
            .get(peer)
            .is_some_and(|held| held.contains(id))
    }

    fn knows(&self, peer: &ProvenPeer) -> bool {
        self.lock().by_peer.contains_key(peer)
    }
}

/// Which live blobs connection belongs to which proven peer.
///
/// iroh-blobs' per-request events carry `connection_id` and nothing else
/// (`provider/events.rs:602`), so the identity has to be remembered from the
/// accept. Populated **only** by [`BlobsAdmission::accept`], which is the second
/// guard: a request arriving on a connection keylink never admitted finds
/// nothing here and is refused.
#[derive(Debug, Default)]
struct Connections(Mutex<HashMap<u64, ProvenPeer>>);

impl Connections {
    fn lock(&self) -> MutexGuard<'_, HashMap<u64, ProvenPeer>> {
        self.0.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn admit(&self, connection_id: u64, peer: ProvenPeer) {
        self.lock().insert(connection_id, peer);
    }

    /// Drop an entry only while it still names the peer that took it.
    ///
    /// `stable_id` is a slab index, and quinn reuses one once its connection is
    /// gone. So a late `forget` from a finished connection can arrive after a
    /// NEW connection has taken the same id, and an unconditional `remove`
    /// would delete a live entry and refuse a peer that did nothing wrong. The
    /// comparison costs one lookup and removes that window.
    fn forget(&self, connection_id: u64, peer: ProvenPeer) {
        let mut live = self.lock();
        if live.get(&connection_id) == Some(&peer) {
            live.remove(&connection_id);
        }
    }

    fn peer(&self, connection_id: u64) -> Option<ProvenPeer> {
        self.lock().get(&connection_id).copied()
    }
}

/// Forgets an admitted connection however the accept ends.
///
/// A plain call after the `await` is not enough: the router drops handler
/// futures on shutdown, and a dropped future never reaches its next line. Each
/// cancelled accept would leave one entry behind forever, which is the leak
/// [`MAX_GRANTS`] exists to prevent one map over.
struct Admitted<'a> {
    live: &'a Connections,
    connection_id: u64,
    peer: ProvenPeer,
}

impl Drop for Admitted<'_> {
    fn drop(&mut self) {
        self.live.forget(self.connection_id, self.peer);
    }
}

// ──────────────────────────────────────────────────────────────── the store

/// How long added content stays fetchable.
///
/// There is deliberately no `Default`, for the same reason
/// [`Eviction`](crate::Eviction) and [`Allowlist`](crate::Allowlist) have none:
/// a store that grows until the disk fills is a decision you type out at the
/// call site, not one you inherit.
///
/// # Only one variant, on purpose
///
/// RFC-01 §6.2 records the cost of the second one. iroh-blobs has no tag expiry
/// API at all: `.await`ing an add calls `with_tag()`, which creates a permanent
/// tag, and `Options.gc` defaults to `None`. Expiry means encoding a deadline
/// in the tag name, sweeping on a timer, and turning garbage collection on —
/// a subsystem, not a parameter. The RFC's own guidance is to ship `Forever`
/// first and keep the enum, so that adding `Expire` later is not a breaking
/// change. That is what this is. A variant that existed and quietly behaved
/// like `Forever` would be worse than one that does not exist.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Retention {
    /// Content lives until the process ends, or until the disk is cleared by
    /// hand. The store grows without bound.
    Forever,
}

/// What keeps the store alive.
///
/// **Level 1.** `MemStore` and `FsStore` are both thin handles over an actor,
/// and `FsStore` owns the sender that actor reads: dropping it shuts the store
/// down. Holding only the derived `Store` handle would compile, serve for a
/// while, and then stop. Each backing therefore lives in the variant that owns
/// it, and there is no `Option<FsStore>` beside a `Store` to get out of step.
#[derive(Debug)]
enum Backing {
    Memory(MemStore),
    #[cfg(feature = "blobs-fs")]
    Disk(iroh_blobs::store::fs::FsStore),
}

impl Backing {
    fn store(&self) -> &Store {
        match self {
            Self::Memory(store) => store,
            #[cfg(feature = "blobs-fs")]
            Self::Disk(store) => store,
        }
    }
}

#[derive(Debug)]
struct Inner {
    backing: Backing,
    grants: Arc<Grants>,
    live: Arc<Connections>,
    /// The policy handle. Dropping it ends the drain task, so the task's
    /// lifetime is this store's lifetime with no shutdown call to forget.
    events: EventSender,
}

/// One node's content store, and the grant ledger that guards it.
///
/// Cheap to clone: every clone shares one store and one ledger, which is the
/// point.
///
/// # One node has one content store
///
/// Read this while choosing a store, not afterwards.
///
/// Two tenants on one node share a **deduplication domain**: identical content
/// is stored once, and a grant from either tenant authorizes those bytes. If
/// two tenants must not share a store, give them separate nodes.
///
/// Distinct content stays isolated. The ledger keys on the hash, so a grant
/// issued by one tenant does not reach another tenant's content, and a hash
/// this node holds but never granted is refused exactly like a hash it does not
/// have — there is no existence oracle to probe.
///
/// So the caveat is about deduplication rather than confidentiality. The only
/// content a grant reaches across a tenant boundary is byte-identical content,
/// and content addressing makes that one object rather than two secrets.
#[derive(Debug, Clone)]
pub struct Blobs {
    inner: Arc<Inner>,
}

impl Blobs {
    fn with_backing(backing: Backing) -> Self {
        let grants = Grants::new();
        let live = Arc::new(Connections::default());
        let events = policy(Arc::clone(&grants), Arc::clone(&live));
        Self {
            inner: Arc::new(Inner {
                backing,
                grants,
                live,
                events,
            }),
        }
    }

    /// Content lives in memory and is gone when the process exits.
    ///
    /// Bounded by RAM, which is a limit an operator already understands. Needs
    /// no feature and no path.
    #[must_use]
    pub fn memory() -> Self {
        Self::with_backing(Backing::Memory(MemStore::new()))
    }

    /// Content lives on disk under `root`.
    ///
    /// # Errors
    ///
    /// [`Error::Io`] when the store cannot be opened.
    #[cfg(feature = "blobs-fs")]
    pub async fn persistent(root: impl AsRef<std::path::Path>, keep: Retention) -> Result<Self> {
        // Exhaustive with no catch-all, so that adding `Retention::Expire`
        // is a compile error here rather than a silently ignored argument.
        match keep {
            // Nothing to configure. iroh-blobs leaves `Options.gc` at `None`,
            // and every `.await`ed add carries a permanent tag.
            Retention::Forever => {}
        }
        let store = iroh_blobs::store::fs::FsStore::load(root)
            .await
            .map_err(|error| Error::Io(std::io::Error::other(error.to_string())))?;
        Ok(Self::with_backing(Backing::Disk(store)))
    }

    /// Store `data` and return its content address.
    ///
    /// Adding content grants nothing. A peer reaches it only once a handler has
    /// handed that peer this [`BlobId`].
    ///
    /// # Errors
    ///
    /// [`Error::Io`] when the store refuses the write.
    pub async fn add_bytes(&self, data: impl Into<Bytes>) -> Result<BlobId> {
        let tag = self
            .store()
            .add_bytes(data)
            .await
            .map_err(|error| Error::Io(std::io::Error::other(error.to_string())))?;
        Ok(BlobId::from_hash(tag.hash))
    }

    /// Record that an admitted peer may fetch `id`.
    ///
    /// Takes an [`AdmittedPeer`] rather than a connection or an id. A
    /// connection would prove only identity, and an id proves nothing at all —
    /// either would let this second accept path write an authorization record
    /// for a peer no [`Allowlist`](crate::Allowlist) admitted. The only source
    /// of an `AdmittedPeer` is `Allowlist::admit`, so the policy has already
    /// run by the time this can be called.
    ///
    /// Generated `blob` dispatch calls it, so a handler that returns a
    /// [`BlobId`] has already granted it and there is no call to forget.
    #[doc(hidden)]
    pub fn grant(&self, peer: &AdmittedPeer, id: BlobId) {
        self.inner.grants.grant(*peer, id);
    }

    /// The mount, with the ledger attached.
    ///
    /// keylink's own wiring. `iroh_blobs::ALPN` is reserved, so this is the
    /// only mount that can reach a running node, and it cannot be built without
    /// the policy.
    #[doc(hidden)]
    #[must_use]
    pub fn protocol(&self) -> BlobsAdmission {
        BlobsAdmission {
            blobs: BlobsProtocol::new(self.store(), Some(self.inner.events.clone())),
            grants: Arc::clone(&self.inner.grants),
            live: Arc::clone(&self.inner.live),
        }
    }

    fn store(&self) -> &Store {
        self.inner.backing.store()
    }
}

// ─────────────────────────────────────────────────────────────── the mount

/// keylink's blobs mount: admission first, then iroh-blobs.
///
/// Public only because generated wiring names it. Nothing here is part of the
/// stable surface.
#[doc(hidden)]
#[derive(Debug, Clone)]
pub struct BlobsAdmission {
    blobs: BlobsProtocol,
    grants: Arc<Grants>,
    live: Arc<Connections>,
}

impl ProtocolHandler for BlobsAdmission {
    async fn accept(&self, connection: Connection) -> std::result::Result<(), AcceptError> {
        // The handshake proved this key, and this is the only place in the
        // blobs path where the connection itself is in hand. Everything
        // downstream sees a `connection_id` and has to look the peer up.
        let peer = ProvenPeer::on(&connection);
        if !self.grants.knows(&peer) {
            // The same code and reason iroh-blobs uses for a refusal, so a
            // denied peer reads `Error::BlobRefused` whether it was turned away
            // at the connection or at the request.
            connection.close(ERR_PERMISSION, b"permission");
            return Ok(());
        }

        // COUPLING, and it is load bearing. `handle_connection` derives its
        // `connection_id` as `connection.stable_id() as u64`
        // (`provider.rs:291`) — an internal detail of a crate whose API moves
        // between minor versions. If that derivation ever changes, every
        // request looks like it arrived on a connection keylink never admitted
        // and is refused, which fails closed but fails silently.
        //
        // `a_peer_with_a_grant_can_fetch_that_content` is the guard: it asserts
        // a granted fetch SUCCEEDS, so a broken derivation turns a test red
        // instead of turning the feature off.
        let connection_id = connection.stable_id() as u64;
        self.live.admit(connection_id, peer);
        let _admitted = Admitted {
            live: &self.live,
            connection_id,
            peer,
        };
        self.blobs.accept(connection).await
    }

    async fn shutdown(&self) {
        // Flushes a persistent store to disk. `Server::shutdown` calls
        // `Router::shutdown`, which calls this on every handler.
        self.blobs.shutdown().await;
    }
}

// ─────────────────────────────────────────────────────────────── the policy

/// Turn the grant ledger into a blobs admission policy.
///
/// Every mask field is named. `EventMask::DEFAULT` disables interception
/// entirely, and `EventMask::ALL_READONLY` is the wrong shape: it sets
/// `get: RequestMode::InterceptLog`, which streams a per-request update channel
/// the handler must drain or the transfer aborts with `ERR_INTERNAL`, and
/// `throttle: ThrottleMode::Intercept`, which costs an event round trip per
/// 16 KiB — 4096 of them on a 64 MiB blob, for a result keylink discards.
///
/// Naming all six also pins the dead-field bug. In 0.103.0 `EventSender::request`
/// reads `mask.get` for get, get-many, push and observe alike, so `mask.push`,
/// `mask.get_many` and `mask.observe` are never read anywhere in the crate.
/// Setting them anyway means an upstream fix changes nothing here.
fn policy(grants: Arc<Grants>, live: Arc<Connections>) -> EventSender {
    let mask = EventMask {
        connected: ConnectMode::Intercept,
        get: RequestMode::Intercept,
        get_many: RequestMode::Intercept,
        push: RequestMode::Disabled,
        observe: ObserveMode::Intercept,
        throttle: ThrottleMode::None,
    };
    let (tx, mut rx) = EventSender::channel(32, mask);

    n0_future::task::spawn(async move {
        while let Some(message) = rx.recv().await {
            // Every verdict is computed in a synchronous helper, so no lock is
            // ever held across the `send`. `clippy::await_holding_lock` is
            // denied, and a policy task that deadlocked would refuse the whole
            // node.
            match message {
                ProviderMessage::ClientConnected(msg) => {
                    let verdict = admits_connection(&live, &msg.inner);
                    msg.tx.send(verdict).await.ok();
                }
                ProviderMessage::GetRequestReceived(msg) => {
                    let verdict = admits_get(&grants, &live, &msg.inner);
                    msg.tx.send(verdict).await.ok();
                }
                // keylink hands out one raw blob per grant. Nothing else is
                // reachable through a `blob` method, so nothing else is served.
                ProviderMessage::GetManyRequestReceived(msg) => {
                    msg.tx.send(Err(AbortReason::Permission)).await.ok();
                }
                ProviderMessage::PushRequestReceived(msg) => {
                    msg.tx.send(Err(AbortReason::Permission)).await.ok();
                }
                ProviderMessage::ObserveRequestReceived(msg) => {
                    msg.tx.send(Err(AbortReason::Permission)).await.ok();
                }
                // Not an authorization decision: `ThrottleMode::None` means we
                // never asked to be consulted about pacing. Answering yes is
                // what "we do not throttle" means; dropping the sender would
                // abort a transfer we already authorized.
                ProviderMessage::Throttle(msg) => {
                    msg.tx.send(Ok(())).await.ok();
                }
                // `ConnectionClosed` needs no answer — `BlobsAdmission::accept`
                // is what forgets the connection, because it is the only place
                // that knows the accept has finished.
                ProviderMessage::ConnectionClosed(_) => {}
                // The `Notify` variants carry no reply channel and are
                // unreachable under this mask, which selects `Intercept`
                // everywhere it selects anything. Named rather than swept into
                // a catch-all so that a new `ProviderMessage` variant upstream
                // is a compile error here — this is an authorization surface on
                // a crate that moves between minor versions, and a `_ => {}`
                // would admit the new case by silently ignoring it.
                ProviderMessage::ClientConnectedNotify(_)
                | ProviderMessage::GetRequestReceivedNotify(_)
                | ProviderMessage::GetManyRequestReceivedNotify(_)
                | ProviderMessage::PushRequestReceivedNotify(_)
                | ProviderMessage::ObserveRequestReceivedNotify(_) => {}
            }
        }
    });

    tx
}

/// Admit a connection keylink itself admitted, and nothing else.
///
/// `msg.endpoint_id` is deliberately not read. See [`ProvenPeer`].
fn admits_connection(
    live: &Connections,
    msg: &ClientConnected,
) -> std::result::Result<(), AbortReason> {
    if live.peer(msg.connection_id).is_some() {
        Ok(())
    } else {
        Err(AbortReason::Permission)
    }
}

/// Admit one get request.
///
/// **Two conditions, and the second is not in RFC-01.** The RFC's policy reads
/// `msg.request.hash` and ignores `msg.request.ranges`. `handle_get_impl`
/// (`provider.rs:417`) serves the granted hash at offset 0 and, for every
/// offset above that, reads the blob as a `HashSeq` and serves the **child** it
/// names. `HashSeq::new` (`hashseq.rs:56`) accepts any byte string whose length
/// divides by 32.
///
/// So under a hash-only check, a grant on any 32-aligned blob is a capability
/// for every blob its bytes happen to address. The trigger is **alignment, not
/// intent**: a 64-byte key file qualifies, and so does anything else whose
/// length is a multiple of 32. Requiring `ranges.is_blob()` — a single range
/// set, starting at offset 0 — refuses the child request outright.
///
/// This stays a runtime check on purpose. It is peer-supplied data arriving at
/// the edge, which is the one place a guard belongs (peak-rust rule 2): no type
/// keylink owns is on this path, because the request was decoded by iroh-blobs.
fn admits_get(
    grants: &Grants,
    live: &Connections,
    msg: &RequestReceived<GetRequest>,
) -> std::result::Result<(), AbortReason> {
    if !msg.request.ranges.is_blob() {
        return Err(AbortReason::Permission);
    }
    let Some(peer) = live.peer(msg.connection_id) else {
        return Err(AbortReason::Permission);
    };
    if grants.permits(&peer, &BlobId::from_hash(msg.request.hash)) {
        Ok(())
    } else {
        Err(AbortReason::Permission)
    }
}

// ─────────────────────────────────────────────────────────────── the client

/// A content address plus the node that holds it.
///
/// Opens the blobs connection internally, so a call site names no ALPN and no
/// second dial.
#[derive(Debug)]
pub struct BlobHandle {
    endpoint: Endpoint,
    holder: EndpointAddr,
    id: BlobId,
}

impl BlobHandle {
    /// Point a handle at content on `holder`.
    ///
    /// Public only because generated `blob` client code names it. A caller
    /// receives handles; it does not build them.
    #[doc(hidden)]
    #[must_use]
    pub fn new(node: &Node, holder: impl Into<EndpointAddr>, id: BlobId) -> Self {
        Self {
            endpoint: node.endpoint().clone(),
            holder: holder.into(),
            id,
        }
    }

    /// The content address, for logging or for a later fetch.
    #[must_use]
    pub fn id(&self) -> BlobId {
        self.id
    }

    /// Stream the verified bytes into `sink`.
    ///
    /// No local store, so no deduplication and no resume across a restart. Peak
    /// memory is one chunk. This is the direct replacement for
    /// [`BodyReader::copy_to`](crate::BodyReader::copy_to).
    ///
    /// # Errors
    ///
    /// [`Error::BlobRefused`] when the holder denies the fetch,
    /// [`Error::Transport`] when the dial fails, and [`Error::Io`] when the
    /// transfer or the sink does.
    pub async fn copy_to<W: AsyncWrite + Unpin>(self, sink: &mut W) -> Result<u64> {
        let connection = self.dial().await?;
        // `get_blob` takes the connection by value. A `Connection` is a handle,
        // so this clone is what keeps the close code readable after the
        // transfer has failed — see `classify`.
        let watched = connection.clone();
        let mut items = get_blob(connection, self.id.hash());
        let mut written: u64 = 0;

        while let Some(item) = items.next().await {
            match item {
                GetBlobItem::Item(BaoContentItem::Leaf(leaf)) => {
                    // RFC-01 §3.3 flagged leaf ordering as unverified: the
                    // iroh-blobs example writes leaves in arrival order and
                    // expects correct output, which ASSUMES a whole-blob
                    // request delivers them in offset order. A sink is not
                    // seekable, so the assumption cannot be worked around — it
                    // can only be checked. Checked here, once per leaf, so a
                    // change in the bao-tree traversal is a loud failure
                    // instead of a file that is silently scrambled.
                    if leaf.offset != written {
                        return Err(Error::Io(std::io::Error::other(format!(
                            "blob {} arrived out of order: leaf at offset {} after {} bytes",
                            self.id, leaf.offset, written
                        ))));
                    }
                    sink.write_all(&leaf.data).await?;
                    written += leaf.data.len() as u64;
                }
                // A hash-tree node, not payload. Verification is bao-tree's
                // job and it has already happened by the time an item arrives.
                GetBlobItem::Item(BaoContentItem::Parent(_)) => {}
                GetBlobItem::Done(_) => {
                    sink.flush().await?;
                    return Ok(written);
                }
                GetBlobItem::Error(error) => return Err(self.classify(error, &watched)),
            }
        }

        // The stream ended without `Done`. Short content is never mistaken for
        // complete content, the same call `Error::BodyTruncated` makes.
        Err(Error::Io(std::io::Error::other(format!(
            "blob {} ended after {written} bytes without completing",
            self.id
        ))))
    }

    /// Fetch into a local store.
    ///
    /// Deduplication and resume come from the store: an interrupted transfer
    /// continues where it stopped, and content already present costs nothing.
    /// Returns the payload bytes that crossed the wire, which is zero for a
    /// blob the store already held.
    ///
    /// # Errors
    ///
    /// The same set as [`copy_to`](Self::copy_to).
    pub async fn fetch_into(self, store: &Blobs) -> Result<u64> {
        let connection = self.dial().await?;
        let watched = connection.clone();
        let stats = store
            .store()
            .remote()
            .fetch(connection, self.id.hash())
            .await
            .map_err(|error| self.classify(error, &watched))?;
        Ok(stats.payload_bytes_read)
    }

    async fn dial(&self) -> Result<Connection> {
        self.endpoint
            .connect(self.holder.clone(), iroh_blobs::ALPN)
            .await
            .map_err(|error| self.classify_dial(error))
    }

    /// Tell "the holder refused us" apart from "the network went wrong".
    ///
    /// RFC-01 §9 Risk 5: a denied peer read `io: connection lost`, which sends
    /// an operator to inspect a network that is working perfectly.
    ///
    /// **Two places to look, because a refusal arrives two ways.** When one
    /// request is denied, the provider resets that stream and
    /// `GetError::iroh_error_code` reads the code straight out of the
    /// `ReadError::Reset`. When the whole connection is denied — the peer holds
    /// no grant at all — the close arrives as a CONNECTION_CLOSE frame that no
    /// stream error carries, and the `GetError` really does render as
    /// `io: connection lost`. That was measured, not assumed: breaking the
    /// connection admission guard produced exactly that string with
    /// `iroh_error_code() == None`.
    ///
    /// The code is on the wire in both cases. The second one is on the
    /// connection rather than on the error, which is why the connection is
    /// still in hand here.
    fn classify(&self, error: GetError, connection: &Connection) -> Error {
        if error.iroh_error_code() == Some(ERR_PERMISSION) || refused(connection) {
            return Error::BlobRefused { id: self.id };
        }
        Error::Io(std::io::Error::other(error.to_string()))
    }

    /// The same question, asked one layer earlier.
    ///
    /// A connection-level refusal races the dial: keylink closes the connection
    /// immediately after the handshake, so the client usually gets its
    /// `Connection` and meets the refusal on the fetch — but not always. The
    /// close code is on the wire in both cases, and reading it here is what
    /// keeps the answer stable regardless of which side won the race.
    fn classify_dial(&self, error: ConnectError) -> Error {
        if let ConnectError::Connection {
            source: closed @ ConnectionError::ApplicationClosed(_),
            ..
        } = &error
        {
            if is_permission(closed) {
                return Error::BlobRefused { id: self.id };
            }
        }
        Error::Transport(error.into())
    }
}

/// Whether a connection was closed with the refusal code.
///
/// `None` means it is still open, which is not a refusal.
fn refused(connection: &Connection) -> bool {
    connection
        .close_reason()
        .is_some_and(|why| is_permission(&why))
}

/// The one place the close frame is read. `ERR_PERMISSION` is what iroh-blobs
/// answers a denial with, and it is what [`BlobsAdmission`] answers with too,
/// so a peer cannot tell which layer turned it away.
fn is_permission(error: &ConnectionError) -> bool {
    matches!(error, ConnectionError::ApplicationClosed(frame) if frame.error_code == ERR_PERMISSION)
}

#[cfg(test)]
mod tests {
    #![allow(clippy::expect_used, clippy::unwrap_used)]

    use iroh_blobs::protocol::{ChunkRanges, ChunkRangesSeq};

    use super::*;

    /// `ProvenPeer` is `authz`'s, so its field is not reachable here. Going
    /// through `Allowlist::anyone().admit(..)` is the only route, which is the
    /// ladder working: even a test cannot conjure an admitted peer.
    fn admitted(seed: u8) -> AdmittedPeer {
        crate::Allowlist::anyone()
            .admit(ProvenPeer::for_tests(seed))
            .expect("anyone admits everyone")
    }

    fn peer(seed: u8) -> ProvenPeer {
        admitted(seed).proven()
    }

    fn blob(seed: u8) -> BlobId {
        BlobId([seed; 32])
    }

    #[test]
    fn a_grant_admits_exactly_the_content_it_names() {
        let grants = Grants::new();
        grants.grant(admitted(1), blob(7));

        assert!(grants.knows(&peer(1)));
        assert!(grants.permits(&peer(1), &blob(7)));
        assert!(!grants.permits(&peer(1), &blob(8)));
        assert!(!grants.permits(&peer(2), &blob(7)));
        assert!(!grants.knows(&peer(2)));
    }

    #[test]
    fn the_ledger_evicts_the_oldest_grant_rather_than_growing() {
        let grants = Grants::with_limit(2);
        grants.grant(admitted(1), blob(1));
        grants.grant(admitted(1), blob(2));
        grants.grant(admitted(1), blob(3));

        assert!(!grants.permits(&peer(1), &blob(1)), "oldest is evicted");
        assert!(grants.permits(&peer(1), &blob(2)));
        assert!(grants.permits(&peer(1), &blob(3)));
        assert_eq!(grants.lock().order.len(), 2);
    }

    #[test]
    fn a_repeated_grant_does_not_consume_a_second_slot() {
        let grants = Grants::with_limit(2);
        grants.grant(admitted(1), blob(1));
        grants.grant(admitted(1), blob(1));
        grants.grant(admitted(1), blob(2));

        // Without the duplicate check, three pushes against a cap of two would
        // have evicted blob(1) — the grant the peer still holds.
        assert!(grants.permits(&peer(1), &blob(1)));
        assert!(grants.permits(&peer(1), &blob(2)));
    }

    #[test]
    fn a_peer_whose_last_grant_is_evicted_is_no_longer_known() {
        let grants = Grants::with_limit(1);
        grants.grant(admitted(1), blob(1));
        grants.grant(admitted(2), blob(2));

        // `knows` gates connection admission. A peer left behind with an empty
        // set would keep connecting while holding nothing.
        assert!(!grants.knows(&peer(1)));
        assert!(grants.knows(&peer(2)));
    }

    #[test]
    fn a_connection_is_forgotten_when_it_ends() {
        let live = Connections::default();
        live.admit(4, peer(1));
        assert_eq!(live.peer(4), Some(peer(1)));

        live.forget(4, peer(1));
        assert_eq!(live.peer(4), None);

        // The id is a slab index and quinn reuses one. A late forget from the
        // connection that USED to hold it must not evict the one that holds it
        // now.
        live.admit(4, peer(2));
        live.forget(4, peer(1));
        assert_eq!(live.peer(4), Some(peer(2)), "a stale forget is ignored");
    }

    /// The router drops handler futures on shutdown, so the line after the
    /// `await` in `BlobsAdmission::accept` is not guaranteed to run. Every
    /// cancelled accept would otherwise leave one entry behind for good.
    #[test]
    fn an_admitted_connection_is_forgotten_however_its_accept_ends() {
        let live = Connections::default();
        live.admit(7, peer(1));
        {
            let _admitted = Admitted {
                live: &live,
                connection_id: 7,
                peer: peer(1),
            };
            assert_eq!(live.peer(7), Some(peer(1)), "control: it is admitted");
        }
        assert_eq!(live.peer(7), None, "and forgotten when the guard drops");
    }

    // ───────────────────────────────────── the policy, on synthetic events
    //
    // `BlobsAdmission::accept` refuses an ungranted peer before iroh-blobs sees
    // the connection, so these two arms cannot be reached from a live peer.
    // That makes them defence in depth, and defence in depth nobody has watched
    // fail is decoration. The events are built by hand instead, which is also
    // the only way to feed the policy an input `BlobsProtocol` never produces.

    /// The event carries `endpoint_id: Option<EndpointId>`, and RFC-01 §5.5
    /// decided admission on it. keylink does not read the field at all, so the
    /// answer must be identical whatever it holds — including the `None` that
    /// no iroh-blobs transport produces today and that would, under the RFC's
    /// policy, be one mistyped match arm away from admitting a stranger.
    #[test]
    fn connection_admission_ignores_the_reported_endpoint_id() {
        let live = Connections::default();
        live.admit(1, peer(1));

        let admitted = ClientConnected {
            connection_id: 1,
            endpoint_id: None,
        };
        let unknown = ClientConnected {
            connection_id: 2,
            endpoint_id: Some(peer(9).id()),
        };

        assert!(admits_connection(&live, &admitted).is_ok());
        assert!(
            admits_connection(&live, &unknown).is_err(),
            "a connection keylink never admitted is refused, whatever identity it claims"
        );
    }

    fn get(connection_id: u64, id: BlobId, ranges: ChunkRangesSeq) -> RequestReceived<GetRequest> {
        RequestReceived {
            connection_id,
            request_id: 0,
            request: GetRequest::new(id.hash(), ranges),
        }
    }

    fn whole_blob() -> ChunkRangesSeq {
        ChunkRangesSeq::from_ranges([ChunkRanges::all()])
    }

    #[test]
    fn a_request_on_a_connection_that_was_never_admitted_is_refused() {
        let grants = Grants::new();
        let live = Connections::default();
        grants.grant(admitted(1), blob(1));
        // Deliberately NOT admitted: `live` is empty. The grant exists, so the
        // refusal can only come from the connection lookup.
        assert!(admits_get(&grants, &live, &get(1, blob(1), whole_blob())).is_err());

        live.admit(1, peer(1));
        assert!(
            admits_get(&grants, &live, &get(1, blob(1), whole_blob())).is_ok(),
            "control: the same request is admitted once the connection is known"
        );
    }

    /// The hash-sequence hole, at the unit the fix lives in.
    /// `a_granted_peer_cannot_fetch_a_child_of_a_granted_hash_seq` proves it end
    /// to end; this pins which condition does the work.
    #[test]
    fn a_request_reaching_past_the_granted_blob_is_refused() {
        let grants = Grants::new();
        let live = Connections::default();
        grants.grant(admitted(1), blob(1));
        live.admit(1, peer(1));

        assert!(
            admits_get(&grants, &live, &get(1, blob(1), whole_blob())).is_ok(),
            "control: the granted blob itself is served"
        );
        assert!(
            admits_get(&grants, &live, &get(1, blob(1), ChunkRangesSeq::all())).is_err(),
            "the same grant must not reach the children a hash sequence names"
        );
    }
}
