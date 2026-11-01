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
//! every peer. `EventMask::DEFAULT` sets `get: RequestMode::None`, so the
//! admission callback is switched off for reads. Up to 0.103.0 it also
//! **accepted writes**: `EventSender::request` read `mask.get` for every request
//! type, so the default's `push: Disabled` was never read. 0.103.1 fixed that,
//! and the read half is still the whole problem: mounting the default beside an
//! allowlisted keylink service puts a store anyone can read on an otherwise
//! closed node.
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
//! Instead: returning a [`Grant`] to a peer records a grant for that exact
//! `(peer, root)` pair, and a blobs request is admitted only against a grant.
//! Every entry was therefore created by an RPC that already passed that
//! service's allowlist and its layers, so the ledger composes across services
//! whose policies disagree.
//!
//! # One grant can cover a whole tree
//!
//! A [`Content::Blob`] grant covers one blob and nothing its bytes name. A
//! [`Content::Closure`] grant covers a hash sequence built by
//! [`Blobs::add_seq`], its root and every blob the root lists, however many
//! there are, and it is still **one** entry in the ledger. The peer reaches a
//! member through the root, by position, because iroh-blobs serves the members
//! the holder's own copy of the root lists and a request names offsets, never
//! hashes. A member asked for by its own hash is refused unless it was granted
//! on its own.
//!
//! A `SeqId` has no constructor but [`Blobs::add_seq`] and cannot be deserialized,
//! and a handler returns a [`Grant`] rather than the deserializable [`Content`],
//! so a peer cannot name a closure: the only ones a handler can grant are
//! sequences this node built.
//!
//! A closure is one level deep. A listed sequence that is itself a sequence is
//! served as bytes, and its members are not covered: build one flat sequence
//! that lists every blob of the tree.
//!
//! **A closure grant authorizes every hash its sequence lists.** List only
//! blobs the recipient may read.
//!
//! # A grant lasts until it is revoked, and the ledger has a count limit
//!
//! Nothing expires a grant. [`Blobs::revoke`], [`Blobs::revoke_peer`] and
//! [`Blobs::revoke_everywhere`] withdraw them, and the ledger holds at most
//! [`GRANT_LIMIT`] of them. A grant past the limit is refused, loudly, with
//! [`RemoteError::GrantsExhausted`]; nothing is evicted to make room, because
//! an evicted capability is one the application believes it handed out. Grants
//! die with the process.
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

use std::collections::HashMap;
use std::num::NonZeroUsize;
use std::ops::Range;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use bao_tree::io::BaoContentItem;
use bytes::Bytes;
use iroh::endpoint::{ConnectError, Connection, ConnectionError};
use iroh::protocol::{AcceptError, ProtocolHandler};
use iroh::{Endpoint, EndpointAddr, EndpointId};
use iroh_blobs::api::blobs::{AddBytesOptions, BlobStatus};
use iroh_blobs::api::Store;
use iroh_blobs::get::request::get_verified_size;
use iroh_blobs::get::{fsm, GetError};
use iroh_blobs::protocol::{
    ChunkRanges, ChunkRangesExt, ChunkRangesSeq, GetRequest, ERR_PERMISSION,
};
use iroh_blobs::provider::events::{
    AbortReason, ClientConnected, ConnectMode, EventMask, EventSender, ObserveMode,
    ProviderMessage, RequestMode, RequestReceived, ThrottleMode,
};
use iroh_blobs::store::mem::MemStore;
use iroh_blobs::{BlobFormat, BlobsProtocol, Hash, HashAndFormat};
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncWrite, AsyncWriteExt};

use crate::{AdmittedPeer, BlobFault, Error, Node, ProvenPeer, RemoteError, Result};

/// The most grants one node's ledger holds, across every peer: 172,032.
///
/// A grant is one `(peer, root)` pair, and one closure is one grant however many
/// blobs it lists. A grant past the limit is refused with
/// [`RemoteError::GrantsExhausted`], and nothing is evicted to make room.
/// Change it per store with [`Blobs::set_grant_limit`]; grants already held are
/// kept when the limit is lowered below them.
///
/// **One count bounds both tables.** A peer has an entry only while it holds at
/// least one grant, so there are never more peers than grants, and each table is
/// rebuilt when its length falls under half the most entries it has held since it
/// was last rebuilt, so a table's allocation follows its live entries and not the
/// largest it ever was, under churn too.
///
/// # What it costs
///
/// Measured on aarch64 macOS, in bytes of heap per grant, for two shapes:
///
/// - **Dense**, a few peers holding many grants each (`grant_allocation`, 8
///   peers): 64.2 filling to 100,000 grants, and at worst 128.5 for 100,000 held
///   after 300,000 trades of one grant for another (deleted slots are
///   tombstones, and hashbrown answers a table full of them by doubling it).
/// - **Sparse**, every peer holding one grant (`blobs::tests`, the sparse
///   ledger test): 386.3 at 1,000 peers and 320.7 at 100,000, because each grant
///   also pays for a slot in the peers' table and a table of its own.
///
/// **The default's size estimate uses the worst, the sparse 386.3**: 172,032
/// grants are 66.5 MB, about 64 MiB. A ledger of the dense shape at the same
/// limit is 11 MB to 22 MB. The figures move with the platform's pointer width
/// and the hash table's layout; the tests print them and assert a ceiling on each
/// run. They leave out the allocator's own bookkeeping and the table of calls in
/// flight, which is bounded by the calls the node is already running and is
/// rebuilt by the same rule.
pub const GRANT_LIMIT: usize = 172_032;

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

/// The address of a hash sequence this store built.
///
/// A hash sequence is its members' addresses laid end to end and stored as one
/// blob, the way iroh-blobs stores a collection. Handing one back as a
/// [`Grant`] grants every blob it lists.
///
/// # It cannot arrive off the wire
///
/// The only constructor is [`Blobs::add_seq`]. There is no `Deserialize` and no
/// `Schema`, so a handler cannot take one as an argument, and a peer cannot name
/// one. That is the boundary this type exists for: a grant over a sequence is
/// reachable only from a sequence this node built, from members its code chose.
/// What a peer sends is a plain [`BlobId`], and it stays a `Blob`. It does not
/// stop a handler that builds a sequence from hashes a peer supplied, because
/// each listed member is still an authorization nobody reviewed. See
/// [`Blobs::add_seq`].
///
/// The first version of this type could be deserialized, and a handler taking
/// one as an argument could grant a closure over bytes a peer had just uploaded.
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct SeqId(BlobId);

impl SeqId {
    /// The root blob's address, for a log line, a revoke, or a raw read of the
    /// member list.
    #[must_use]
    pub fn id(&self) -> BlobId {
        self.0
    }
}

impl std::fmt::Display for SeqId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl std::fmt::Debug for SeqId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "SeqId({})", self.0)
    }
}

/// What a reply tells the caller it may fetch.
///
/// This is the **wire** value: the caller reads it off a reply
/// ([`BlobHandle::content`]) and may build one to spend
/// ([`BlobHandle::new`]). Holding one grants nothing, which is why it can be
/// deserialized. What a handler returns to grant something is a [`Grant`].
///
/// # Exhaustive, on purpose
///
/// Callers construct and match it, and neither variant is feature-gated.
#[derive(
    Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize, postcard_schema::Schema,
)]
pub enum Content {
    /// One blob: its bytes, and nothing they name.
    Blob(BlobId),
    /// The root of a hash sequence, and every blob it lists, one level deep.
    /// Nested sequences are not followed.
    Closure(BlobId),
}

impl Content {
    /// The address the grant is recorded under: the blob, or the sequence's
    /// root.
    #[must_use]
    pub fn root(&self) -> BlobId {
        match self {
            Self::Blob(id) | Self::Closure(id) => *id,
        }
    }
}

impl From<BlobId> for Content {
    fn from(id: BlobId) -> Self {
        Self::Blob(id)
    }
}

impl From<SeqId> for Content {
    fn from(seq: SeqId) -> Self {
        Self::Closure(seq.id())
    }
}

/// What a `blob` handler returns to grant something to the caller.
///
/// A [`BlobId`] grants that one blob, and a [`SeqId`] grants the sequence and
/// every blob it lists. Both convert with `.into()`, and nothing else does.
///
/// **Not [`Content`], on purpose.** `Content` is deserializable, so a handler
/// could take one as an argument and return it, which would let a peer choose a
/// closure. A `Grant` has no serde and no constructor from `Content`, so the
/// only closures a handler can grant are ones [`Blobs::add_seq`] returned.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Grant(Content);

impl Grant {
    /// What the caller will be told it may fetch.
    #[must_use]
    pub fn content(&self) -> Content {
        self.0
    }
}

impl From<BlobId> for Grant {
    fn from(id: BlobId) -> Self {
        Self(Content::Blob(id))
    }
}

impl From<SeqId> for Grant {
    fn from(seq: SeqId) -> Self {
        Self(Content::Closure(seq.id()))
    }
}

/// What a store lacks of some content, answered from that store alone.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Missing {
    /// The store holds every blob the content names, in full.
    Nothing,
    /// The root is not held in full, so the members are not known yet.
    All,
    /// The root is held, and these members are not held in full, in sequence
    /// order (a member listed twice repeats).
    Members(Vec<BlobId>),
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
// - READING one takes a `ProvenPeer`, because a blobs connection is
//   allowlisted by nobody. `Blobs` is per NODE and `Allowlist` is per SERVICE,
//   so a mount that called `admit` itself would have to pick one allowlist —
//   or, worse, accept the union of them. Then a peer admitted by the chatty
//   public service would be admitted at the blobs layer for content the
//   private one granted. That union is exactly what RFC-01 §5.1 says must
//   never exist, and it is the reason this ledger exists instead of the
//   allowlist being routed into blobs admission.
//
// So the mount deliberately does NOT mint an `AdmittedPeer` and admits nobody.
// It proves who is calling and asks one question: does this peer hold a grant
// for this exact hash. A grant can only exist because some service's allowlist
// already said yes to that peer, at the moment its handler returned the id.

// ────────────────────────────────────────────────────────────── the ledger

/// How far a grant reaches.
///
/// Ordered, because a closure includes its root: `Closure > Blob`, and a
/// committed repeat grant widens an entry and never narrows it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum Scope {
    /// The root blob, and nothing its bytes name.
    Blob,
    /// The root and every blob it lists, reached through the root.
    Closure,
}

impl From<Content> for Scope {
    fn from(content: Content) -> Self {
        match content {
            Content::Blob(_) => Self::Blob,
            Content::Closure(_) => Self::Closure,
        }
    }
}

/// One `(peer, root)` grant.
#[derive(Debug)]
struct Entry {
    /// What replies that were written have granted. `None` while every
    /// reservation is still pending, and then the entry admits nothing.
    ///
    /// **Only a committing permit widens it.** A reservation's scope lives on
    /// its permit and nowhere else: if the entry took the scope of whichever
    /// reservation came first, a blob commit would make a pending closure live.
    committed: Option<Scope>,
    /// Reservations that have not committed or been dropped yet.
    pending: u32,
    /// Which incarnation of this `(peer, root)` pair this is. A revoke removes
    /// the entry, and a permit from before it must not be able to flip an entry
    /// somebody reserved after it.
    generation: u64,
}

/// A hash table that remembers the most entries it has held at once since it
/// was last rebuilt.
///
/// hashbrown reports capacity as live entries plus room before the next growth,
/// and a deleted slot stays a tombstone, so neither `capacity()` nor `len()`
/// says how much is allocated. What does is the high-water mark: the table
/// grows only when its live entries need it, so its allocation follows the
/// largest live count it has had. Rebuilding the table when its length falls
/// under half the mark bounds what it retains by a constant times what it holds
/// now.
#[derive(Debug)]
struct Marked<K, V> {
    map: HashMap<K, V>,
    high: usize,
}

impl<K, V> Default for Marked<K, V> {
    fn default() -> Self {
        Self {
            map: HashMap::new(),
            high: 0,
        }
    }
}

impl<K: Eq + std::hash::Hash, V> Marked<K, V> {
    fn is_empty(&self) -> bool {
        self.map.is_empty()
    }

    fn contains_key(&self, key: &K) -> bool {
        self.map.contains_key(key)
    }

    fn get(&self, key: &K) -> Option<&V> {
        self.map.get(key)
    }

    fn get_mut(&mut self, key: &K) -> Option<&mut V> {
        self.map.get_mut(key)
    }

    /// Raise the mark to the current length.
    fn raise(&mut self) {
        self.high = self.high.max(self.map.len());
    }

    fn insert(&mut self, key: K, value: V) {
        self.map.insert(key, value);
        self.raise();
    }

    fn remove(&mut self, key: &K) -> Option<V> {
        self.map.remove(key)
    }

    /// Rebuild the table if it has fallen under half its mark, and give back an
    /// empty one entirely.
    fn settle(&mut self) {
        if self.map.is_empty() {
            *self = Self::default();
        } else if 2 * self.map.len() < self.high {
            self.map.shrink_to_fit();
            self.high = self.map.len();
        }
    }
}

/// Which peer may fetch which content.
///
/// Written when a handler returns [`Content`], read by blobs admission. The
/// ladder is in the signatures: [`reserve`](Self::reserve) takes an
/// [`AdmittedPeer`] and is the only write, [`scope`](Self::scope) takes a
/// [`ProvenPeer`] and is the only read, and the revoke calls take a bare
/// `EndpointId` because removing a capability cannot widen access.
#[derive(Debug)]
struct Grants {
    inner: Mutex<Book>,
}

/// The grants, and what they are charged.
#[derive(Debug)]
struct Book {
    /// Keyed by `EndpointId` so a revoke is one lookup.
    by_peer: Marked<EndpointId, Marked<BlobId, Entry>>,
    /// Grants held or being written: the entries across every peer's table.
    grants: usize,
    limit: usize,
    next_generation: u64,
    /// Calls that were dispatched for a peer and have not finished, with the
    /// peer's revocation epoch. An entry exists only while one is in flight, so
    /// the live entries are bounded by the calls the node is already running,
    /// and the table is rebuilt by the same rule as the others so its
    /// allocation is too. Not charged to `used`.
    inflight: Marked<EndpointId, Flight>,
}

#[derive(Debug, Default)]
struct Flight {
    calls: u32,
    /// Moves on every [`Grants::revoke_peer`].
    epoch: u64,
}

/// Why a grant was not made.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum NotGranted {
    /// The ledger already holds its limit of grants.
    Exhausted { limit: usize },
    /// The peer's grants were withdrawn after the call that produced this one
    /// was dispatched.
    Withdrawn,
}

/// What a dispatched call holds from before its handler runs until its reply is
/// written, so that [`Blobs::revoke_peer`] can tell a call that started before
/// it from one that started after.
///
/// The reservation is made only when the handler returns, which is too late to
/// give a revoke anything to invalidate: a handler still running while the peer
/// is revoked would reserve and commit afterwards, for a peer nobody admits any
/// more. The fence is taken when the call's stream is accepted, before anything
/// else is done for it, and checked at reservation.
#[derive(Debug)]
pub(crate) struct Fence {
    grants: Arc<Grants>,
    peer: EndpointId,
    epoch: u64,
}

impl Drop for Fence {
    fn drop(&mut self) {
        let mut book = self.grants.lock();
        let last = book.inflight.get_mut(&self.peer).is_some_and(|flight| {
            flight.calls -= 1;
            flight.calls == 0
        });
        if last {
            book.inflight.remove(&self.peer);
            book.inflight.settle();
        }
    }
}

/// A reservation in the ledger, committed once the reply that carries the
/// grant is on its way.
///
/// Dropping one releases the reservation, which is what lets an early return
/// in `serve_one` stay truthful: a reply that was never written grants nothing.
#[derive(Debug)]
#[must_use = "dropping a permit releases its reservation"]
pub(crate) struct Permit {
    grants: Arc<Grants>,
    peer: EndpointId,
    root: BlobId,
    scope: Scope,
    generation: u64,
    done: bool,
}

impl Grants {
    fn new() -> Arc<Self> {
        Self::with_limit(GRANT_LIMIT)
    }

    fn with_limit(limit: usize) -> Arc<Self> {
        Arc::new(Self {
            inner: Mutex::new(Book {
                by_peer: Marked::default(),
                grants: 0,
                limit,
                next_generation: 0,
                inflight: Marked::default(),
            }),
        })
    }

    /// Poison recovery rather than propagation, the same call
    /// `Ledger` makes: a handler that panicked must not
    /// wedge admission for every later peer. Nothing here can panic while the
    /// lock is held, so this arm is unreachable in practice and must still be
    /// typed — `.unwrap()` and `.expect()` are denied by the workspace lints.
    fn lock(&self) -> MutexGuard<'_, Book> {
        self.inner.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn set_limit(&self, limit: usize) {
        self.lock().limit = limit;
    }

    /// Fence a call that was just accepted for `peer`, then ask `open` whether
    /// the admission it came in under still stands.
    ///
    /// **In that order, and the order is the point.** Admission is decided once
    /// per connection and copied into every call, so a call that was accepted
    /// before the peer was removed can still be on its way to its fence when
    /// the removal and `revoke_peer` both land; the fence would then read the
    /// new epoch and look like a call admitted after the revoke. The recipe for
    /// removing a peer closes its connection first, so a fence taken after
    /// `revoke_peer` finds the connection closed, and one taken before it
    /// carries the old epoch. Either way the call cannot grant.
    fn fence(
        self: &Arc<Self>,
        peer: &AdmittedPeer,
        open: impl FnOnce() -> bool,
    ) -> std::result::Result<Fence, NotGranted> {
        let fence = self.take(peer);
        if open() {
            Ok(fence)
        } else {
            Err(NotGranted::Withdrawn)
        }
    }

    /// Count one more call in flight for `peer`, at its current epoch.
    fn take(self: &Arc<Self>, peer: &AdmittedPeer) -> Fence {
        let peer = peer.id();
        let mut book = self.lock();
        let flight = book.inflight.map.entry(peer).or_default();
        flight.calls += 1;
        let epoch = flight.epoch;
        book.inflight.raise();
        Fence {
            grants: Arc::clone(self),
            peer,
            epoch,
        }
    }

    /// Reserve a grant. Takes an [`AdmittedPeer`], not a [`ProvenPeer`]: a
    /// grant is an authorization record, so writing one must be downstream of
    /// an allowlist saying yes.
    ///
    /// Refused as [`NotGranted::Withdrawn`] when `peer` was revoked after
    /// `fence` was taken.
    ///
    /// A repeat or widening grant for a pair that already has an entry is
    /// free. A new pair is refused when the ledger already holds its limit.
    fn reserve(
        self: &Arc<Self>,
        peer: &AdmittedPeer,
        content: Content,
        fence: &Fence,
    ) -> std::result::Result<Permit, NotGranted> {
        let peer = peer.id();
        let root = content.root();
        let mut book = self.lock();
        let book = &mut *book;
        let current = book.inflight.get(&peer).map(|flight| flight.epoch);
        if fence.peer != peer || current != Some(fence.epoch) {
            return Err(NotGranted::Withdrawn);
        }
        let existing = book
            .by_peer
            .get_mut(&peer)
            .and_then(|held| held.get_mut(&root));
        let generation = if let Some(entry) = existing {
            entry.pending += 1;
            entry.generation
        } else {
            if book.grants >= book.limit {
                return Err(NotGranted::Exhausted { limit: book.limit });
            }
            let generation = book.next_generation;
            book.next_generation += 1;
            book.grants += 1;
            if !book.by_peer.contains_key(&peer) {
                book.by_peer.insert(peer, Marked::default());
            }
            if let Some(held) = book.by_peer.get_mut(&peer) {
                held.insert(
                    root,
                    Entry {
                        committed: None,
                        pending: 1,
                        generation,
                    },
                );
            }
            generation
        };
        Ok(Permit {
            grants: Arc::clone(self),
            peer,
            root,
            scope: content.into(),
            generation,
            done: false,
        })
    }

    /// How far `peer`'s grant on `root` reaches, if it holds a live one.
    fn scope(&self, peer: &ProvenPeer, root: &BlobId) -> Option<Scope> {
        self.lock()
            .by_peer
            .get(&peer.id())?
            .get(root)
            .and_then(|entry| entry.committed)
    }

    /// Whether `peer` holds, or is about to hold, any grant. A reserved entry
    /// counts, so a peer whose first grant is still being written passes the
    /// connection guard; every request it sends is then refused by
    /// [`scope`](Self::scope), which is the real check.
    fn knows(&self, peer: &ProvenPeer) -> bool {
        self.lock().by_peer.contains_key(&peer.id())
    }

    fn revoke(&self, peer: EndpointId, root: BlobId) -> bool {
        let mut book = self.lock();
        Self::remove(&mut book, peer, &root).is_some_and(|entry| entry.committed.is_some())
    }

    fn revoke_peer(&self, peer: EndpointId) -> usize {
        let mut book = self.lock();
        // Calls dispatched before this point must not grant anything after it.
        if let Some(flight) = book.inflight.get_mut(&peer) {
            flight.epoch += 1;
        }
        let Some(held) = book.by_peer.remove(&peer) else {
            return 0;
        };
        book.grants -= held.map.len();
        book.by_peer.settle();
        held.map
            .values()
            .filter(|entry| entry.committed.is_some())
            .count()
    }

    fn revoke_everywhere(&self, root: BlobId) -> usize {
        let mut book = self.lock();
        let peers: Vec<EndpointId> = book
            .by_peer
            .map
            .iter()
            .filter(|(_, held)| held.contains_key(&root))
            .map(|(peer, _)| *peer)
            .collect();
        peers
            .into_iter()
            .filter_map(|peer| Self::remove(&mut book, peer, &root))
            .filter(|entry| entry.committed.is_some())
            .count()
    }

    /// Take one entry out, and the peer when it was its last, and rebuild what
    /// has fallen under half its mark.
    fn remove(book: &mut Book, peer: EndpointId, root: &BlobId) -> Option<Entry> {
        let held = book.by_peer.get_mut(&peer)?;
        let entry = held.remove(root)?;
        book.grants -= 1;
        held.settle();
        if held.is_empty() {
            // Not tidiness: `knows` reads this map, so a peer left behind with
            // an empty table would keep passing connection admission while
            // holding no grant at all.
            book.by_peer.remove(&peer);
            book.by_peer.settle();
        }
        Some(entry)
    }
}

impl Permit {
    /// The reply is on its way: make the grant live.
    ///
    /// A no-op when the entry was revoked meanwhile, or revoked and reserved
    /// again by somebody else, which the generation tells apart.
    pub(crate) fn commit(mut self) {
        self.done = true;
        let mut book = self.grants.lock();
        if let Some(entry) = book
            .by_peer
            .get_mut(&self.peer)
            .and_then(|held| held.get_mut(&self.root))
            .filter(|entry| entry.generation == self.generation)
        {
            entry.pending -= 1;
            entry.committed = entry.committed.max(Some(self.scope));
        }
    }
}

impl Drop for Permit {
    fn drop(&mut self) {
        if self.done {
            return;
        }
        let mut book = self.grants.lock();
        let Some(entry) = book
            .by_peer
            .get_mut(&self.peer)
            .and_then(|held| held.get_mut(&self.root))
            .filter(|entry| entry.generation == self.generation)
        else {
            return;
        };
        entry.pending -= 1;
        if entry.committed.is_none() && entry.pending == 0 {
            Grants::remove(&mut book, self.peer, &self.root);
        }
    }
}

/// Closes a live connection when its peer's grants are withdrawn.
///
/// A trait, and not a [`Connection`] in the table, so the unit tests can admit
/// a connection quinn never saw and watch it being closed.
trait Hangup: std::fmt::Debug + Send + Sync {
    fn hang_up(&self);
}

impl Hangup for Connection {
    /// The same code and reason iroh-blobs and [`BlobsAdmission`] refuse with,
    /// so the peer reads [`Error::BlobRefused`].
    fn hang_up(&self) {
        self.close(ERR_PERMISSION, b"permission");
    }
}

/// Which live blobs connection belongs to which proven peer.
///
/// iroh-blobs' per-request events carry `connection_id` and nothing else
/// (`provider/events.rs:602`), so the identity has to be remembered from the
/// accept. Populated **only** by [`BlobsAdmission::accept`], which is the second
/// guard: a request arriving on a connection keylink never admitted finds
/// nothing here and is refused. It also keeps each connection's handle, so
/// [`Blobs::revoke_peer`] can close the ones a peer has open.
#[derive(Debug, Default)]
struct Connections(Mutex<HashMap<u64, Live>>);

/// One admitted connection.
#[derive(Debug)]
struct Live {
    peer: ProvenPeer,
    handle: Arc<dyn Hangup>,
}

impl Connections {
    fn lock(&self) -> MutexGuard<'_, HashMap<u64, Live>> {
        self.0.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn admit(&self, connection_id: u64, peer: ProvenPeer, handle: Arc<dyn Hangup>) {
        self.lock().insert(connection_id, Live { peer, handle });
    }

    /// Register a connection of `peer` if the ledger still knows it, and hang
    /// it up if not. Returns whether it was kept.
    ///
    /// **Registers first and asks second**, which is the whole point. The other
    /// order checks that the peer holds a grant and then registers, and a
    /// `revoke_peer` that lands between the two has already swept the
    /// connections it could see, so this one would be accepted by a node that
    /// promised to refuse the peer. Registering first means a revoke either sees
    /// the connection in its sweep, or removed the grants before the question
    /// below is asked.
    fn admit_known(
        &self,
        grants: &Grants,
        connection_id: u64,
        peer: ProvenPeer,
        handle: Arc<dyn Hangup>,
    ) -> bool {
        self.admit(connection_id, peer, Arc::clone(&handle));
        if grants.knows(&peer) {
            return true;
        }
        self.forget(connection_id, peer);
        handle.hang_up();
        false
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
        if live
            .get(&connection_id)
            .is_some_and(|held| held.peer == peer)
        {
            live.remove(&connection_id);
        }
    }

    fn peer(&self, connection_id: u64) -> Option<ProvenPeer> {
        self.lock().get(&connection_id).map(|held| held.peer)
    }

    /// Close every connection `peer` has open. Returns how many.
    fn close_peer(&self, peer: EndpointId) -> usize {
        // Collected first so the handles are called with the lock released.
        let handles: Vec<Arc<dyn Hangup>> = self
            .lock()
            .values()
            .filter(|held| held.peer.id() == peer)
            .map(|held| Arc::clone(&held.handle))
            .collect();
        for handle in &handles {
            handle.hang_up();
        }
        handles.len()
    }
}

/// Forgets an admitted connection however the accept ends.
///
/// A plain call after the `await` is not enough: the router drops handler
/// futures on shutdown, and a dropped future never reaches its next line. Each
/// cancelled accept would leave one entry behind forever, which is a leak one
/// map over from the one the ledger's grant limit bounds.
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
        let events = policy(
            Arc::clone(&grants),
            Arc::clone(&live),
            backing.store().clone(),
        );
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
        let tag = self.store().add_bytes(data).await.map_err(store_error)?;
        Ok(BlobId::from_hash(tag.hash))
    }

    /// Store a hash sequence listing `members`, in order, and return its
    /// address.
    ///
    /// Adding grants nothing. A member this store does not hold makes the
    /// fetcher's closure fetch fail for that member. **A
    /// [`Content::Closure`] grant authorizes every hash the sequence lists**, so
    /// build one only from members the recipient may read. In particular, do not
    /// build one from hashes a peer supplied: that peer could list any hash this
    /// store holds and read it, and so probe what is stored.
    ///
    /// A closure is one level deep: a member that is itself a sequence is served
    /// as bytes and its own members are not covered, so list every blob of a
    /// tree in the one sequence.
    ///
    /// # Errors
    ///
    /// [`Error::Io`] when the store refuses the write.
    pub async fn add_seq(&self, members: impl IntoIterator<Item = BlobId>) -> Result<SeqId> {
        let data: Vec<u8> = members.into_iter().flat_map(|member| member.0).collect();
        let tag = self
            .store()
            .add_bytes_with_opts(AddBytesOptions {
                data: Bytes::from(data),
                format: BlobFormat::HashSeq,
            })
            .await
            .map_err(store_error)?;
        Ok(SeqId(BlobId::from_hash(tag.hash)))
    }

    /// What this store lacks of `content`, without asking any peer.
    ///
    /// For a [`Content::Blob`] that is the blob or nothing. For a
    /// [`Content::Closure`] whose root is held, the root is read in 32 KiB
    /// windows and each listed member is asked of the store, so the memory is
    /// one window plus the list of what is missing.
    ///
    /// An rpc that relays this answer to a peer makes it an existence oracle on
    /// this store, unless the handler limits it to hashes that peer may know
    /// about.
    ///
    /// # Errors
    ///
    /// [`Error::Io`] when the store fails, and [`Error::BlobUnverified`] with
    /// [`BlobFault::NotASequence`] when a closure's root is not a whole number of
    /// 32-byte addresses.
    pub async fn missing(&self, content: Content) -> Result<Missing> {
        let root = content.root();
        let Content::Closure(_) = content else {
            return Ok(if self.has(root).await? {
                Missing::Nothing
            } else {
                Missing::All
            });
        };
        // The root is checked before it is read, for whole addresses.
        if self.members(root).await?.is_none() {
            return Ok(Missing::All);
        }
        let mut lacking = Vec::new();
        let mut offset = 0u64;
        loop {
            let window = self
                .store()
                .export_ranges(root.hash(), offset..offset + ROOT_WINDOW)
                .concatenate()
                .await
                .map_err(store_error)?;
            let (members, rest) = window.as_chunks::<32>();
            if !rest.is_empty() {
                return Err(Error::BlobUnverified {
                    id: root,
                    fault: BlobFault::NotASequence {
                        len: offset + window.len() as u64,
                    },
                });
            }
            for member in members {
                let member = BlobId(*member);
                if !self.has(member).await? {
                    lacking.push(member);
                }
            }
            if (window.len() as u64) < ROOT_WINDOW {
                break;
            }
            offset += ROOT_WINDOW;
        }
        Ok(if lacking.is_empty() {
            Missing::Nothing
        } else {
            Missing::Members(lacking)
        })
    }

    /// The members `range` of the sequence at `root`, read off this store and
    /// nothing else of it. The root must be held in full and checked.
    async fn listed(&self, root: BlobId, range: Range<u64>) -> Result<Vec<BlobId>> {
        let bytes = self
            .store()
            .export_ranges(root.hash(), range.start * 32..range.end * 32)
            .concatenate()
            .await
            .map_err(store_error)?;
        let (members, rest) = bytes.as_chunks::<32>();
        if !rest.is_empty() || members.len() as u64 != range.end - range.start {
            return Err(Error::Io(std::io::Error::other(format!(
                "closure root {root} returned {} bytes for members {range:?}",
                bytes.len()
            ))));
        }
        Ok(members.iter().map(|member| BlobId(*member)).collect())
    }

    /// How many members the sequence at `root` lists, if this store holds it in
    /// full, and an error when what it holds is not a whole number of addresses.
    async fn members(&self, root: BlobId) -> Result<Option<u64>> {
        let status = self
            .store()
            .blobs()
            .status(root.hash())
            .await
            .map_err(store_error)?;
        match status {
            BlobStatus::Complete { size } => count_members(root, size).map(Some),
            _ => Ok(None),
        }
    }

    /// Whether this store holds `id` in full.
    async fn has(&self, id: BlobId) -> Result<bool> {
        self.store()
            .blobs()
            .has(id.hash())
            .await
            .map_err(store_error)
    }

    /// Commit everything this store holds, so that it survives the process.
    ///
    /// # Why a store needs this at all
    ///
    /// iroh-blobs' metadata actor keeps one redb write transaction open across
    /// a **batch** of commands and commits when the batch ends: when its
    /// configurable maximum write duration runs out, or when a top-level command
    /// forces it. So an awaited
    /// [`add_bytes`](Self::add_bytes) or
    /// [`fetch_into`](BlobHandle::fetch_into) has been *accepted* rather than
    /// *committed*, and a process that exits promptly loses it. Measured on a
    /// 58-byte payload, ten process pairs per arm: dropping the store committed
    /// it 2 times out of 10, and this call committed it 10 out of 10.
    ///
    /// A payload under iroh-blobs' 16 KiB inline threshold lives entirely in
    /// redb, so losing the transaction loses the content outright rather than
    /// leaving a recoverable data file.
    ///
    /// # The one path that already flushed, and why it is not enough
    ///
    /// A store handed to a service through
    /// [`with_blobs`](crate::ServiceBuilder::with_blobs) is committed by
    /// [`Server::shutdown`](crate::Server::shutdown), through iroh's router. A
    /// store that is only a fetch *target* is mounted on nothing, so before this
    /// method no public call could commit one — which made
    /// [`Retention::Forever`]'s promise reachable only by inventing a service to
    /// hang the store on.
    ///
    /// # It leaves the store usable
    ///
    /// Deliberately `sync_db` and not `shutdown`. Both commit; only one lets the
    /// caller carry on. A caller that wants the store closed drops it after
    /// this.
    ///
    /// On a [`memory`](Self::memory) store this is a no-op that succeeds, so a
    /// caller writes the same line either way.
    ///
    /// # Errors
    ///
    /// [`Error::Io`] when the commit fails. What was already committed is
    /// intact.
    pub async fn flush(&self) -> Result<()> {
        self.store().sync_db().await.map_err(store_error)
    }

    /// Withdraw `peer`'s grant on `root`; its next request for it is refused.
    /// Returns whether one was held.
    ///
    /// A request that was already admitted runs to completion, bounded by what
    /// that one request asked for. Use [`revoke_peer`](Self::revoke_peer) for an
    /// immediate cut. Bytes already delivered are verified content in the
    /// fetcher's store, and nothing recalls them.
    pub fn revoke(&self, peer: EndpointId, root: BlobId) -> bool {
        self.inner.grants.revoke(peer, root)
    }

    /// Withdraw every grant `peer` holds and close its live blobs connections.
    /// Returns how many grants.
    ///
    /// Its in-flight transfers stop at once and the fetcher reads
    /// [`Error::BlobRefused`], and a new connection is refused at accept. A call
    /// whose handler was already running when this lands is fenced too: the
    /// content it returns is not granted, and the caller reads
    /// [`RemoteError::GrantsWithdrawn`].
    /// Removing a peer from an [`Allowlist`](crate::Allowlist) or a
    /// [`DynamicAdmission`](crate::DynamicAdmission) does not touch its blob
    /// grants, because the ledger is per node and does not record which service
    /// granted what: drop the peer from admission first, then call this.
    ///
    /// **That recipe is complete only if admission closes every RPC connection
    /// of the peer.** A call is fenced when its stream is accepted and refused if
    /// its connection is closed by then, so a connection admission left open can
    /// still take a fresh fence after this has run. [`DynamicAdmission`](crate::DynamicAdmission) keeps one
    /// revocation handle per peer today, so a second live connection of the same
    /// peer survives a `replace` and is exactly that. The `trust` branch fixes
    /// the registry, which already tracks connections by id; until it lands,
    /// close the peer's connections some other way (restart the server, or admit
    /// by an allowlist that cannot hold a peer twice) before relying on this.
    pub fn revoke_peer(&self, peer: EndpointId) -> usize {
        let grants = self.inner.grants.revoke_peer(peer);
        self.inner.live.close_peer(peer);
        grants
    }

    /// Withdraw every peer's grant on `root`. Returns how many grants.
    ///
    /// The same in-flight rule as [`revoke`](Self::revoke).
    pub fn revoke_everywhere(&self, root: BlobId) -> usize {
        self.inner.grants.revoke_everywhere(root)
    }

    /// Set the limit on grants of the ledger every clone of this store shares.
    /// The default is [`GRANT_LIMIT`].
    ///
    /// Takes effect at the next new grant. Grants already held are kept even
    /// when they exceed it.
    pub fn set_grant_limit(&self, grants: NonZeroUsize) {
        self.inner.grants.set_limit(grants.get());
    }

    /// Record a grant outside generated dispatch. Test fixtures only; dispatch
    /// reserves, and commits once the reply is on its way.
    ///
    /// Takes an [`AdmittedPeer`] rather than a connection or an id. A
    /// connection would prove only identity, and an id proves nothing at all —
    /// either would let this second accept path write an authorization record
    /// for a peer no [`Allowlist`](crate::Allowlist) admitted. The only source
    /// of an `AdmittedPeer` is `Allowlist::admit`, so the policy has already
    /// run by the time this can be called.
    ///
    /// # Errors
    ///
    /// [`Error::Remote`] carrying [`RemoteError::GrantsExhausted`] when the
    /// limit refuses it.
    #[doc(hidden)]
    pub fn grant(&self, peer: &AdmittedPeer, content: Content) -> Result<()> {
        let fence = self
            .fence(peer, || true)
            .map_err(|refused| Error::Remote(refusal(&refused)))?;
        self.reserve(peer, content, &fence)
            .map_err(|refused| Error::Remote(refusal(&refused)))?
            .commit();
        Ok(())
    }

    /// Fence a call just accepted for `peer`, refusing it when `open` says the
    /// admission it came in under is gone. See [`Fence`] and the order it takes.
    pub(crate) fn fence(
        &self,
        peer: &AdmittedPeer,
        open: impl FnOnce() -> bool,
    ) -> std::result::Result<Fence, NotGranted> {
        self.inner.grants.fence(peer, open)
    }

    /// Reserve a grant, to be committed once the reply that carries it is
    /// written.
    pub(crate) fn reserve(
        &self,
        peer: &AdmittedPeer,
        content: Content,
        fence: &Fence,
    ) -> std::result::Result<Permit, NotGranted> {
        self.inner.grants.reserve(peer, content, fence)
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

    /// Whether two handles are the same store.
    ///
    /// Pointer identity on the shared `Inner`, not a comparison of contents:
    /// the question `ServerBuilder::spawn` asks is "were these cloned from one
    /// `Blobs`", and two `Blobs::memory()` calls are two stores even while both
    /// are empty.
    pub(crate) fn is_same_store(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.inner, &other.inner)
    }
}

/// Whether a reply hands out content.
///
/// Implemented by every generated `Response`, so the RPC layer can record a
/// grant without knowing anything about the service's types. `serve_one` is the
/// only reader, and it is the only place that holds both the store and the
/// [`AdmittedPeer`] — the handler has neither, and
/// `accept` has no address.
///
/// This is what keeps the store out of `Dispatch`, `Next` and `Context`: the
/// address travels back on the reply that already exists rather than the store
/// travelling down through every middleware.
///
/// # It used to have a second, dishonest implementor
///
/// A keyed actor system answered with an opaque `ActorReply` — a `Vec<u8>` the
/// system never decoded — so its `Granting` impl returned `None` under an
/// eighteen-line comment explaining that the address was genuinely out of reach.
/// The consequence was `blob` having no actor-handle method at all: an address
/// handed out that way was granted by nobody and refused on fetch.
///
/// A keyed actor answers with the service's own `Response` now, so the generated
/// impl below is the only one, it is honest, and `blob` works on a keyed actor.
pub trait Granting {
    /// The content this reply hands out, if it hands out any.
    fn granted(&self) -> Option<Content>;
}

/// How much of a closure's root is read per store call: 1024 members.
const ROOT_WINDOW: u64 = 32 * 1024;

/// How many members one request of a closure fetch asks for.
///
/// Each window is planned from its own slice of the member list: the slice is
/// read off the store, each of its members is asked of the store, and the
/// request names the ones it lacks, one run per stretch of them. So the plan's
/// memory is a window's and the request is a few bytes per gap, **whatever the
/// closure's length**: this is what lets a closure have no member limit.
///
/// **It does not make the work linear.** iroh-blobs 0.104 walks a request's
/// offsets one at a time (`iter_non_empty_infinite`), on both ends, so a window
/// that starts at member `k` costs `k` steps before it reaches its first member,
/// and a whole closure of `N` members costs about `N^2 / 4096` of them. That is
/// not measured on its own. The sizes measured: a 300,000-member closure with
/// alternating gaps takes 74 s from a cold start (176 s on a loaded machine) (`mise run closure-300k`, which
/// includes adding both stores' members and about a million store round trips
/// asking what is held), against 8 s for 12,289. It is the thing to look at
/// first if closures get much larger.
///
/// A member held only in part counts as lacking and is requested whole, so a
/// window's request can have an entry for every one of its members, and the
/// check below is that even that stays far under what iroh-blobs will read.
const FETCH_WINDOW: u64 = 4096;

// A window's request, by construction. Inside a window every run starts within
// `FETCH_WINDOW` offsets of the one before, so its gap is a two-byte varint; a
// selecting run is that, the length of a one-element spec and the element
// (1 + 1 + 2 bytes at most), and an empty run is the gap and a zero length. That
// is 7 bytes per member at the very worst, plus the first run's offset, which is
// a varint of at most 10 bytes, and the list's own length.
const _: () = assert!(
    FETCH_WINDOW as usize * 7 + 16 < iroh_blobs::protocol::MAX_MESSAGE_SIZE / 16,
    "a fetch window's request must stay far under iroh-blobs' message limit"
);

/// The member count of a root of `size` bytes, or why it is not a closure.
fn count_members(root: BlobId, size: u64) -> Result<u64> {
    if !size.is_multiple_of(32) {
        return Err(Error::BlobUnverified {
            id: root,
            fault: BlobFault::NotASequence { len: size },
        });
    }
    Ok(size / 32)
}

/// The request for exactly the members of `root` that `lacking` marks, where
/// `lacking[i]` is member `start + i`, and nothing else: no bytes of the root,
/// and no member that is not marked.
///
/// Built from its wire form, a run for each stretch of marked members (from its
/// first offset, selecting everything) and a run that selects nothing after it,
/// rather than with the request builder, which expands a vector of every offset
/// below the last one it is given. Offset 0 is the root and member `n` is offset
/// `n + 1`. One run per stretch keeps a window's request to a few bytes per gap,
/// which for 4096 members is at most about 12 KB against a 1 MiB cap.
fn missing_request(root: BlobId, start: u64, lacking: &[bool]) -> Result<GetRequest> {
    let mut runs: Vec<(u64, Vec<u64>)> = Vec::new();
    let mut at = 0u64;
    let mut selected = false;
    for (index, lack) in (0u64..).zip(lacking) {
        if *lack != selected {
            let offset = start + index + 1;
            runs.push((offset - at, if *lack { vec![0] } else { Vec::new() }));
            at = offset;
            selected = *lack;
        }
    }
    if selected {
        let end = start + lacking.len() as u64 + 1;
        runs.push((end - at, Vec::new()));
    }
    let wire = postcard::to_stdvec(&runs).map_err(store_error)?;
    let ranges = postcard::from_bytes(&wire).map_err(store_error)?;
    Ok(GetRequest::new(root.hash(), ranges))
}

/// A store failure, as keylink reports every one.
fn store_error(error: impl std::fmt::Display) -> Error {
    Error::Io(std::io::Error::other(error.to_string()))
}

/// The refusal a caller reads when the ledger will not take its grant.
pub(crate) fn refusal(refused: &NotGranted) -> RemoteError {
    match refused {
        NotGranted::Exhausted { limit } => RemoteError::GrantsExhausted {
            limit: *limit as u64,
        },
        NotGranted::Withdrawn => RemoteError::GrantsWithdrawn,
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
        //
        // Refused here when the peer holds no grant, with the same code and
        // reason iroh-blobs uses for a refusal, so a denied peer reads
        // `Error::BlobRefused` whether it was turned away at the connection or
        // at the request.
        if !self.live.admit_known(
            &self.grants,
            connection_id,
            peer,
            Arc::new(connection.clone()),
        ) {
            return Ok(());
        }
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
/// Naming all six is what made the upstream fix a no-op here. In 0.103.0
/// `EventSender::request` read `mask.get` for get, get-many, push and observe
/// alike, so the other three fields were dead. 0.103.1 (#270) made each request
/// kind read its own field, and 0.104.0 keeps that: `push: Disabled` is now the
/// mode a push meets, where before it met `get`'s.
fn policy(grants: Arc<Grants>, live: Arc<Connections>, store: Store) -> EventSender {
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
                    match admits_get(&grants, &live, &msg.inner) {
                        Ok(Admit::Now) => {
                            msg.tx.send(Ok(())).await.ok();
                        }
                        Err(refusal) => {
                            msg.tx.send(Err(refusal)).await.ok();
                        }
                        // One metadata read, in a task of its own so a slow store
                        // cannot hold up the verdicts behind it.
                        Ok(Admit::IfRootLists { root, furthest }) => {
                            let store = store.clone();
                            n0_future::task::spawn(async move {
                                let verdict = root_lists(&store, root, furthest).await;
                                msg.tx.send(verdict).await.ok();
                            });
                        }
                    }
                }
                // No keylink caller sends one: a closure's members travel by
                // offset under their root, so a request that names hashes of
                // its own has no use here and is not served.
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

/// What admission decided without the store, and what it needs the store for.
#[derive(Debug, PartialEq, Eq)]
enum Admit {
    /// Admit the request.
    Now,
    /// Admit it if the root lists enough members for the request's furthest run
    /// to start at one of them. See [`lists`].
    IfRootLists { root: Hash, furthest: u64 },
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
/// length is a multiple of 32. A [`Scope::Blob`] grant is therefore admitted
/// only for a request that stays on the root (`ranges.is_blob()`: one range set
/// at offset 0).
///
/// A [`Scope::Closure`] grant is admitted for any ranges under its root,
/// because its root was built by [`Blobs::add_seq`] and intent is stated: the
/// holder lists exactly the members it means to hand out. iroh-blobs serves
/// only the members its own copy of the root lists, by offset, and the request
/// names no hash.
///
/// **One thing about the request is checked against the root.** iroh-blobs walks
/// a request's offsets one at a time, and a request can start a run at an offset
/// near `u64::MAX` in a few bytes, so it would walk every empty offset up to it
/// before it looked at the sequence. A request whose furthest non-empty run
/// starts past the root's own member count is refused, and the count is the
/// root's size over 32, one metadata read ([`root_lists`]) that is made only
/// after the grant has been found, so a hash nobody was granted is still
/// refused without touching the store. The request is read off its encoded runs
/// and never expanded. A run that starts at a member and goes on is fine; it
/// yields members until the sequence ends, which is how a whole closure is asked
/// for.
///
/// This stays a runtime check on purpose. It is peer-supplied data arriving at
/// the edge, which is the one place a guard belongs (peak-rust rule 2): no type
/// keylink owns is on this path, because the request was decoded by iroh-blobs.
fn admits_get(
    grants: &Grants,
    live: &Connections,
    msg: &RequestReceived<GetRequest>,
) -> std::result::Result<Admit, AbortReason> {
    let Some(peer) = live.peer(msg.connection_id) else {
        return Err(AbortReason::Permission);
    };
    let Some(scope) = grants.scope(&peer, &BlobId::from_hash(msg.request.hash)) else {
        return Err(AbortReason::Permission);
    };
    if msg.request.ranges.is_blob() {
        return Ok(Admit::Now);
    }
    // Reaches past the root. Only a closure grant covers that.
    if scope != Scope::Closure {
        return Err(AbortReason::Permission);
    }
    match furthest_run_start(&msg.request.ranges) {
        None => Err(AbortReason::Permission),
        // The root itself, or nothing: no walk to speak of.
        Some(0) => Ok(Admit::Now),
        Some(furthest) => Ok(Admit::IfRootLists {
            root: msg.request.hash,
            furthest,
        }),
    }
}

/// The offset at which the last non-empty run of `ranges` starts, where offset
/// 0 is the root and offset `n + 1` is member `n`; 0 when there is none.
/// `None` when the value will not encode, which counts as refusable.
///
/// Read off the encoded runs and never expanded: a run is a start offset and the
/// ranges that apply from there until the next run, so the work is the size of
/// the request and not the size of the offsets it names. The runs are taken from
/// the wire form of the value, which is the one view of them iroh-blobs offers.
fn furthest_run_start(ranges: &ChunkRangesSeq) -> Option<u64> {
    let wire = postcard::to_stdvec(ranges).ok()?;
    let runs = postcard::from_bytes::<Vec<(u64, Vec<u64>)>>(&wire).ok()?;
    let mut offset = 0u64;
    let mut furthest = 0;
    for (gap, spans) in runs {
        offset = offset.checked_add(gap)?;
        // A run with no spans selects nothing, wherever it starts.
        if !spans.is_empty() {
            furthest = offset;
        }
    }
    Some(furthest)
}

/// Whether a root of `size` bytes lists a member at offset `furthest`: it is a
/// whole number of addresses and has at least `furthest` of them.
fn lists(size: u64, furthest: u64) -> bool {
    size.is_multiple_of(32) && furthest <= size / 32
}

/// Ask the store for the size of `root` and decide [`Admit::IfRootLists`]. A
/// root the store does not hold in full cannot serve a closure, and is refused.
async fn root_lists(
    store: &Store,
    root: Hash,
    furthest: u64,
) -> std::result::Result<(), AbortReason> {
    match store.blobs().status(root).await {
        Ok(BlobStatus::Complete { size }) if lists(size, furthest) => Ok(()),
        _ => Err(AbortReason::Permission),
    }
}

// ─────────────────────────────────────────────────────────────── the client

/// Content plus the node that holds it.
///
/// Opens the blobs connection internally, so a call site names no ALPN and no
/// second dial.
///
/// # Cloning opens a second read, it does not share one
///
/// Every read dials its own connection, so two clones interleave without
/// seeing each other's bytes — which is what an HTTP gateway needs when one
/// address feeds a range request and a full download at once. The cost is that
/// two clones can start two concurrent downloads of the same content; that is
/// an API cost the caller controls, not a way to corrupt a stream.
#[derive(Debug, Clone)]
pub struct BlobHandle {
    endpoint: Endpoint,
    holder: EndpointAddr,
    content: Content,
}

impl BlobHandle {
    /// Point a handle at content on `holder`.
    ///
    /// A `blob` method hands back a [`BlobHandle`] already built, and that is
    /// the ordinary route. This constructor is for the other one: a [`BlobId`]
    /// is `Serialize`, and so is the [`Content`] a reply carries (pass a
    /// closure on as `seq.id()` with [`Content::Closure`], since a [`SeqId`]
    /// itself cannot be serialized), so a peer can pass an address on to
    /// somebody else, and that recipient needs a way to spend it.
    ///
    /// **An address is not a capability.** The grant is recorded for the exact
    /// `(peer, root)` pair the *handler* answered, so a handle built here
    /// reaches the content only if the holder has already granted it to this
    /// node, at least as widely as the handle claims. Building one for an
    /// address nobody granted you is refused at the fetch, not here.
    #[must_use]
    pub fn new(node: &Node, holder: impl Into<EndpointAddr>, content: impl Into<Content>) -> Self {
        Self::on(node.endpoint().clone(), holder, content.into())
    }

    /// The same handle, built from the endpoint a live client is already
    /// dialling on. Generated `blob` client code takes this route, because an
    /// `RpcClient` holds an endpoint and a peer rather than a `Node`.
    pub(crate) fn on(
        endpoint: Endpoint,
        holder: impl Into<EndpointAddr>,
        content: Content,
    ) -> Self {
        Self {
            endpoint,
            holder: holder.into(),
            content,
        }
    }

    /// The root address: the blob, or the sequence's root.
    ///
    /// [`open`](Self::open), [`open_range`](Self::open_range),
    /// [`verified_len`](Self::verified_len) and [`copy_to`](Self::copy_to) read
    /// this blob, which for a closure is its member list.
    #[must_use]
    pub fn id(&self) -> BlobId {
        self.content.root()
    }

    /// What the holder granted: one blob, or a closure.
    #[must_use]
    pub fn content(&self) -> Content {
        self.content
    }

    /// What `store` lacks of this content.
    ///
    /// For a closure whose root `store` does not hold, this first fetches the
    /// root into `store`, on one dial, because the members cannot be named
    /// before it. That fetch is the one request the holder's closure grant
    /// admits for it, and it moves only the member list.
    ///
    /// # Errors
    ///
    /// The same set as [`fetch_into`](Self::fetch_into), and
    /// [`Error::BlobUnverified`] with [`BlobFault::NotASequence`] when the root
    /// is not a whole number of addresses.
    pub async fn missing(&self, store: &Blobs) -> Result<Missing> {
        if let Content::Closure(_) = self.content {
            // Dials only when the store lacks the root.
            if store.members(self.id()).await?.is_none() {
                let connection = self.dial().await?;
                self.ensure_root(&connection, store).await?;
            }
        }
        store.missing(self.content).await
    }

    /// The verified size of the content, without transferring it.
    ///
    /// One last-chunk request, validated against the address. iroh-blobs' own
    /// caveat applies and is worth repeating: this proves the holder can
    /// produce the last chunk of content with this hash, not that it holds
    /// every chunk in between.
    ///
    /// # Errors
    ///
    /// [`Error::BlobRefused`] when the holder denies the probe,
    /// [`Error::Transport`] when the dial fails, and [`Error::Io`] when the
    /// answer does not validate.
    pub async fn verified_len(&self) -> Result<u64> {
        let connection = self.dial().await?;
        self.verified_len_on(&connection).await
    }

    async fn verified_len_on(&self, connection: &Connection) -> Result<u64> {
        get_verified_size(connection, &self.id().hash())
            .await
            .map(|(size, _)| size)
            .map_err(|error| self.classify(error, connection))
    }

    /// Open a verified read of the whole blob.
    ///
    /// Completes the size preflight and compares the data response against it
    /// before returning, so [`BlobReader::total_len`] is a number the content
    /// address has already vouched for.
    ///
    /// # Errors
    ///
    /// [`Error::BlobRefused`] when the holder denies the fetch,
    /// [`Error::Transport`] when the dial fails, and [`Error::Io`] when the
    /// holder answers something a correct one would not.
    pub async fn open(&self) -> Result<BlobReader> {
        let connection = self.dial().await?;
        let total = self.verified_len_on(&connection).await?;
        self.reader(connection, total, 0, total, ChunkRanges::all())
            .await
    }

    /// Open a verified read of one half-open byte range.
    ///
    /// The same preflight as [`open`](Self::open), so
    /// [`BlobReader::total_len`] is still the whole blob and
    /// [`BlobReader::selected_len`] is what this reader will yield.
    ///
    /// # Errors
    ///
    /// The same set as [`open`](Self::open).
    pub async fn open_range(&self, range: Range<u64>) -> Result<BlobReader> {
        // Empty and reversed need no size to refuse, so they never reach the
        // dial: the holder pays nothing at all for `500..100`, not a handshake
        // and not a probe. The guard IS the subtraction rather than a check
        // beside one, so there is no `-` here that a later edit could leave
        // unprotected — that shape had already produced `attempt to subtract
        // with overflow` when its guard was broken.
        let Some(len) = range.end.checked_sub(range.start).filter(|len| *len > 0) else {
            return Err(self.no_such_range(0, &range));
        };
        let connection = self.dial().await?;
        let total = self.verified_len_on(&connection).await?;
        // Against the VERIFIED size, and before the data request, so a range
        // nobody can satisfy costs one probe and no bao traversal.
        if range.end > total {
            return Err(self.no_such_range(total, &range));
        }
        // `ChunkRanges::bytes` and not `ChunkRanges::from`. `from` takes CHUNK
        // numbers, accepts byte offsets through `ChunkNum` without complaint,
        // and silently requests a region 1024 times away from the one asked
        // for. The conversion is upstream's, and its own note is that byte
        // bounds round out to whole chunks — which is why the reader slices
        // every leaf rather than writing it.
        let ranges = ChunkRanges::bytes(range.clone());
        self.reader(connection, total, range.start, len, ranges)
            .await
    }

    fn no_such_range(&self, len: u64, range: &Range<u64>) -> Error {
        Error::BlobRange {
            id: self.id(),
            len,
            start: range.start,
            end: range.end,
        }
    }

    /// Send the data request and hold the answer to the verified size.
    ///
    /// **The comparison is not theatre, and it is not an atomic snapshot
    /// either.** Two requests can meet two different behaviours from one
    /// holder. What it buys is precise: the probe validated the advertised
    /// size and last chunk against this address, the data response advertised
    /// the same number, and bao validates every delivered leaf against the
    /// same address as it is consumed. A holder that changes its answer
    /// between the two produces an error, never a successful wrong read —
    /// different bytes cannot pass validation for one hash.
    ///
    /// What it prevents is narrower and is the reason it runs before a reader
    /// exists: a size that reading would eventually reject is still a size an
    /// HTTP gateway announces as `Content-Length` before the first byte moves.
    async fn reader(
        &self,
        connection: Connection,
        total: u64,
        start: u64,
        len: u64,
        ranges: ChunkRanges,
    ) -> Result<BlobReader> {
        let (content, announced) = self.request(&connection, ranges).await?;
        if announced != total {
            return Err(Error::BlobUnverified {
                id: self.id(),
                fault: BlobFault::Size {
                    announced,
                    verified: total,
                },
            });
        }
        Ok(BlobReader {
            connection,
            total,
            take: Selection::new(self.id(), start, len),
            state: State::Reading(content),
        })
    }

    /// Send one root-only get request and read the size header off its answer.
    ///
    /// The non-root arm is refused rather than `unreachable!()`, and the reason
    /// is NOT that a peer chose it. `ConnectedNext` comes from the request's own
    /// ranges iterator (`iroh-blobs get.rs:375-395`), which is walked before a
    /// single response byte is read, so it is decided by the request built two
    /// lines above and by nothing on the wire.
    ///
    /// It is refused because it is unreachable *for the requests this function
    /// builds*: [`ChunkRangesSeq::from_ranges`] over one non-empty range yields
    /// offset 0, which is also exactly what keylink's own admission requires.
    /// An upstream change to that walk should therefore surface as an error
    /// here rather than as a panic on a path nobody exercises.
    async fn request(
        &self,
        connection: &Connection,
        ranges: ChunkRanges,
    ) -> Result<(fsm::AtBlobContent, u64)> {
        let request = GetRequest::new(self.id().hash(), ChunkRangesSeq::from_ranges([ranges]));
        let at = fsm::start(connection.clone(), request, fsm::RequestCounters::default());
        let connected = at
            .next()
            .await
            .map_err(|error| self.classify(error.into(), connection))?;
        let fsm::ConnectedNext::StartRoot(root) = connected
            .next()
            .await
            .map_err(|error| self.classify(error.into(), connection))?
        else {
            // Both remaining variants land here and they are different facts:
            // `StartChild` is child content, `Closing` is iroh-blobs deciding
            // the request selected nothing. Naming only the first would send a
            // reader of the second to look in the wrong place.
            return Err(Error::Io(std::io::Error::other(format!(
                "blob {} root-only request did not start at the root",
                self.id()
            ))));
        };
        root.next()
            .next()
            .await
            .map_err(|error| self.classify(error.into(), connection))
    }

    /// Stream the verified bytes into `sink`.
    ///
    /// No local store, so no deduplication and no resume across a restart. Peak
    /// memory is one chunk. One extra round trip compared with a bare fetch,
    /// because it goes through [`open`](Self::open) and therefore pays for the
    /// size preflight — one more stream on the same connection. This is the
    /// direct replacement for [`BodyReader::copy_to`](crate::BodyReader::copy_to).
    ///
    /// # Errors
    ///
    /// The same set as [`open`](Self::open), plus [`Error::Io`] when the sink
    /// fails.
    pub async fn copy_to<W: AsyncWrite + Unpin>(&self, sink: &mut W) -> Result<u64> {
        self.open().await?.copy_to(sink).await
    }

    /// Fetch into a local store everything this handle grants that `store`
    /// lacks. Returns the payload bytes that crossed the wire.
    ///
    /// Deduplication and resume come from the store: an interrupted transfer
    /// continues where it stopped, and content already present costs nothing,
    /// which is zero for a blob the store already held. Measured by
    /// `interrupted_fetch_into_resumes_from_store_progress`: after an answer
    /// cut in half, the second fetch read 37232 of 70000 bytes, so the two
    /// whole 16 KiB blocks the store had already validated were kept.
    ///
    /// # A closure is fetched in phases, on one dial
    ///
    /// First the root alone, which costs no request when the store already holds
    /// it, and which is checked for whole addresses before it is downloaded.
    /// Then the members, and
    /// only the members the store lacks, 4096 of them per request: each window
    /// reads its slice of the root, asks the store which of those members it
    /// holds in full, and requests the rest, so a window keeps both its plan and
    /// its request small however long the closure is. A member the store holds
    /// only in part is requested whole again. Fetching the sequence in one
    /// request would send the whole closure when the root is absent, because
    /// the fetcher cannot know which members it holds until it holds the list.
    /// The holder's grant is its claim that it has the closure.
    ///
    /// # It does not inherit the reader's size guarantee, on purpose
    ///
    /// A blob goes through iroh-blobs' own `Remote::fetch`, which decides what is
    /// missing and asks for exactly that, and a closure's members through
    /// requests this crate builds from what the store lacks. Neither makes the
    /// [`verified_len`](Self::verified_len) preflight and comparison the reader
    /// does: there is no number handed back to announce. The content is
    /// validated against its address as it lands in the store, and the return
    /// value describes bytes that already arrived rather than bytes to expect.
    /// The one size check a closure makes is its root's, before it is
    /// downloaded, and the last chunk that check carries is counted in the
    /// return value. Reach for [`open`](Self::open) when a length has to be
    /// trusted before the transfer.
    ///
    /// # Errors
    ///
    /// The same set as [`copy_to`](Self::copy_to).
    pub async fn fetch_into(&self, store: &Blobs) -> Result<u64> {
        let connection = self.dial().await?;
        let (mut bytes, members) = match self.content {
            Content::Blob(_) => (self.fetch_root(&connection, store).await?, 0),
            Content::Closure(_) => self.ensure_root(&connection, store).await?,
        };
        // The members, a window of them per request. The root is complete in
        // the store by now. Each window reads only its own 4096 addresses off
        // it, asks the store which of them it lacks, and requests only those.
        // iroh-blobs' planner (`Remote::local_for_request`) is not used here,
        // because it indexes every member of the root before it looks at the
        // window, which would repeat the whole-root work for every window.
        //
        // A member the store holds only in part is requested whole again: the
        // question asked is whether it is held in full, and what is already
        // there is verified and rewritten rather than skipped.
        let remote = store.store().remote();
        let mut start = 0;
        while start < members {
            let end = (start + FETCH_WINDOW).min(members);
            let listed = store.listed(self.id(), start..end).await?;
            // Only the first occurrence of a hash that is lacking is requested:
            // a sequence may list the same blob twice, and one transfer fills
            // both places. The sequence itself is untouched, so order and
            // duplicates stay; only the request is deduplicated.
            let mut asked = std::collections::HashSet::new();
            let mut lacking = Vec::with_capacity(listed.len());
            for member in &listed {
                lacking.push(!store.has(*member).await? && asked.insert(*member));
            }
            if lacking.contains(&true) {
                let stats = remote
                    .execute_get(
                        connection.clone(),
                        missing_request(self.id(), start, &lacking)?,
                    )
                    .await
                    .map_err(|error| self.classify(error, &connection))?;
                bytes += stats.payload_bytes_read;
            }
            start = end;
        }
        Ok(bytes)
    }

    /// The root alone, as a raw blob.
    async fn fetch_root(&self, connection: &Connection, store: &Blobs) -> Result<u64> {
        self.fetch_with(connection, store, HashAndFormat::raw(self.id().hash()))
            .await
    }

    /// Make sure `store` holds this closure's root, and say how many members it
    /// lists and how many payload bytes it cost to get it.
    ///
    /// The root is checked for whole addresses **before it is downloaded**, from
    /// the size the holder proves with the last chunk. A root the store already
    /// holds is checked the same way, so a closure whose list is not a sequence
    /// fails here and not as a fetch that succeeds. There is no limit on how
    /// many members it lists: each window of them is planned from its own slice
    /// of the root.
    async fn ensure_root(&self, connection: &Connection, store: &Blobs) -> Result<(u64, u64)> {
        if let Some(members) = store.members(self.id()).await? {
            return Ok((0, members));
        }
        let (size, proof) = get_verified_size(connection, &self.id().hash())
            .await
            .map_err(|error| self.classify(error, connection))?;
        count_members(self.id(), size)?;
        // The proof is payload that crossed the wire too: the root's last chunk,
        // which the download below sends again.
        let bytes = proof.payload_bytes_read + self.fetch_root(connection, store).await?;
        let members = store.members(self.id()).await?.ok_or_else(|| {
            Error::Io(std::io::Error::other(format!(
                "closure root {} did not land in the store",
                self.id()
            )))
        })?;
        Ok((bytes, members))
    }

    async fn fetch_with(
        &self,
        connection: &Connection,
        store: &Blobs,
        what: HashAndFormat,
    ) -> Result<u64> {
        let stats = store
            .store()
            .remote()
            .fetch(connection.clone(), what)
            .await
            .map_err(|error| self.classify(error, connection))?;
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
        classify(self.id(), error, connection)
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
                return Error::BlobRefused { id: self.id() };
            }
        }
        Error::Transport(error.into())
    }
}

// ─────────────────────────────────────────────────────── the verified reader

/// Turns verified leaves into the selected bytes, in order.
///
/// # Two counters, and they are not the same number
///
/// `next` is the RAW protocol offset the following leaf must carry. Leaves
/// arrive on 16 KiB block boundaries whatever byte range was asked for
/// (`IROH_BLOCK_SIZE` is `BlockSize::from_chunk_log(4)`), so for an unaligned
/// range the first one begins BEFORE the requested bytes and the last one ends
/// after them. `emitted` counts only what the caller received.
///
/// Checking a leaf offset against `emitted` reads as equivalent and is off by
/// the head overhang on the very first leaf. Keeping the two apart is the whole
/// reason this is a type rather than two locals in a loop.
#[derive(Debug)]
struct Selection {
    id: BlobId,
    start: u64,
    len: u64,
    next: Option<u64>,
    emitted: u64,
    /// Whether this read already failed.
    ///
    /// `finish` without it is a completeness test and nothing more, so a
    /// failure arriving once every selected byte is already out satisfies it
    /// and the next call reads as a clean end. Unreachable over the network
    /// today — the decoder stops at the last selected block, so nothing follows
    /// it to go wrong — and one read-ahead or one multi-range request away from
    /// being reachable.
    broken: bool,
}

impl Selection {
    /// `len` bytes from `start`.
    ///
    /// Taking a length rather than a `Range` is what makes a reversed
    /// selection unrepresentable: every caller has already checked the range
    /// against the verified size, and there is no subtraction left to underflow.
    fn new(id: BlobId, start: u64, len: u64) -> Self {
        Self {
            id,
            start,
            len,
            next: None,
            emitted: 0,
            broken: false,
        }
    }

    /// Record that this read failed somewhere the selection could not see.
    fn broke(&mut self) {
        self.broken = true;
    }

    fn end(&self) -> u64 {
        self.start + self.len
    }

    /// The part of one verified leaf the caller asked for, if any.
    ///
    /// # Errors
    ///
    /// [`Error::Io`] when a leaf does not continue where the last one ended.
    /// RFC-01 §3.3 flagged leaf ordering as unverified: the iroh-blobs example
    /// writes leaves in arrival order and expects correct output, which ASSUMES
    /// offset order. A sink is not seekable, so the assumption cannot be worked
    /// around — only checked, once per leaf, so a change in the bao-tree
    /// traversal is a loud failure instead of a file that is silently scrambled.
    fn leaf(&mut self, offset: u64, data: Bytes) -> Result<Option<Bytes>> {
        if let Some(expected) = self.next {
            if offset != expected {
                self.broken = true;
                return Err(Error::BlobUnverified {
                    id: self.id,
                    fault: BlobFault::Order { offset, expected },
                });
            }
        }
        let stop = offset.saturating_add(data.len() as u64);
        self.next = Some(stop);

        let head = self.start.max(offset);
        let tail = self.end().min(stop);
        if head >= tail {
            return Ok(None);
        }
        self.emitted += tail - head;
        Ok(Some(
            data.slice((head - offset) as usize..(tail - offset) as usize),
        ))
    }

    /// # Errors
    ///
    /// [`Error::Io`] when fewer bytes were emitted than the selection names.
    /// A transfer that stopped early is never reported as a complete one, the
    /// same call [`Error::BodyTruncated`] makes.
    fn finish(&self) -> Result<()> {
        if self.broken {
            return Err(Error::BlobUnverified {
                id: self.id,
                fault: BlobFault::Aborted {
                    received: self.emitted,
                },
            });
        }
        if self.emitted == self.len {
            return Ok(());
        }
        Err(Error::BlobUnverified {
            id: self.id,
            fault: BlobFault::Incomplete {
                received: self.emitted,
                selected: self.len,
            },
        })
    }
}

/// The get state, or the fact that there is none left.
///
/// Two states and not three, deliberately. "This read broke" lives in
/// [`Selection`], because that is where a test can reach it: a reader can only
/// be driven by a real holder, and no holder can produce a failure after the
/// last selected byte — the decoder stops there. A third state here would be
/// defence in depth nobody could watch fail.
#[derive(Debug)]
enum State {
    Reading(fsm::AtBlobContent),
    Ended,
}

/// One verified read of a blob, or of one byte range of it.
///
/// # Dropping it cancels the transfer
///
/// The reader owns the connection **and** the get state. quinn sends
/// `STOP_SENDING` when a receive stream with unread data is dropped, so
/// abandoning a reader stops the holder rather than letting it finish into a
/// buffer nobody reads. A reader that kept only the `Connection` and let the
/// stream state go early would compile, read correctly, and cancel nothing.
///
/// # The end of the selection is the end of the read
///
/// [`next_chunk`](Self::next_chunk) stops at the last selected byte and does
/// not walk iroh-blobs' closing state, deliberately. `AtClosing::next` waits
/// for the holder to finish the stream and only *logs* trailing data rather
/// than rejecting it, so running it would hand a holder that sends valid
/// content and then keeps the stream open a way to park the reader with no
/// timeout to stop it.
///
/// The cost is that a completed read still drops an unfinished receive stream,
/// so the holder sees `STOP_SENDING` after a normal read as well as after an
/// abandoned one. Nothing here reads that signal, and neither should a caller:
/// completion is `Ok(None)` from `next_chunk`, which fires only once every
/// selected byte arrived and was validated.
pub struct BlobReader {
    /// Held so a refusal stays distinguishable from a network fault after the
    /// handle is gone, and so the connection outlives the read.
    connection: Connection,
    total: u64,
    take: Selection,
    state: State,
}

/// Written by hand because the derived form prints the whole quinn connection
/// and the bao decoder state, which is a page of transport internals in every
/// assertion message a test writes about a reader.
impl std::fmt::Debug for BlobReader {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BlobReader")
            .field("id", &self.take.id)
            .field("total_len", &self.total)
            .field("selected_len", &self.take.len)
            .field("emitted", &self.take.emitted)
            .finish()
    }
}

impl BlobReader {
    /// The verified size of the whole blob.
    ///
    /// Safe to announce as an HTTP `Content-Length`, and that is a claim about
    /// the NUMBER rather than about the bytes. Two requests agreed on it and
    /// the content address vouched for the last chunk, so no holder can later
    /// deliver a different length and be believed.
    ///
    /// It is not a promise that the holder can deliver them. iroh-blobs' own
    /// probe "does not validate that the peer has all the data", so a holder
    /// with a hole in the middle passes both checks and fails during the read.
    /// A gateway that has already sent this as a `Content-Length` then has a
    /// broken body and nothing else to report with.
    #[must_use]
    pub fn total_len(&self) -> u64 {
        self.total
    }

    /// The bytes this reader will yield. Equal to [`total_len`](Self::total_len)
    /// for a whole-blob read.
    #[must_use]
    pub fn selected_len(&self) -> u64 {
        self.take.len
    }

    /// The next verified slice, or `None` once the selection is complete.
    ///
    /// # Errors
    ///
    /// [`Error::BlobRefused`] when the holder denies the transfer part way, and
    /// [`Error::Io`] when a leaf fails verification, arrives out of order, or
    /// the stream ends before every selected byte arrived.
    pub async fn next_chunk(&mut self) -> Result<Option<Bytes>> {
        loop {
            // Taking the state before the `await` is what stops a failed reader
            // continuing a transfer it has already reported broken. What stops
            // it reporting a CLEAN END is `Selection::broke`, not this line.
            match std::mem::replace(&mut self.state, State::Ended) {
                State::Ended => return self.take.finish().map(|()| None),
                State::Reading(content) => match content.next().await {
                    fsm::BlobContentNext::More((next, item)) => {
                        let item = item.map_err(|error| {
                            // `Selection` remembers, so the terminal call above
                            // reports the failure instead of a clean end.
                            self.take.broke();
                            classify(self.take.id, error.into(), &self.connection)
                        })?;
                        // A hash-tree node, not payload. Verification is
                        // bao-tree's job and has already happened by the time an
                        // item arrives.
                        let slice = match item {
                            BaoContentItem::Leaf(leaf) => self.take.leaf(leaf.offset, leaf.data)?,
                            BaoContentItem::Parent(_) => None,
                        };
                        self.state = State::Reading(next);
                        if let Some(bytes) = slice {
                            return Ok(Some(bytes));
                        }
                    }
                    fsm::BlobContentNext::Done(_) => {
                        self.take.finish()?;
                        self.state = State::Ended;
                        return Ok(None);
                    }
                },
            }
        }
    }

    /// Stream every selected byte into `sink`.
    ///
    /// Consuming, because a read is linear: there is no second pass over a
    /// reader that has already been drained.
    ///
    /// # Errors
    ///
    /// The same set as [`next_chunk`](Self::next_chunk), plus [`Error::Io`]
    /// when the sink fails.
    pub async fn copy_to<W: AsyncWrite + Unpin>(mut self, sink: &mut W) -> Result<u64> {
        let mut written = 0;
        while let Some(chunk) = self.next_chunk().await? {
            sink.write_all(&chunk).await?;
            written += chunk.len() as u64;
        }
        sink.flush().await?;
        Ok(written)
    }
}

/// The one implementation of [`BlobHandle::classify`], shared with
/// [`BlobReader`], which meets the same two refusal shapes after the handle it
/// came from is gone.
fn classify(id: BlobId, error: GetError, connection: &Connection) -> Error {
    if error.iroh_error_code() == Some(ERR_PERMISSION) || refused(connection) {
        return Error::BlobRefused { id };
    }
    Error::Io(std::io::Error::other(error.to_string()))
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

    /// A grant as dispatch records it: reserved, then committed.
    fn grant(grants: &Arc<Grants>, seed: u8, content: impl Into<Content>) {
        reserve(grants, seed, content)
            .expect("within budget")
            .commit();
    }

    fn reserve_under(
        grants: &Arc<Grants>,
        seed: u8,
        content: impl Into<Content>,
        fence: &Fence,
    ) -> std::result::Result<Permit, NotGranted> {
        grants.reserve(&admitted(seed), content.into(), fence)
    }

    /// A reservation as dispatch makes it: under a fence taken at dispatch.
    fn reserve(
        grants: &Arc<Grants>,
        seed: u8,
        content: impl Into<Content>,
    ) -> std::result::Result<Permit, NotGranted> {
        let fence = grants.fence(&admitted(seed), || true).expect("open");
        grants.reserve(&admitted(seed), content.into(), &fence)
    }

    fn scope(grants: &Grants, seed: u8, root: BlobId) -> Option<Scope> {
        grants.scope(&peer(seed), &root)
    }

    fn seq(seed: u8) -> SeqId {
        SeqId(blob(seed))
    }

    /// Records the hang-ups it is asked for, in place of a quinn connection.
    #[derive(Debug, Default)]
    struct Hung(std::sync::atomic::AtomicUsize);

    impl Hangup for Hung {
        fn hang_up(&self) {
            self.0.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }
    }

    fn hung(live: &Connections, id: u64, seed: u8) -> Arc<Hung> {
        let handle = Arc::new(Hung::default());
        live.admit(id, peer(seed), handle.clone());
        handle
    }

    fn hangups(handle: &Hung) -> usize {
        handle.0.load(std::sync::atomic::Ordering::Relaxed)
    }

    #[test]
    fn a_grant_admits_exactly_the_content_it_names() {
        let grants = Grants::new();
        grant(&grants, 1, blob(7));

        assert!(grants.knows(&peer(1)));
        assert_eq!(scope(&grants, 1, blob(7)), Some(Scope::Blob));
        assert_eq!(scope(&grants, 1, blob(8)), None);
        assert_eq!(scope(&grants, 2, blob(7)), None);
        assert!(!grants.knows(&peer(2)));
    }

    /// The old ledger kept 4096 grants and evicted the oldest beyond that, so a
    /// peer's capability could vanish without anybody revoking it. 5000 is past
    /// that deleted `MAX_GRANTS`, and every one of them is still held.
    #[test]
    fn grants_past_the_old_cap_of_4096_are_all_kept() {
        let grants = Grants::new();
        for nth in 0..5000u32 {
            let mut root = [0u8; 32];
            root[..4].copy_from_slice(&nth.to_le_bytes());
            grant(&grants, 1, BlobId(root));
        }
        for nth in 0..5000u32 {
            let mut root = [0u8; 32];
            root[..4].copy_from_slice(&nth.to_le_bytes());
            assert_eq!(
                scope(&grants, 1, BlobId(root)),
                Some(Scope::Blob),
                "grant {nth} was evicted"
            );
        }
    }

    #[test]
    fn a_closure_costs_one_grant_whatever_it_lists() {
        let grants = Grants::new();
        grant(&grants, 1, seq(5));

        let book = grants.lock();
        assert_eq!(book.grants, 1);
        assert_eq!(book.by_peer.map.len(), 1);
        assert_eq!(
            book.by_peer.map.values().next().map(|held| held.map.len()),
            Some(1)
        );
    }

    /// The limit refuses, and refusing evicts nothing.
    ///
    /// Watched red with `scripts/break-once.sh` (row `HK52`) by replacing
    /// `if book.grants >= book.limit {`
    /// with `if false {`: the third `reserve` is then granted and the test
    /// panics at `the third grant does not fit`.
    #[test]
    fn a_grant_past_the_limit_is_refused_and_nothing_is_evicted() {
        let grants = Grants::with_limit(2);
        grant(&grants, 1, blob(1));
        grant(&grants, 2, blob(2));

        let refused = reserve(&grants, 1, blob(3)).expect_err("the third grant does not fit");
        assert_eq!(refused, NotGranted::Exhausted { limit: 2 });
        assert_eq!(scope(&grants, 1, blob(1)), Some(Scope::Blob));
        assert_eq!(scope(&grants, 2, blob(2)), Some(Scope::Blob));
        assert_eq!(scope(&grants, 1, blob(3)), None);
        assert!(
            !grants.lock().by_peer.map[&admitted(1).id()].contains_key(&blob(3)),
            "a refused reservation leaves no entry behind"
        );
        assert_eq!(grants.lock().grants, 2, "and does not count");

        // A peer's second grant counts like anyone's, and a revoke frees a place.
        assert!(grants.revoke(admitted(2).id(), blob(2)));
        grant(&grants, 1, blob(3));
        assert_eq!(grants.lock().grants, 2);
    }

    #[test]
    fn a_repeat_or_widening_grant_costs_nothing() {
        let grants = Grants::new();
        grant(&grants, 1, blob(4));
        let held = grants.lock().grants;

        grant(&grants, 1, blob(4));
        assert_eq!(grants.lock().grants, held, "a repeat is free");
        assert_eq!(scope(&grants, 1, blob(4)), Some(Scope::Blob));

        grant(&grants, 1, seq(4));
        assert_eq!(grants.lock().grants, held, "a widening is free");
        assert_eq!(scope(&grants, 1, blob(4)), Some(Scope::Closure));

        grant(&grants, 1, blob(4));
        assert_eq!(
            scope(&grants, 1, blob(4)),
            Some(Scope::Closure),
            "a narrower repeat does not narrow it"
        );
    }

    /// A revoke wins over a grant still being written, and a permit from before
    /// a revoke cannot make a later reservation live.
    ///
    /// Watched red by editing `Permit::commit`, whose `.filter(..)` line is the
    /// same text as the one in `Permit::drop`, so no row can quote it: replace
    /// the commit's `.filter(|entry| entry.generation == self.generation)` with
    /// `.filter(|_| true)`. The assertion `a stale permit does not flip
    /// somebody else's entry` fails.
    #[test]
    fn a_revoke_while_a_grant_is_pending_wins() {
        let grants = Grants::new();
        let pending = reserve(&grants, 1, blob(1)).expect("fits");
        assert!(
            !grants.revoke(admitted(1).id(), blob(1)),
            "a reserved grant is not a held one"
        );
        pending.commit();
        assert_eq!(scope(&grants, 1, blob(1)), None, "the revoke won");

        // The generation: revoke, then another reservation takes the pair, then
        // the STALE permit commits. It must not make the new entry live.
        let stale = reserve(&grants, 1, blob(2)).expect("fits");
        grants.revoke(admitted(1).id(), blob(2));
        let fresh = reserve(&grants, 1, blob(2)).expect("fits");
        stale.commit();
        assert_eq!(
            scope(&grants, 1, blob(2)),
            None,
            "a stale permit does not flip somebody else's entry"
        );
        fresh.commit();
        assert_eq!(scope(&grants, 1, blob(2)), Some(Scope::Blob));
    }

    /// Review finding 1. A pending closure must not become live because some
    /// other reservation for the same pair committed.
    ///
    /// Watched red with `scripts/break-once.sh` (row `HK54`) by replacing
    /// `entry.committed = entry.committed.max(Some(self.scope));`
    /// with `entry.committed = Some(Scope::Closure);`: the blob commit then
    /// leaves a closure behind and the first assertion fails.
    #[test]
    fn a_blob_commit_does_not_activate_a_pending_closure() {
        let grants = Grants::new();
        let closure = reserve(&grants, 1, seq(1)).expect("fits");
        let blob_only = reserve(&grants, 1, blob(1)).expect("fits");
        blob_only.commit();
        assert_eq!(
            scope(&grants, 1, blob(1)),
            Some(Scope::Blob),
            "only the blob grant committed"
        );

        // The closure's reply is never written, so its permit drops.
        drop(closure);
        assert_eq!(
            scope(&grants, 1, blob(1)),
            Some(Scope::Blob),
            "and a dropped closure reservation leaves it a blob"
        );

        // The control: a closure that does commit widens it.
        let widening = reserve(&grants, 1, seq(1)).expect("fits");
        widening.commit();
        assert_eq!(scope(&grants, 1, blob(1)), Some(Scope::Closure));
    }

    /// Review finding 5. The recipe is "remove the peer from admission, then
    /// `revoke_peer`", and a handler already running when it lands has no entry
    /// for the revoke to remove: it would reserve and commit afterwards.
    ///
    /// Watched red with `scripts/break-once.sh` (row `HK55`) by replacing
    /// `flight.epoch += 1;`
    /// with `flight.epoch += 0;`: the late reservation is then granted and the
    /// first assertion fails.
    #[test]
    fn a_peer_revoke_defeats_a_grant_from_a_call_dispatched_before_it() {
        let grants = Grants::new();
        let early = grants.fence(&admitted(1), || true).expect("open");
        let other = grants.fence(&admitted(2), || true).expect("open");
        assert_eq!(grants.revoke_peer(admitted(1).id()), 0, "nothing held yet");

        assert_eq!(
            grants
                .reserve(&admitted(1), blob(1).into(), &early)
                .expect_err("the call predates the revoke"),
            NotGranted::Withdrawn
        );
        assert!(!grants.knows(&peer(1)), "and nothing was left behind");
        assert_eq!(grants.lock().grants, 0);

        grants
            .reserve(&admitted(2), blob(1).into(), &other)
            .expect("another peer's call is not affected")
            .commit();

        // A call dispatched after the revoke is a new admission decision.
        let late = grants.fence(&admitted(1), || true).expect("open");
        grants
            .reserve(&admitted(1), blob(1).into(), &late)
            .expect("dispatched after")
            .commit();
        assert_eq!(scope(&grants, 1, blob(1)), Some(Scope::Blob));

        drop((early, other, late));
        assert!(
            grants.lock().inflight.is_empty() && grants.lock().inflight.high == 0,
            "a finished call leaves nothing in the fence table"
        );
    }

    fn root(nth: u32) -> BlobId {
        let mut bytes = [0u8; 32];
        bytes[..4].copy_from_slice(&nth.to_le_bytes());
        BlobId(bytes)
    }

    /// Counts the bytes this thread has allocated and not yet freed, so the
    /// sparse shape (one grant per peer) can be measured here, where peers can be
    /// minted without a connection each. The dense shape, over real peers, is
    /// measured by `tests/grant_allocation.rs`.
    mod heap {
        #![allow(unsafe_code)]

        use std::alloc::{GlobalAlloc, Layout, System};
        use std::cell::Cell;

        thread_local! {
            static LIVE: Cell<isize> = const { Cell::new(0) };
        }

        pub(super) struct Counting;

        // SAFETY: every call is delegated unchanged to `System`; the counter is a
        // plain thread-local cell with no destructor and no allocation of its own.
        unsafe impl GlobalAlloc for Counting {
            unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
                // SAFETY: the layout is passed through unchanged.
                let pointer = unsafe { System.alloc(layout) };
                if !pointer.is_null() {
                    let _ = LIVE.try_with(|live| live.set(live.get() + layout.size() as isize));
                }
                pointer
            }

            unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
                let _ = LIVE.try_with(|live| live.set(live.get() - layout.size() as isize));
                // SAFETY: `pointer` and `layout` came from the allocation above.
                unsafe { System.dealloc(pointer, layout) }
            }
        }

        /// Net bytes this thread has allocated.
        pub(super) fn live() -> isize {
            LIVE.with(Cell::get)
        }
    }

    #[global_allocator]
    static HEAP: heap::Counting = heap::Counting;

    /// The sparse shape: every peer holds one grant, so each grant also pays for
    /// a peer slot in the outer table and a table of its own. N peers are made
    /// before the measurement starts, so only the ledger is counted.
    ///
    /// Watched red with `scripts/break-once.sh` by replacing the ceiling's `450.0`
    /// below with `190.0`, less than half the measured figure: the run fails with
    /// `1000 peers with one grant each: 386.3 bytes per grant is over 190`.
    #[test]
    fn a_ledger_of_one_grant_per_peer_costs_a_bounded_number_of_bytes_per_grant() {
        for count in [1_000u64, 100_000] {
            let peers: Vec<AdmittedPeer> = (0..count)
                .map(|seed| {
                    crate::Allowlist::anyone()
                        .admit(ProvenPeer::for_tests_wide(seed))
                        .expect("anyone admits everyone")
                })
                .collect();
            let grants = Grants::new();
            let before = heap::live();
            for (nth, peer) in (0u32..).zip(&peers) {
                let fence = grants.fence(peer, || true).expect("open");
                grants
                    .reserve(peer, root(nth).into(), &fence)
                    .expect("within the limit")
                    .commit();
            }
            let per = (heap::live() - before) as f64 / count as f64;
            println!(
                "grant_allocation: one grant per peer, {count} peers: {per:.1} bytes per grant"
            );
            assert!(
                per <= SPARSE_CEILING,
                "{count} peers with one grant each: {per:.1} bytes per grant is over {SPARSE_CEILING}"
            );
        }
    }

    /// A table of 4 buckets per peer plus its slot in the peers' table: measured
    /// at 386.3 bytes per grant for 1,000 peers and 320.7 for 100,000, on aarch64
    /// macOS. The ceiling leaves room above the larger.
    const SPARSE_CEILING: f64 = 450.0;

    /// A table is rebuilt when its length falls under half the most entries it
    /// has held since it was last rebuilt, and not before. What that does to the
    /// bytes a ledger retains is measured by `tests/grant_allocation.rs`, which
    /// counts the heap; this pins the rule itself.
    #[test]
    fn a_table_is_rebuilt_when_it_falls_under_half_its_high_water_mark() {
        let grants = Grants::new();
        for nth in 0..400 {
            grant(&grants, 1, root(nth));
        }
        let mark = |grants: &Arc<Grants>| grants.lock().by_peer.map[&admitted(1).id()].high;
        assert_eq!(mark(&grants), 400);

        // Down to exactly half: not under it.
        for nth in 0..200 {
            assert!(grants.revoke(admitted(1).id(), root(nth)));
        }
        assert_eq!(mark(&grants), 400, "half is not under half");

        // One more, and the table is rebuilt at its new length.
        assert!(grants.revoke(admitted(1).id(), root(200)));
        assert_eq!(mark(&grants), 199);

        // Growing again raises the mark from there.
        for nth in 1000..1100 {
            grant(&grants, 1, root(nth));
        }
        assert_eq!(mark(&grants), 299);
    }

    /// Second-round review finding 1. Admission is decided once per connection
    /// and copied into every call, so a call accepted before a peer was removed
    /// can still be on its way to its fence when the removal and `revoke_peer`
    /// both land, and would read the new epoch as if it were admitted after.
    /// The fence is taken first and the connection is asked second, so one of the
    /// two always notices.
    ///
    /// Watched red with `scripts/break-once.sh`, with a search literal of two
    /// lines that a registry row cannot quote (the lines are `let fence =
    /// self.take(peer);` and `if open() {`), replaced by `let opened = open();`,
    /// `let fence = self.take(peer);` and `if opened {`: the barrier case then
    /// hands out a fresh fence, the reservation is granted, and the assertion
    /// `the fence predates the revoke` panics.
    #[test]
    fn a_call_that_reaches_its_fence_after_a_peer_revoke_cannot_grant() {
        let grants = Grants::new();

        // The barrier: the revoke lands between taking the fence and asking
        // whether the connection is open, which is the instant a call accepted
        // under the old admission is deciding what it is.
        let late = grants
            .fence(&admitted(1), || {
                grants.revoke_peer(admitted(1).id());
                true
            })
            .expect("the connection was still open when it was asked");
        assert_eq!(
            grants
                .reserve(&admitted(1), blob(1).into(), &late)
                .expect_err("the fence predates the revoke"),
            NotGranted::Withdrawn
        );
        drop(late);

        // And one that reaches its fence after the revoke finds the connection
        // closed, which is the first step of removing a peer.
        assert_eq!(
            grants
                .fence(&admitted(1), || false)
                .expect_err("closed before it was fenced"),
            NotGranted::Withdrawn
        );
        assert!(
            grants.lock().inflight.is_empty(),
            "a refused call leaves nothing in the fence table"
        );

        // The control: a call on an open connection is fenced and may grant.
        let fence = grants.fence(&admitted(1), || true).expect("open");
        reserve_under(&grants, 1, blob(1), &fence)
            .expect("an open connection grants")
            .commit();
        assert_eq!(scope(&grants, 1, blob(1)), Some(Scope::Blob));
    }

    /// A reply that was never written grants nothing and costs nothing.
    ///
    /// Watched red with `scripts/break-once.sh` (row `HK53`) by replacing
    /// `if entry.committed.is_none() && entry.pending == 0 {`
    /// with `if false {`: the dropped reservation stays counted and the
    /// `assert_eq!(grants.lock().grants, 0)` fails.
    #[test]
    fn an_uncommitted_permit_releases_its_charge() {
        let grants = Grants::new();
        let permit = reserve(&grants, 1, blob(1)).expect("fits");
        assert!(grants.knows(&peer(1)), "a pending peer passes the guard");
        drop(permit);

        assert_eq!(grants.lock().grants, 0);
        assert!(!grants.knows(&peer(1)));
    }

    #[test]
    fn a_peer_whose_last_grant_is_revoked_is_no_longer_known() {
        let grants = Grants::new();
        grant(&grants, 1, blob(1));
        grant(&grants, 1, blob(2));
        grant(&grants, 2, blob(1));

        assert!(grants.revoke(admitted(1).id(), blob(1)));
        assert!(grants.knows(&peer(1)), "it still holds blob 2");
        assert!(grants.revoke(admitted(1).id(), blob(2)));

        // `knows` gates connection admission. A peer left behind with an empty
        // table would keep connecting while holding nothing.
        assert!(!grants.knows(&peer(1)));
        assert!(grants.knows(&peer(2)));
        assert_eq!(grants.lock().grants, 1);
        assert!(!grants.revoke(admitted(1).id(), blob(2)), "already gone");
    }

    #[test]
    fn revoke_everywhere_removes_that_root_from_every_peer_and_nothing_else() {
        let grants = Grants::new();
        grant(&grants, 1, blob(1));
        grant(&grants, 1, blob(2));
        grant(&grants, 2, blob(1));
        grant(&grants, 3, blob(3));

        assert_eq!(grants.revoke_everywhere(blob(1)), 2);

        assert_eq!(scope(&grants, 1, blob(1)), None);
        assert_eq!(scope(&grants, 2, blob(1)), None);
        assert_eq!(scope(&grants, 1, blob(2)), Some(Scope::Blob));
        assert_eq!(scope(&grants, 3, blob(3)), Some(Scope::Blob));
        assert!(!grants.knows(&peer(2)), "its only grant went");
        assert_eq!(grants.lock().grants, 2);
    }

    #[test]
    fn revoking_a_peer_withdraws_its_grants_and_hangs_up_its_connections() {
        let grants = Grants::new();
        let live = Connections::default();
        grant(&grants, 1, blob(1));
        grant(&grants, 1, seq(2));
        grant(&grants, 2, blob(1));
        let first = hung(&live, 1, 1);
        let second = hung(&live, 2, 1);
        let other = hung(&live, 3, 2);

        assert_eq!(grants.revoke_peer(admitted(1).id()), 2);
        assert_eq!(live.close_peer(admitted(1).id()), 2);

        assert!(!grants.knows(&peer(1)));
        assert_eq!(hangups(&first), 1);
        assert_eq!(hangups(&second), 1);
        assert_eq!(
            hangups(&other),
            0,
            "another peer's connection is left alone"
        );
        assert_eq!(scope(&grants, 2, blob(1)), Some(Scope::Blob));
        assert_eq!(grants.lock().grants, 1);
    }

    /// Review finding 8. An accept that has checked the ledger but not yet
    /// registered its connection is invisible to `revoke_peer`'s sweep, so the
    /// check has to come after the registration.
    ///
    /// Watched red with `scripts/break-once.sh` (row `HK59`) by replacing
    /// `if grants.knows(&peer) {`
    /// with `if true {`: the connection of a peer that was revoked before it
    /// registered is then kept.
    #[test]
    fn a_connection_registered_after_a_peer_revoke_is_hung_up() {
        let grants = Grants::new();
        let live = Connections::default();
        grant(&grants, 1, blob(1));

        // The control: a known peer is kept, and left alone.
        let kept = Arc::new(Hung::default());
        assert!(live.admit_known(&grants, 1, peer(1), kept.clone()));
        assert_eq!(live.peer(1), Some(peer(1)));
        assert_eq!(hangups(&kept), 0);

        // The revoke and its sweep both run while a second accept is between
        // "the peer is known" and "the connection is registered".
        grants.revoke_peer(admitted(1).id());
        live.close_peer(admitted(1).id());
        assert_eq!(hangups(&kept), 1, "the sweep closed the one it could see");

        let late = Arc::new(Hung::default());
        assert!(!live.admit_known(&grants, 2, peer(1), late.clone()));
        assert_eq!(hangups(&late), 1, "the late connection is hung up too");
        assert_eq!(live.peer(2), None, "and not left registered");
    }

    #[test]
    fn a_connection_is_forgotten_when_it_ends() {
        let live = Connections::default();
        hung(&live, 4, 1);
        assert_eq!(live.peer(4), Some(peer(1)));

        live.forget(4, peer(1));
        assert_eq!(live.peer(4), None);

        // The id is a slab index and quinn reuses one. A late forget from the
        // connection that USED to hold it must not evict the one that holds it
        // now.
        hung(&live, 4, 2);
        live.forget(4, peer(1));
        assert_eq!(live.peer(4), Some(peer(2)), "a stale forget is ignored");
    }

    /// The router drops handler futures on shutdown, so the line after the
    /// `await` in `BlobsAdmission::accept` is not guaranteed to run. Every
    /// cancelled accept would otherwise leave one entry behind for good.
    #[test]
    fn an_admitted_connection_is_forgotten_however_its_accept_ends() {
        let live = Connections::default();
        hung(&live, 7, 1);
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
        hung(&live, 1, 1);

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
        grant(&grants, 1, blob(1));
        // Deliberately NOT admitted: `live` is empty. The grant exists, so the
        // refusal can only come from the connection lookup.
        assert!(admits_get(&grants, &live, &get(1, blob(1), whole_blob())).is_err());

        hung(&live, 1, 1);
        assert!(
            admits_get(&grants, &live, &get(1, blob(1), whole_blob())).is_ok(),
            "control: the same request is admitted once the connection is known"
        );
    }

    /// Range reads must not have widened admission.
    ///
    /// keylink's own client now sends two request shapes it never sent before:
    /// the last-chunk size preflight and an arbitrary byte range. Both are
    /// root-only, so `is_blob()` admits them and the hash-sequence hole stays
    /// shut. Had it not, the obvious repair would have been to relax the
    /// check — which is the change this row exists to make loud.
    ///
    /// Watched red with `scripts/break-once.sh` (row `HK6`) by replacing
    /// `if msg.request.ranges.is_blob() {`
    /// with `if true {`.
    #[test]
    fn blob_admission_accepts_only_finite_root_ranges() {
        let grants = Grants::new();
        let live = Connections::default();
        grant(&grants, 1, blob(1));
        hung(&live, 1, 1);
        let admits = |ranges| admits_get(&grants, &live, &get(1, blob(1), ranges)).is_ok();

        assert!(admits(whole_blob()), "the whole blob");
        assert!(
            admits(ChunkRangesSeq::from_ranges([ChunkRanges::last_chunk()])),
            "the size preflight `open` and `verified_len` send"
        );
        assert!(
            admits(ChunkRangesSeq::from_ranges([ChunkRanges::bytes(
                1_000..40_000
            )])),
            "an unaligned byte range"
        );
        assert!(
            admits(ChunkRangesSeq::from_ranges([ChunkRanges::bytes(0..1)])),
            "a single byte"
        );

        assert!(
            !admits(ChunkRangesSeq::all()),
            "every child of a hash sequence"
        );
        assert!(
            !admits(ChunkRangesSeq::from_ranges_infinite([ChunkRanges::all()])),
            "an infinite sequence"
        );
        assert!(
            !admits(ChunkRangesSeq::from_ranges([
                ChunkRanges::empty(),
                ChunkRanges::all()
            ])),
            "a selection that skips the root and names a child"
        );
    }

    // ───────────────────────────── the selection, on injected verified items
    //
    // `Selection` is the part of a read with no network in it: leaves arrive
    // already verified by bao, and what is left is ordering, slicing, and
    // knowing when the selection is complete. Feeding it items directly is
    // what makes the two failures below reachable at all — a correct holder
    // cannot produce them, and neither can the forging holder in
    // `tests/blobs.rs`, because a leaf whose offset moved fails bao
    // verification before `Selection` ever sees it. Synchronous, so there is
    // no terminator to prove.

    fn leaf(len: usize) -> Bytes {
        Bytes::from(vec![7u8; len])
    }

    /// Leaves must continue where the last one ended.
    ///
    /// RFC-01 §3.3: a sink is not seekable, so a traversal that stopped
    /// arriving in offset order would silently scramble a file. Watched red
    /// with `scripts/break-once.sh` (row `HK4`) by replacing
    /// `if offset != expected {`
    /// with `if false {`.
    #[test]
    fn blob_reader_rejects_gap_and_out_of_order_offsets() {
        let mut take = Selection::new(blob(1), 0, 40_000);
        assert!(
            take.leaf(0, leaf(16_384)).is_ok(),
            "control: the first leaf is accepted wherever it starts"
        );
        assert!(
            take.leaf(32_768, leaf(16_384)).is_err(),
            "a leaf that skips a whole block is a gap"
        );

        let mut take = Selection::new(blob(1), 0, 40_000);
        take.leaf(0, leaf(16_384)).expect("the first leaf");
        take.leaf(16_384, leaf(16_384)).expect("the second leaf");
        assert!(
            take.leaf(0, leaf(16_384)).is_err(),
            "and one that goes backwards is out of order"
        );
    }

    /// A stream that ends early is a failure, never a shorter success.
    ///
    /// The same call [`Error::BodyTruncated`] makes. Watched red with
    /// `scripts/break-once.sh` (row `HK5`) by replacing
    /// `if self.emitted == self.len {`
    /// with `if true {`.
    #[test]
    fn blob_reader_rejects_short_eof() {
        let mut take = Selection::new(blob(1), 0, 40_000);
        take.leaf(0, leaf(16_384)).expect("the first leaf");
        assert!(
            take.finish().is_err(),
            "16384 of 40000 selected bytes has not completed"
        );

        take.leaf(16_384, leaf(16_384)).expect("the second leaf");
        take.leaf(32_768, leaf(16_384)).expect("the third leaf");
        assert!(
            take.finish().is_ok(),
            "control: the same selection completes once every byte arrived"
        );
    }

    /// A selection that rejected a leaf never reports completion.
    ///
    /// The counterexample is codex's, and it is why `broken` exists: with a
    /// one-byte selection the very first leaf satisfies `emitted == len`, so a
    /// failure AFTER that point leaves a completeness test with nothing to
    /// object to. The reader's terminal call then answers `Ok(None)` and every
    /// layer above reads a clean end off a read that broke.
    ///
    /// Watched red with `scripts/break-once.sh` (row `HK9`) by replacing
    /// `if self.broken {`
    /// with `if false {`.
    #[test]
    fn a_selection_that_rejected_a_leaf_never_reports_completion() {
        let mut take = Selection::new(blob(1), 0, 1);
        take.leaf(0, leaf(16_384))
            .expect("the first leaf covers it");
        assert!(
            take.finish().is_ok(),
            "control: one byte was asked for and one byte was delivered"
        );

        assert!(
            take.leaf(2, leaf(16_384)).is_err(),
            "and then a gap arrives"
        );
        assert!(
            take.finish().is_err(),
            "after which there is no clean end left to report"
        );
    }

    /// The hash-sequence hole, at the unit the fix lives in.
    /// `a_granted_peer_cannot_fetch_a_child_of_a_granted_hash_seq` proves it end
    /// to end; this pins which condition does the work.
    #[test]
    fn a_request_reaching_past_the_granted_blob_is_refused() {
        let grants = Grants::new();
        let live = Connections::default();
        grant(&grants, 1, blob(1));
        hung(&live, 1, 1);

        assert!(
            admits_get(&grants, &live, &get(1, blob(1), whole_blob())).is_ok(),
            "control: the granted blob itself is served"
        );
        assert!(
            admits_get(&grants, &live, &get(1, blob(1), ChunkRangesSeq::all())).is_err(),
            "the same grant must not reach the children a hash sequence names"
        );
    }

    /// A closure grant reaches past the root, whatever shape the reach takes,
    /// and does it without reading a store: `admits_get` has none.
    ///
    /// The other half of the same guard is what keeps a `Blob` grant on the
    /// root. Watched red with `scripts/break-once.sh` (row `HK50`) by replacing
    /// `if scope != Scope::Closure {`
    /// with `if false {`: a `Blob` grant then reaches the children a hash
    /// sequence names, and `a_granted_peer_cannot_fetch_a_child_of_a_granted_hash_seq`
    /// fails with `a request that reaches past the granted blob must be refused,
    /// got Ok(Stats { .. payload_bytes_read: 57 ..})`. The unit tests in this
    /// module that hold the same half are `a_request_reaching_past_the_granted_blob_is_refused`
    /// and `blob_admission_accepts_only_finite_root_ranges`.
    #[test]
    fn a_closure_grant_admits_every_range_shape_under_its_root() {
        let grants = Grants::new();
        let live = Connections::default();
        grant(&grants, 1, seq(1));
        hung(&live, 1, 1);
        let admits = |ranges| admits_get(&grants, &live, &get(1, blob(1), ranges)).is_ok();

        assert!(admits(whole_blob()), "the root alone");
        assert!(admits(ChunkRangesSeq::all()), "the root and every member");
        assert!(
            admits(ChunkRangesSeq::from_ranges_infinite([ChunkRanges::all()])),
            "an infinite sequence"
        );
        assert!(
            admits(ChunkRangesSeq::from_ranges([
                ChunkRanges::empty(),
                ChunkRanges::all()
            ])),
            "members only"
        );
    }

    /// Review finding 3. A closure grant admitted any offset, and a request can
    /// select nothing until offset `1 << 60` in a few bytes while iroh-blobs
    /// walks the empty offsets one at a time before it looks at the sequence.
    ///
    /// Admission now reads the furthest run's start off the encoded runs, and
    /// leaves it to the root's own size, read once from the store, to say
    /// whether that many members exist. This is the part that needs no store:
    /// what admission asks, and the rule that answers it.
    #[test]
    fn a_closure_request_names_the_offset_it_will_walk_to_without_walking_to_it() {
        let grants = Grants::new();
        let live = Connections::default();
        grant(&grants, 1, seq(1));
        hung(&live, 1, 1);
        // Built from its wire form: `GetRequest::builder` expands every offset
        // below the one it is given into a vector, which is the very cost under
        // test. One run: `offset` empty offsets, then everything, then nothing.
        let far = |offset: u64| {
            let wire = postcard::to_stdvec(&vec![(offset, vec![0u64]), (1u64, Vec::<u64>::new())])
                .expect("encodes");
            RequestReceived {
                connection_id: 1,
                request_id: 0,
                request: GetRequest::new(
                    blob(1).hash(),
                    postcard::from_bytes(&wire).expect("a valid sparse request"),
                ),
            }
        };

        for offset in [1, 5, 1 << 60, u64::MAX - 1] {
            assert_eq!(
                admits_get(&grants, &live, &far(offset)),
                Ok(Admit::IfRootLists {
                    root: blob(1).hash(),
                    furthest: offset
                }),
                "a run that starts at {offset} is checked against the root"
            );
        }
        assert_eq!(
            admits_get(
                &grants,
                &live,
                &get(
                    1,
                    blob(1),
                    ChunkRangesSeq::from_ranges_infinite([ChunkRanges::all()])
                )
            ),
            Ok(Admit::Now),
            "control: an open-ended request starts at the root and needs no look"
        );
    }

    /// The rule the store's answer is put to: the root is a whole number of
    /// addresses and has a member at the offset the furthest run starts at
    /// (offset `n + 1` is member `n`).
    ///
    /// Watched red with `scripts/break-once.sh` (row `HK56`) by replacing
    /// `size.is_multiple_of(32) && furthest <= size / 32`
    /// with `true`: a request that starts far past the root is then admitted
    /// and the third assertion fails.
    #[test]
    fn a_root_lists_an_offset_only_if_it_has_that_many_members() {
        let four_members = 4 * 32;
        assert!(lists(four_members, 1), "member 0");
        assert!(lists(four_members, 4), "member 3, the last");
        assert!(!lists(four_members, 5), "one past the last");
        assert!(!lists(four_members, 1 << 60), "far past it");
        assert!(
            !lists(four_members + 1, 1),
            "not a whole number of addresses"
        );
        assert!(!lists(0, 1), "an empty root lists nothing");
    }

    /// A request carries the root's hash and offsets, so a member named by its
    /// own hash is just another hash, and only a grant on it admits it.
    #[test]
    fn a_member_named_by_its_own_hash_is_refused() {
        let grants = Grants::new();
        let live = Connections::default();
        grant(&grants, 1, seq(1));
        hung(&live, 1, 1);

        assert!(
            admits_get(&grants, &live, &get(1, blob(1), whole_blob())).is_ok(),
            "control: the root is admitted"
        );
        assert!(
            admits_get(&grants, &live, &get(1, blob(2), whole_blob())).is_err(),
            "a member the closure lists, asked for by its own hash"
        );
    }
}
