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
//! # A grant lasts until it is revoked, and the ledger has a byte budget
//!
//! Nothing expires a grant. [`Blobs::revoke`], [`Blobs::revoke_peer`] and
//! [`Blobs::revoke_everywhere`] withdraw them, and the ledger charges every
//! grant against a byte budget ([`GRANT_BUDGET`]). A grant that would exceed it
//! is refused, loudly, with [`RemoteError::GrantsExhausted`]; nothing is
//! evicted to make room, because an evicted capability is one the application
//! believes it handed out. Grants die with the process.
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

/// Default byte budget of one node's grant ledger: 64 MiB.
///
/// The ledger charges [`GRANT_COST`] for each grant and [`PEER_COST`] for each
/// peer that holds at least one, and refuses a grant that would take the total
/// past the budget. One closure is one grant, however many blobs it lists.
/// Change it per store with [`Blobs::set_grant_budget`].
pub const GRANT_BUDGET: usize = 64 * 1024 * 1024;

/// The most members one closure may list: 262,144, a root of 8 MiB.
///
/// Both ends hold the line. A holder refuses a request that selects a member
/// past it, because iroh-blobs walks the offsets of a request one at a time and
/// a request can name an offset near `u64::MAX` in a few bytes. A fetcher
/// refuses a root longer than this before it downloads or plans anything,
/// because the member list is the holder's to make as long as it likes and the
/// fetcher's memory pays for it. A tree that needs more is split across
/// several closures.
pub const MAX_CLOSURE_MEMBERS: usize = 1 << 18;

/// Bytes the ledger charges for one grant.
///
/// An upper bound on one `(BlobId, Entry)` slot of a hash table, computed from
/// `size_of` at compile time rather than written down: one control byte per
/// slot, times 16/7, which is the emptiest a hashbrown table is just after it
/// grows. **Derived from the type layout, not measured.** A counting-allocator
/// test could pin it; none does yet.
pub const GRANT_COST: usize = ((size_of::<(BlobId, Entry)>() + 1) * 16).div_ceil(7);

/// Bytes the ledger charges for each peer that holds at least one grant.
///
/// The peer's slot in the outer table at the same worst load as [`GRANT_COST`],
/// plus the smallest inner table (4 buckets, their control bytes and one group
/// of padding). **Derived from the type layout, not measured.**
pub const PEER_COST: usize = ((size_of::<(EndpointId, HashMap<BlobId, Entry>)>() + 1) * 16)
    .div_ceil(7)
    + 4 * (size_of::<(BlobId, Entry)>() + 1)
    + 16;

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
    by_peer: HashMap<EndpointId, HashMap<BlobId, Entry>>,
    /// Bytes charged: [`GRANT_COST`] per entry plus [`PEER_COST`] per key of
    /// `by_peer`.
    used: usize,
    budget: usize,
    next_generation: u64,
    /// Calls that were dispatched for a peer and have not finished, with the
    /// peer's revocation epoch. An entry exists only while one is in flight, so
    /// this is bounded by the calls the node is already running.
    inflight: HashMap<EndpointId, Flight>,
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
    /// The grant would not fit the budget.
    Exhausted { budget: usize },
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
/// more. The fence is taken at dispatch and checked at reservation.
#[derive(Debug)]
pub(crate) struct Fence {
    grants: Arc<Grants>,
    peer: EndpointId,
    epoch: u64,
}

impl Drop for Fence {
    fn drop(&mut self) {
        let mut book = self.grants.lock();
        if let Some(flight) = book.inflight.get_mut(&self.peer) {
            flight.calls -= 1;
            if flight.calls == 0 {
                book.inflight.remove(&self.peer);
            }
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
        Self::with_budget(GRANT_BUDGET)
    }

    fn with_budget(budget: usize) -> Arc<Self> {
        Arc::new(Self {
            inner: Mutex::new(Book {
                by_peer: HashMap::new(),
                used: 0,
                budget,
                next_generation: 0,
                inflight: HashMap::new(),
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

    fn set_budget(&self, budget: usize) {
        self.lock().budget = budget;
    }

    /// Fence a call that is about to be dispatched for `peer`.
    fn fence(self: &Arc<Self>, peer: &AdmittedPeer) -> Fence {
        let peer = peer.id();
        let mut book = self.lock();
        let flight = book.inflight.entry(peer).or_default();
        flight.calls += 1;
        let epoch = flight.epoch;
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
    /// A repeat or widening grant for a pair that already has an entry costs
    /// nothing. A new pair costs [`GRANT_COST`], plus [`PEER_COST`] when it is
    /// the peer's first, and is refused when that would pass the budget.
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
            let cost = GRANT_COST
                + if book.by_peer.contains_key(&peer) {
                    0
                } else {
                    PEER_COST
                };
            if book.used.saturating_add(cost) > book.budget {
                return Err(NotGranted::Exhausted {
                    budget: book.budget,
                });
            }
            let generation = book.next_generation;
            book.next_generation += 1;
            book.used += cost;
            book.by_peer.entry(peer).or_default().insert(
                root,
                Entry {
                    committed: None,
                    pending: 1,
                    generation,
                },
            );
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
        trim(&mut book.by_peer);
        book.used -= PEER_COST + GRANT_COST * held.len();
        held.values()
            .filter(|entry| entry.committed.is_some())
            .count()
    }

    fn revoke_everywhere(&self, root: BlobId) -> usize {
        let mut book = self.lock();
        let peers: Vec<EndpointId> = book
            .by_peer
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

    /// Take one entry out and uncharge it, and its peer when it was the last.
    fn remove(book: &mut Book, peer: EndpointId, root: &BlobId) -> Option<Entry> {
        let held = book.by_peer.get_mut(&peer)?;
        let entry = held.remove(root)?;
        trim(held);
        book.used -= GRANT_COST;
        if held.is_empty() {
            // Not tidiness: `knows` reads this map, so a peer left behind with
            // an empty table would keep passing connection admission while
            // holding no grant at all.
            book.by_peer.remove(&peer);
            trim(&mut book.by_peer);
            book.used -= PEER_COST;
        }
        Some(entry)
    }
}

/// Give back the memory a table no longer needs.
///
/// A hash table keeps its allocation when entries leave, so refunding
/// [`GRANT_COST`] on every revoke would let churn retain far more than the
/// budget says: fill a peer's table, revoke all but one root, and the refund
/// pays for filling another peer's table while the first keeps its buckets.
/// Shrinking whenever the capacity passes twice the length keeps the retained
/// buckets within the 16/7 per-slot charge the constants are derived from, at
/// both levels (a peer's roots, and the peers). It is the same check at every
/// call, and a no-op when the table is already as small as it can be.
fn trim<K: Eq + std::hash::Hash, V>(table: &mut HashMap<K, V>) {
    if table.capacity() > 2 * table.len() {
        table.shrink_to_fit();
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
/// map over from the one the ledger's byte budget bounds.
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
        // The root is checked before it is read: a longer list than
        // `MAX_CLOSURE_MEMBERS` is refused here, so the list of what is missing
        // below is bounded by it.
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

    /// How many members the sequence at `root` lists, if this store holds it in
    /// full, and an error when what it holds is not a closure this crate will
    /// plan for: not a whole number of addresses, or more than
    /// [`MAX_CLOSURE_MEMBERS`].
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
    /// [`Error::BlobRefused`], and a new connection is refused at accept.
    /// Removing a peer from an [`Allowlist`](crate::Allowlist) or a
    /// [`DynamicAdmission`](crate::DynamicAdmission) does not touch its blob
    /// grants, because the ledger is per node and does not record which service
    /// granted what: drop the peer from admission first, then call this.
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

    /// Set the byte budget of the grant ledger every clone of this store
    /// shares. The default is [`GRANT_BUDGET`].
    ///
    /// Takes effect at the next new grant. Grants already held are kept even
    /// when they exceed it.
    pub fn set_grant_budget(&self, bytes: NonZeroUsize) {
        self.inner.grants.set_budget(bytes.get());
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
    /// budget refuses it.
    #[doc(hidden)]
    pub fn grant(&self, peer: &AdmittedPeer, content: Content) -> Result<()> {
        let fence = self.fence(peer);
        self.reserve(peer, content, &fence)
            .map_err(|refused| Error::Remote(refusal(&refused)))?
            .commit();
        Ok(())
    }

    /// Fence a call about to be dispatched for `peer`. See [`Fence`].
    pub(crate) fn fence(&self, peer: &AdmittedPeer) -> Fence {
        self.inner.grants.fence(peer)
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
/// iroh-blobs plans a request by building a map entry per member it names, and
/// encodes one run per missing member, so asking for a closure in windows
/// bounds both the plan's memory and the request's size (4096 members is about
/// 12 KB, against a 1 MiB cap) whatever the closure's length.
const FETCH_WINDOW: u64 = 4096;

/// The member count of a root of `size` bytes, or why it is not a closure.
fn count_members(root: BlobId, size: u64) -> Result<u64> {
    if !size.is_multiple_of(32) {
        return Err(Error::BlobUnverified {
            id: root,
            fault: BlobFault::NotASequence { len: size },
        });
    }
    let members = size / 32;
    if members > MAX_CLOSURE_MEMBERS as u64 {
        return Err(Error::BlobUnverified {
            id: root,
            fault: BlobFault::TooManyMembers {
                members,
                limit: MAX_CLOSURE_MEMBERS as u64,
            },
        });
    }
    Ok(members)
}

/// The request for members `start..end` of the closure at `root`, and nothing
/// else: no bytes of the root, and nothing past `end`.
///
/// Built from its wire form (a run from offset `start + 1` that selects
/// everything, then a run that selects nothing) rather than with the request
/// builder, which expands a vector of every offset below the last one it is given.
fn window_request(root: BlobId, start: u64, end: u64) -> Result<GetRequest> {
    let runs = vec![(start + 1, vec![0u64]), (end - start, Vec::<u64>::new())];
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
        NotGranted::Exhausted { budget } => RemoteError::GrantsExhausted {
            budget: *budget as u64,
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
/// names no hash. Admission reads nothing from the store and costs the same for
/// a five-member sequence and a fifty-thousand-member one.
///
/// This stays a runtime check on purpose. It is peer-supplied data arriving at
/// the edge, which is the one place a guard belongs (peak-rust rule 2): no type
/// keylink owns is on this path, because the request was decoded by iroh-blobs.
fn admits_get(
    grants: &Grants,
    live: &Connections,
    msg: &RequestReceived<GetRequest>,
) -> std::result::Result<(), AbortReason> {
    let Some(peer) = live.peer(msg.connection_id) else {
        return Err(AbortReason::Permission);
    };
    let Some(scope) = grants.scope(&peer, &BlobId::from_hash(msg.request.hash)) else {
        return Err(AbortReason::Permission);
    };
    if !msg.request.ranges.is_blob() {
        // Reaches past the root. Only a closure grant covers that.
        if scope != Scope::Closure {
            return Err(AbortReason::Permission);
        }
        // And only as far as a closure can be listed. Refused here because
        // iroh-blobs would otherwise walk every empty offset before it looks at
        // the sequence.
        if selects_past(&msg.request.ranges, MAX_CLOSURE_MEMBERS as u64) {
            return Err(AbortReason::Permission);
        }
    }
    Ok(())
}

/// Whether `ranges` selects anything at an offset above `last_member`, where
/// offset 0 is the root and offset `n + 1` is member `n`.
///
/// Read off the encoded runs and never expanded: a run is a start offset and the
/// ranges that apply from there until the next run, so the work is the size of
/// the request and not the size of the offsets it names. The runs are taken from
/// the wire form of the value, which is the one view of them iroh-blobs offers
/// and which a request has already been bounded to by its 1 MiB message cap. A
/// value that will not encode counts as reaching past.
fn selects_past(ranges: &ChunkRangesSeq, last_member: u64) -> bool {
    let Ok(wire) = postcard::to_stdvec(ranges) else {
        return true;
    };
    let Ok(runs) = postcard::from_bytes::<Vec<(u64, Vec<u64>)>>(&wire) else {
        return true;
    };
    let mut offset = 0u64;
    for (gap, spans) in runs {
        let Some(start) = offset.checked_add(gap) else {
            return true;
        };
        offset = start;
        // A run with no spans selects nothing, wherever it starts.
        if !spans.is_empty() && offset > last_member {
            return true;
        }
    }
    false
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
    /// or a [`SeqId`] is `Serialize`, so a peer can pass an address on to
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
    /// it, and which is checked for whole addresses and for
    /// [`MAX_CLOSURE_MEMBERS`] before it is downloaded. Then the members, and
    /// only the members the store lacks, 4096 of them per request: iroh-blobs
    /// computes what is missing from the local copy of the root and asks for
    /// exactly that, and a window keeps both its plan and its request small
    /// however long the closure is. Fetching the sequence in one request would
    /// send the whole closure when the root is absent, because the fetcher
    /// cannot know which members it holds until it holds the list. The holder's
    /// grant is its claim that it has the closure.
    ///
    /// # It does not inherit the reader's size guarantee, on purpose
    ///
    /// This hands the connection to iroh-blobs' own `Remote::fetch`, which
    /// decides what is missing and asks for exactly that. There is no
    /// [`verified_len`](Self::verified_len) preflight and no comparison,
    /// because there is no number handed back to announce: the content is
    /// validated against its address as it lands in the store, and the return
    /// value describes bytes that already arrived rather than bytes to expect.
    /// Reach for [`open`](Self::open) when a length has to be trusted before
    /// the transfer.
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
        // the store by now, so each window plans from its own slice of the list.
        let remote = store.store().remote();
        let mut start = 0;
        while start < members {
            let end = (start + FETCH_WINDOW).min(members);
            let local = remote
                .local_for_request(window_request(self.id(), start, end)?)
                .await
                .map_err(store_error)?;
            if !local.is_complete() {
                let stats = remote
                    .execute_get(connection.clone(), local.missing())
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
    /// The root is checked against [`MAX_CLOSURE_MEMBERS`] and for whole
    /// addresses **before it is downloaded**, from the size the holder proves
    /// with the last chunk, because the holder chooses how long it is. A root
    /// the store already holds is checked the same way, so a closure whose list
    /// is not a sequence fails here and not as a fetch that succeeds.
    async fn ensure_root(&self, connection: &Connection, store: &Blobs) -> Result<(u64, u64)> {
        if let Some(members) = store.members(self.id()).await? {
            return Ok((0, members));
        }
        let (size, _) = get_verified_size(connection, &self.id().hash())
            .await
            .map_err(|error| self.classify(error, connection))?;
        count_members(self.id(), size)?;
        let bytes = self.fetch_root(connection, store).await?;
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

    /// A reservation as dispatch makes it: under a fence taken at dispatch.
    fn reserve(
        grants: &Arc<Grants>,
        seed: u8,
        content: impl Into<Content>,
    ) -> std::result::Result<Permit, NotGranted> {
        let fence = grants.fence(&admitted(seed));
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
        assert_eq!(book.used, PEER_COST + GRANT_COST);
        assert_eq!(book.by_peer.len(), 1);
        assert_eq!(book.by_peer.values().next().map(HashMap::len), Some(1));
    }

    /// The budget refuses, and refusing evicts nothing.
    ///
    /// Watched red with `scripts/break-once.sh` (row `HK52`) by replacing
    /// `if book.used.saturating_add(cost) > book.budget {`
    /// with `if false {`: the third `reserve` is then granted and the test
    /// panics at `the third grant does not fit`.
    #[test]
    fn a_grant_past_the_budget_is_refused_and_nothing_is_evicted() {
        let budget = PEER_COST + 2 * GRANT_COST;
        let grants = Grants::with_budget(budget);
        grant(&grants, 1, blob(1));
        grant(&grants, 1, blob(2));

        let refused = reserve(&grants, 1, blob(3)).expect_err("the third grant does not fit");
        assert_eq!(refused, NotGranted::Exhausted { budget });
        assert_eq!(scope(&grants, 1, blob(1)), Some(Scope::Blob));
        assert_eq!(scope(&grants, 1, blob(2)), Some(Scope::Blob));
        assert_eq!(scope(&grants, 1, blob(3)), None);
        assert!(
            !grants.lock().by_peer[&admitted(1).id()].contains_key(&blob(3)),
            "a refused reservation leaves no entry behind"
        );
    }

    #[test]
    fn a_repeat_or_widening_grant_costs_nothing() {
        let grants = Grants::new();
        grant(&grants, 1, blob(4));
        let used = grants.lock().used;

        grant(&grants, 1, blob(4));
        assert_eq!(grants.lock().used, used, "a repeat is free");
        assert_eq!(scope(&grants, 1, blob(4)), Some(Scope::Blob));

        grant(&grants, 1, seq(4));
        assert_eq!(grants.lock().used, used, "a widening is free");
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

    /// A reply that was never written grants nothing and costs nothing.
    ///
    /// Watched red with `scripts/break-once.sh` (row `HK53`) by replacing
    /// `if entry.committed.is_none() && entry.pending == 0 {`
    /// with `if false {`: the dropped reservation stays charged and the
    /// `assert_eq!(grants.lock().used, 0)` fails.
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
        let early = grants.fence(&admitted(1));
        let other = grants.fence(&admitted(2));
        assert_eq!(grants.revoke_peer(admitted(1).id()), 0, "nothing held yet");

        assert_eq!(
            grants
                .reserve(&admitted(1), blob(1).into(), &early)
                .expect_err("the call predates the revoke"),
            NotGranted::Withdrawn
        );
        assert!(!grants.knows(&peer(1)), "and nothing was left behind");
        assert_eq!(grants.lock().used, 0);

        grants
            .reserve(&admitted(2), blob(1).into(), &other)
            .expect("another peer's call is not affected")
            .commit();

        // A call dispatched after the revoke is a new admission decision.
        let late = grants.fence(&admitted(1));
        grants
            .reserve(&admitted(1), blob(1).into(), &late)
            .expect("dispatched after")
            .commit();
        assert_eq!(scope(&grants, 1, blob(1)), Some(Scope::Blob));

        drop((early, other, late));
        assert!(
            grants.lock().inflight.is_empty(),
            "a finished call leaves nothing in the fence table"
        );
    }

    /// What a hash table of `slot` bytes per entry retains for `capacity`: the
    /// bucket count hashbrown derives from it, a control byte each, and one
    /// group of padding.
    fn retained(capacity: usize, slot: usize) -> usize {
        let buckets = if capacity < 8 {
            capacity + 1
        } else {
            capacity / 7 * 8
        };
        buckets * (slot + 1) + 16
    }

    /// Review finding 4. Revoking refunds [`GRANT_COST`], and a table that
    /// keeps its allocation would make that refund pay for memory still held.
    /// Fill each of many peers, revoke all but one root each, and compare what
    /// the tables really retain with what the ledger says it has charged, at
    /// both levels.
    ///
    /// Watched red with `scripts/break-once.sh` (row `HK57`) by replacing
    /// `if table.capacity() > 2 * table.len() {`
    /// with `    if false {`: retained is then several times `used`.
    #[test]
    fn churn_cannot_retain_more_than_the_budget_has_charged() {
        let grants = Grants::new();
        for seed in 1..=60u8 {
            for nth in 0..400u32 {
                let mut root = [0u8; 32];
                root[..4].copy_from_slice(&nth.to_le_bytes());
                grant(&grants, seed, BlobId(root));
            }
            for nth in 1..400u32 {
                let mut root = [0u8; 32];
                root[..4].copy_from_slice(&nth.to_le_bytes());
                assert!(grants.revoke(admitted(seed).id(), BlobId(root)));
            }
        }
        // And a whole generation of peers leaving, which empties the outer table.
        for seed in 1..=50u8 {
            grants.revoke_peer(admitted(seed).id());
        }

        let book = grants.lock();
        let slot = size_of::<(BlobId, Entry)>();
        let inner: usize = book
            .by_peer
            .values()
            .map(|held| retained(held.capacity(), slot))
            .sum();
        let outer = retained(
            book.by_peer.capacity(),
            size_of::<(EndpointId, HashMap<BlobId, Entry>)>(),
        );
        assert_eq!(book.by_peer.len(), 10);
        assert!(
            inner + outer <= book.used,
            "the tables retain {} bytes against {} charged",
            inner + outer,
            book.used
        );
    }

    #[test]
    fn an_uncommitted_permit_releases_its_charge() {
        let grants = Grants::new();
        let permit = reserve(&grants, 1, blob(1)).expect("fits");
        assert!(grants.knows(&peer(1)), "a pending peer passes the guard");
        drop(permit);

        assert_eq!(grants.lock().used, 0);
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
        assert_eq!(grants.lock().used, PEER_COST + GRANT_COST);
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
        assert_eq!(grants.lock().used, 2 * PEER_COST + 2 * GRANT_COST);
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
        assert_eq!(grants.lock().used, PEER_COST + GRANT_COST);
    }

    /// `GRANT_COST` and `PEER_COST` are computed from `size_of`, and this pins
    /// the arithmetic that has to hold for them to be upper bounds: a slot plus
    /// its control byte at hashbrown's emptiest load after a grow, 16/7.
    #[test]
    fn the_cost_constants_cover_the_layout_they_were_derived_from() {
        assert!(GRANT_COST >= (size_of::<(BlobId, Entry)>() + 1) * 16 / 7);
        assert!(
            PEER_COST
                >= (size_of::<(EndpointId, HashMap<BlobId, Entry>)>() + 1) * 16 / 7
                    + 4 * size_of::<(BlobId, Entry)>()
        );
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
    /// `if !msg.request.ranges.is_blob() {`
    /// with `if false {`.
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
    /// Watched red with `scripts/break-once.sh` (row `HK56`) by replacing
    /// `if selects_past(&msg.request.ranges, MAX_CLOSURE_MEMBERS as u64) {`
    /// with `if false {`: the far request is then admitted.
    #[test]
    fn a_closure_request_for_a_far_offset_is_refused_without_walking_to_it() {
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
        let last = MAX_CLOSURE_MEMBERS as u64;

        assert!(
            admits_get(&grants, &live, &far(last)).is_ok(),
            "control: the offset of the last member a closure may list"
        );
        assert!(admits_get(&grants, &live, &far(last + 1)).is_err());
        assert!(admits_get(&grants, &live, &far(1 << 60)).is_err());
        assert!(admits_get(&grants, &live, &far(u64::MAX - 1)).is_err());
        assert!(
            admits_get(
                &grants,
                &live,
                &get(
                    1,
                    blob(1),
                    ChunkRangesSeq::from_ranges_infinite([ChunkRanges::all()])
                )
            )
            .is_ok(),
            "control: an open-ended request starts at the root and is walked as far as the list goes"
        );
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
