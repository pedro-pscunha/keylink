//! Blobs admission, over two real iroh endpoints on the loopback interface.
//!
//! Everything here runs against a genuine QUIC transport with no relay and no
//! discovery, so a refusal is a refusal on the wire rather than a mocked one.
//!
//! # The stand-in for generated dispatch
//!
//! RFC-01 §3.2 puts the grant inside the code the `service!` macro writes for a
//! `blob` method, so a handler that returns a [`BlobId`] has already granted it.
//! That macro kind is a later commit. Until it lands, [`Granting`] below is the
//! same shape by hand: a real `ProtocolHandler`, a real accept, and a grant
//! taken from the connection iroh proved — which is the only argument
//! `Blobs::grant` accepts.
//!
//! For the same reason the mount is built here rather than by
//! `ServerBuilder::spawn`: `iroh_blobs::ALPN` is reserved, so `spawn` refuses it
//! from a user and does not yet offer it from a `Blobs`.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::time::Duration;

use iroh::endpoint::{presets, Connection, ConnectionError};
use iroh::protocol::{AcceptError, ProtocolHandler, Router};
use iroh::Endpoint;
use iroh_blobs::protocol::{
    ChunkRanges, ChunkRangesSeq, GetManyRequest, GetRequest, ObserveRequest, PushRequest,
};
use iroh_blobs::store::mem::MemStore;
use keylink::{BlobHandle, BlobId, Blobs, Error, Node};
use n0_future::StreamExt;

/// Where a test peer asks for the grants it wants. Stands in for an ordinary
/// keylink `blob` rpc, which is what will carry this in production.
const GRANT_ALPN: &[u8] = b"/keylink-test/grant/1";

// ────────────────────────────────────────────────────────────────── fixture

/// The stand-in for generated `blob` dispatch.
///
/// A peer asks for a list of addresses and the handler records a grant for each
/// one against **this connection's** proven peer. A real handler would compute
/// the list instead of being told it; nothing else about the shape differs, and
/// the argument `Blobs::grant` takes is what forces that.
#[derive(Debug, Clone)]
struct Granting {
    blobs: Blobs,
}

impl ProtocolHandler for Granting {
    async fn accept(&self, connection: Connection) -> Result<(), AcceptError> {
        let (mut send, mut recv) = connection.accept_bi().await?;
        let asked = recv
            .read_to_end(64 * 1024)
            .await
            .map_err(AcceptError::from_err)?;
        let wanted: Vec<BlobId> = postcard::from_bytes(&asked).map_err(AcceptError::from_err)?;

        for id in wanted {
            self.blobs.grant(&connection, id);
        }

        // Answered only after every grant is recorded, so a test that has read
        // this byte knows the ledger is written.
        send.write_all(b"granted")
            .await
            .map_err(AcceptError::from_err)?;
        send.finish().map_err(AcceptError::from_err)?;
        connection.closed().await;
        Ok(())
    }
}

/// One serving node with a store, and two peers that can talk to it.
struct Cluster {
    server: Node,
    peer: Node,
    other: Node,
    blobs: Blobs,
    _router: Router,
}

async fn local_node() -> Node {
    Node::from_endpoint(
        Endpoint::bind(presets::Minimal)
            .await
            .expect("a local endpoint binds"),
    )
}

async fn cluster() -> Cluster {
    let server = local_node().await;
    let blobs = Blobs::memory();
    let router = Router::builder(server.endpoint().clone())
        .accept(iroh_blobs::ALPN, blobs.protocol())
        .accept(
            GRANT_ALPN,
            Granting {
                blobs: blobs.clone(),
            },
        )
        .spawn();

    Cluster {
        peer: local_node().await,
        other: local_node().await,
        server,
        blobs,
        _router: router,
    }
}

impl Cluster {
    /// Ask the serving node to grant `ids` to `who`.
    async fn grant(&self, who: &Node, ids: &[BlobId]) {
        let conn = who
            .endpoint()
            .connect(self.server.endpoint().addr(), GRANT_ALPN)
            .await
            .expect("dials the granting protocol");
        let (mut send, mut recv) = conn.open_bi().await.expect("opens a stream");
        let asked = postcard::to_stdvec(&ids.to_vec()).expect("encodes the request");
        send.write_all(&asked).await.expect("writes the request");
        send.finish().expect("finishes the request");
        let answer = recv.read_to_end(64).await.expect("reads the answer");
        assert_eq!(answer, b"granted", "the handler recorded the grants");
    }

    /// Fetch through the public client path, with no local store.
    async fn fetch(&self, who: &Node, id: BlobId) -> keylink::Result<Vec<u8>> {
        let mut sink = Vec::new();
        BlobHandle::new(who, self.server.endpoint().addr(), id)
            .copy_to(&mut sink)
            .await?;
        Ok(sink)
    }

    /// A raw blobs connection, for the request shapes keylink's own client
    /// never sends. Every one of them is something a hostile peer can send
    /// anyway, which is the reason to test them.
    async fn dial_blobs(&self, who: &Node) -> Connection {
        who.endpoint()
            .connect(self.server.endpoint().addr(), iroh_blobs::ALPN)
            .await
            .expect("dials the blobs protocol")
    }

    async fn add(&self, content: &str) -> BlobId {
        self.blobs
            .add_bytes(content.as_bytes().to_vec())
            .await
            .expect("the store accepts content")
    }
}

/// A `BlobId` for content this node never added.
///
/// keylink offers no constructor on purpose — an address is something a peer is
/// handed — so this takes the route a peer's address actually takes: the wire.
/// `BlobId` encodes as the raw 32 bytes, which is what
/// `the_blob_id_encodes_identically_to_an_iroh_blobs_hash` pins.
fn address_of(hash: iroh_blobs::Hash) -> BlobId {
    postcard::from_bytes(hash.as_bytes()).expect("32 raw bytes decode as a BlobId")
}

/// The other direction, for the raw iroh-blobs requests keylink's own client
/// never sends.
fn hash_of(id: BlobId) -> iroh_blobs::Hash {
    iroh_blobs::Hash::from(*id.as_bytes())
}

fn assert_refused(error: &Error, what: &str) {
    assert!(
        matches!(error, Error::BlobRefused { .. }),
        "{what}: expected a refusal, got {error:?}"
    );
}

// ──────────────────────────────────────────────────────────── the grant works

/// The control, and it has to come first. A deny that always denies proves
/// nothing, and this is also the guard on the one coupling in the design: the
/// per-request events carry only a `connection_id`, derived inside iroh-blobs
/// as `connection.stable_id() as u64`. If that derivation ever moves, keylink
/// stops recognising its own admitted connections and refuses everything —
/// which every other test in this file would happily pass.
#[tokio::test]
async fn a_peer_with_a_grant_can_fetch_that_content() {
    let cluster = cluster().await;
    let id = cluster.add("the granted datasheet").await;
    cluster.grant(&cluster.peer, &[id]).await;

    let content = cluster
        .fetch(&cluster.peer, id)
        .await
        .expect("a granted fetch must succeed");
    assert_eq!(content, b"the granted datasheet");
}

/// The grant is durable, which is what makes resume possible at all: a resumed
/// transfer is a second get request, so a single-use grant would refuse it.
#[tokio::test]
async fn a_grant_survives_a_second_fetch_so_resume_works() {
    let cluster = cluster().await;
    let id = cluster.add("fetched twice").await;
    cluster.grant(&cluster.peer, &[id]).await;

    assert_eq!(
        cluster.fetch(&cluster.peer, id).await.unwrap(),
        b"fetched twice"
    );
    assert_eq!(
        cluster.fetch(&cluster.peer, id).await.unwrap(),
        b"fetched twice"
    );
}

// ─────────────────────────────────────────────────────────── the grant binds

/// The hash is handed over out of band, exactly as a leak would hand it over.
/// A test where the peer has to guess a BLAKE3 hash proves nothing.
#[tokio::test]
async fn a_peer_without_a_grant_cannot_fetch_a_hash_it_knows() {
    let cluster = cluster().await;
    let known = cluster.add("private").await;
    let granted = cluster.add("public").await;
    cluster.grant(&cluster.peer, &[granted]).await;

    let error = cluster
        .fetch(&cluster.peer, known)
        .await
        .expect_err("a hash the peer knows but was never granted");
    assert_refused(&error, "a known but ungranted hash");
}

/// A store is one flat namespace. A grant must be a key to one blob, not to the
/// store the blob happens to live in.
#[tokio::test]
async fn a_granted_peer_cannot_fetch_a_different_hash_in_the_same_store() {
    let cluster = cluster().await;
    let mine = cluster.add("mine").await;
    let theirs = cluster.add("theirs").await;
    cluster.grant(&cluster.peer, &[mine]).await;

    assert_eq!(cluster.fetch(&cluster.peer, mine).await.unwrap(), b"mine");
    let error = cluster.fetch(&cluster.peer, theirs).await.unwrap_err();
    assert_refused(&error, "another blob in the same store");
}

/// A grant names a peer as well as a hash.
#[tokio::test]
async fn a_grant_to_one_peer_does_not_admit_another_peer() {
    let cluster = cluster().await;
    let id = cluster.add("for the first peer only").await;
    cluster.grant(&cluster.peer, &[id]).await;
    // `other` holds a grant of its own, so it gets past connection admission
    // and is refused on the request. Without this the test would pass for the
    // weaker reason.
    let decoy = cluster.add("for the second peer").await;
    cluster.grant(&cluster.other, &[decoy]).await;

    assert_eq!(
        cluster.fetch(&cluster.peer, id).await.unwrap(),
        b"for the first peer only"
    );
    let error = cluster.fetch(&cluster.other, id).await.unwrap_err();
    assert_refused(&error, "a grant issued to a different peer");
}

/// Content in the store that no handler ever handed out.
#[tokio::test]
async fn content_that_nobody_was_granted_is_refused() {
    let cluster = cluster().await;
    let ungranted = cluster.add("nobody was told about this").await;
    let ticket = cluster.add("something else entirely").await;
    cluster.grant(&cluster.peer, &[ticket]).await;

    let error = cluster.fetch(&cluster.peer, ungranted).await.unwrap_err();
    assert_refused(&error, "content nobody was granted");
}

/// A peer with no grants at all never reaches the provider. Asserted at the
/// connection, not at the fetch, because that is the layer being claimed: the
/// close arrives before a single request is parsed.
#[tokio::test]
async fn a_peer_with_no_grants_at_all_is_refused_at_the_connection() {
    let cluster = cluster().await;
    let id = cluster.add("content").await;
    // Somebody holds a grant, so the ledger is not simply empty.
    cluster.grant(&cluster.peer, &[id]).await;

    let stranger = cluster.dial_blobs(&cluster.other).await;
    // Bounded, because the failure mode of a broken guard here is that nothing
    // ever closes. That would wedge the run instead of turning it red, and a
    // test that hangs teaches nobody anything. This is not an assertion about
    // timing: a load spike can only make the wait longer, never wrong.
    let closed = tokio::time::timeout(Duration::from_secs(30), stranger.closed())
        .await
        .expect("an ungranted peer must be refused, not left connected");
    let ConnectionError::ApplicationClosed(frame) = &closed else {
        panic!("expected an application close, got {closed:?}");
    };
    assert_eq!(
        frame.error_code,
        iroh_blobs::protocol::ERR_PERMISSION,
        "an ungranted peer is closed with the permission code, not a network error"
    );
}

/// The classic attack on deduplicated storage: probe for content by telling a
/// "we have it but you may not" apart from a "we do not have it".
///
/// This is the one that has to be written carefully. Asserting that both
/// attempts fail passes without testing the oracle at all, so the two failures
/// are compared to each other with only the address substituted out.
#[tokio::test]
async fn a_present_but_ungranted_hash_is_indistinguishable_from_an_absent_one() {
    let cluster = cluster().await;
    let present = cluster.add("held by this node, granted to nobody").await;
    let ticket = cluster.add("the peer's own content").await;
    cluster.grant(&cluster.peer, &[ticket]).await;

    // Never added anywhere. A valid address of content that does not exist.
    let absent = {
        let elsewhere = MemStore::new();
        let tag = elsewhere
            .add_bytes(b"this node has never seen these bytes".to_vec())
            .await
            .expect("adds to a throwaway store");
        address_of(tag.hash)
    };

    let present_error = cluster.fetch(&cluster.peer, present).await.unwrap_err();
    let absent_error = cluster.fetch(&cluster.peer, absent).await.unwrap_err();

    let shape = |error: &Error, id: BlobId| error.to_string().replace(&id.to_string(), "<id>");
    assert_eq!(
        shape(&present_error, present),
        shape(&absent_error, absent),
        "a present-but-ungranted hash must look exactly like an absent one"
    );
}

/// Content addressing makes two identical payloads one object, which is the
/// whole reason a grant authorizes bytes rather than a service's copy of them.
#[tokio::test]
async fn identical_content_added_twice_is_one_blob() {
    let cluster = cluster().await;
    let first = cluster.add("the very same bytes").await;
    let second = cluster.add("the very same bytes").await;
    assert_eq!(first, second, "there is no second copy to reach");
}

// ─────────────────────────────────────── the hash sequence hole RFC-01 missed

/// **A grant on a 32-aligned blob must not be a grant on everything it names.**
///
/// RFC-01 §5.5's policy reads `msg.request.hash` and ignores
/// `msg.request.ranges`. `handle_get_impl` (`provider.rs:417`) serves the
/// granted hash at offset 0 and, for every offset above it, re-reads that blob
/// as a `HashSeq` and serves the **child** it names. `HashSeq::new`
/// (`hashseq.rs:56`) accepts any byte string whose length divides by 32.
///
/// The trigger is therefore **alignment, not intent**. Nobody has to publish a
/// collection: a 32, 64 or 96 byte payload qualifies, and whatever those bytes
/// happen to spell is served as a child. Here the granted blob is literally the
/// secret's address, which is the cheapest way to write the attack down — but
/// any aligned blob whose bytes collide with a stored hash is the same hole.
#[tokio::test]
async fn a_granted_peer_cannot_fetch_a_child_of_a_granted_hash_seq() {
    let cluster = cluster().await;
    let secret = cluster.add("the secret nobody granted").await;
    // A blob whose content is exactly one 32-byte hash: a valid `HashSeq`.
    let root = cluster
        .blobs
        .add_bytes(secret.as_bytes().to_vec())
        .await
        .expect("stores the sequence");
    cluster.grant(&cluster.peer, &[root]).await;

    let store = MemStore::new();
    let conn = cluster.dial_blobs(&cluster.peer).await;
    let attack = store
        .remote()
        // Offset 0 is the granted root; every offset above it is a child.
        .execute_get(conn, GetRequest::all(hash_of(root)))
        .await;

    assert!(
        attack.is_err(),
        "a request that reaches past the granted blob must be refused, got {attack:?}"
    );
    assert!(
        store.get_bytes(hash_of(secret)).await.is_err(),
        "the child must not have been served"
    );
}

/// The control beside it. Without this the test above passes just as well when
/// the policy refuses every request, which is not the property being claimed.
#[tokio::test]
async fn a_granted_hash_seq_root_is_still_served_as_a_blob() {
    let cluster = cluster().await;
    let secret = cluster.add("the secret nobody granted").await;
    let root = cluster
        .blobs
        .add_bytes(secret.as_bytes().to_vec())
        .await
        .expect("stores the sequence");
    cluster.grant(&cluster.peer, &[root]).await;

    let served = cluster
        .fetch(&cluster.peer, root)
        .await
        .expect("the granted blob itself is still served");
    assert_eq!(
        served,
        secret.as_bytes(),
        "refusing children must not refuse the root"
    );
}

// ────────────────────────────────────── every other request type is refused

/// `EventMask::DEFAULT` sets `push: RequestMode::Disabled`, and that field is
/// dead: `EventSender::request` reads `mask.get` for every request type. So a
/// default mount accepts writes from a stranger — the write hole RFC-01 §4.2
/// measured. Under keylink's policy the write does not land.
///
/// **The push's own return value is not evidence, and the test does not use
/// it.** `execute_push_sink` (`api/remote.rs:617`) returns
/// `Ok(Default::default())` the moment the request is a plain blob, before it
/// has heard anything from the server — the same all-zero `Stats` a successful
/// push returns. RFC-01 §9 saw this and it holds: a client cannot learn from a
/// blobs success report whether the server accepted a write.
///
/// So the store is asked instead. The address is granted afterwards, which
/// removes the ledger from the answer: a fetch that fails here fails because
/// there is nothing to serve.
///
/// **Asked repeatedly, and that is deliberate.** The push returns before the
/// server has imported anything, so a single check right after it passes
/// whether the write was refused or merely slow. This is the shape CONTRIBUTING
/// calls for: the assertion is that the content NEVER appears, and a slower
/// machine only gives the push more time to land, so load can only strengthen
/// the test.
#[tokio::test]
async fn a_push_from_any_peer_is_refused_and_the_store_is_unchanged() {
    let cluster = cluster().await;
    let ticket = cluster.add("a grant, so the connection is admitted").await;
    cluster.grant(&cluster.peer, &[ticket]).await;

    let store = MemStore::new();
    let unsolicited = store
        .add_bytes(b"unsolicited payload from a stranger".to_vec())
        .await
        .expect("adds to the pushing peer's own store");

    let conn = cluster.dial_blobs(&cluster.peer).await;
    // Held open across the whole check. `execute_push` consumes its
    // `Connection`, and dropping the last handle closes the QUIC connection —
    // which would abort the server's import for a reason that has nothing to do
    // with admission, and make this test pass whatever the policy says.
    let _held = conn.clone();
    let _reported = store
        .remote()
        .execute_push(
            conn,
            PushRequest::new(
                unsolicited.hash,
                ChunkRangesSeq::from_ranges([ChunkRanges::all()]),
            ),
        )
        .await;

    let landed = address_of(unsolicited.hash);
    cluster.grant(&cluster.peer, &[landed]).await;

    for _ in 0..20 {
        match cluster.fetch(&cluster.peer, landed).await {
            Ok(bytes) => panic!(
                "the store holds content a stranger pushed: {:?}",
                String::from_utf8_lossy(&bytes)
            ),
            Err(Error::BlobRefused { .. }) => {
                panic!("the grant is missing, so this test proves nothing")
            }
            Err(_) => {}
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// Observe leaks which ranges of a blob a node holds. keylink hands out one raw
/// blob per grant and nothing else, so this is refused whoever asks.
#[tokio::test]
async fn an_observe_request_from_any_peer_is_refused() {
    let cluster = cluster().await;
    let id = cluster.add("observable, in principle").await;
    cluster.grant(&cluster.peer, &[id]).await;

    let store = MemStore::new();
    let conn = cluster.dial_blobs(&cluster.peer).await;
    let mut observed = Box::pin(
        store
            .remote()
            .observe(conn, ObserveRequest::new(hash_of(id))),
    );

    let first = observed.next().await;
    assert!(
        matches!(first, None | Some(Err(_))),
        "observe must be refused even on granted content, got {first:?}"
    );
}

/// Get-many asks for several blobs at once, and the requester supplies the
/// sequence. Refused for the same reason as observe.
#[tokio::test]
async fn a_get_many_request_from_any_peer_is_refused() {
    let cluster = cluster().await;
    let id = cluster.add("reachable one at a time").await;
    cluster.grant(&cluster.peer, &[id]).await;

    let store = MemStore::new();
    let conn = cluster.dial_blobs(&cluster.peer).await;
    let many = store
        .remote()
        .execute_get_many(
            conn,
            GetManyRequest::new(
                vec![hash_of(id)],
                ChunkRangesSeq::from_ranges([ChunkRanges::all()]),
            ),
        )
        .await;

    assert!(many.is_err(), "get-many must be refused, got {many:?}");
}

// ─────────────────────────────────────────────────────── diagnosable refusal

/// RFC-01 §9 Risk 5: a denied peer used to read `io: connection lost`, which
/// sends whoever finds it in a log to inspect a network that is working
/// perfectly. The refusal is on the wire as `ERR_PERMISSION`, and it has to
/// arrive as a typed error naming the content.
#[tokio::test]
async fn a_refusal_says_it_is_a_refusal_and_names_the_content() {
    let cluster = cluster().await;
    let ungranted = cluster.add("not for this peer").await;
    let ticket = cluster.add("this one is").await;
    cluster.grant(&cluster.peer, &[ticket]).await;

    let error = cluster.fetch(&cluster.peer, ungranted).await.unwrap_err();
    let Error::BlobRefused { id } = &error else {
        panic!("expected a typed refusal, got {error:?}");
    };
    assert_eq!(
        *id, ungranted,
        "the error names the content that was refused"
    );
    assert!(
        !error.to_string().contains("connection lost"),
        "a refusal must not read like a network failure: {error}"
    );
}

/// The other half of Risk 5, and the half that was genuinely broken until this
/// test existed.
///
/// A peer refused at the **request** meets a stream reset, and the code rides
/// on the error iroh-blobs hands back. A peer refused at the **connection** —
/// one holding no grant at all — meets a CONNECTION_CLOSE that no stream error
/// carries, so `GetError::iroh_error_code()` answers `None` and the error
/// renders as `io: connection lost`. That is the exact string RFC-01 §9 says
/// must stop appearing, and it was still appearing.
///
/// Both must arrive as the same typed refusal, because from the caller's side
/// they are the same event: the holder said no.
#[tokio::test]
async fn a_peer_refused_at_the_connection_reads_the_same_refusal() {
    let cluster = cluster().await;
    let id = cluster.add("granted to somebody else").await;
    cluster.grant(&cluster.peer, &[id]).await;

    // `other` holds no grant at all, so it is turned away before the provider
    // ever sees a request.
    let error = cluster
        .fetch(&cluster.other, id)
        .await
        .expect_err("a peer with no grants is refused");
    assert_refused(&error, "a refusal at the connection");
    assert!(
        !error.to_string().contains("connection lost"),
        "a refusal must not read like a network failure: {error}"
    );
}

// ──────────────────────────────────────────────────────────── the wire shape

/// `BlobId` exists because `iroh_blobs::Hash` does not implement
/// `postcard_schema::Schema`. It is only safe to own a parallel type while the
/// two encode identically — this fails the day iroh-blobs changes its `Hash`
/// encoding, which is exactly when somebody needs to know.
#[tokio::test]
async fn the_blob_id_encodes_identically_to_an_iroh_blobs_hash() {
    let store = MemStore::new();
    let tag = store
        .add_bytes(b"content to address".to_vec())
        .await
        .expect("adds");

    let ours = postcard::to_stdvec(&address_of(tag.hash)).expect("encodes a BlobId");
    let theirs = postcard::to_stdvec(&tag.hash).expect("encodes an iroh_blobs::Hash");

    assert_eq!(ours, theirs, "the two must be wire-identical");
    assert_eq!(ours.len(), 32, "and both must be the raw 32 bytes");
}

// ───────────────────────────────────────────────────────────────── behaviour

/// Bodies are capped at `MAX_HEADER`. Blobs are not: the payload never passes
/// through a header frame.
#[tokio::test]
async fn a_blob_larger_than_the_header_cap_transfers_intact() {
    let cluster = cluster().await;
    let big: Vec<u8> = (0..2 * 1024 * 1024).map(|i| (i % 251) as u8).collect();
    let id = cluster
        .blobs
        .add_bytes(big.clone())
        .await
        .expect("stores two megabytes");
    cluster.grant(&cluster.peer, &[id]).await;

    let fetched = cluster
        .fetch(&cluster.peer, id)
        .await
        .expect("a blob past the header cap still transfers");
    assert_eq!(fetched.len(), big.len());
    assert_eq!(fetched, big, "and arrives byte for byte");
}

/// Deduplication, which is one of the two reasons to adopt blobs at all.
#[tokio::test]
async fn a_blob_fetched_twice_into_one_store_costs_one_transfer() {
    let cluster = cluster().await;
    let id = cluster.add("worth caching").await;
    cluster.grant(&cluster.peer, &[id]).await;

    let local = Blobs::memory();
    let first = BlobHandle::new(&cluster.peer, cluster.server.endpoint().addr(), id)
        .fetch_into(&local)
        .await
        .expect("the first fetch transfers");
    assert!(first > 0, "the first fetch moved bytes");

    let second = BlobHandle::new(&cluster.peer, cluster.server.endpoint().addr(), id)
        .fetch_into(&local)
        .await
        .expect("the second fetch is free");
    assert_eq!(second, 0, "content already held costs no payload bytes");
}

// ────────────────────────────────────────────────────────── the reservation

/// `RESERVED_ALPNS` now holds `iroh_blobs::ALPN` itself rather than a copy of
/// its bytes. `a_reserved_alpn_cannot_be_mounted_by_hand` in `protocol.rs` pins
/// the literal; this pins the constant to the same refusal, so the two cannot
/// drift apart without one of them going red.
#[tokio::test]
async fn the_reserved_alpn_is_the_one_iroh_blobs_serves() {
    let node = local_node().await;
    let error = keylink::Server::builder(&node)
        .service(keylink::Service::foreign(
            iroh_blobs::ALPN,
            Granting {
                blobs: Blobs::memory(),
            },
        ))
        .spawn()
        .expect_err("iroh_blobs::ALPN must be refused");

    assert!(
        error.to_string().contains("reserved"),
        "the message must say why: {error}"
    );
}
