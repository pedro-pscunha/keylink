//! Blobs admission, over two real iroh endpoints on the loopback interface.
//!
//! Everything here runs against a genuine QUIC transport with no relay and no
//! discovery, so a refusal is a refusal on the wire rather than a mocked one.
//!
//! # The stand-in for generated dispatch
//!
//! RFC-01 §3.2 puts the grant inside the code the `service!` macro writes for a
//! `blob` method, so a handler that returns a [`Grant`] has already granted it.
//! That macro kind is a later commit. Until it lands, [`Granting`] below is the
//! same shape by hand: a real `ProtocolHandler`, a real accept, and a grant
//! taken from the connection iroh proved — which is the only argument
//! `Blobs::grant` accepts.
//!
//! For the same reason the mount is built here rather than by
//! `ServerBuilder::spawn`: `iroh_blobs::ALPN` is reserved, so `spawn` refuses it
//! from a user and does not yet offer it from a `Blobs`.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use keylink::prelude::*;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use iroh::endpoint::{Connection, ConnectionError};
use iroh::protocol::{AcceptError, ProtocolHandler, Router};
use iroh_blobs::protocol::{
    ChunkRanges, ChunkRangesExt, ChunkRangesSeq, GetManyRequest, GetRequest, ObserveRequest,
    PushRequest,
};
use iroh_blobs::store::mem::MemStore;
use keylink::{
    Allowlist, BlobHandle, BlobId, Blobs, Caller, Content, Context, Error, Grant, Node, ProvenPeer,
};
use n0_future::StreamExt;

mod common;
use common::local_node;

/// Where a test peer asks for the grants it wants. Stands in for an ordinary
/// keylink `blob` rpc, which is what will carry this in production.
const GRANT_ALPN: &[u8] = b"/keylink-test/grant/1";

// ────────────────────────────────────────────────────────────────── fixture

/// The stand-in for generated `blob` dispatch.
///
/// A peer asks for a list of content and the handler records a grant for each
/// one against **this connection's** proven peer. A real handler would compute
/// the list instead of being told it; nothing else about the shape differs, and
/// the argument `Blobs::grant` takes is what forces that.
#[derive(Debug, Clone)]
struct Granting {
    blobs: Blobs,
    /// The handler's own policy, exactly as a generated service carries one.
    /// `Blobs::grant` will not take anything this has not admitted.
    allow: Allowlist,
}

impl ProtocolHandler for Granting {
    async fn accept(&self, connection: Connection) -> Result<(), AcceptError> {
        let (mut send, mut recv) = connection.accept_bi().await?;
        let asked = recv
            .read_to_end(64 * 1024)
            .await
            .map_err(AcceptError::from_err)?;
        let wanted: Vec<Content> = postcard::from_bytes(&asked).map_err(AcceptError::from_err)?;

        // The ladder, in the order a real service walks it: iroh proves the
        // peer, the service's own `Allowlist` admits it, and only the value
        // that came out of `admit` can write a grant.
        let Some(admitted) = self.allow.admit(ProvenPeer::on(&connection)) else {
            connection.close(1u32.into(), b"unauthorized");
            return Ok(());
        };
        for id in wanted {
            self.blobs.grant(&admitted, id).expect("within budget");
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

async fn cluster() -> Cluster {
    cluster_over(Blobs::memory()).await
}

/// The same, over a store the test already holds.
async fn cluster_over(blobs: Blobs) -> Cluster {
    let server = local_node().await;
    let router = Router::builder(server.endpoint().clone())
        .accept(iroh_blobs::ALPN, blobs.protocol())
        .accept(
            GRANT_ALPN,
            Granting {
                blobs: blobs.clone(),
                allow: Allowlist::anyone(),
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
    /// Ask the serving node to grant `ids` to `who`, each as one blob.
    async fn grant(&self, who: &Node, ids: &[BlobId]) {
        let content: Vec<Content> = ids.iter().copied().map(Content::from).collect();
        self.grant_content(who, &content).await;
    }

    /// Ask the serving node to grant `content` to `who`.
    async fn grant_content(&self, who: &Node, content: &[Content]) {
        let conn = who
            .endpoint()
            .connect(self.server.endpoint().addr(), GRANT_ALPN)
            .await
            .expect("dials the granting protocol");
        let (mut send, mut recv) = conn.open_bi().await.expect("opens a stream");
        let asked = postcard::to_stdvec(&content.to_vec()).expect("encodes the request");
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
        self.add_bytes(content.as_bytes()).await
    }

    async fn add_bytes(&self, content: &[u8]) -> BlobId {
        self.blobs
            .add_bytes(content.to_vec())
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

/// **The ladder, end to end.** `Blobs::grant` takes an `AdmittedPeer`, and the
/// only source of one is `Allowlist::admit`. So a handler serving a peer its
/// own allowlist refuses has nothing to pass, and no grant can be written.
///
/// This is the runtime half of the claim. The compile-time half is in
/// `representability.rs`: there is no second constructor and no `From` impl, so
/// a handler cannot promote the `ProvenPeer` it already holds.
///
/// Without the ladder, this mount was a second accept path that could write an
/// authorization record from a bare `connection.remote_id()` — reaching past
/// every allowlist on the node with the compiler silent.
#[tokio::test]
async fn a_peer_no_allowlist_admits_cannot_be_granted_anything() {
    let server = local_node().await;
    let peer = local_node().await;
    let stranger = local_node().await;
    let blobs = Blobs::memory();
    let id = blobs
        .add_bytes(b"only for the admitted peer".to_vec())
        .await
        .expect("stores");

    // The granting service admits `peer` and nobody else.
    let _router = Router::builder(server.endpoint().clone())
        .accept(iroh_blobs::ALPN, blobs.protocol())
        .accept(
            GRANT_ALPN,
            Granting {
                blobs: blobs.clone(),
                allow: Allowlist::only([peer.endpoint().id()]),
            },
        )
        .spawn();

    let ask = |who: &Node| {
        let addr = server.endpoint().addr();
        let endpoint = who.endpoint().clone();
        async move {
            let conn = endpoint
                .connect(addr, GRANT_ALPN)
                .await
                .expect("dials the granting protocol");
            let (mut send, mut recv) = conn.open_bi().await.expect("opens a stream");
            let asked = postcard::to_stdvec(&vec![Content::from(id)]).expect("encodes");
            send.write_all(&asked).await.expect("writes");
            send.finish().expect("finishes");
            recv.read_to_end(64).await
        }
    };

    // The control. Without it this passes just as well when granting is broken
    // for everybody.
    assert_eq!(
        ask(&peer).await.expect("the admitted peer is answered"),
        b"granted"
    );
    let mut sink = Vec::new();
    BlobHandle::new(&peer, server.endpoint().addr(), id)
        .copy_to(&mut sink)
        .await
        .expect("an admitted peer that was granted the id can fetch it");
    assert_eq!(sink, b"only for the admitted peer");

    // The stranger is refused by the allowlist, so nothing was ever granted.
    assert!(
        ask(&stranger).await.is_err(),
        "the granting service must refuse a peer its allowlist does not admit"
    );
    let mut sink = Vec::new();
    let error = BlobHandle::new(&stranger, server.endpoint().addr(), id)
        .copy_to(&mut sink)
        .await
        .expect_err("and the blobs mount must not serve it either");
    assert_refused(&error, "a peer no allowlist admitted");
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

/// `EventMask::DEFAULT` sets `push: RequestMode::Disabled`, and up to
/// iroh-blobs 0.103.0 that field was dead: `EventSender::request` read
/// `mask.get` for every request type, so a default mount accepted writes from a
/// stranger — the write hole RFC-01 §4.2 measured. 0.103.1 made each request
/// kind read its own field. Under keylink's policy the write does not land,
/// whichever version decides it.
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

// ───────────────────────────────────────────────── verified readers and ranges

/// Content whose every byte depends on its position.
///
/// A slice taken from the wrong offset cannot coincidentally equal the right
/// one, which is what makes the unaligned-range assertions able to fail.
fn payload(len: usize) -> Vec<u8> {
    (0..len).map(|i| (i % 251) as u8).collect()
}

/// Longer than one 16 KiB bao block, and aligned to none of them: an unaligned
/// range over this has to slice a first leaf and a last leaf, not just one.
const SPANNING: usize = 70_000;

// ─────────────────────────────────────────── a holder that answers dishonestly
//
// Everything else in this file runs against a real provider on a real
// connection, because framing and flow control are transport behaviour and a
// hand-written stand-in would encode the assumptions under test. These three
// answers are the exception the goal sanctions: a wrong size, a corrupted bao
// frame and a truncated one are answers a CORRECT provider cannot produce, so
// there is no honest peer that could send them.
//
// The forgery is still real on both sides of the client. The request is parsed
// by iroh-blobs' own `Request::read_async`, the reply is a real provider's
// answer with one deliberate mutation, and the code under test is the real
// client FSM.

/// The bytes to answer one parsed request with.
///
/// Async because two rows need the answer to depend on a live provider: the
/// resume row cannot know which ranges the store will still be missing until
/// the store asks for them, so its holder relays the real request onward.
type Reply = Arc<dyn Fn(GetRequest) -> BoxFuture<Vec<u8>> + Send + Sync>;

type BoxFuture<T> = std::pin::Pin<Box<dyn std::future::Future<Output = T> + Send>>;

/// What one forging holder did, in the order it did it.
#[derive(Debug, PartialEq, Eq)]
enum Seen {
    /// A whole answer went out and the stream was finished.
    Answered,
    /// The reader went away before the answer was finished.
    Abandoned,
    /// The connection ended.
    Closed,
}

/// A blobs holder whose answer the test chooses.
#[derive(Clone)]
struct Forger {
    /// Every request this holder parsed, in order. Both the observation event
    /// and the counter: a test that expects no data request asserts on the
    /// length after the call it is watching has already returned.
    asked: Arc<Mutex<Vec<GetRequest>>>,
    seen: tokio::sync::mpsc::UnboundedSender<Seen>,
    reply: Reply,
}

impl std::fmt::Debug for Forger {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Forger").finish_non_exhaustive()
    }
}

/// Reports the end of a connection however the accept ends.
///
/// Two `?` sites below return without reaching the end of the loop, and the
/// `Forger` holds the sender for the router's whole life, so the channel never
/// closes. A test waiting on `seen()` would then wait for ever instead of
/// failing. `keylink::blobs`' own `Admitted` guard exists for exactly this,
/// under the same reason: a dropped future never reaches its next line.
struct Ended(tokio::sync::mpsc::UnboundedSender<Seen>);

impl Drop for Ended {
    fn drop(&mut self) {
        self.0.send(Seen::Closed).ok();
    }
}

impl ProtocolHandler for Forger {
    async fn accept(&self, connection: Connection) -> Result<(), AcceptError> {
        let _ended = Ended(self.seen.clone());
        loop {
            // One connection carries the size preflight and the data request on
            // two streams. Accepting until the reader goes away is what lets a
            // single forger answer both, and the loop ending is what reports
            // that the reader went away.
            let Ok((mut send, mut recv)) = connection.accept_bi().await else {
                break;
            };
            let (request, _) = iroh_blobs::protocol::Request::read_async(&mut recv)
                .await
                .map_err(AcceptError::from_err)?;
            let iroh_blobs::protocol::Request::Get(request) = request else {
                connection.close(1u32.into(), b"only get");
                break;
            };
            let reply = (self.reply)(request.clone()).await;
            self.asked
                .lock()
                .expect("the record is not poisoned")
                .push(request);
            // A failed write is not an error here, it is the observation: a
            // reader that was dropped mid-transfer stops the holder, and that
            // is what the cancellation row asserts on.
            let outcome = match send.write_all(&reply).await {
                Ok(()) => {
                    send.finish().map_err(AcceptError::from_err)?;
                    Seen::Answered
                }
                Err(_) => Seen::Abandoned,
            };
            self.seen.send(outcome).ok();
        }
        Ok(())
    }
}

/// A dishonest holder and a real client pointed at it.
struct Forgery {
    reader: Node,
    holder: iroh::EndpointAddr,
    asked: Arc<Mutex<Vec<GetRequest>>>,
    seen: tokio::sync::mpsc::UnboundedReceiver<Seen>,
    _holder: Node,
    _router: Router,
}

async fn forgery<F>(reply: impl Fn(GetRequest) -> F + Send + Sync + 'static) -> Forgery
where
    F: std::future::Future<Output = Vec<u8>> + Send + 'static,
{
    let holder = local_node().await;
    let asked = Arc::new(Mutex::new(Vec::new()));
    let (seen, received) = tokio::sync::mpsc::unbounded_channel();
    let router = Router::builder(holder.endpoint().clone())
        .accept(
            iroh_blobs::ALPN,
            Forger {
                asked: Arc::clone(&asked),
                seen,
                reply: Arc::new(move |request| Box::pin(reply(request))),
            },
        )
        .spawn();

    Forgery {
        reader: local_node().await,
        holder: holder.endpoint().addr(),
        asked,
        seen: received,
        _holder: holder,
        _router: router,
    }
}

impl Forgery {
    fn handle(&self, content: impl Into<Content>) -> BlobHandle {
        BlobHandle::new(&self.reader, self.holder.clone(), content)
    }

    fn asked(&self) -> Vec<GetRequest> {
        self.asked
            .lock()
            .expect("the record is not poisoned")
            .clone()
    }

    /// The next thing the holder did. Every branch of the accept loop reports,
    /// so this cannot outlast the holder.
    async fn seen(&mut self) -> Seen {
        self.seen
            .recv()
            .await
            .expect("the holder reports every outcome")
    }
}

/// What the client's size preflight asks for. `get_verified_size` requests the
/// last chunk, so this is how a forged answer tells the two requests apart.
fn preflight_ranges() -> ChunkRangesSeq {
    ChunkRangesSeq::from_ranges([ChunkRanges::last_chunk()])
}

/// A holder that answers the size preflight honestly and the data request with
/// whatever `forge` makes of a correct answer.
///
/// Six rows wanted this shape. Copying it six times is how a registered anchor
/// stops resolving exactly once, which already cost `HJ9` a gate failure on
/// this branch.
async fn forging(cluster: &Cluster, id: BlobId, forge: impl FnOnce(Vec<u8>) -> Vec<u8>) -> Forgery {
    let preflight = honest_answer(cluster, GetRequest::new(hash_of(id), preflight_ranges())).await;
    let data = forge(honest_answer(cluster, GetRequest::blob(hash_of(id))).await);
    forgery(move |request: GetRequest| {
        let answer = if request.ranges == preflight_ranges() {
            preflight.clone()
        } else {
            data.clone()
        };
        std::future::ready(answer)
    })
    .await
}

/// The exact bytes a correct provider answers `request` with.
///
/// Captured from the real store rather than encoded by hand, so the honest
/// prefix of every forged answer is real bao that the real client accepts.
async fn honest_answer(cluster: &Cluster, request: GetRequest) -> Vec<u8> {
    ask(
        cluster.peer.endpoint().clone(),
        cluster.server.endpoint().addr(),
        request,
    )
    .await
}

/// The same question, asked from an owned endpoint so a forging holder can
/// relay onward from inside its reply.
async fn ask(from: iroh::Endpoint, holder: iroh::EndpointAddr, request: GetRequest) -> Vec<u8> {
    let connection = from
        .connect(holder, iroh_blobs::ALPN)
        .await
        .expect("dials the blobs protocol");
    let (mut send, mut recv) = connection.open_bi().await.expect("opens a request stream");
    let asked = postcard::to_stdvec(&iroh_blobs::protocol::Request::Get(request))
        .expect("encodes a get request");
    send.write_all(&asked).await.expect("writes the request");
    send.finish().expect("finishes the request");
    recv.read_to_end(16 * 1024 * 1024)
        .await
        .expect("reads the whole answer")
}

/// Two reads from one address must not share a stream.
///
/// The clone is what a caller reaches for when one address feeds two consumers
/// — an HTTP range request beside a full download, in the gateway's case. If
/// they shared transport state, each would see the other's bytes; reading them
/// one after the other would hide exactly that, so they are interleaved here.
#[tokio::test]
async fn blob_handle_clone_opens_independent_full_reads() {
    let cluster = cluster().await;
    let content = payload(SPANNING);
    let id = cluster.add_bytes(&content).await;
    cluster.grant(&cluster.peer, &[id]).await;

    let handle = BlobHandle::new(&cluster.peer, cluster.server.endpoint().addr(), id);
    let clone = handle.clone();

    let mut first = handle.open().await.expect("opens the first read");
    let mut second = clone.open().await.expect("opens the second read");
    assert_eq!(first.total_len(), content.len() as u64);
    assert_eq!(second.total_len(), content.len() as u64);

    let mut left = Vec::new();
    let mut right = Vec::new();
    loop {
        let a = first.next_chunk().await.expect("the first read continues");
        let b = second
            .next_chunk()
            .await
            .expect("the second read continues");
        match (a, b) {
            (None, None) => break,
            (a, b) => {
                left.extend_from_slice(a.as_deref().unwrap_or_default());
                right.extend_from_slice(b.as_deref().unwrap_or_default());
            }
        }
    }

    assert_eq!(left, content, "the first read is complete");
    assert_eq!(right, content, "and so is the second, interleaved with it");
}

/// The data response's size must be the one the preflight verified.
///
/// The assertion is that `open` FAILS, not merely that reading fails. Watched
/// red with `scripts/break-once.sh` (row `HK1`) by replacing
/// `if announced != total {`
/// in `keylink/src/blobs.rs` with `if false {`, and the reader that came back
/// is what says why the comparison earns its place:
///
/// ```text
/// BlobReader { id: BlobId(5a8ef06db0fc4e6c27f6e4a44e333916934b860d747d8d65f56df4597f0421bf), total_len: 70000, selected_len: 70000, emitted: 0 }
/// ```
///
/// It announces 70000 because `total` still comes from the preflight — while
/// its decoder was built on the forged 1024, so it can never deliver them. The
/// forgery is still caught eventually: one chunk cannot pass bao validation
/// against a 70 KiB root hash. But by then a gateway has read `total_len`,
/// sent `200` with `Content-Length: 70000`, and the only report left is a
/// broken body. The comparison moves that from a failed download to a refused
/// open.
#[tokio::test]
async fn blob_open_rejects_data_size_different_from_verified_size() {
    let cluster = cluster().await;
    let content = payload(SPANNING);
    let id = cluster.add_bytes(&content).await;
    cluster.grant(&cluster.peer, &[id]).await;

    // Only the eight size bytes move. Everything the client checks before the
    // comparison still passes, so the comparison is the only thing that can
    // fail first.
    let holder = forging(&cluster, id, |mut honest| {
        honest[..8].copy_from_slice(&1024u64.to_le_bytes());
        honest
    })
    .await;

    let error = holder
        .handle(id)
        .open()
        .await
        .expect_err("a data size the preflight did not verify yields no reader");
    // Matched on the type, not on the message. An assertion over a format
    // string is a contract on prose, and the numbers are what a gateway logs.
    assert!(
        matches!(
            error,
            Error::BlobUnverified {
                fault: keylink::BlobFault::Size {
                    announced: 1024,
                    verified: 70_000
                },
                ..
            }
        ),
        "the refusal names both sizes: {error:?}"
    );
}

/// A copy that fails must say so, never return a shorter success.
///
/// **The first version of this row was measured and thrown away.** It forged
/// the data size and asserted that `copy_to` refused with an empty sink, on the
/// reasoning that a detached copy path would copy happily. Under `HK1`'s break
/// it came back GREEN, `exit=0`: bao rejects a 1024-byte tree against a 70 KiB
/// root hash on its own, so `copy_to` failed and wrote nothing either way. The
/// forged size is a case a second checker already covers, and a guard another
/// layer also enforces is not a guard on this one.
///
/// What has no second checker is the shape below. A truncated answer leaves
/// `copy_to` holding real, verified, short content, and reporting that as a
/// success is the one outcome every layer above would believe.
///
/// Watched red with `scripts/break-once.sh` (row `HK7`) by replacing
/// `while let Some(chunk) = self.next_chunk().await? {`
/// in `keylink/src/blobs.rs` with `while let Ok(Some(chunk)) =
/// self.next_chunk().await {`.
#[tokio::test]
async fn blob_copy_to_reports_the_readers_failure_instead_of_a_short_success() {
    let cluster = cluster().await;
    let content = payload(SPANNING);
    let id = cluster.add_bytes(&content).await;
    cluster.grant(&cluster.peer, &[id]).await;

    let holder = forging(&cluster, id, |honest| {
        let half = honest.len() / 2;
        honest[..half].to_vec()
    })
    .await;

    let mut sink = Vec::new();
    holder
        .handle(id)
        .copy_to(&mut sink)
        .await
        .expect_err("a copy reports the read's failure");
    assert!(
        sink.len() < content.len(),
        "control: it really did stop short, so a silent success was available"
    );
}

/// Read to EOF, keeping whatever arrived before a failure.
async fn read_all(reader: &mut keylink::BlobReader) -> (Vec<u8>, Option<Error>) {
    let mut out = Vec::new();
    loop {
        match reader.next_chunk().await {
            Ok(Some(chunk)) => out.extend_from_slice(&chunk),
            Ok(None) => return (out, None),
            Err(error) => return (out, Some(error)),
        }
    }
}

/// One flipped payload byte must break the read, and must never be handed on.
///
/// **Dependency-path evidence, and it carries no keylink anchor.** bao owns
/// this rejection, not keylink, and the plan says so. What is worth measuring
/// is that driving the FSM by hand did not lose it: the previous
/// implementation went through `get_blob`, which validated on keylink's behalf,
/// and `BlobReader` walks `fsm` directly.
///
/// The second assertion is the one with teeth. An error alone would also
/// follow from a reader that emitted the corrupt bytes and complained
/// afterwards, which is the failure that matters — a gateway has already sent
/// them by then.
#[tokio::test]
async fn blob_open_rejects_corrupt_bao_from_real_fsm() {
    let cluster = cluster().await;
    let content = payload(SPANNING);
    let id = cluster.add_bytes(&content).await;
    cluster.grant(&cluster.peer, &[id]).await;

    // The last byte of the answer is payload in the final leaf, so every
    // earlier leaf still validates and the break lands as late as it can.
    let holder = forging(&cluster, id, |mut honest| {
        let last = honest.len() - 1;
        honest[last] ^= 0xff;
        honest
    })
    .await;

    let mut reader = holder
        .handle(id)
        .open()
        .await
        .expect("the size header is honest, so the reader opens");
    let (read, error) = read_all(&mut reader).await;

    assert!(error.is_some(), "a corrupt leaf must fail the read");
    assert!(
        content.starts_with(&read),
        "and nothing that failed verification may reach the caller"
    );
    assert!(
        read.len() < content.len(),
        "control: the corrupt tail really was withheld"
    );
}

/// An answer that stops early must not read as a complete short blob.
///
/// Also dependency-path evidence. The distinction it draws is the one
/// `Error::BodyTruncated` draws for bodies: a transfer that ended before its
/// content did is a failure, never a shorter success. Both the FSM's own EOF
/// handling and `Selection::finish` can report it; the assertion names neither,
/// because either is correct and pinning one would fail on a dependency bump
/// that changed nothing about the guarantee.
#[tokio::test]
async fn blob_open_rejects_truncated_bao_from_real_fsm() {
    let cluster = cluster().await;
    let content = payload(SPANNING);
    let id = cluster.add_bytes(&content).await;
    cluster.grant(&cluster.peer, &[id]).await;

    let holder = forging(&cluster, id, |honest| {
        let half = honest.len() / 2;
        honest[..half].to_vec()
    })
    .await;

    let mut reader = holder
        .handle(id)
        .open()
        .await
        .expect("the size header is honest, so the reader opens");
    assert_eq!(
        reader.total_len(),
        content.len() as u64,
        "the length it promised is the verified one"
    );
    let (read, error) = read_all(&mut reader).await;

    assert!(error.is_some(), "a truncated answer must fail the read");
    assert!(
        read.len() < content.len(),
        "control: it really did stop short, so a silent success was available"
    );
}

/// Read to EOF and require it to get there.
async fn collect(reader: &mut keylink::BlobReader) -> Vec<u8> {
    let (read, error) = read_all(reader).await;
    assert!(error.is_none(), "the read reaches EOF: {error:?}");
    read
}

/// One serving cluster with content, and a handle onto it.
async fn served(content: &[u8]) -> (Cluster, BlobHandle) {
    let cluster = cluster().await;
    let id = cluster.add_bytes(content).await;
    cluster.grant(&cluster.peer, &[id]).await;
    let handle = BlobHandle::new(&cluster.peer, cluster.server.endpoint().addr(), id);
    (cluster, handle)
}

/// A whole-blob read reports the verified length and stops at it.
#[tokio::test]
async fn blob_reader_reads_verified_full_content() {
    let content = payload(SPANNING);
    let (_cluster, handle) = served(&content).await;

    assert_eq!(
        handle.verified_len().await.expect("the size is verified"),
        content.len() as u64
    );

    let mut reader = handle.open().await.expect("opens a full read");
    assert_eq!(reader.total_len(), content.len() as u64);
    assert_eq!(
        reader.selected_len(),
        content.len() as u64,
        "a full read selects everything"
    );

    assert_eq!(collect(&mut reader).await, content);
    assert!(
        reader
            .next_chunk()
            .await
            .expect("EOF is idempotent")
            .is_none(),
        "and stays at EOF"
    );
}

/// One 16 KiB block, asked for on its own boundaries.
///
/// The block size is the unit that arrives, not the unit that is asked for:
/// `IROH_BLOCK_SIZE` is `BlockSize::from_chunk_log(4)`, so a leaf is 16 KiB
/// while range selection is in 1024-byte chunks. An aligned range is therefore
/// the case where the two happen to agree and no slicing is required — which is
/// exactly why it cannot stand in for the unaligned one below.
#[tokio::test]
async fn blob_reader_reads_aligned_range() {
    let content = payload(SPANNING);
    let (_cluster, handle) = served(&content).await;

    let want = 16_384..32_768;
    let mut reader = handle
        .open_range(want.clone())
        .await
        .expect("opens an aligned range");

    assert_eq!(
        reader.total_len(),
        content.len() as u64,
        "the whole-blob length is still the verified one"
    );
    assert_eq!(reader.selected_len(), 16_384);
    assert_eq!(
        collect(&mut reader).await,
        &content[want.start as usize..want.end as usize]
    );
}

/// A range that lands inside a leaf at both ends.
///
/// Both overhangs are real and both are 16 KiB wide, so writing whole leaves
/// would overrun by up to 32 KiB. `payload` makes every byte depend on its
/// position, so an output of the right LENGTH taken from the wrong offset still
/// fails this comparison.
///
/// Both clamps are watched, because HK2 alone would prove only one of them.
///
/// Head, row `HK2`: replacing
/// `let head = self.start.max(offset);`
/// in `keylink/src/blobs.rs` with `let head = offset;` reports
/// `Incomplete { received: 40000, selected: 39000 }`.
///
/// Tail, row `HK8`: replacing
/// `let tail = self.end().min(stop);`
/// with `let tail = stop;` reports
/// `Incomplete { received: 39960, selected: 39000 }`.
///
/// The overhangs are 1000 and 960 bytes only because of where this range
/// falls. Each is up to 16 KiB in general, and each is the accidental
/// off-by-one the separated counters exist to prevent.
#[tokio::test]
async fn blob_reader_slices_unaligned_first_and_last_chunks() {
    let content = payload(SPANNING);
    let (_cluster, handle) = served(&content).await;

    let want = 1_000..40_000;
    let mut reader = handle
        .open_range(want.clone())
        .await
        .expect("opens an unaligned range");

    assert_eq!(reader.selected_len(), 39_000);
    assert_eq!(
        collect(&mut reader).await,
        &content[want.start as usize..want.end as usize]
    );
}

/// Zero bytes is content, not an error, and nothing about it may underflow.
///
/// The arithmetic that could: `last_chunk()` is `ChunkNum(u64::MAX)..`, and
/// the range check subtracts. codex found no upstream end-to-end test for the
/// 0-byte case, so this proves it here rather than assuming iroh-blobs does.
#[tokio::test]
async fn blob_reader_handles_empty_blob_without_underflow() {
    let (_cluster, handle) = served(&[]).await;

    assert_eq!(
        handle.verified_len().await.expect("a zero size verifies"),
        0
    );

    let mut reader = handle.open().await.expect("opens an empty read");
    assert_eq!(reader.total_len(), 0);
    assert_eq!(reader.selected_len(), 0);
    assert!(
        reader
            .next_chunk()
            .await
            .expect("an empty read succeeds")
            .is_none(),
        "a full read of nothing is EOF, not a failure"
    );

    for want in [0..1, 0..u64::MAX, 1..2, u64::MAX - 1..u64::MAX] {
        assert!(
            handle.open_range(want.clone()).await.is_err(),
            "{want:?} cannot be satisfied by a blob with no bytes"
        );
    }
}

/// A range the content cannot satisfy must cost the holder nothing.
///
/// The holder here replays a real provider's captured answers, so the only
/// thing it adds is a record of what was asked. That record is the proof: an
/// unsatisfiable range must be refused against the verified size, not sent and
/// then discovered — a data request costs the holder a stream and a bao
/// traversal, and a caller who can spend those by asking for `500..100` has
/// found a way to make somebody else work.
///
/// Two guards, watched separately, because they are refused in different
/// places and only one of them needs the size.
///
/// Out of bounds, row `HK3`: replacing
/// `if range.end > total {`
/// in `keylink/src/blobs.rs` with `if false {` reddens on `70000..70001` and
/// on nothing earlier. An empty or reversed range converts to an empty chunk
/// set, which the FSM refuses on its own, so out of bounds is the case this
/// half uniquely owns — and it is the one an HTTP `Range` header reaches first.
///
/// Empty and reversed, row `HK10`: replacing
/// `let Some(len) = range.end.checked_sub(range.start).filter(|len| *len > 0) else {`
/// with `let Some(len) = Some(range.end.wrapping_sub(range.start)) else {`
/// takes the holder from 5 requests to 10 — every free refusal turns into a
/// handshake and a probe. An earlier shape kept the guard beside a plain `-`
/// and its break panicked with `attempt to subtract with overflow` before this
/// assertion could run, which is why the guard is now the subtraction itself.
#[tokio::test]
#[expect(
    clippy::reversed_empty_ranges,
    reason = "a reversed range is one of the inputs under test, not a mistake"
)]
async fn blob_reader_rejects_invalid_ranges_before_data_request() {
    let cluster = cluster().await;
    let content = payload(SPANNING);
    let id = cluster.add_bytes(&content).await;
    cluster.grant(&cluster.peer, &[id]).await;
    let len = content.len() as u64;

    // Honest bytes: this holder is a request RECORD, not a liar.
    let holder = forging(&cluster, id, |honest| honest).await;
    let handle = holder.handle(id);

    // Empty and reversed need no size to refuse, so they cost the holder
    // nothing at all — not even a handshake.
    let free = [0..0, 40..40, 500..100, len..len, u64::MAX..u64::MAX];
    // Out of bounds needs the verified size, so each of these buys one probe
    // and must stop there.
    let probed = [len..len + 1, 0..len + 1, (len - 1)..u64::MAX];

    for want in free.iter().chain(probed.iter()) {
        assert!(
            handle.open_range(want.clone()).await.is_err(),
            "{want:?} is not inside {len} verified bytes"
        );
    }

    // Control: the holder does answer data requests, so a harness that silently
    // never asked would not be what makes the counts below hold.
    let mut reader = handle.open().await.expect("a whole-blob read is served");
    assert_eq!(collect(&mut reader).await.len(), content.len());

    let asked = holder.asked();
    let data = asked
        .iter()
        .filter(|request| request.ranges != preflight_ranges())
        .count();
    assert_eq!(
        data, 1,
        "only the control read reached the holder as a data request: {asked:?}"
    );
    assert_eq!(
        asked.len(),
        probed.len() + 2,
        "one probe per out-of-bounds range, plus the control's probe and data \
         request — and nothing at all for the {} that need no size: {asked:?}",
        free.len()
    );
}

/// Abandoning a reader must stop the holder, not just stop reading.
///
/// The payload is deliberately larger than a QUIC stream's receive window, so
/// the holder is still inside `write_all` when the reader goes away. On a small
/// blob the whole answer fits in the window before the first `next_chunk`
/// returns, and `Seen::Abandoned` would be unreachable however the reader
/// behaved — the test would then be measuring the window, not the cancellation.
///
/// The window is quinn's default and keylink overrides no transport config
/// here: `STREAM_RWND = MAX_STREAM_BANDWIDTH / 1000 * EXPECTED_RTT` is
/// `1_250_000` bytes at `quinn-proto-0.11.15/src/config/transport.rs:355-366`.
/// 4 MiB is 3.4 times that. The constant cannot be imported, so it is recorded
/// here rather than derived, and a quinn bump that raised it past 4 MiB would
/// turn this row green for the wrong reason.
///
/// **It proves that dropping cancels; it does not attribute which signal did
/// it.** Two leave at the same instant — `STOP_SENDING` from the dropped
/// `RecvStream`, and the connection close from the last `Connection` handle
/// going — and either alone fails the holder's write. Ownership of both is the
/// design; the assertion below covers the pair, not one of them.
#[tokio::test]
async fn dropping_blob_reader_cancels_get_and_allows_reopen() {
    let cluster = cluster().await;
    let content = payload(4 * 1024 * 1024);
    let id = cluster.add_bytes(&content).await;
    cluster.grant(&cluster.peer, &[id]).await;

    // Honest bytes again: this holder is an observation point.
    let mut holder = forging(&cluster, id, |honest| honest).await;
    let handle = holder.handle(id);

    let mut first = handle.open().await.expect("opens the first read");
    assert!(
        first.next_chunk().await.expect("bytes arrive").is_some(),
        "the transfer is under way"
    );
    assert_eq!(
        holder.seen().await,
        Seen::Answered,
        "the size preflight completed"
    );

    drop(first);
    assert_eq!(
        holder.seen().await,
        Seen::Abandoned,
        "the holder stopped part way through the answer"
    );
    assert_eq!(
        holder.seen().await,
        Seen::Closed,
        "and the connection ended"
    );

    let mut second = handle.open().await.expect("a later read reopens");
    assert_eq!(collect(&mut second).await, content, "and completes");
}

/// Whether an interrupted `fetch_into` really resumes.
///
/// A characterization test, and it is here because the rustdoc on `fetch_into`
/// promises resume and nothing measured it. The holder relays every request to
/// the real provider and cuts the first data answer in half, so the first fetch
/// fails with a genuinely partial store and the second asks the same real
/// provider for whatever is left.
///
/// The number that decides it is the second fetch's payload count. Equal to the
/// content length means nothing was kept and the promise is wrong; smaller
/// means the store asked only for the suffix.
///
/// Measured, by tightening the bound to `second < 1` and reading the failure:
/// **37232 of 70000 bytes**. So the store kept the 32768 bytes it had already
/// verified — two whole 16 KiB blocks — and the rustdoc promise stands as
/// written.
#[tokio::test]
async fn interrupted_fetch_into_resumes_from_store_progress() {
    let cluster = cluster().await;
    let content = payload(SPANNING);
    let id = cluster.add_bytes(&content).await;
    cluster.grant(&cluster.peer, &[id]).await;

    let from = cluster.peer.endpoint().clone();
    let to = cluster.server.endpoint().addr();
    let cut = Arc::new(std::sync::atomic::AtomicBool::new(true));
    let holder = forgery(move |request: GetRequest| {
        let from = from.clone();
        let to = to.clone();
        let cut = Arc::clone(&cut);
        async move {
            let answer = ask(from, to, request.clone()).await;
            if request.ranges != preflight_ranges()
                && cut.swap(false, std::sync::atomic::Ordering::SeqCst)
            {
                answer[..answer.len() / 2].to_vec()
            } else {
                answer
            }
        }
    })
    .await;

    let local = Blobs::memory();
    let handle = holder.handle(id);
    assert!(
        handle.fetch_into(&local).await.is_err(),
        "an answer that stops half way does not complete a fetch"
    );

    let second = handle
        .fetch_into(&local)
        .await
        .expect("the second fetch completes");
    assert!(
        second > 0,
        "control: the first fetch did not quietly finish, so there was a suffix to ask for"
    );
    assert!(
        second < content.len() as u64,
        "resume asks only for what the store is missing, not {second} of {} bytes",
        content.len()
    );
}

/// The three ranges an off-by-one reaches first.
#[tokio::test]
async fn blob_reader_reads_first_last_and_single_byte_ranges() {
    let content = payload(SPANNING);
    let (_cluster, handle) = served(&content).await;
    let len = content.len() as u64;

    for want in [0..1, (len - 1)..len, 20_000..20_001, 0..len] {
        let mut reader = handle
            .open_range(want.clone())
            .await
            .unwrap_or_else(|error| panic!("opens {want:?}: {error}"));
        assert_eq!(reader.selected_len(), want.end - want.start, "{want:?}");
        assert_eq!(
            collect(&mut reader).await,
            &content[want.start as usize..want.end as usize],
            "{want:?}"
        );
    }
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
                allow: Allowlist::anyone(),
            },
        ))
        .spawn()
        .expect_err("iroh_blobs::ALPN must be refused");

    assert!(
        error.to_string().contains("reserved"),
        "the message must say why: {error}"
    );
}

// ═══════════════════════════════════════════════════ the `blob` method kind
//
// Everything above drives the mount by hand, because the macro kind did not
// exist. These drive it the way a user will: declare `blob`, return an address,
// and let the generated dispatch record the grant.

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, postcard_schema::Schema)]
pub struct Manual {
    pub pages: u32,
}

keylink::service! {
    service Catalog;

    rpc  Ping() -> String;
    blob Datasheet(sku: String) -> Manual;
}

// A second service that also serves content, for the one-store-per-node checks.
keylink::service! {
    service Depot;

    blob Parcel(sku: String) -> Manual;
}

// A service with no `blob` method at all. `DECLARES_BLOBS` is false here, and
// that is what `a_service_with_no_blob_methods_needs_no_store` pins.
keylink::service! {
    service Plain;

    rpc Echo(word: String) -> String;
}

/// The handler names no ledger, no ALPN and no connection. It returns a content
/// address, and returning it IS the grant.
#[derive(Debug, Clone)]
struct Warehouse {
    published: BlobId,
    /// In the same store, never returned by any method. Nothing should reach it.
    unpublished: BlobId,
}

impl catalog::Handler for Warehouse {
    async fn ping(&self, _cx: &Context) -> String {
        "pong".to_owned()
    }

    async fn datasheet(&self, _cx: &Context, _sku: String) -> (Manual, Grant) {
        (Manual { pages: 42 }, self.published.into())
    }
}

/// The actor twin. `blob` generates an `Actor` method just as it generates a
/// `Handler` one — which is exactly why the test below exists: the method is
/// reachable, and the address it returns is worth nothing.
impl catalog::Actor for Warehouse {
    async fn ping(&mut self, _cx: Caller<'_>) -> String {
        "pong".to_owned()
    }

    async fn datasheet(&mut self, _cx: Caller<'_>, _sku: String) -> (Manual, Grant) {
        (Manual { pages: 42 }, self.published.into())
    }
}

impl depot::Handler for Warehouse {
    async fn parcel(&self, _cx: &Context, _sku: String) -> (Manual, Grant) {
        (Manual { pages: 1 }, self.published.into())
    }
}

#[derive(Debug, Clone)]
struct Plain;

impl plain::Handler for Plain {
    async fn echo(&self, _cx: &Context, word: String) -> String {
        word
    }
}

struct Catalogue {
    client: catalog::Client,
    warehouse: Warehouse,
    server_addr: iroh::EndpointAddr,
    _server: keylink::Server,
    _nodes: [Node; 2],
}

async fn catalogue() -> Catalogue {
    let server = local_node().await;
    let caller = local_node().await;
    let blobs = Blobs::memory();
    let warehouse = Warehouse {
        published: blobs
            .add_bytes(b"the published datasheet".to_vec())
            .await
            .expect("stores"),
        unpublished: blobs
            .add_bytes(b"never handed to anybody".to_vec())
            .await
            .expect("stores"),
    };

    // No hand-rolled mount: `spawn` sees the store this service carries and
    // mounts `iroh_blobs::ALPN` itself, with the ledger attached.
    let running = keylink::Server::builder(&server)
        .service(
            catalog::service(warehouse.clone(), Allowlist::anyone())
                .with_blobs(&blobs)
                .build(),
        )
        .spawn()
        .expect("one service with a store");

    let server_addr = server.endpoint().addr();
    let client = catalog::Client::connect(&caller, server_addr.clone())
        .await
        .expect("dials the catalog");

    Catalogue {
        client,
        warehouse,
        server_addr,
        _server: running,
        _nodes: [server, caller],
    }
}

/// The whole feature, at the call site. No ALPN, no hash, no second dial, no
/// ledger call — and the mount was never written by hand.
#[tokio::test]
async fn a_blob_method_hands_back_content_the_caller_can_fetch() {
    let cluster = catalogue().await;

    let (manual, handle) = cluster
        .client
        .datasheet("ADU-014".to_owned())
        .await
        .expect("the blob rpc answers");
    assert_eq!(manual.pages, 42);

    let mut sink = Vec::new();
    let copied = handle
        .copy_to(&mut sink)
        .await
        .expect("the content follows");
    assert_eq!(sink, b"the published datasheet");
    assert_eq!(copied, sink.len() as u64);
}

/// The unary path still works beside it, on the same connection.
#[tokio::test]
async fn a_blob_method_and_a_unary_method_share_one_service() {
    let cluster = catalogue().await;
    assert_eq!(cluster.client.ping().await.unwrap(), "pong");
    let (manual, _handle) = cluster.client.datasheet("x".to_owned()).await.unwrap();
    assert_eq!(manual.pages, 42);
}

/// The grant covers the address the handler returned and nothing else, even
/// though both blobs sit in one store and the peer is fully allowlisted.
#[tokio::test]
async fn the_grant_covers_the_returned_address_and_nothing_else() {
    let cluster = catalogue().await;
    let (_manual, handle) = cluster.client.datasheet("x".to_owned()).await.unwrap();

    // The control: what the handler returned is fetchable.
    let mut sink = Vec::new();
    handle.copy_to(&mut sink).await.expect("granted");
    assert_eq!(sink, b"the published datasheet");

    // The sibling in the same store, whose address the caller somehow knows.
    let mut sink = Vec::new();
    let error = BlobHandle::new(
        &cluster._nodes[1],
        cluster.server_addr.clone(),
        cluster.warehouse.unpublished,
    )
    .copy_to(&mut sink)
    .await
    .expect_err("content no handler returned to this peer");
    assert_refused(&error, "an address the handler never returned");
}

/// A peer that never made the call holds no grant, even for the same address.
/// This is what "returning it IS the grant" has to mean: per peer, not per
/// blob.
#[tokio::test]
async fn a_peer_that_never_called_holds_no_grant_for_the_same_address() {
    let cluster = catalogue().await;
    let (_manual, handle) = cluster.client.datasheet("x".to_owned()).await.unwrap();
    let id = handle.id();

    let mut sink = Vec::new();
    handle
        .copy_to(&mut sink)
        .await
        .expect("the caller was granted it");

    // A third node, allowlisted by the service, that simply never asked.
    let bystander = local_node().await;
    let mut sink = Vec::new();
    let error = BlobHandle::new(&bystander, cluster.server_addr.clone(), id)
        .copy_to(&mut sink)
        .await
        .expect_err("a peer that never called must hold no grant");
    assert_refused(&error, "a peer that never made the call");
}

/// The store is reachable through the handle the ordinary way too.
#[tokio::test]
async fn a_blob_method_result_can_be_fetched_into_a_store() {
    let cluster = catalogue().await;
    let (_manual, handle) = cluster.client.datasheet("x".to_owned()).await.unwrap();

    let local = Blobs::memory();
    let moved = handle
        .fetch_into(&local)
        .await
        .expect("fetches into a store");
    assert!(moved > 0, "the first fetch moved bytes");
}

// ───────────────────────────────────────────────── configuration is checked

/// `DECLARES_BLOBS` is what makes this able to fire. Without the macro half it
/// would be a check whose flag is always false.
#[tokio::test]
async fn a_service_that_declares_blob_methods_without_a_store_is_refused() {
    let node = local_node().await;
    let warehouse = Warehouse {
        published: Blobs::memory()
            .add_bytes(b"x".to_vec())
            .await
            .expect("stores"),
        unpublished: Blobs::memory()
            .add_bytes(b"y".to_vec())
            .await
            .expect("stores"),
    };

    let error = keylink::Server::builder(&node)
        .service(catalog::service(warehouse, Allowlist::anyone()).build())
        .spawn()
        .expect_err("a blob service with no store must be refused at startup");

    assert!(
        matches!(
            error,
            Error::Config(keylink::ConfigError::MissingBlobStore { .. })
        ),
        "expected MissingBlobStore, got {error:?}"
    );
}

/// The control. The check must not fire for a service that serves no content —
/// otherwise every existing service would need a store it never uses.
#[tokio::test]
async fn a_service_with_no_blob_methods_needs_no_store() {
    let node = local_node().await;
    let _running = keylink::Server::builder(&node)
        .service(plain::service(Plain, Allowlist::anyone()).build())
        .spawn()
        .expect("a service with no blob methods needs no store");
}

/// One node has one blobs ALPN, so it can serve one store.
#[tokio::test]
async fn two_services_with_different_blob_stores_are_refused() {
    let node = local_node().await;
    let one = Blobs::memory();
    let two = Blobs::memory();
    let warehouse = Warehouse {
        published: one.add_bytes(b"x".to_vec()).await.expect("stores"),
        unpublished: one.add_bytes(b"y".to_vec()).await.expect("stores"),
    };

    let error = keylink::Server::builder(&node)
        .service(
            catalog::service(warehouse.clone(), Allowlist::anyone())
                .with_blobs(&one)
                .build(),
        )
        .service(
            depot::service(warehouse, Allowlist::anyone())
                .with_blobs(&two)
                .build(),
        )
        .spawn()
        .expect_err("two different stores on one node must be refused");

    assert!(
        matches!(
            error,
            Error::Config(keylink::ConfigError::ConflictingBlobStores)
        ),
        "expected ConflictingBlobStores, got {error:?}"
    );
}

/// The control beside it. Sharing ONE store between two services is the normal
/// case and must still work — without this, the test above passes just as well
/// when any two blob services are refused.
#[tokio::test]
async fn two_services_sharing_one_blob_store_are_allowed() {
    let node = local_node().await;
    let shared = Blobs::memory();
    let warehouse = Warehouse {
        published: shared.add_bytes(b"x".to_vec()).await.expect("stores"),
        unpublished: shared.add_bytes(b"y".to_vec()).await.expect("stores"),
    };

    let _running = keylink::Server::builder(&node)
        .service(
            catalog::service(warehouse.clone(), Allowlist::anyone())
                .with_blobs(&shared)
                .build(),
        )
        .service(
            depot::service(warehouse, Allowlist::anyone())
                .with_blobs(&shared)
                .build(),
        )
        .spawn()
        .expect("two services may share one store");
}

// ───────────────────────────────────────────────────────── the wire contract

/// A `blob` method must be visible on the wire, because the response carries a
/// `BlobId` that a peer built before the method existed cannot decode.
///
/// Written against a private shape rather than by moving
/// `protocol.rs`'s `PINNED` literal. CONTRIBUTING.md governs that literal:
/// decide whether a change SHOULD be wire-visible, and only then update it.
/// Adding a kind to the macro does not change the `probe` service, so nothing
/// there should move — and if it did, that would be the bug this style of test
/// exists to keep visible.
#[tokio::test]
async fn adding_a_blob_method_moves_the_alpn() {
    // `Catalog` as it would have been with `Datasheet` declared `rpc`: the same
    // name, the same arguments, the same return type, and no address.
    #[derive(Debug, Clone, serde::Serialize, serde::Deserialize, postcard_schema::Schema)]
    #[allow(dead_code)]
    enum ResponseWithoutBlob {
        Ping(String),
        Datasheet(Manual),
    }

    #[derive(postcard_schema::Schema)]
    #[allow(dead_code)]
    struct ContractBefore {
        request: keylink::Envelope<catalog::Request>,
        response: keylink::Envelope<keylink::WireResponse<ResponseWithoutBlob>>,
    }

    const P: &str = concat!(env!("CARGO_PKG_NAME"), "/Catalog");
    assert_ne!(
        keylink::derive_alpn::<ContractBefore>(P),
        catalog::alpn().to_vec(),
        "a blob method must move the ALPN: the response carries a BlobId, and a \
         peer built before it would decode the address as part of the reply"
    );
}

/// The other half of the same claim: the address on the wire is the raw 32
/// bytes beside the reply, not a wrapper. `Response` is what the contract
/// hashes, so this pins what the hash is over.
#[tokio::test]
async fn a_blob_response_carries_the_reply_and_the_address_together() {
    let store = MemStore::new();
    let tag = store.add_bytes(b"addressed".to_vec()).await.expect("adds");
    let id = address_of(tag.hash);

    let ours = postcard::to_stdvec(&(Manual { pages: 42 }, id)).expect("encodes the pair");
    let mut expected = postcard::to_stdvec(&Manual { pages: 42 }).expect("encodes the reply");
    expected.extend_from_slice(tag.hash.as_bytes());

    assert_eq!(
        ours, expected,
        "a blob reply is its value followed by 32 raw hash bytes"
    );
}

// ═══════════════════════════════════════════ the actor path, ruled back IN
//
// RFC-01 §11.2a, answered a second time and differently. Putting the `BlobId`
// in the per-actor payload was right for the ALPN, and it ALSO put the address
// out of reach of the granting seam — because the keyed reply was an opaque
// `ActorReply` the actor system never decoded, and `serve_one` reads a grant off
// the reply through `Granting`.
//
// So `blob` worked on plain services and on a singleton actor served with
// `Handle::service`, and NOT through the keyed system. Two tests pinned that:
// `ActorRef` had no `datasheet` method, and the address reachable the hard way
// was refused on fetch. Both are deleted here, deliberately, because both stated
// a limitation that no longer exists.
//
// A keyed actor answers with the service's own typed `Response`. `Granting` is
// the same generated impl the singleton path uses, `serve_one` reads it in the
// same place, and the grant is recorded before the reply is written. The
// limitation was a consequence of payload opacity, not of keyed addressing —
// which is what removing the opacity demonstrates.
//
// The outcome that would be unacceptable is unchanged: appearing to work while
// never granting. So this pins the grant end to end, over two real nodes, rather
// than asserting that a method exists.

/// A `blob` method on a KEYED actor hands out an address the peer can fetch.
///
/// The whole chain in one test, because every link is new: the generated
/// `ActorRef::datasheet` exists at all; the reply is typed, so `Granting` can see
/// the address; `registry.service(..).with_blobs(..)` puts the store where
/// `serve_one` can record against it; and the fetch then succeeds on the blobs
/// connection.
///
/// The unpublished id in the same store is the control. It proves the grant is
/// per-address rather than "this peer may fetch anything", which is the failure
/// a test that only fetched the granted id would pass.
///
/// Made red by `scripts/break-once.sh keylink/src/macros.rs`, replacing the
/// generated `Granting::granted` arm's
/// `$crate::__keylink_granted_id!([<$kind>], __value)` with
/// `{ let _ = __value; ::core::option::Option::None }` — a reply that carries an
/// address and never declares it, which is the shape of the defect `Granting`
/// exists to prevent. Watched: `the address a keyed actor
/// returned must be fetchable: BlobRefused { id: BlobId(78063835..) }`.
///
/// **That pair is RECONSTRUCTED from the recorded output and has not been replayed.**
/// What is unverified is that THIS anchor and THIS replacement are what produced the
/// sentence. `mise run replay-anchors` on R28 is what settles that, and nothing else can.
///
/// **The sentence's PROVENANCE is unestablished, and an earlier version of this record
/// said it was "inherited from the original observation".** That attributed it to a
/// watched run nobody can point to. The anchor this row carried when the sentence was
/// written was the bare macro name, and the paragraph below proves that pair cannot
/// compile — so whatever produced the sentence, it was not the recorded substitution. It
/// may have been a real panic from a hand-made edit; that is plausible and it is not
/// established.
///
/// **What IS established is that the recorded ADDRESS is the right one, which makes the
/// sentence WELL-FORMED for this break.** `BlobId` prints hex, and BLAKE3 of this test's
/// own published bytes — `the datasheet a keyed actor handed out` — is
/// `78063835b32825bd..`, the recorded prefix exactly. So the refused address is the
/// PUBLISHED blob's, not the unpublished control's `1bbf29b2..`, and refusing the
/// published address is precisely what a `granted()` answering `None` causes.
///
/// **That arithmetic cannot show the line was ever PRINTED, and it would be flattering to
/// read it that way.** This test is handed the published address on a GREEN run, so a
/// sentence assembled from a green run's id plus hand-written error text carries the same
/// prefix as one copied out of a red run. The hash separates the right address from the
/// wrong address. It does not separate observed from assembled — so the register here is
/// **well-founded**, and never "watched".
///
/// **The anchor here used to be the bare macro name, and the replacement used to
/// be `None`. That pair cannot produce the output above.** The old anchor was
/// `__keylink_granted_id!` alone, and `break-once.sh` replaces text, so it would
/// have left the argument list behind — `$crate::None([<$kind>], __value)`, an
/// unresolved path called with two arguments. The break would have failed to
/// COMPILE, and this record would have read as a watched panic. The replacement is
/// a block because the arm must yield `::core::option::Option<$crate::BlobId>`, and
/// `let _ = __value;` keeps the pattern's binding used.
///
/// **That is MEASURED now rather than inferred, which makes the repair necessary
/// rather than tidy.** A positive control compiled both pairs against an isolated
/// archive of `25cbbac`, on 2026-08-08: the OLD pair FAILS to compile, the NEW pair
/// COMPILES. Dated because it is a claim about one tree and one toolchain, and a
/// re-measurer on a later tree is entitled to know which. The recorded error appears
/// verbatim —
/// `error[E0425]: cannot find function, tuple struct or tuple variant None in
/// module $crate`.
///
/// **A replayer matching that text must expect MORE than that one error.** The same
/// run also reported `cannot find value rpc / recv / send in this scope`, because
/// this macro expands at several sites and one broken body errors at every one of
/// them. Reading the paragraph above as promising a single diagnostic, then treating
/// the extra ones as a different failure, is the mistake it invites.
///
/// **And a compile check still says nothing about redness.** It settles two things
/// only: that the old pair could not have produced the recorded panic, and that the
/// new pair gets far enough to run. Whether the new pair produces THIS sentence is
/// still `mise run replay-anchors` and nothing else, exactly as the reconstruction
/// paragraph above says.
///
/// **The narrow anchor is quoted above as HISTORY, and that is the whole reason it
/// is there.** The paragraphs before it explain why the old pair could not produce
/// the recorded output, and that explanation is about one specific substitution, so
/// it has to name it. `__keylink_granted_id!` → `None` is the thing that fails to
/// compile, and an account of it that will not quote it stops parsing. The quote is
/// part of the argument rather than a device.
///
/// **It is NOT kept to satisfy `mise run anchors`, and saying so matters because the
/// reason it once had has expired.** The registry is committed carrying the wide
/// literal, so the wide span is what the record-quote check reads and the narrow one
/// contributes nothing to it. For a while the pair existed so that the row and this
/// record could be edited in either order, and that problem is spent. A reason that
/// has lapsed is precisely what gets a quote deleted later, so it is retired here in
/// writing instead of being left to be rediscovered as dead weight.
///
/// **Deleting the narrow quote has already been proposed and reversed twice.** It
/// reads as a leftover to anyone who checks only whether the gate still needs it,
/// and the gate does not. What needs it is the reader of the paragraph above.
#[tokio::test]
async fn a_keyed_actors_blob_method_grants_the_address_it_returns() {
    let server = local_node().await;
    let caller = local_node().await;
    let blobs = Blobs::memory();
    let published = blobs
        .add_bytes(b"the datasheet a keyed actor handed out".to_vec())
        .await
        .expect("stores");
    let unpublished = blobs
        .add_bytes(b"in the same store, returned by nobody".to_vec())
        .await
        .expect("stores");

    let system: catalog::Registry = catalog::Registry::builder(keylink::Eviction::Never).build();
    system
        .mount(
            keylink::ActorId::new("warehouse").expect("a valid id"),
            catalog::spawn(Warehouse {
                published,
                unpublished,
            }),
        )
        .expect("mounts");

    let _running = keylink::Server::builder(&server)
        .service(
            system
                .service(Allowlist::anyone())
                .with_blobs(&blobs)
                .build(),
        )
        .spawn()
        .expect("a keyed actor system that serves content");

    let client = catalog::RegistryClient::connect(&caller, server.endpoint().addr())
        .await
        .expect("dials the keyed system");
    let actor = client.at(keylink::ActorId::new("warehouse").expect("a valid id"));

    let (manual, handle) = actor
        .datasheet("ADU-014".to_owned())
        .await
        .expect("the actor answers with a reply and a handle");
    assert_eq!(manual.pages, 42);

    let mut fetched = Vec::new();
    handle
        .copy_to(&mut fetched)
        .await
        .expect("the address a keyed actor returned must be fetchable");
    assert_eq!(fetched, b"the datasheet a keyed actor handed out");

    // And nothing else in the store came with it.
    let mut sink = Vec::new();
    let error = BlobHandle::new(&caller, server.endpoint().addr(), unpublished)
        .copy_to(&mut sink)
        .await
        .expect_err("an address nobody returned must not be fetchable");
    assert_refused(&error, "content the keyed actor never named");
}

#[tokio::test]
async fn a_local_keyed_blob_method_returns_its_address() {
    let blobs = Blobs::memory();
    let published = blobs
        .add_bytes(b"the local keyed blob".to_vec())
        .await
        .expect("stores");
    let system: catalog::Registry = catalog::Registry::builder(keylink::Eviction::Never).build();
    let actor_id = keylink::ActorId::new("warehouse").expect("a valid id");
    system
        .mount(
            actor_id.clone(),
            catalog::spawn(Warehouse {
                published,
                unpublished: published,
            }),
        )
        .expect("mounts");

    let (manual, returned) = catalog::at_local(&system, actor_id)
        .datasheet("ADU-LOCAL".to_owned())
        .await
        .expect("the local keyed actor answers");
    assert_eq!(manual.pages, 42);
    assert_eq!(returned.content(), Content::Blob(published));
}

/// The startup refusal that could not fire before, and now can.
///
/// `ServerBuilder::spawn` compares `DECLARES_BLOBS` against the store a service
/// was given. The keyed path never reported the flag — `Registry::service` did not
/// call `__keylink_declares_blobs` — so `ConfigError::MissingBlobStore` was a
/// check whose flag was always false there, and a keyed service declaring `blob`
/// started happily and granted nothing.
///
/// It is a real check now, and this is a behaviour change worth pinning rather
/// than discovering: a keyed service with a `blob` method and no store is
/// refused at startup instead of at the first fetch.
#[tokio::test]
async fn a_keyed_service_with_blob_methods_and_no_store_is_refused_at_startup() {
    let node = local_node().await;
    let system: catalog::Registry = catalog::Registry::builder(keylink::Eviction::Never).build();

    let error = keylink::Server::builder(&node)
        .service(system.service(Allowlist::anyone()).build())
        .spawn()
        .expect_err("a keyed service declaring `blob` must be given a store");

    assert!(
        matches!(
            error,
            Error::Config(keylink::ConfigError::MissingBlobStore { .. })
        ),
        "expected a missing-store config error, got {error:?}"
    );
}

// ═════════════════════════════ a replayed address is not a bearer token
//
// Two lifetimes cross here and they are deliberately unequal:
//
// - the idempotency record, bounded by `MAX_RECORDS`, `MAX_RECORDED_BYTES` and
//   `RECORD_TTL`, held in the service's `Ledger`;
// - the grant, which lasts until the holder revokes it, held in the node's
//   grant book.
//
// A `blob` reply is recordable — the generated dispatch answers with
// `Body::empty()`, so `declared_len()` is `Some(0)`, so `Guard::record` is
// handed `carries_body: false` and stores the frame rather than
// `Slot::Unrecordable`. And `serve_one` returns at `Claimed::Replay` **before**
// the grant site, so a replay re-sends the recorded `Content` and records no
// grant.
//
// The question that makes that either harmless or a hole is whether the address
// carries any authority of its own. It does not: `admits_get` reads the grant
// book on **every** request, so an address whose grant has been revoked is
// refused however the caller came by it. The test below drives exactly the
// sequence — record kept, grant revoked, key retried — and pins the refusal.
// Before grants could be revoked the sequence needed 4096 filler grants to push
// the first one off the end of a bounded queue; a revoke is the same event
// stated directly.

// The service for the sequence: one pinned address under an idempotency key,
// and a second method whose grant keeps the caller known to the connection
// guard.
keylink::service! {
    service Vault;

    blob Receipt(order: String) -> Manual;
    blob Decoy() -> Manual;
}

#[derive(Debug, Clone)]
struct Strongroom {
    receipt: BlobId,
    decoy: BlobId,
    /// How many times `receipt` was entered. A replayed reply and a re-run one
    /// are byte identical, so this is the only way to prove which path the retry
    /// took — and "the replay never reaches the grant site" is a claim about the
    /// path, not about the bytes.
    entered: std::sync::Arc<std::sync::atomic::AtomicUsize>,
}

impl vault::Handler for Strongroom {
    async fn receipt(&self, _cx: &Context, _order: String) -> (Manual, Grant) {
        self.entered
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        (Manual { pages: 7 }, self.receipt.into())
    }

    async fn decoy(&self, _cx: &Context) -> (Manual, Grant) {
        (Manual { pages: 0 }, self.decoy.into())
    }
}

/// **The sequence, over two real endpoints: the record outlives the grant, the
/// replay hands the address back, and the fetch is still refused.**
///
/// Countable rather than timed. Nothing here waits for `RECORD_TTL`; the record
/// is shown to be present by asking the ledger (`key_state` is `Done`), and the
/// grant is shown to be absent by revoking it.
///
/// The caller then takes one live grant on a **different** blob, and that is
/// load bearing rather than scenery — see step 3. It is what puts the refusal
/// at `admits_get` rather than at `BlobsAdmission::accept`, and `admits_get` is
/// the only guard the claim above is about.
#[tokio::test]
async fn a_replayed_blob_reply_hands_out_an_address_whose_grant_was_revoked_and_the_fetch_is_refused(
) {
    let server = local_node().await;
    let caller = local_node().await;
    let blobs = Blobs::memory();

    let receipt = blobs
        .add_bytes(b"the receipt, recorded under a key".to_vec())
        .await
        .expect("stores");
    let decoy = blobs
        .add_bytes(b"a second blob, to keep the caller known".to_vec())
        .await
        .expect("stores");

    let entered = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let strongroom = Strongroom {
        receipt,
        decoy,
        entered: std::sync::Arc::clone(&entered),
    };
    let _running = keylink::Server::builder(&server)
        .service(
            vault::service(strongroom, Allowlist::anyone())
                .with_blobs(&blobs)
                .build(),
        )
        .spawn()
        .expect("one service with a store");
    let server_addr = server.endpoint().addr();

    let client = vault::Client::connect(&caller, server_addr.clone())
        .await
        .expect("the caller dials");

    // 1. The keyed call. The grant is recorded at the grant site, and the reply
    //    frame is recorded in the ledger because a `blob` reply carries no body.
    let key = keylink::IdempotencyKey::new("receipt-42").expect("a valid key");
    let (manual, handle) = client
        .with_idempotency(key.clone())
        .receipt("42".to_owned())
        .await
        .expect("the first call runs");
    assert_eq!(manual.pages, 7);

    // The control: the address the first reply carried is fetchable. Without
    // this the refusal at the end proves nothing.
    let mut sink = Vec::new();
    handle.copy_to(&mut sink).await.expect("granted");
    assert_eq!(sink, b"the receipt, recorded under a key");

    // 2. The holder withdraws the grant. The record in the idempotency ledger is
    //    a different structure and is untouched.
    assert!(
        blobs.revoke(caller.id(), receipt),
        "the first call recorded a grant, so there is one to revoke"
    );

    // 3. ONE live grant for the caller, on a DIFFERENT blob.
    //
    //    **This is the step that makes the refusal at the end mean anything,
    //    and without it this test passed for the wrong reason.** Revoking the
    //    caller's only grant removes the caller from the ledger outright. A
    //    caller holding zero grants is turned away by `BlobsAdmission::accept`
    //    at `knows(&peer)` — the connection is closed before a single request
    //    is parsed, so `admits_get` is never called. Both refusals arrive as
    //    `Error::BlobRefused`, so `assert_refused` cannot tell them apart, and
    //    deleting the `scope` check from `admits_get` — the exact check this
    //    test exists to hold — left it green.
    //
    //    With this grant the connection is admitted, and the per-request check
    //    is the only thing left standing between a replayed address and the
    //    bytes. Same idea as `a_grant_to_one_peer_does_not_admit_another_peer`,
    //    which keeps a decoy grant for the same reason.
    let (_, kept) = client.decoy().await.expect("the decoy call runs");
    let mut sink = Vec::new();
    kept.copy_to(&mut sink).await.expect(
        "the caller must get past connection admission, or the refusal below \
         is the connection talking and not the grant book",
    );
    assert_eq!(sink, b"a second blob, to keep the caller known");

    // 4. The record is still there. This is the half of "two lifetimes" that
    //    makes the sequence reachable at all, and it is asserted rather than
    //    assumed.
    assert!(
        matches!(
            client.key_state(key.clone()).await.expect("answers"),
            keylink::KeyState::Done
        ),
        "the reply frame must still be recorded, or there is nothing to replay"
    );

    // 5. The retry is answered from the record, and the recorded frame carries
    //    the same address — the replay path never reaches the grant site.
    let (replayed_manual, replayed) = client
        .with_idempotency(key)
        .receipt("42".to_owned())
        .await
        .expect("the retry is answered from the record");
    assert_eq!(replayed_manual.pages, 7);
    assert_eq!(replayed.id(), receipt, "the same address came back");
    assert_eq!(
        entered.load(std::sync::atomic::Ordering::Relaxed),
        1,
        "the retry must have been replayed, not re-run: a re-run would pass \
         through the grant site and re-record the grant, which is the very thing \
         the replay path is said to skip"
    );

    // 6. And it is worth nothing. The grant book is read per request, so the
    //    address the replay handed back is refused exactly like an address the
    //    handler never returned — on a connection that step 3 just proved is
    //    admitted, which is what makes this `admits_get` talking.
    let mut sink = Vec::new();
    let error = replayed
        .copy_to(&mut sink)
        .await
        .expect_err("a replayed address whose grant was revoked must be refused");
    assert_refused(&error, "a replayed address with no live grant");
    assert!(
        sink.is_empty(),
        "nothing may be served before the refusal: got {} bytes",
        sink.len()
    );

    // The same refusal for a peer that spells the address out by hand, so the
    // refusal above is the grant book talking and not the handle's own state.
    let mut sink = Vec::new();
    let error = BlobHandle::new(&caller, server_addr, receipt)
        .copy_to(&mut sink)
        .await
        .expect_err("the revoked grant is gone for every route to the address");
    assert_refused(&error, "the address spelled out by hand");
}

// ═══════════════════════════════════════ closures: one grant, a whole tree
//
// A `Content::Closure` grant covers a hash sequence and every blob it lists,
// and it is ONE entry in the ledger however many members there are. These tests
// run it over real endpoints: the grant comes from generated dispatch where the
// property is about dispatch, and from the `Granting` fixture where the test
// needs a raw blobs connection to ask for something keylink's client never
// sends.

keylink::service! {
    service Shelf;

    blob Snapshot() -> u32;
    blob Single(nth: u64) -> u32;
}

/// 1024 bytes that depend on `nth`, so two members never share an address and a
/// member read from the wrong place cannot coincidentally equal the right one.
fn member(nth: u32) -> Vec<u8> {
    nth.to_le_bytes().repeat(256)
}

/// Add `member(0..count)` and return their addresses, in order.
async fn add_members(blobs: &Blobs, count: u32) -> Vec<BlobId> {
    let mut ids = Vec::with_capacity(count as usize);
    for nth in 0..count {
        ids.push(blobs.add_bytes(member(nth)).await.expect("stores a member"));
    }
    ids
}

#[derive(Clone)]
struct Stacks {
    seq: keylink::SeqId,
    ids: Arc<Vec<BlobId>>,
}

impl shelf::Handler for Stacks {
    async fn snapshot(&self, _cx: &Context) -> (u32, Grant) {
        (self.ids.len() as u32, self.seq.into())
    }

    async fn single(&self, _cx: &Context, nth: u64) -> (u32, Grant) {
        (nth as u32, self.ids[nth as usize].into())
    }
}

/// A serving node that lists `count` members, and a caller that has dialled it.
struct Stack {
    blobs: Blobs,
    seq: keylink::SeqId,
    ids: Vec<BlobId>,
    caller: Node,
    client: shelf::Client,
    _server: keylink::Server,
    _node: Node,
}

async fn stack(count: u32) -> Stack {
    let node = local_node().await;
    let caller = local_node().await;
    let blobs = Blobs::memory();
    let ids = add_members(&blobs, count).await;
    let seq = blobs
        .add_seq(ids.clone())
        .await
        .expect("stores the sequence");
    let server = keylink::Server::builder(&node)
        .service(
            shelf::service(
                Stacks {
                    seq,
                    ids: Arc::new(ids.clone()),
                },
                Allowlist::anyone(),
            )
            .with_blobs(&blobs)
            .build(),
        )
        .spawn()
        .expect("one service with a store");
    let client = shelf::Client::connect(&caller, node.endpoint().addr())
        .await
        .expect("the caller dials");
    Stack {
        blobs,
        seq,
        ids,
        caller,
        client,
        _server: server,
        _node: node,
    }
}

/// Read `ids` back out of `store`, by serving it to a peer and fetching.
///
/// `Blobs` has no read of its own, and a test that only asked the store whether
/// it `missing`-lists nothing would be trusting the answer it is checking.
async fn read_back(store: &Blobs, ids: &[BlobId]) -> Vec<Vec<u8>> {
    let held = cluster_over(store.clone()).await;
    held.grant(&held.peer, ids).await;
    let mut read = Vec::new();
    for id in ids {
        read.push(
            held.fetch(&held.peer, *id)
                .await
                .expect("the store holds it"),
        );
    }
    read
}

/// **The limit, gone: one grant for a closure of 5000 members.**
///
/// The caller makes one call and holds one grant. It then fetches 5000 blobs it
/// was never granted one by one, which the old ledger could not express: it
/// would have needed 5000 grants, and it kept 4096.
#[tokio::test]
async fn a_closure_of_5000_members_is_fetched_with_one_grant() {
    let stack = stack(5000).await;
    let (count, handle) = stack.client.snapshot().await.expect("the one call");
    assert_eq!(count, 5000);
    assert_eq!(handle.content(), Content::Closure(stack.seq.id()));

    let local = Blobs::memory();
    let moved = handle
        .fetch_into(&local)
        .await
        .expect("fetches the closure");
    // The member list is 160000 bytes, and the size proof that precedes it
    // carries the list's last chunk (the remainder after 156 whole chunks).
    let list = 5000 * 32;
    let proof = (list - 1) % 1024 + 1;
    assert_eq!(
        moved,
        5000 * 1024 + list + proof,
        "every member's payload, the member list, and the last chunk of it the \
         size proof carried: all payload that crossed the wire"
    );
    assert_eq!(
        local.missing(handle.content()).await.expect("answers"),
        keylink::Missing::Nothing
    );

    let probes = [0usize, 2499, 4999];
    let ids: Vec<BlobId> = probes.iter().map(|nth| stack.ids[*nth]).collect();
    let read = read_back(&local, &ids).await;
    for (nth, bytes) in probes.iter().zip(read) {
        assert_eq!(bytes, member(*nth as u32), "member {nth} reads back intact");
    }
}

/// A peer holding a closure on `S` asks for a hash `S` does not list. It reads
/// the same refusal as for a hash the holder never had.
#[tokio::test]
async fn a_hash_outside_the_closure_is_refused_like_a_missing_one() {
    let cluster = cluster().await;
    let ids = add_members(&cluster.blobs, 4).await;
    let seq = cluster.blobs.add_seq(ids).await.expect("stores");
    let secret = cluster.add("in the store, listed by nobody").await;
    let absent = {
        let elsewhere = MemStore::new();
        let tag = elsewhere
            .add_bytes(b"this node has never seen these bytes".to_vec())
            .await
            .expect("adds to a throwaway store");
        address_of(tag.hash)
    };
    cluster
        .grant_content(&cluster.peer, &[Content::Closure(seq.id())])
        .await;

    let store = MemStore::new();
    let ask = |hash| {
        let store = store.clone();
        let cluster = &cluster;
        async move {
            let conn = cluster.dial_blobs(&cluster.peer).await;
            store
                .remote()
                .execute_get(conn, GetRequest::blob(hash))
                .await
        }
    };
    let outside = ask(hash_of(secret)).await.expect_err("not in the closure");
    let missing = ask(hash_of(absent)).await.expect_err("not held at all");

    assert_eq!(
        outside.iroh_error_code(),
        Some(iroh_blobs::protocol::ERR_PERMISSION)
    );
    assert_eq!(missing.iroh_error_code(), outside.iroh_error_code());
    assert_eq!(outside.to_string(), missing.to_string());
    assert!(
        store.get_bytes(hash_of(secret)).await.is_err(),
        "the secret was not served"
    );
}

/// A member is reached through its root, by position, and never by its own
/// hash.
#[tokio::test]
async fn a_member_is_reached_through_its_root_and_not_by_its_own_hash() {
    let cluster = cluster().await;
    let ids = add_members(&cluster.blobs, 10).await;
    let seq = cluster.blobs.add_seq(ids.clone()).await.expect("stores");
    cluster
        .grant_content(&cluster.peer, &[Content::Closure(seq.id())])
        .await;

    let store = MemStore::new();
    let connect = || cluster.dial_blobs(&cluster.peer);
    // The root first, so the child index has a list to resolve against.
    store
        .remote()
        .fetch(
            connect().await,
            iroh_blobs::HashAndFormat::raw(hash_of(seq.id())),
        )
        .await
        .expect("a closure grant admits its root alone");
    store
        .remote()
        .execute_get(
            connect().await,
            GetRequest::builder()
                .child(7, ChunkRanges::all())
                .build(hash_of(seq.id())),
        )
        .await
        .expect("member 7, through its root");
    assert_eq!(
        store
            .get_bytes(hash_of(ids[7]))
            .await
            .expect("member 7 landed")
            .as_ref(),
        member(7).as_slice()
    );

    let by_hash = store
        .remote()
        .execute_get(connect().await, GetRequest::blob(hash_of(ids[8])))
        .await;
    assert!(by_hash.is_err(), "member 8 by its own hash is refused");
    assert!(
        store.get_bytes(hash_of(ids[8])).await.is_err(),
        "and was not served"
    );
}

/// The have/want half: a store that holds most of a closure is told what it
/// lacks, and fetches exactly that.
#[tokio::test]
async fn missing_names_exactly_the_members_the_store_lacks() {
    let stack = stack(5000).await;
    let (_, handle) = stack.client.snapshot().await.expect("the one call");

    let local = Blobs::memory();
    add_members(&local, 4000).await;
    assert_eq!(
        local.missing(handle.content()).await.expect("answers"),
        keylink::Missing::All,
        "without the root the members are not known yet"
    );
    assert_eq!(
        handle
            .missing(&local)
            .await
            .expect("fetches the root, answers"),
        keylink::Missing::Members(stack.ids[4000..5000].to_vec()),
        "in sequence order, and only what the store lacks"
    );

    let moved = handle.fetch_into(&local).await.expect("fetches the rest");
    assert_eq!(
        moved,
        1000 * 1024,
        "the root was already held, and so was the rest"
    );
    assert_eq!(
        handle.missing(&local).await.expect("answers"),
        keylink::Missing::Nothing
    );
    assert_eq!(handle.fetch_into(&local).await.expect("free"), 0);
}

/// Revoking a closure takes effect at the next request.
#[tokio::test]
async fn a_revoked_closure_is_refused_on_the_next_request() {
    let stack = stack(20).await;
    let (_, handle) = stack.client.snapshot().await.expect("the one call");
    // A grant on something else, so the caller stays known to the connection
    // guard and the refusal below comes from `admits_get`.
    let (_, kept) = stack.client.single(0).await.expect("the decoy call");

    let local = Blobs::memory();
    handle
        .fetch_into(&local)
        .await
        .expect("granted, so it fetches");
    handle.open().await.expect("and the root opens");

    assert!(
        stack.blobs.revoke(stack.caller.id(), stack.seq.id()),
        "a live grant was withdrawn"
    );
    let error = handle.open().await.expect_err("the grant is gone");
    assert_refused(&error, "a revoked closure");
    kept.open()
        .await
        .expect("control: the other grant is untouched");
}

/// `revoke_peer` is the immediate cut: the transfer already running stops.
#[tokio::test]
async fn revoking_a_peer_cuts_its_transfer_in_flight() {
    let cluster = cluster().await;
    let content = vec![7u8; 64 * 1024 * 1024];
    let id = cluster.add_bytes(&content).await;
    cluster.grant(&cluster.peer, &[id]).await;

    let handle = BlobHandle::new(&cluster.peer, cluster.server.endpoint().addr(), id);
    let mut reader = handle.open().await.expect("opens");
    reader
        .next_chunk()
        .await
        .expect("reads")
        .expect("the first chunk");

    assert_eq!(cluster.blobs.revoke_peer(cluster.peer.id()), 1);

    let mut read = 0usize;
    let end = loop {
        match reader.next_chunk().await {
            Ok(Some(chunk)) => read += chunk.len(),
            Ok(None) => break None,
            Err(error) => break Some(error),
        }
    };
    let error = end.expect("the transfer did not run to its end");
    assert_refused(&error, "a transfer cut by revoke_peer");
    assert!(read < content.len(), "and it stopped short: {read} bytes");
}

/// One level deep: a sequence listed by a closure is served as bytes, and its
/// own members are not covered.
#[tokio::test]
async fn a_nested_sequence_is_not_covered_by_its_parents_closure() {
    let cluster = cluster().await;
    let secret = cluster.add("listed only by the inner sequence").await;
    let inner = cluster.blobs.add_seq([secret]).await.expect("stores");
    let outer = cluster.blobs.add_seq([inner.id()]).await.expect("stores");
    cluster
        .grant_content(&cluster.peer, &[Content::Closure(outer.id())])
        .await;

    let store = MemStore::new();
    let connect = || cluster.dial_blobs(&cluster.peer);
    store
        .remote()
        .fetch(
            connect().await,
            iroh_blobs::HashAndFormat::raw(hash_of(outer.id())),
        )
        .await
        .expect("the outer root");

    // The control: the inner sequence's own bytes are the outer's member 0.
    store
        .remote()
        .execute_get(
            connect().await,
            GetRequest::builder()
                .child(0, ChunkRanges::all())
                .build(hash_of(outer.id())),
        )
        .await
        .expect("a member of the outer sequence");
    assert_eq!(
        store
            .get_bytes(hash_of(inner.id()))
            .await
            .expect("the inner sequence landed as bytes")
            .as_ref(),
        secret.as_bytes()
    );

    let reach = store
        .remote()
        .execute_get(connect().await, GetRequest::all(hash_of(inner.id())))
        .await;
    assert!(reach.is_err(), "the inner sequence's members are refused");
    assert!(
        store.get_bytes(hash_of(secret)).await.is_err(),
        "the secret was not served"
    );
}

/// The limit refuses loudly and evicts nothing.
#[tokio::test]
async fn a_grant_past_the_limit_answers_grants_exhausted_and_evicts_nothing() {
    let stack = stack(4).await;
    stack
        .blobs
        .set_grant_limit(std::num::NonZeroUsize::new(2).expect("non-zero"));

    let (_, first) = stack.client.single(0).await.expect("fits");
    let (_, second) = stack.client.single(1).await.expect("fits");
    let error = stack
        .client
        .single(2)
        .await
        .expect_err("the third grant does not fit");
    assert!(
        matches!(
            error,
            Error::Remote(keylink::RemoteError::GrantsExhausted { limit: 2 })
        ),
        "expected GrantsExhausted, got {error:?}"
    );

    let local = Blobs::memory();
    first
        .fetch_into(&local)
        .await
        .expect("the first still fetches");
    second
        .fetch_into(&local)
        .await
        .expect("the second still fetches");
}

// ═══════════════════════════════════════ a revoke reaches a call already running

keylink::service! {
    service Turnstile;

    blob Held() -> u32;
}

/// A handler that stops before it returns, so a test can revoke while the call
/// is dispatched and has made no reservation yet.
#[derive(Clone)]
struct Held {
    blob: BlobId,
    entered: Arc<tokio::sync::Notify>,
    release: Arc<tokio::sync::Notify>,
}

impl turnstile::Handler for Held {
    async fn held(&self, _cx: &Context) -> (u32, Grant) {
        self.entered.notify_one();
        self.release.notified().await;
        (1, self.blob.into())
    }
}

/// Review finding 5. "Remove the peer from admission, then `revoke_peer`" must
/// hold for a call that was already inside its handler: the handler has not
/// reserved anything, so there is no entry for the revoke to remove.
///
/// The revoke lands while the handler is paused; the handler then returns.
/// The caller is told its content was not granted, and holds nothing.
#[tokio::test]
async fn a_peer_revoke_lands_on_a_call_whose_handler_is_still_running() {
    let server = local_node().await;
    let caller = local_node().await;
    let blobs = Blobs::memory();
    let blob = blobs
        .add_bytes(b"granted after the revoke, if the fence is missing".to_vec())
        .await
        .expect("stores");
    let held = Held {
        blob,
        entered: Arc::new(tokio::sync::Notify::new()),
        release: Arc::new(tokio::sync::Notify::new()),
    };
    let _running = keylink::Server::builder(&server)
        .service(
            turnstile::service(held.clone(), Allowlist::anyone())
                .with_blobs(&blobs)
                .build(),
        )
        .spawn()
        .expect("one service with a store");
    let client = turnstile::Client::connect(&caller, server.endpoint().addr())
        .await
        .expect("the caller dials");

    let call = tokio::spawn({
        let client = client.clone();
        async move { client.held().await }
    });
    // Bounded, because the failure mode of a handler that never starts is a
    // hang; this is not an assertion about timing.
    tokio::time::timeout(Duration::from_secs(30), held.entered.notified())
        .await
        .expect("the handler was entered");

    assert_eq!(
        blobs.revoke_peer(caller.id()),
        0,
        "the caller holds nothing yet"
    );
    held.release.notify_one();

    let answer = call.await.expect("the call task ended");
    assert!(
        matches!(
            answer,
            Err(Error::Remote(keylink::RemoteError::GrantsWithdrawn))
        ),
        "expected the grant to be withdrawn, got {answer:?}"
    );

    let mut sink = Vec::new();
    let error = BlobHandle::new(&caller, server.endpoint().addr(), blob)
        .copy_to(&mut sink)
        .await
        .expect_err("the withdrawn grant admits nothing");
    assert_refused(&error, "content a revoked peer's running call returned");
}

// ═══════════════════════════════════════ a few bytes cannot make the holder walk

/// Review finding 3, end to end. A request that selects nothing until offset
/// `1 << 60` is a few bytes on the wire, and iroh-blobs walks the empty offsets
/// one at a time before it looks at the sequence, so a closure grant that
/// admitted it would hold a thread of the holder for good.
///
/// The holder reads the root's own size, once, and refuses a request whose
/// furthest run starts past the members the root lists: here a root of four
/// members, so offset 4 (member 3) is served and offset 5 is not.
///
/// Sent as raw bytes, because iroh-blobs' own client walks the same offsets
/// before it sends, and so does the request builder. The holder must answer
/// with a refusal and not with silence.
///
/// **Without the guard this test hangs rather than failing**, on any runtime:
/// the walk never yields, so it starves the timeout below on the default
/// single-threaded runtime, and on a multi-threaded one it still keeps the
/// runtime from shutting down after the timeout has panicked. Watched that way
/// with `scripts/break-once.sh` (row `HK56`), replacing
/// `size.is_multiple_of(32) && furthest <= size / 32`
/// with `true` in `keylink/src/blobs.rs`: the run was still going after two
/// minutes and was ended by its process id. The unit test
/// `a_root_lists_an_offset_only_if_it_has_that_many_members` is the one that
/// goes red cleanly. The wait below asserts nothing about time.
#[tokio::test]
async fn a_far_offset_in_a_closure_request_is_refused_and_not_walked() {
    let cluster = cluster().await;
    let ids = add_members(&cluster.blobs, 4).await;
    let seq = cluster.blobs.add_seq(ids).await.expect("stores");
    cluster
        .grant_content(&cluster.peer, &[Content::Closure(seq.id())])
        .await;

    for (offset, served) in [
        (4u64, true),
        (5, false),
        (1 << 60, false),
        (u64::MAX - 1, false),
    ] {
        let wire = postcard::to_stdvec(&vec![(offset, vec![0u64]), (1u64, Vec::<u64>::new())])
            .expect("encodes the runs");
        let ranges: ChunkRangesSeq = postcard::from_bytes(&wire).expect("a valid sparse request");
        let request = GetRequest::new(hash_of(seq.id()), ranges);

        let connection = cluster.dial_blobs(&cluster.peer).await;
        let (mut send, mut recv) = connection.open_bi().await.expect("opens a stream");
        let asked = postcard::to_stdvec(&iroh_blobs::protocol::Request::Get(request))
            .expect("encodes the request");
        send.write_all(&asked).await.expect("writes the request");
        send.finish().expect("finishes the request");

        let answer = tokio::time::timeout(Duration::from_secs(60), recv.read_to_end(1 << 20))
            .await
            .expect("the holder answered instead of walking to the offset");
        assert_eq!(
            answer.is_ok(),
            served,
            "offset {offset}: served is {served}, got {answer:?}"
        );
    }
}

/// Review finding 7. `fetch_into` used to succeed for a root that is not a
/// sequence, because upstream reads the member count off the size and a bad
/// list has none, while `missing` refused the same content. Both refuse it now,
/// whether the root is fetched or already held.
#[tokio::test]
async fn a_root_that_is_not_a_whole_number_of_addresses_is_refused_by_both_calls() {
    let cluster = cluster().await;
    for bytes in [vec![7u8], [vec![1u8; 32], vec![9u8]].concat()] {
        let len = bytes.len() as u64;
        let root = cluster.add_bytes(&bytes).await;
        cluster
            .grant_content(&cluster.peer, &[Content::Closure(root)])
            .await;
        let handle = BlobHandle::new(
            &cluster.peer,
            cluster.server.endpoint().addr(),
            Content::Closure(root),
        );
        let refused = |error: &Error| {
            matches!(
                error,
                Error::BlobUnverified {
                    fault: keylink::BlobFault::NotASequence { len: got },
                    ..
                } if *got == len
            )
        };

        // Fetched from the holder.
        let fresh = Blobs::memory();
        let error = handle.fetch_into(&fresh).await.expect_err("not a sequence");
        assert!(refused(&error), "{len} bytes, fetched: {error:?}");
        let error = handle.missing(&fresh).await.expect_err("not a sequence");
        assert!(refused(&error), "{len} bytes, missing: {error:?}");

        // Already held, so nothing is downloaded to notice it.
        let held = Blobs::memory();
        held.add_bytes(bytes.clone()).await.expect("stores");
        let error = handle.fetch_into(&held).await.expect_err("not a sequence");
        assert!(refused(&error), "{len} bytes, held: {error:?}");
        let error = held
            .missing(Content::Closure(root))
            .await
            .expect_err("not a sequence");
        assert!(refused(&error), "{len} bytes, held missing: {error:?}");
    }
}

// ═══════════════════════════════════════ a closure bigger than one window

/// Second-round review finding 3. A closure is fetched a window of 4096 members
/// at a time, each window planned from its own slice of the member list. 20,000
/// members cross four boundaries, and the holder is asked for the root once and
/// for each window once and for nothing else.
#[tokio::test]
async fn a_closure_of_20000_members_is_fetched_window_by_window() {
    let cluster = cluster().await;
    let ids = add_members(&cluster.blobs, 20_000).await;
    let seq = cluster.blobs.add_seq(ids.clone()).await.expect("stores");
    cluster
        .grant_content(&cluster.peer, &[Content::Closure(seq.id())])
        .await;

    // Relayed through a holder that records what it is asked, so the requests
    // themselves can be read and not only their total.
    let from = cluster.peer.endpoint().clone();
    let to = cluster.server.endpoint().addr();
    let holder = forgery(move |request: GetRequest| {
        let (from, to) = (from.clone(), to.clone());
        async move { ask(from, to, request).await }
    })
    .await;
    let handle = holder.handle(seq);

    let local = Blobs::memory();
    let moved = handle.fetch_into(&local).await.expect("fetches 20,000");
    let list = 20_000 * 32;
    let proof = (list - 1) % 1024 + 1;
    assert_eq!(moved, 20_000 * 1024 + list + proof);
    assert_eq!(
        local.missing(handle.content()).await.expect("answers"),
        keylink::Missing::Nothing
    );
    assert_eq!(handle.fetch_into(&local).await.expect("free"), 0);

    // The size proof, the root, and five windows (4096 four times, then 3616).
    let asked: Vec<GetRequest> = holder
        .asked()
        .into_iter()
        .filter(|request| request.ranges != preflight_ranges())
        .collect();
    assert_eq!(asked.len(), 6, "the root and five windows: {asked:?}");
    assert!(
        asked[0].ranges.is_blob(),
        "the first thing fetched is the root alone"
    );
    for window in &asked[1..] {
        let wire = postcard::to_stdvec(&window.ranges).expect("encodes");
        assert!(
            wire.len() < 16 * 1024,
            "a window's request is a few bytes per run, not the closure: {} bytes",
            wire.len()
        );
    }

    // Members either side of each window boundary, read back out of the store.
    let probes = [
        0usize, 4095, 4096, 8191, 8192, 12_287, 12_288, 16_383, 16_384, 19_999,
    ];
    let probed: Vec<BlobId> = probes.iter().map(|nth| ids[*nth]).collect();
    let read = read_back(&local, &probed).await;
    for (nth, bytes) in probes.iter().zip(read) {
        assert_eq!(bytes, member(*nth as u32), "member {nth} reads back intact");
    }
}

/// Second-round review finding 3, resume. The holder cuts the answer to the
/// third window in half. The fetch fails, the store keeps what verified, and
/// the next fetch asks only for the members it still lacks: it does not go back
/// over the two windows already held, nor over the members of the third that
/// arrived whole.
///
/// Watched red with `scripts/break-once.sh` (row `HK62`) by replacing
/// `lacking.push(!store.has(*member).await? && asked.insert(*member));`
/// with `lacking.push(true);` in `keylink/src/blobs.rs`: every window then asks
/// for all of its members, and the retry moves every member again (20,480,000
/// bytes) instead of the roughly half it lacked.
#[tokio::test]
async fn an_interrupted_closure_fetch_resumes_from_the_members_the_store_holds() {
    let cluster = cluster().await;
    let ids = add_members(&cluster.blobs, 20_000).await;
    let seq = cluster.blobs.add_seq(ids).await.expect("stores");
    cluster
        .grant_content(&cluster.peer, &[Content::Closure(seq.id())])
        .await;

    let from = cluster.peer.endpoint().clone();
    let to = cluster.server.endpoint().addr();
    let data_requests = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let holder = forgery({
        let data_requests = Arc::clone(&data_requests);
        move |request: GetRequest| {
            let (from, to) = (from.clone(), to.clone());
            let data_requests = Arc::clone(&data_requests);
            async move {
                let answer = ask(from, to, request.clone()).await;
                if request.ranges == preflight_ranges() {
                    return answer;
                }
                // 1 is the root, 2 and 3 the first two windows, 4 the third.
                let nth = data_requests.fetch_add(1, std::sync::atomic::Ordering::SeqCst) + 1;
                if nth == 4 {
                    answer[..answer.len() / 2].to_vec()
                } else {
                    answer
                }
            }
        }
    })
    .await;
    let handle = holder.handle(seq);
    let local = Blobs::memory();

    handle
        .fetch_into(&local)
        .await
        .expect_err("an answer that stops half way does not complete a fetch");
    let held_after_cut = match local.missing(handle.content()).await.expect("answers") {
        keylink::Missing::Members(lacking) => 20_000 - lacking.len(),
        other => panic!("expected a partial closure, got {other:?}"),
    };
    assert!(
        held_after_cut >= 2 * 4096,
        "the two windows before the cut are held: {held_after_cut}"
    );

    let second = handle
        .fetch_into(&local)
        .await
        .expect("the retry completes");
    let lacking_before = (20_000 - held_after_cut) as u64;
    assert_eq!(
        second,
        lacking_before * 1024,
        "exactly the members that were lacking: the root is held, the windows already \
         fetched are not asked for again, and the members of the third window that \
         arrived whole are not either"
    );
    assert_eq!(
        local.missing(handle.content()).await.expect("answers"),
        keylink::Missing::Nothing
    );
}

// ═══════════════════════════════════════ a closure with no member cap

/// A closure of `members` tiny distinct members, of which the fetcher already
/// holds every other one, so every window has a gap after every member it lacks.
/// The fetcher is asked, from its own store, what it lacks (every odd member, in
/// order), fetches exactly those, and ends with `Missing::Nothing`.
async fn a_closure_with_alternating_gaps_is_fetched(members: u32) {
    let cluster = cluster().await;

    let mut ids = Vec::with_capacity(members as usize);
    for nth in 0..members {
        ids.push(
            cluster
                .blobs
                .add_bytes(nth.to_le_bytes().to_vec())
                .await
                .expect("stores"),
        );
    }
    let seq = cluster
        .blobs
        .add_seq(ids.clone())
        .await
        .expect("stores the list");
    cluster
        .grant_content(&cluster.peer, &[Content::Closure(seq.id())])
        .await;
    let handle = BlobHandle::new(
        &cluster.peer,
        cluster.server.endpoint().addr(),
        Content::Closure(seq.id()),
    );

    // The fetcher holds every even member, so it lacks every odd one.
    let local = Blobs::memory();
    for nth in (0..members).step_by(2) {
        local
            .add_bytes(nth.to_le_bytes().to_vec())
            .await
            .expect("stores");
    }
    let lacking: Vec<BlobId> = ids.iter().copied().skip(1).step_by(2).collect();
    assert_eq!(
        handle
            .missing(&local)
            .await
            .expect("fetches the list, answers"),
        keylink::Missing::Members(lacking),
        "every odd member, in order"
    );

    let moved = handle.fetch_into(&local).await.expect("fetches the rest");
    assert_eq!(
        moved,
        u64::from(members / 2) * 4,
        "the odd members, 4 bytes each"
    );
    assert_eq!(
        handle.missing(&local).await.expect("answers"),
        keylink::Missing::Nothing
    );
    assert_eq!(handle.fetch_into(&local).await.expect("free"), 0);
}

/// Three whole windows and one member over (3 * 4096 + 1), with alternating
/// gaps: the smallest closure that crosses several window boundaries and ends in
/// a window of one. This is the one the gate runs.
#[tokio::test]
async fn a_closure_of_three_windows_and_one_member_with_alternating_gaps_is_fetched() {
    a_closure_with_alternating_gaps_is_fetched(3 * 4096 + 1).await;
}

/// The biggest closure the suite fetches: 300,000 tiny distinct members, 150,000
/// of them lacking and scattered. A closure has no length limit; what bounds the
/// fetch is that each window of 4096 members is planned from its own slice of
/// the list and its request has at most 4096 entries.
///
/// **Not in the gate.** It costs 100 to 266 seconds a run (about 1.2 million
/// round trips to the store to ask what it holds), and the case above covers the
/// windows. `mise run closure-300k` runs it, and its output goes in the pull
/// request that changes the fetch.
///
/// Watched red before it went green, in a scratch copy of the tree before
/// closures were fetched a window at a time, which asked for the whole closure
/// in one request: `fetch_into` failed with `fetches the rest: BlobRefused { id:
/// BlobId(0eed953ea9b4cb2a16d8d9738eeb36ebf165b27e8a979e62f74c6331dae580f2) }`
/// (the holder refused a request whose runs start past the limit it then had on
/// closure size). On the tree that had that limit but windows planned from the
/// whole root it stopped at `BlobUnverified { fault: TooManyMembers { members:
/// 300000, limit: 262144 } }`. With the limit gone and each window planned from
/// its own slice it passes.
#[tokio::test]
#[ignore = "100 to 266 s a run, outside the gate: run it with `mise run closure-300k`"]
async fn a_closure_of_300000_tiny_members_with_alternating_gaps_is_fetched() {
    a_closure_with_alternating_gaps_is_fetched(300_000).await;
}

// ═══════════════════════════════════════ one blob listed twice crosses the wire once

/// A sequence may list the same blob more than once, and one transfer fills
/// every place it is listed. Inside a window only the first occurrence of a
/// member the store lacks is requested; the sequence keeps its order and its
/// repeats, and `missing` still reports them as listed.
///
/// Three blobs of 1000, 2000 and 3000 bytes, listed `a b a c b a`: six places and
/// three transfers, so 6000 payload bytes where requesting every place would move
/// 10000.
///
/// Watched red with `scripts/break-once.sh` (row `HK63`) by replacing
/// `lacking.push(!store.has(*member).await? && asked.insert(*member));`
/// with `lacking.push(!store.has(*member).await? && { asked.insert(*member); true });`
/// in `keylink/src/blobs.rs`: the fetch then moves every listed place and fails
/// with `each blob crossed once` (left: 10000, right: 6000).
#[tokio::test]
async fn a_member_listed_more_than_once_in_a_window_crosses_the_wire_once() {
    let cluster = cluster().await;
    let a = cluster.add_bytes(&payload(1000)).await;
    let b = cluster.add_bytes(&payload(2000)).await;
    let c = cluster.add_bytes(&payload(3000)).await;
    let listed = [a, b, a, c, b, a];
    let seq = cluster
        .blobs
        .add_seq(listed)
        .await
        .expect("stores the list");
    cluster
        .grant_content(&cluster.peer, &[Content::Closure(seq.id())])
        .await;
    let handle = BlobHandle::new(
        &cluster.peer,
        cluster.server.endpoint().addr(),
        Content::Closure(seq.id()),
    );

    let local = Blobs::memory();
    assert_eq!(
        handle.missing(&local).await.expect("fetches the list"),
        keylink::Missing::Members(listed.to_vec()),
        "the sequence keeps its order and its repeats"
    );
    let moved = handle
        .fetch_into(&local)
        .await
        .expect("fetches the members");
    assert_eq!(moved, 1000 + 2000 + 3000, "each blob crossed once");
    assert_eq!(
        handle.missing(&local).await.expect("answers"),
        keylink::Missing::Nothing
    );
    let read = read_back(&local, &[a, b, c]).await;
    assert_eq!(
        read,
        vec![payload(1000), payload(2000), payload(3000)],
        "and every place it was listed is filled"
    );
}
