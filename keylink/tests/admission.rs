//! Changing your mind about a peer, without restarting.
//!
//! An [`Allowlist`] is read once at startup, so revoking a key used to mean a
//! restart — and a restart drops every connection, including the ones that were
//! fine. `DynamicAdmission` is the same value behind a lock, plus the one thing
//! a lock around a value cannot give you: the connections a new policy no
//! longer admits, closed.
//!
//! Two real nodes over a real handshake, because admission is the one decision
//! that happens on the accept path and nowhere else.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use keylink::prelude::*;
use std::sync::Arc;
use std::time::Duration;

use keylink::{Admission, Admittance, Allowlist, Context, DynamicAdmission, Error, ProvenPeer};
use keylink::{Revocable, Server};

mod common;
use common::local_node;

keylink::service! {
    service Gate;

    rpc Knock() -> u64;
}

struct Gate;

impl gate::Handler for Gate {
    async fn knock(&self, _cx: &Context) -> u64 {
        1
    }
}

/// A policy whose backend is down. Every answer is `Unavailable`, which refuses
/// — and stays distinguishable from `Refused`, which is the whole point.
struct Unreachable;

impl Admission for Unreachable {
    fn admits(&self, _peer: &ProvenPeer) -> Admittance {
        Admittance::Unavailable("the policy service did not answer".into())
    }
}

/// A peer added while the service is running is admitted on its next dial, with
/// no restart and without disturbing anybody already connected.
#[tokio::test]
async fn a_newly_added_peer_is_admitted_without_a_restart() {
    let node = local_node().await;
    let welcome = local_node().await;
    let latecomer = local_node().await;

    let policy = Arc::new(DynamicAdmission::new(Allowlist::only([welcome.id()])));
    let _running = Server::builder(&node)
        .service(
            gate::service(Gate, Allowlist::anyone())
                .admission(policy.clone())
                .build(),
        )
        .spawn()
        .expect("serves");

    let inside = gate::Client::connect(&welcome, node.endpoint().addr())
        .await
        .expect("dials");
    assert_eq!(inside.knock().await.expect("answers"), 1);

    let outside = gate::Client::connect(&latecomer, node.endpoint().addr())
        .await
        .expect("the handshake completes; admission is one layer up");
    assert!(
        outside.knock().await.is_err(),
        "a peer not on the list must not be answered"
    );

    let closed = policy.replace(Allowlist::only([welcome.id(), latecomer.id()]));
    assert!(
        closed.is_empty(),
        "widening a policy closes nobody: {closed:?}"
    );

    let admitted = gate::Client::connect(&latecomer, node.endpoint().addr())
        .await
        .expect("dials");
    assert_eq!(
        admitted.knock().await.expect("answers"),
        1,
        "the new peer must be admitted without a restart"
    );

    // And the peer that was already inside was never disturbed.
    assert_eq!(inside.knock().await.expect("still answers"), 1);
}

/// Revocation reaches a connection that is already open, which is the
/// capability a plain [`Admission`] cannot have: it is consulted at accept and
/// never again, so without this a revoked peer keeps its connection until it
/// chooses to hang up.
#[tokio::test]
async fn revoking_a_peer_closes_its_connection() {
    let node = local_node().await;
    let peer = local_node().await;
    let keeper = local_node().await;

    let policy = Arc::new(DynamicAdmission::new(Allowlist::only([
        peer.id(),
        keeper.id(),
    ])));
    let _running = Server::builder(&node)
        .service(
            gate::service(Gate, Allowlist::anyone())
                .admission(policy.clone())
                .build(),
        )
        .spawn()
        .expect("serves");

    let revoked = gate::Client::connect(&peer, node.endpoint().addr())
        .await
        .expect("dials");
    let kept = gate::Client::connect(&keeper, node.endpoint().addr())
        .await
        .expect("dials");
    assert_eq!(revoked.knock().await.expect("answers"), 1);
    assert_eq!(kept.knock().await.expect("answers"), 1);

    let closed = policy.replace(Allowlist::only([keeper.id()]));
    assert_eq!(closed, vec![peer.id()], "exactly one peer was revoked");

    let error = revoked
        .knock()
        .await
        .expect_err("a revoked peer's live connection must be closed");
    assert!(matches!(error, Error::Transport(_)), "got {error:?}");

    assert_eq!(
        kept.knock().await.expect("answers"),
        1,
        "revoking one peer must not disturb another"
    );
}

/// The ordering is the correctness property: the policy is swapped **before**
/// anything is closed. Closing first leaves a window in which a revoked peer
/// reconnects and is admitted by the policy on its way out — and a window that
/// small is one nobody would ever reproduce from a bug report.
///
/// Proven from the outside, which is the only place it is observable: the
/// instant `replace` returns, a fresh dial by the revoked peer is already
/// refused. If the swap happened after the close, this dial would race it.
#[tokio::test]
async fn a_revoked_peer_cannot_reconnect_during_the_revocation() {
    let node = local_node().await;
    let peer = local_node().await;

    let policy = Arc::new(DynamicAdmission::new(Allowlist::only([peer.id()])));
    let _running = Server::builder(&node)
        .service(
            gate::service(Gate, Allowlist::anyone())
                .admission(policy.clone())
                .build(),
        )
        .spawn()
        .expect("serves");

    let first = gate::Client::connect(&peer, node.endpoint().addr())
        .await
        .expect("dials");
    assert_eq!(first.knock().await.expect("answers"), 1);

    let closed = policy.replace(Allowlist::only([node.id()]));
    assert_eq!(closed, vec![peer.id()]);

    let again = gate::Client::connect(&peer, node.endpoint().addr())
        .await
        .expect("the handshake still completes");
    assert!(
        again.knock().await.is_err(),
        "the new policy must already be in force when replace returns"
    );
}

/// A policy that cannot be consulted refuses, and refuses **distinguishably**.
/// A backend that answered `false` would be byte-identical to "not on the
/// list", and an operator who cannot tell a dead authorizer from an unlisted
/// peer reaches for `Allowlist::anyone()` during an incident — a fail-open
/// arrived at through a diagnostic hole.
#[tokio::test]
async fn a_policy_that_cannot_be_consulted_refuses() {
    let node = local_node().await;
    let peer = local_node().await;

    let _running = Server::builder(&node)
        .service(
            gate::service(Gate, Allowlist::anyone())
                .admission(Arc::new(Unreachable))
                .build(),
        )
        .spawn()
        .expect("serves");

    let client = gate::Client::connect(&peer, node.endpoint().addr())
        .await
        .expect("the handshake completes");
    let error = client
        .knock()
        .await
        .expect_err("an unreachable policy admits nobody");
    assert!(matches!(error, Error::Transport(_)), "got {error:?}");
}

/// The registry forgets a peer that leaves on its own, so a later revocation
/// does not report closing a connection that was already gone.
#[tokio::test]
async fn a_departed_peer_is_not_reported_as_revoked() {
    let node = local_node().await;
    let peer = local_node().await;

    let policy = Arc::new(DynamicAdmission::new(Allowlist::only([peer.id()])));
    let _running = Server::builder(&node)
        .service(
            gate::service(Gate, Allowlist::anyone())
                .admission(policy.clone())
                .build(),
        )
        .spawn()
        .expect("serves");

    let client = gate::Client::connect(&peer, node.endpoint().addr())
        .await
        .expect("dials");
    assert_eq!(client.knock().await.expect("answers"), 1);
    assert_eq!(policy.live(), 1);

    client.close().await.expect("closes");
    for _ in 0..100 {
        if policy.live() == 0 {
            break;
        }
        n0_future::time::sleep(Duration::from_millis(50)).await;
    }
    assert_eq!(policy.live(), 0, "a departed peer must be forgotten");

    assert!(
        policy.replace(Allowlist::only([node.id()])).is_empty(),
        "revoking a peer that already left closes nothing"
    );
}

/// `Allowlist` is the canonical policy, not a legacy path: a service built the
/// ordinary way admits through the same port.
#[tokio::test]
async fn an_allowlist_is_itself_an_admission() {
    let node = local_node().await;
    let stranger = local_node().await;

    let _running = gate::serve(&node, Gate, Allowlist::only([node.id()])).expect("serves");

    let client = gate::Client::connect(&stranger, node.endpoint().addr())
        .await
        .expect("the handshake completes");
    assert!(
        client.knock().await.is_err(),
        "the fixed list must still fail closed"
    );
}
