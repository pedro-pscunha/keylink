//! Effectively-once for one `rpc`, composed rather than assumed.
//!
//! Every piece of this already had its own test: the ledger dedupes by
//! `(peer, key)` and replays byte for byte (`tests/idempotency.rs`), and `Peer`
//! re-issues a failed call exactly once against a freshly dialled connection
//! (`tests/peer.rs`). What had no test was the **composition** — the property a
//! user actually wants, which is that losing a connection between the work and
//! the answer costs the answer and not the work.
//!
//! The ledger is created in `ServiceBuilder::build`, so there is one per built
//! `Service` and it is held by the router for as long as the process serves.
//! That is what makes the claim provable across a reconnect: the connection is
//! genuinely a different connection, and the record is genuinely the same
//! record.
//!
//! Nothing here is mocked. Two iroh endpoints on the loopback interface, one
//! real ledger, and a connection the serving side really closes.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use keylink::{
    Admission, Admittance, AdmittedPeer, Allowlist, Context, Departure, IdempotencyKey, KeyState,
    Peer, ProvenPeer, Revocation, Server as KeylinkServer, MAX_ATTACH, MAX_RECORDS, MAX_TOMBSTONES,
    RECORD_TTL,
};
use postcard_schema::Schema;
use serde::{Deserialize, Serialize};
use tokio::sync::{mpsc, watch};

// ────────────────────────────────────────────────────────────── the service

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Schema)]
pub struct Receipt {
    /// Which run of the handler produced this. A repeated number is proof the
    /// handler was not entered again, and it needs nothing from inside the
    /// serving side to read.
    pub run: u64,
    pub sku: String,
}

keylink::service! {
    service Orders;

    rpc Submit(sku: String, qty: u32) -> Receipt;
}

/// A handler that counts its runs and can be held inside one.
///
/// The hold is what makes "the caller lost its answer" a fact the test arranges
/// instead of a race it hopes for: the handler announces itself on `entered`,
/// the test throws the caller away, and only then is the handler let go.
struct Desk {
    runs: AtomicU64,
    entered: mpsc::UnboundedSender<u64>,
    release: watch::Receiver<bool>,
}

impl orders::Handler for Desk {
    async fn submit(&self, _cx: &Context, sku: String, _qty: u32) -> Receipt {
        let run = self.runs.fetch_add(1, Ordering::SeqCst) + 1;
        // A receiver the test has dropped means the test is already failing;
        // there is nothing useful for a handler to do about that.
        let _ = self.entered.send(run);
        let mut release = self.release.clone();
        // `wait_for` re-reads the current value before parking, so a release
        // that lands between the send above and this line is not missed.
        let _ = release.wait_for(|open| *open).await;
        Receipt { run, sku }
    }
}

// ──────────────────────────────────────────────────── counting the handshakes

/// An admission that counts connections and keeps the last one's kill switch.
///
/// Two jobs, both needed to make "it reconnected" a fact: `admits` runs once per
/// connection, so a count of calls is a count of handshakes; and `admitted`
/// hands over a [`Revocation`], which is how the serving side closes a live
/// connection without shutting the service — and therefore its ledger — down.
struct Gate {
    allow: Allowlist,
    handshakes: AtomicUsize,
    live: Mutex<Option<Revocation>>,
}

impl Gate {
    fn new(allow: Allowlist) -> Self {
        Self {
            allow,
            handshakes: AtomicUsize::new(0),
            live: Mutex::new(None),
        }
    }

    fn handshakes(&self) -> usize {
        self.handshakes.load(Ordering::SeqCst)
    }

    /// Close the most recently admitted connection.
    fn revoke(&self) {
        self.slot()
            .as_ref()
            .expect("a connection was admitted")
            .close();
    }

    /// Poison recovery rather than propagation: nothing here can panic while
    /// the lock is held.
    fn slot(&self) -> std::sync::MutexGuard<'_, Option<Revocation>> {
        self.live.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

impl Admission for Gate {
    fn admits(&self, peer: &ProvenPeer) -> Admittance {
        self.handshakes.fetch_add(1, Ordering::SeqCst);
        if self.allow.admit(*peer).is_some() {
            Admittance::Admitted
        } else {
            Admittance::Refused
        }
    }

    fn admitted(&self, _peer: &AdmittedPeer, live: Revocation) {
        *self.slot() = Some(live);
    }
}

// ────────────────────────────────────────────────────────────────── fixture

mod common;
use common::local_node;

/// How long the test waits for something that should already have happened.
///
/// A liveness bound, never a measurement. Nothing here is asserted to be *fast*;
/// this is what turns a deadlock into a named failure instead of a hung test
/// runner, and the passing path never comes near it.
const NO_PROGRESS: Duration = Duration::from_secs(20);

// ───────────────────────────────────────────── the composed guarantee

/// The whole recipe, end to end: the work runs once, the answer survives the
/// connection that was supposed to carry it, and the retry is answered from the
/// record rather than by running the handler again.
#[tokio::test]
async fn a_retried_call_across_a_reconnect_runs_exactly_once() {
    let (entered, mut entries) = mpsc::unbounded_channel();
    let (release, held) = watch::channel(false);
    let gate = Arc::new(Gate::new(Allowlist::anyone()));

    let server_node = local_node().await;
    let client_node = local_node().await;
    let _server = KeylinkServer::builder(&server_node)
        .service(
            orders::service(
                Desk {
                    runs: AtomicU64::new(0),
                    entered,
                    release: held,
                },
                // Replaced by `admission` below, which is how that method is
                // documented to work: the list is itself an `Admission`, and
                // this one delegates to the same list.
                Allowlist::anyone(),
            )
            .admission(gate.clone())
            .build(),
        )
        .spawn()
        .expect("one service, one ALPN");

    let peer: Peer<orders::Client> = Peer::new(&client_node, server_node.endpoint().addr());
    let key = IdempotencyKey::new("submit-4471").expect("a valid key");

    // ── 1. One connection, and a watch on it, so "it died" is observed rather
    //       than assumed. A `Watch` registers on a weak handle, so holding one
    //       does not keep the connection open.
    let first = peer.connect().await.expect("the first dial");
    let departed = first.watch();
    drop(first);
    assert_eq!(gate.handshakes(), 1, "one dial, one handshake");

    // ── 2. The call reaches the handler, and then its caller goes away. This is
    //       the failure the ledger exists for: the work happened and nobody is
    //       left to be told.
    let mut attempt = Box::pin(peer.call(|client| {
        let key = key.clone();
        async move {
            client
                .with_idempotency(key)
                .submit("ADU-014".to_string(), 2)
                .await
        }
    }));
    let run = tokio::select! {
        finished = &mut attempt => {
            panic!("the handler is held; the call cannot answer yet, but it returned {finished:?}")
        }
        entered = entries.recv() => entered.expect("the handler announces itself on entry"),
    };
    assert_eq!(run, 1, "the first attempt entered the handler");
    drop(attempt);
    release
        .send(true)
        .expect("the handler is still holding a receiver");

    // ── 3. Wait for the record. It lands between the handler returning and the
    //       reply being written, and no signal from the handler can observe that
    //       step — so ask the ledger, over the wire, which is the only thing
    //       that knows. Every turn of this loop is a real round trip, so it is a
    //       convergence wait rather than a spin.
    let settled = tokio::time::timeout(NO_PROGRESS, async {
        loop {
            let state = peer
                .call(|client| {
                    let key = key.clone();
                    async move { client.key_state(key).await }
                })
                .await
                .expect("the key-state query answers");
            if state == KeyState::Done {
                break state;
            }
        }
    })
    .await;
    assert_eq!(
        settled.expect("the ledger settles the reply it could not deliver"),
        KeyState::Done,
    );

    // ── 4. A genuine reconnect, forced from the serving side. The admission
    //       policy closes the connection the first attempt used; the service,
    //       and therefore its ledger, keeps running.
    gate.revoke();
    let ending = tokio::time::timeout(NO_PROGRESS, departed)
        .await
        .expect("the first connection ends");
    assert!(
        matches!(ending, Departure::Closed),
        "the first connection ended because the far side closed it, and got {ending:?}",
    );

    // ── 5. The retry. `Peer::call` meets the dead connection, dials a new one,
    //       and re-issues the same key against it exactly once.
    let again = peer
        .call(|client| {
            let same_key = key.clone();
            async move {
                client
                    .with_idempotency(same_key)
                    .submit("ADU-014".to_string(), 2)
                    .await
            }
        })
        .await
        .expect("the retry is answered");

    assert_eq!(
        again,
        Receipt {
            run,
            sku: "ADU-014".to_string()
        },
        "the retry must be answered from the record, byte for byte",
    );
    assert_eq!(
        gate.handshakes(),
        2,
        "the retry crossed a second connection, not the first",
    );
    assert!(
        matches!(entries.try_recv(), Err(mpsc::error::TryRecvError::Empty)),
        "the handler announced itself a second time, so the work ran twice",
    );
}

// ────────────────────────────────────────────────── the limits, pinned

/// The README states these numbers in prose, and prose does not compile.
///
/// This is what stops the two drifting: change a bound and the test that names
/// the README goes red, which is the moment to decide whether the prose or the
/// constant is wrong. The same argument as the ALPN pin in `protocol.rs`.
#[test]
fn the_limits_the_readme_names_are_the_ones_the_ledger_uses() {
    assert_eq!(RECORD_TTL, Duration::from_secs(600));
    assert_eq!(MAX_TOMBSTONES, 8192);
    assert_eq!(MAX_RECORDS, 1024);
    assert_eq!(MAX_ATTACH, Duration::from_secs(30));
}
