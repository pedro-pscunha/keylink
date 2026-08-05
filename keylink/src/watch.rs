//! Learning that a peer is gone, without calling it.
//!
//! Every other failure in this crate arrives as the answer to a question you
//! asked. A dead peer does not. You find out on your next call, as an error,
//! however long after the fact that happens to be. On one machine that gap is a
//! curiosity; across a network it is the failure mode.
//!
//! A [`Watch`] closes the gap with the cheapest thing that could: a future that
//! resolves when the connection ends.
//!
//! ```ignore
//! let watch = client.watch();          // no call, no protocol, no wire change
//! match watch.await {
//!     Departure::Closed  => info!("peer said goodbye"),
//!     Departure::Local   => {}         // we shut down; nothing happened
//!     Departure::Lost(e) => reconnect(e).await,
//! }
//! ```
//!
//! # This is notification, not supervision
//!
//! RFC-04 §8 rejects supervision, and it is right to: restarting a remote actor
//! means nothing without durable state, and a peer that vanished is not fixed
//! by restarting something local. Being *told* is a different thing, and it is
//! the part that was worth keeping — it reports a fact instead of acting on
//! one. Nothing here restarts, retries or reconnects. What to do about a
//! departure stays where it belongs, with the caller.
//!
//! # What it costs
//!
//! Nothing runs. There is no task, no timer and no channel of ours: iroh keeps
//! a one-shot per waiter on the connection itself and fires all of them when it
//! ends. Creating a watch costs one registration. Dropping one removes it.
//!
//! # It does not keep the connection alive
//!
//! A watch holds a weak handle, never a `Connection`. That is precisely why
//! [`Connection::closed`] is not what sits underneath this: it borrows a strong
//! handle, and a diagnostic that pins its subject open is a leak wearing a
//! diagnostic's clothes. Drop the last real handle with a watch outstanding and
//! the connection still closes — and the watch still reports it, because the
//! registration was made while a strong handle existed.
//!
//! [`Connection::closed`]: iroh::endpoint::Connection::closed

use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

use iroh::endpoint::{Closed, Connection, ConnectionError};

use crate::rpc::is_clean_close;
use crate::{RpcClient, Transport};

/// How a connection ended.
///
/// Three endings, kept apart, because what they ask of you is different: a peer
/// that said goodbye asks for nothing, a peer that vanished asks for a
/// reconnect, and an ending we caused ourselves asks for silence. Folding the
/// third into the second is how an orderly shutdown becomes a page at 3am.
///
/// The same discipline the rest of the crate applies to a clean close and a
/// truncated frame ([`Error::Closed`](crate::Error::Closed) against
/// [`Error::FrameTruncated`](crate::Error::FrameTruncated)), applied one layer
/// up.
///
/// Deliberately exhaustive. A caller matching on this should have to confront
/// all three, and a wildcard arm is exactly the collapse this type exists to
/// prevent.
#[derive(Debug)]
pub enum Departure {
    /// The peer closed the connection, or its QUIC stack closed it for them.
    /// Nothing failed.
    ///
    /// The far side dropping its last `Connection` handle lands here too: QUIC
    /// still emits a close frame, so it is indistinguishable from an explicit
    /// goodbye — and it is one.
    Closed,

    /// We ended it: [`RpcClient::close`], a [`Server`](crate::Server) shutting
    /// down, or the last handle on this side going out of scope.
    ///
    /// Never a reason to reconnect and never a reason to log a failure. The
    /// peer may be in perfect health; we simply stopped talking to it.
    Local,

    /// The connection failed. The peer is unreachable, was reset, or stopped
    /// answering for longer than the idle timeout allows.
    ///
    /// This is the ending a watch exists to report. It is the one you would
    /// otherwise discover on your next call, and only then.
    Lost(Transport),
}

impl Departure {
    /// Classify an ending.
    ///
    /// `None` means no strong handle existed when the watch was created, so
    /// iroh had nothing to register against. [`Watch::on`] takes `&Connection`
    /// and [`RpcClient::watch`] holds one, so it is unreachable from here; if
    /// it ever were reached, this side had already let the connection go, which
    /// is [`Self::Local`].
    fn classify(closed: Option<Closed>) -> Self {
        let Some(closed) = closed else {
            return Self::Local;
        };
        match closed.reason {
            // Before `is_clean_close`, which counts this as clean — correctly,
            // for its own question. "Not a failure" and "not the peer" are
            // different questions, and this is the arm that separates them.
            ConnectionError::LocallyClosed => Self::Local,
            reason if is_clean_close(&reason) => Self::Closed,
            reason => Self::Lost(reason.into()),
        }
    }
}

/// A future that resolves when a connection ends.
///
/// Cancel it by dropping it. Nothing was spawned, so there is nothing to stop
/// and nothing to join; the drop removes one registration and returns.
///
/// Hold as many as you like on one connection. Each is independent and every
/// one fires, so a watch is not a resource to be shared, handed around or
/// counted.
#[must_use = "a watch does nothing unless you await it"]
pub struct Watch {
    /// Boxed because iroh returns an opaque `impl Future`, which cannot be
    /// named in a field. The allocation buys the property that matters:
    /// registration happens *here*, while a strong handle is guaranteed to
    /// exist, rather than at first poll — by which time the connection may
    /// already be gone, taking the reason with it.
    closed: Pin<Box<dyn Future<Output = Option<Closed>> + Send>>,
}

impl Watch {
    /// Watch a connection you already hold.
    ///
    /// This is the accept side. A [`ProtocolHandler`](iroh::protocol::ProtocolHandler)
    /// — including one mounted with [`Service::foreign`](crate::Service::foreign)
    /// — is handed a `Connection`, and this turns it into "tell me when this
    /// peer leaves". [`RpcClient::watch`] is the dialling side of the same
    /// thing.
    pub fn on(connection: &Connection) -> Self {
        Self {
            closed: Box::pin(connection.weak_handle().closed()),
        }
    }
}

impl Future for Watch {
    type Output = Departure;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.get_mut()
            .closed
            .as_mut()
            .poll(cx)
            .map(Departure::classify)
    }
}

impl fmt::Debug for Watch {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Watch").finish_non_exhaustive()
    }
}

impl<Req, Resp> RpcClient<Req, Resp> {
    /// Resolve when this peer goes away.
    ///
    /// The dialling side of [`Watch::on`]. Takes `&self` and holds nothing, so
    /// a client behind an `Arc` can hand a watch to every caller that wants one
    /// while the client itself stays usable for calls.
    pub fn watch(&self) -> Watch {
        Watch::on(self.connection())
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::expect_used, clippy::unwrap_used)]

    use std::time::Duration;

    use iroh::endpoint::{presets, Connection, QuicTransportConfig};
    use iroh::Endpoint;

    use super::*;
    use crate::testing;

    /// Long enough that a slow machine never trips it, short enough that a
    /// watch which fails to fire is reported as a failure rather than a hang.
    const PATIENCE: Duration = Duration::from_secs(10);

    async fn within<F: Future>(fut: F) -> F::Output {
        tokio::time::timeout(PATIENCE, fut)
            .await
            .expect("the watch resolves rather than hanging")
    }

    #[tokio::test]
    async fn a_peer_closing_cleanly_is_reported_as_a_clean_close() {
        let link = testing::link().await;
        let watch = Watch::on(&link.client);

        link.server.close(0u32.into(), b"bye");

        assert!(matches!(within(watch).await, Departure::Closed));
    }

    /// The far side dropping its handle is still a goodbye on the wire: QUIC
    /// emits a close frame with code 0. Worth pinning, because "dropped" reads
    /// like a failure and is not one.
    #[tokio::test]
    async fn a_peer_dropping_its_connection_is_still_a_clean_close() {
        let link = testing::link().await;
        let watch = Watch::on(&link.client);

        drop(link.server);

        assert!(matches!(within(watch).await, Departure::Closed));
    }

    /// The distinction the whole type exists for. `is_clean_close` counts a
    /// local close as clean, which is right for its question and wrong for this
    /// one: shutting ourselves down must never look like a peer leaving.
    #[tokio::test]
    async fn our_own_shutdown_is_not_reported_as_the_peer_leaving() {
        let link = testing::link().await;
        let watch = Watch::on(&link.client);

        link.client.close(0u32.into(), b"done");

        assert!(
            matches!(within(watch).await, Departure::Local),
            "our own close must not be confused with the peer's"
        );
    }

    /// Every waiter is independent, so there is no broadcast to fan out and no
    /// primary waiter whose departure starves the rest.
    #[tokio::test]
    async fn many_watchers_on_one_connection_all_fire() {
        const WATCHERS: usize = 16;

        let link = testing::link().await;
        let watches: Vec<Watch> = (0..WATCHERS).map(|_| Watch::on(&link.client)).collect();

        link.server.close(0u32.into(), b"bye");

        for watch in watches {
            assert!(matches!(within(watch).await, Departure::Closed));
        }
    }

    /// A watch must not pin its subject open.
    ///
    /// The observation is made from the far side: drop the last handle here and
    /// the peer has to see the connection close. An implementation that held a
    /// strong `Connection` — or spawned a task that did — would keep it alive
    /// and the peer would wait for the idle timeout instead.
    ///
    /// The second half is iroh's guarantee that a registration made while a
    /// strong handle existed still reports after every handle is gone.
    #[tokio::test]
    async fn a_watch_does_not_keep_the_connection_alive() {
        let link = testing::link().await;
        let from_the_peer = Watch::on(&link.server);
        let watch = Watch::on(&link.client);

        drop(link.client);

        assert!(
            matches!(within(from_the_peer).await, Departure::Closed),
            "the connection must close even with a watch outstanding"
        );
        assert!(
            matches!(within(watch).await, Departure::Local),
            "and the outstanding watch must still report"
        );
    }

    /// Cancelling costs nothing and takes nothing with it: the dropped watch
    /// releases the connection, and its siblings on the same connection are
    /// unaffected.
    #[tokio::test]
    async fn a_dropped_watch_leaks_nothing_and_breaks_no_sibling() {
        let link = testing::link().await;
        let from_the_peer = Watch::on(&link.server);
        let survivor = Watch::on(&link.client);

        drop(Watch::on(&link.client));

        drop(link.client);

        assert!(
            matches!(within(from_the_peer).await, Departure::Closed),
            "a dropped watch must not hold the connection open"
        );
        assert!(
            matches!(within(survivor).await, Departure::Local),
            "and must not consume the notification a sibling was waiting for"
        );
    }

    /// The ending this feature exists for, over a real connection.
    ///
    /// The peer's endpoint goes away without a graceful close, which iroh's own
    /// `Endpoint::close` documentation names as the case that makes "the remote
    /// end time out". No close frame arrives, so the observer learns only when
    /// the idle timeout expires — the discovery that would otherwise wait for
    /// the next call.
    #[tokio::test]
    async fn a_peer_that_vanishes_is_reported_as_lost_not_as_a_close() {
        let (observer_endpoint, observer, peer_endpoint, peer) = vanishing_link().await;

        // Order matters: the endpoint owns the socket, so it goes first. The
        // connection's own implicit close then has nothing to travel over,
        // which is exactly what a peer losing power looks like from here.
        drop(peer_endpoint);
        drop(peer);

        let departure = within(Watch::on(&observer)).await;
        assert!(
            matches!(departure, Departure::Lost(_)),
            "a vanished peer is a failure, not a goodbye: got {departure:?}"
        );
        drop(observer_endpoint);
    }

    /// Two endpoints whose connection fails fast when one side stops answering.
    ///
    /// Local to this module rather than in `testing`, because the short idle
    /// timeout is the whole point of the one test that uses it and is wrong
    /// everywhere else.
    async fn vanishing_link() -> (Endpoint, Connection, Endpoint, Connection) {
        const ALPN: &[u8] = b"keylink/watch-testing/1";
        /// The observer's patience for silence. Bounds one test's runtime.
        const IDLE: Duration = Duration::from_secs(2);

        let peer_endpoint = Endpoint::builder(presets::Minimal)
            .alpns(vec![ALPN.to_vec()])
            .bind()
            .await
            .expect("the peer endpoint binds");
        let observer_endpoint = Endpoint::builder(presets::Minimal)
            .transport_config(
                QuicTransportConfig::builder()
                    .max_idle_timeout(Some(IDLE.try_into().expect("a valid idle timeout")))
                    .build(),
            )
            .bind()
            .await
            .expect("the observer endpoint binds");

        let addr = peer_endpoint.addr();
        let accepting = {
            let endpoint = peer_endpoint.clone();
            tokio::spawn(async move {
                endpoint
                    .accept()
                    .await
                    .expect("an incoming connection")
                    .await
                    .expect("a completed handshake")
            })
        };
        let observer = observer_endpoint
            .connect(addr, ALPN)
            .await
            .expect("the dial succeeds");
        let peer = accepting.await.expect("the accept task finishes");

        (observer_endpoint, observer, peer_endpoint, peer)
    }
}
