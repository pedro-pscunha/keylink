//! A real QUIC connection between two endpoints on this machine.
//!
//! Framing is not something to test against a mock. The behaviours that matter
//! — a clean finish, a truncated stream, flow control, chunk boundaries that
//! ignore write boundaries — belong to the transport, and a hand-written fake
//! would just encode the assumptions under test.
//!
//! `presets::Minimal` binds with no relay and no discovery, so these tests need
//! no network beyond the loopback interface.

#![allow(clippy::expect_used)]

use iroh::endpoint::{presets, Connection};
use iroh::Endpoint;

const ALPN: &[u8] = b"keylink/testing/1";

/// Both ends of one connection, plus the endpoints keeping it alive.
pub(crate) struct Link {
    pub(crate) client: Connection,
    pub(crate) server: Connection,
    _endpoints: [Endpoint; 2],
}

/// Dial a local endpoint by its address and hand back both sides.
pub(crate) async fn link() -> Link {
    let server_endpoint = Endpoint::builder(presets::Minimal)
        .alpns(vec![ALPN.to_vec()])
        .bind()
        .await
        .expect("the server endpoint binds");
    let client_endpoint = Endpoint::bind(presets::Minimal)
        .await
        .expect("the client endpoint binds");

    let addr = server_endpoint.addr();
    let accepting = {
        let endpoint = server_endpoint.clone();
        tokio::spawn(async move {
            endpoint
                .accept()
                .await
                .expect("an incoming connection")
                .await
                .expect("a completed handshake")
        })
    };

    let client = client_endpoint
        .connect(addr, ALPN)
        .await
        .expect("the dial succeeds");
    let server = accepting.await.expect("the accept task finishes");

    Link {
        client,
        server,
        _endpoints: [server_endpoint, client_endpoint],
    }
}
