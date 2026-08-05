//! The request/response plumbing the generated code sits on.
//!
//! Callers of a `service!`-generated client never name anything in here.
//!
//! # One stream per call
//!
//! QUIC streams are free: opening one costs no round trip, because the stream
//! id rides on the first frame of data. Serialising every call behind a mutex
//! on one shared stream therefore buys nothing and costs the two things QUIC
//! was built to remove — concurrency and head-of-line blocking. A slow handler
//! used to stall every later call on the connection.
//!
//! So each call gets its own bidirectional stream. There is no client mutex,
//! `call` takes `&self`, and the server answers requests in parallel. The
//! number of in-flight calls is bounded by QUIC's `max_concurrent_bidi_streams`
//! (100 by default), so an unbounded number of handler tasks is not reachable
//! from the network.
//!
//! It also removes a whole category of protocol design: with one stream per
//! call there is nothing to correlate, so there are no request ids, no
//! out-of-order reply matching, and no cancellation bookkeeping. Dropping the
//! stream cancels the call.

use std::future::Future;
use std::marker::PhantomData;
use std::pin::Pin;
use std::sync::Arc;

use iroh::{
    endpoint::{Connection, ConnectionError, RecvStream, SendStream},
    protocol::{AcceptError, ProtocolHandler, Router},
    EndpointAddr, EndpointId,
};
use serde::{de::DeserializeOwned, Serialize};
use tokio::task::JoinSet;

use crate::wire::FrameReader;
use crate::{wire, Allowlist, Body, BodyReader, Error, Node, Result};

type BoxFut<T> = Pin<Box<dyn Future<Output = T> + Send>>;

/// An erased async handler: request plus its incoming body, answered with a
/// response plus its outgoing body. The macro boxes the user's `async fn` into
/// this so `RpcService` needs no extra generic parameters.
pub type Dispatch<Req, Resp> =
    Arc<dyn Fn(Req, BodyReader) -> BoxFut<(Resp, Body)> + Send + Sync>;

/// Serves one protocol: authorizes the peer once per connection, then answers
/// every stream that connection opens, concurrently.
pub struct RpcService<Req, Resp> {
    dispatch: Dispatch<Req, Resp>,
    allow: Arc<Allowlist>,
    _p: PhantomData<fn(Req) -> Resp>,
}

impl<Req, Resp> Clone for RpcService<Req, Resp> {
    fn clone(&self) -> Self {
        Self {
            dispatch: self.dispatch.clone(),
            allow: self.allow.clone(),
            _p: PhantomData,
        }
    }
}

impl<Req, Resp> std::fmt::Debug for RpcService<Req, Resp> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RpcService")
            .field("allow", &self.allow)
            .finish_non_exhaustive()
    }
}

impl<Req, Resp> RpcService<Req, Resp>
where
    Req: DeserializeOwned + Send + 'static,
    Resp: Serialize + Send + 'static,
{
    /// Build a service from an allowlist and an erased async handler.
    pub fn new(allow: Allowlist, dispatch: Dispatch<Req, Resp>) -> Self {
        Self {
            dispatch,
            allow: Arc::new(allow),
            _p: PhantomData,
        }
    }
}

/// Answer one call on its own stream.
async fn serve_one<Req, Resp>(
    dispatch: Dispatch<Req, Resp>,
    mut send: SendStream,
    recv: RecvStream,
) -> Result<()>
where
    Req: DeserializeOwned,
    Resp: Serialize,
{
    let mut frames = FrameReader::new(recv);
    // A stream opened and finished without a request is not an error; the peer
    // simply changed its mind.
    let Some(request) = frames.read_header::<Req>().await? else {
        return Ok(());
    };

    let (response, body) = dispatch(request.message, BodyReader::new(frames, request.body_len)).await;

    wire::write_header(&mut send, body.declared_len(), &response).await?;
    body.write_to(&mut send).await?;
    send.finish()?;
    Ok(())
}

/// A connection ending is not a failure. Anything else is.
fn is_clean_close(error: &ConnectionError) -> bool {
    matches!(
        error,
        ConnectionError::ApplicationClosed(_)
            | ConnectionError::ConnectionClosed(_)
            | ConnectionError::LocallyClosed
    )
}

impl<Req, Resp> ProtocolHandler for RpcService<Req, Resp>
where
    Req: DeserializeOwned + Send + Sync + 'static,
    Resp: Serialize + Send + Sync + 'static,
{
    async fn accept(&self, connection: Connection) -> std::result::Result<(), AcceptError> {
        // The handshake proved this key. Whether it is welcome is our call.
        let peer: EndpointId = connection.remote_id();
        if !self.allow.permits(&peer) {
            connection.close(1u32.into(), b"unauthorized");
            return Ok(());
        }

        let mut calls = JoinSet::new();
        loop {
            // Reap finished calls so the set tracks in-flight work, not history.
            while calls.try_join_next().is_some() {}

            match connection.accept_bi().await {
                Ok((send, recv)) => {
                    calls.spawn(serve_one(self.dispatch.clone(), send, recv));
                }
                Err(error) if is_clean_close(&error) => break,
                Err(error) => return Err(AcceptError::from_err(error)),
            }
        }

        // The connection is gone, so every in-flight call is writing to a dead
        // stream. Aborting them is the honest end, not a lost result.
        calls.shutdown().await;
        Ok(())
    }
}

/// Start serving. The returned `Router` owns the accept loop; drop or
/// `shutdown()` it to stop.
pub fn serve<Req, Resp>(node: &Node, alpn: &'static [u8], service: RpcService<Req, Resp>) -> Router
where
    Req: DeserializeOwned + Send + Sync + 'static,
    Resp: Serialize + Send + Sync + 'static,
{
    Router::builder(node.endpoint().clone())
        .accept(alpn, service)
        .spawn()
}

/// A connected client.
///
/// `call` takes `&self` and holds no lock, so a client behind an `Arc` serves
/// any number of concurrent callers at full connection speed.
#[derive(Debug)]
pub struct RpcClient<Req, Resp> {
    conn: Connection,
    _p: PhantomData<fn(Req) -> Resp>,
}

impl<Req, Resp> RpcClient<Req, Resp>
where
    Req: Serialize + Send + Sync,
    Resp: DeserializeOwned + Send + Sync,
{
    /// Dial a peer by public key.
    ///
    /// An [`EndpointId`] is the normal argument and needs nothing else. A full
    /// [`EndpointAddr`] is accepted too, for a LAN or air-gapped network where
    /// no discovery service can resolve the key to a route.
    ///
    /// # Errors
    ///
    /// [`Error::Transport`] when the peer is unreachable, refuses us, or was
    /// built against a different schema — the ALPN gate reports that as
    /// "peer doesn't support any known protocol".
    pub async fn connect(
        node: &Node,
        peer: impl Into<EndpointAddr>,
        alpn: &'static [u8],
    ) -> Result<Self> {
        let conn = node.endpoint().connect(peer, alpn).await?;
        Ok(Self {
            conn,
            _p: PhantomData,
        })
    }

    /// Send one request with its body and await the response and its body.
    ///
    /// # Errors
    ///
    /// [`Error::Closed`] when the peer finishes the stream without answering,
    /// plus any transport or codec failure.
    pub async fn call(&self, request: Req, body: Body) -> Result<(Resp, BodyReader)> {
        let (mut send, recv) = self.conn.open_bi().await?;

        wire::write_header(&mut send, body.declared_len(), &request).await?;
        body.write_to(&mut send).await?;
        // The request is complete. Finishing tells the peer so without a
        // sentinel frame, and lets it start work before we read a byte.
        send.finish()?;

        let mut frames = FrameReader::new(recv);
        let Some(response) = frames.read_header::<Resp>().await? else {
            return Err(Error::Closed);
        };
        Ok((
            response.message,
            BodyReader::new(frames, response.body_len),
        ))
    }

    /// Close the connection gracefully.
    pub async fn close(self) -> Result<()> {
        self.conn.close(0u32.into(), b"done");
        self.conn.closed().await;
        Ok(())
    }
}
