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

use std::collections::BTreeSet;
use std::future::Future;
use std::marker::PhantomData;
use std::pin::Pin;
use std::sync::Arc;

use iroh::{
    endpoint::{Connection, ConnectionError, RecvStream, SendStream},
    protocol::{AcceptError, DynProtocolHandler, ProtocolHandler, Router},
    EndpointAddr, EndpointId,
};
use serde::{de::DeserializeOwned, Serialize};
use tokio::task::JoinSet;

use crate::wire::FrameReader;
use crate::error::ConfigError;
use crate::{wire, Allowlist, Body, BodyReader, Error, Node, Outcome, Result};

type BoxFut<T> = Pin<Box<dyn Future<Output = T> + Send>>;

/// An erased async handler: request plus its incoming body, answered with a
/// response plus its outgoing body. The macro boxes the user's `async fn` into
/// this so `RpcService` needs no extra generic parameters.
pub type Dispatch<Req, Resp> =
    Arc<dyn Fn(Req, BodyReader) -> BoxFut<(Outcome<Resp>, Body)> + Send + Sync>;

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

    let (outcome, body) = dispatch(request.message, BodyReader::new(frames, request.body_len)).await;

    wire::write_header(&mut send, body.declared_len(), &outcome).await?;
    body.write_to(&mut send).await?;
    send.finish()?;
    Ok(())
}

/// Tell "your peer runs a different schema" apart from "the network is down".
///
/// TLS rejects an unknown ALPN with `no_application_protocol`, which is exactly
/// what the derived-ALPN drift gate looks like from the dialling side. Leaving
/// it as a generic transport error sends whoever reads the log to inspect the
/// network, when the actual fix is to rebuild one of the two peers.
fn classify_connect(err: iroh::endpoint::ConnectError, alpn: &[u8]) -> Error {
    // Matched on the rendered cause rather than the error enum: the alert code
    // is carried inside quinn's transport frame, several private layers down,
    // and the string is the only stable surface iroh exposes for it today.
    //
    // The markers are what iroh 1.0.3 actually prints, verified against a live
    // mismatch rather than taken from the TLS constant names:
    //   "aborted by peer: the cryptographic handshake failed:
    //    error 120: peer doesn't support any known protocol"
    // 120 is the TLS `no_application_protocol` alert. Narrow on purpose —
    // anything unrecognised stays a transport error.
    let rendered = format!("{err:?} {err}").to_ascii_lowercase();
    if rendered.contains("support any known protocol") || rendered.contains("error 120") {
        return Error::SchemaMismatch {
            ours: String::from_utf8_lossy(alpn).into_owned(),
        };
    }
    Error::Transport(err.into())
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

impl<Req, Resp> RpcService<Req, Resp>
where
    Req: DeserializeOwned + Send + Sync + 'static,
    Resp: Serialize + Send + Sync + 'static,
{
    /// Erase the schema so services with unrelated types can share one node.
    pub fn into_service(self, alpn: &'static [u8]) -> Service {
        Service {
            alpn,
            handler: Box::new(self),
        }
    }
}

/// One service, ready to register on a [`Server`].
///
/// The request and response types are erased here, which is what lets services
/// with completely unrelated schemas sit in one collection — and therefore on
/// one endpoint.
pub struct Service {
    alpn: &'static [u8],
    handler: Box<dyn DynProtocolHandler>,
}

impl Service {
    /// The ALPN this service answers on — `<crate>/<Service>/<schema hash>`.
    pub fn alpn(&self) -> &'static [u8] {
        self.alpn
    }
}

impl std::fmt::Debug for Service {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Service")
            .field("alpn", &String::from_utf8_lossy(self.alpn))
            .finish()
    }
}

/// The accept loop for every service on one node.
///
/// Drop it or call [`Server::shutdown`] to stop serving.
#[derive(Debug)]
#[must_use = "the server stops when dropped; hold it for as long as you serve"]
pub struct Server {
    router: Router,
}

impl Server {
    /// Register services on `node`.
    pub fn builder(node: &Node) -> ServerBuilder<'_> {
        ServerBuilder {
            node,
            services: Vec::new(),
        }
    }

    /// Stop accepting, and wait for in-flight connections to finish.
    pub async fn shutdown(self) -> Result<()> {
        self.router
            .shutdown()
            .await
            .map_err(|e| Error::HandlerPanic(e.to_string()))
    }

    /// The underlying iroh router, so a keylink service can share a node with a
    /// foreign iroh protocol such as `iroh-blobs` or `iroh-gossip`.
    pub fn router(&self) -> &Router {
        &self.router
    }
}

/// Collects the services one node will answer.
///
/// This exists because [`iroh::protocol::Router::spawn`] calls
/// `Endpoint::set_alpns`, which *overrides* the ALPN list rather than adding to
/// it. Building one router per service therefore left only the last service
/// reachable. Every service must go into a single router.
#[derive(Debug)]
pub struct ServerBuilder<'a> {
    node: &'a Node,
    services: Vec<Service>,
}

impl ServerBuilder<'_> {
    /// Add a service. Each keeps its own allowlist and its own ALPN.
    #[must_use]
    pub fn service(mut self, service: Service) -> Self {
        self.services.push(service);
        self
    }

    /// Start the accept loop.
    ///
    /// Two services on the same ALPN is a configuration error, not a
    /// last-one-wins race: the second would silently shadow the first.
    pub fn spawn(self) -> Result<Server> {
        let mut seen: BTreeSet<&'static [u8]> = BTreeSet::new();
        let mut builder = Router::builder(self.node.endpoint().clone());
        for service in self.services {
            if !seen.insert(service.alpn) {
                return Err(Error::Config(ConfigError::DuplicateAlpn(
                    String::from_utf8_lossy(service.alpn).into_owned(),
                )));
            }
            builder = builder.accept(service.alpn, service.handler);
        }
        Ok(Server {
            router: builder.spawn(),
        })
    }
}

/// Serve a single service. Shorthand for [`Server::builder`] with one service.
pub fn serve<Req, Resp>(
    node: &Node,
    alpn: &'static [u8],
    service: RpcService<Req, Resp>,
) -> Result<Server>
where
    Req: DeserializeOwned + Send + Sync + 'static,
    Resp: Serialize + Send + Sync + 'static,
{
    Server::builder(node)
        .service(service.into_service(alpn))
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
    /// [`Error::SchemaMismatch`] when the peer was built against a different
    /// contract, and [`Error::Transport`] when it is unreachable or refuses us.
    pub async fn connect(
        node: &Node,
        peer: impl Into<EndpointAddr>,
        alpn: &'static [u8],
    ) -> Result<Self> {
        let conn = node
            .endpoint()
            .connect(peer, alpn)
            .await
            .map_err(|err| classify_connect(err, alpn))?;
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
        let Some(response) = frames.read_header::<Outcome<Resp>>().await? else {
            return Err(Error::Closed);
        };
        match response.message {
            Outcome::Ok(value) => Ok((value, BodyReader::new(frames, response.body_len))),
            // A refusal carries no body: the handler never ran, or never
            // finished, so there is nothing to stream.
            Outcome::Err(refused) => Err(Error::Remote(refused)),
        }
    }

    /// Close the connection gracefully.
    pub async fn close(self) -> Result<()> {
        self.conn.close(0u32.into(), b"done");
        self.conn.closed().await;
        Ok(())
    }
}
