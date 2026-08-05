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

use bytes::Bytes;
use iroh::{
    endpoint::{Connection, ConnectionError, RecvStream, SendStream},
    protocol::{AcceptError, DynProtocolHandler, ProtocolHandler, Router},
    EndpointAddr, EndpointId,
};
use serde::{de::DeserializeOwned, Serialize};
use tokio::task::JoinSet;

use crate::wire::FrameReader;
use crate::error::ConfigError;
use crate::idempotency::{Claim, Ledger};
use crate::middleware::wrap;
use crate::{
    wire, Allowlist, Body, BodyReader, Context, Error, IdempotencyKey, IdempotencyRefusal,
    Middleware, Next, Node, Outcome, RemoteError, Result, Rpc, WireResponse,
};

type BoxFut<T> = Pin<Box<dyn Future<Output = T> + Send>>;

/// An erased async handler: who is calling, the request plus its incoming body,
/// answered with a response plus its outgoing body. The macro boxes the user's
/// `async fn` into this so `RpcService` needs no extra generic parameters.
///
/// This is the innermost [`Next`]: the same shape, named for the one position
/// that has no layer beneath it.
///
/// The [`Context`] is here because the proven peer identity used to reach the
/// edge of the handler and be thrown away. An actor that answers a peer later,
/// or writes an audit record, needs `cx.peer()`, and the only alternative is a
/// peer id carried in the request body — which is a claim, not a proof.
pub type Dispatch<Req, Resp> =
    Arc<dyn Fn(Context, Req, BodyReader) -> BoxFut<(Outcome<Resp>, Body)> + Send + Sync>;

/// Serves one protocol: authorizes the peer once per connection, then answers
/// every stream that connection opens, concurrently.
pub struct RpcService<Req, Resp> {
    next: Next<Req, Resp>,
    allow: Arc<Allowlist>,
    /// Every recorded reply for THIS service. One per service rather than one
    /// per node, because a record is an encoded reply of this service's
    /// response type. See [`Ledger`].
    ledger: Arc<Ledger>,
    _p: PhantomData<fn(Req) -> Resp>,
}

impl<Req, Resp> Clone for RpcService<Req, Resp> {
    fn clone(&self) -> Self {
        Self {
            next: self.next.clone(),
            allow: self.allow.clone(),
            ledger: self.ledger.clone(),
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
    /// Build a service from an allowlist and an erased async handler, with no
    /// layers around it. Use [`ServiceBuilder`] to add layers.
    pub fn new(allow: Allowlist, dispatch: Dispatch<Req, Resp>) -> Self
    where
        Req: 'static,
        Resp: 'static,
    {
        Self {
            next: dispatch,
            allow: Arc::new(allow),
            ledger: Ledger::new(),
            _p: PhantomData,
        }
    }
}

/// What the caller's idempotency key is worth, or `None` when it sent no key.
///
/// The `validate` here is not a second opinion about
/// [`IdempotencyKey::new`]. It is the only check this value has ever had:
/// `derive(Deserialize)` builds the string field by field and never calls a
/// constructor, so a peer's key arrives unchecked whatever our constructor
/// allows. Skipping it would key a map on a string a peer chose the length of.
fn claim_key(ledger: &Arc<Ledger>, peer: EndpointId, key: Option<IdempotencyKey>) -> Option<Claim> {
    let key = key?;
    if let Err(error) = key.validate() {
        return Some(Claim::Refused(
            IdempotencyRefusal::Malformed(error.to_string()).into(),
        ));
    }
    Some(ledger.claim(peer, key))
}

/// Write a recorded reply again, byte for byte.
async fn replay(send: &mut SendStream, frame: Bytes) -> Result<()> {
    wire::write_frame(send, frame).await?;
    send.finish()?;
    Ok(())
}

/// Refuse a call that was never entered. Carries no body, like every refusal.
async fn refuse<Resp: Serialize>(send: &mut SendStream, refusal: RemoteError) -> Result<()> {
    wire::write_header(send, Some(0), None, &Outcome::<Resp>::Err(refusal)).await?;
    send.finish()?;
    Ok(())
}

/// Answer one call on its own stream.
async fn serve_one<Req, Resp>(
    next: Next<Req, Resp>,
    ledger: Arc<Ledger>,
    peer: EndpointId,
    mut send: SendStream,
    recv: RecvStream,
) -> Result<()>
where
    Req: Rpc + DeserializeOwned,
    Resp: Serialize,
{
    let mut frames = FrameReader::new(recv);
    // A stream opened and finished without a request is not an error; the peer
    // simply changed its mind.
    let Some(mut request) = frames.read_header::<Req>().await? else {
        return Ok(());
    };

    let cx = Context::new(peer, Req::SERVICE, request.message.method())
        .with_actor(request.message.actor().cloned());

    // Claimed BEFORE the handler is entered, which is the whole at-most-once
    // property: the only way back from a claimed key to a vacant one is
    // eviction.
    let guard = match claim_key(&ledger, peer, request.idempotency.take()) {
        None => None,
        Some(Claim::Fresh(guard)) => Some(guard),
        Some(Claim::Replay(frame)) => return replay(&mut send, frame).await,
        Some(Claim::Refused(refusal)) => return refuse::<Resp>(&mut send, refusal).await,
    };

    let (outcome, body) = next(
        cx,
        request.message,
        BodyReader::new(frames, request.body_len),
    )
    .await;

    let frame = wire::encode_header(body.declared_len(), None, &outcome)?;
    // Recorded before the reply is written, so a reply lost on the way back
    // still replays. That is the failure this exists for: the handler finished
    // and the caller never heard.
    //
    // A reply with a payload behind it is recorded as unrecordable rather than
    // as itself. Bodies are streamed and never held, and a header replayed
    // without its payload decodes into a body that never arrives.
    if let Some(guard) = guard {
        guard.record(&frame, body.declared_len() != Some(0));
    }
    wire::write_frame(&mut send, frame).await?;
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
pub(crate) fn is_clean_close(error: &ConnectionError) -> bool {
    matches!(
        error,
        ConnectionError::ApplicationClosed(_)
            | ConnectionError::ConnectionClosed(_)
            | ConnectionError::LocallyClosed
    )
}

impl<Req, Resp> ProtocolHandler for RpcService<Req, Resp>
where
    Req: Rpc + DeserializeOwned + Send + Sync + 'static,
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
                    calls.spawn(serve_one(
                        self.next.clone(),
                        self.ledger.clone(),
                        peer,
                        send,
                        recv,
                    ));
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
    Req: Rpc + DeserializeOwned + Send + Sync + 'static,
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

/// Assembles one service: its allowlist, its handler, and the layers around it.
///
/// Returned by the generated `service()`, so a caller adds behaviour without
/// the macro knowing anything about it.
pub struct ServiceBuilder<Req, Resp> {
    alpn: &'static [u8],
    allow: Allowlist,
    base: Next<Req, Resp>,
    layers: Vec<Arc<dyn Middleware<Req, Resp>>>,
}

impl<Req, Resp> ServiceBuilder<Req, Resp>
where
    Req: Rpc + DeserializeOwned + Send + Sync + 'static,
    Resp: Serialize + Send + Sync + 'static,
{
    /// Called by generated code. `dispatch` is the call into the user's handler.
    #[doc(hidden)]
    pub fn new(alpn: &'static [u8], allow: Allowlist, dispatch: Dispatch<Req, Resp>) -> Self {
        Self {
            alpn,
            allow,
            base: dispatch,
            layers: Vec::new(),
        }
    }

    /// Add a layer. The first layer added is the outermost: it sees the request
    /// first and the reply last.
    #[must_use]
    pub fn layer(mut self, layer: impl Middleware<Req, Resp>) -> Self {
        self.layers.push(Arc::new(layer));
        self
    }

    /// Freeze the chain and erase the schema.
    pub fn build(self) -> Service {
        let mut chain = self.base;
        for layer in self.layers.into_iter().rev() {
            chain = wrap(layer, chain);
        }
        Service {
            alpn: self.alpn,
            handler: Box::new(RpcService {
                next: chain,
                allow: Arc::new(self.allow),
                ledger: Ledger::new(),
                _p: PhantomData,
            }),
        }
    }
}

impl<Req, Resp> std::fmt::Debug for ServiceBuilder<Req, Resp> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ServiceBuilder")
            .field("alpn", &String::from_utf8_lossy(self.alpn))
            .field("allow", &self.allow)
            .field("layers", &self.layers.len())
            .finish()
    }
}

/// ALPNs keylink refuses to mount by hand.
///
/// These belong to protocols keylink means to mount ITSELF, with a policy
/// attached. `iroh-blobs` is the reason this list exists: its own examples and
/// module documentation all write `BlobsProtocol::new(&store, None)`, which
/// serves every hash to every peer and accepts writes, because
/// `EventMask::DEFAULT` disables the admission callback entirely. Mounting that
/// beside an allowlisted keylink service puts an unauthenticated protocol on an
/// otherwise closed node.
///
/// A prose warning does not stop that; a startup error does.
const RESERVED_ALPNS: &[&[u8]] = &[
    // iroh_blobs::ALPN
    b"/iroh-bytes/4",
];

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

    /// Register a protocol keylink did not generate, on the same node.
    ///
    /// `iroh-gossip` and similar are ordinary [`ProtocolHandler`]s, so they
    /// belong in the same router as our services. They must be registered here,
    /// before [`ServerBuilder::spawn`]: iroh's `Router` accepts protocols only
    /// through its builder, and `spawn` consumes it.
    ///
    /// ```ignore
    /// Server::builder(&node)
    ///     .service(inventory::service(handler, allow).build())
    ///     .service(Service::foreign(iroh_gossip::ALPN, gossip))
    ///     .spawn()?;
    /// ```
    ///
    /// # Authorization
    ///
    /// A foreign protocol brings its own admission rules, or none. [`Allowlist`]
    /// does NOT cover it, and neither does [`Middleware`](crate::Middleware).
    /// Read the protocol's documentation before you mount it, and assume it
    /// admits everyone until you have found the sentence that says otherwise.
    ///
    /// # Reserved ALPNs
    ///
    /// A few ALPNs are refused here, and [`ServerBuilder::spawn`] returns
    /// [`ConfigError::ReservedAlpn`]. Those are protocols keylink means to mount
    /// itself, with a policy attached. Hand-mounting one would be a way to get
    /// an unauthorized protocol onto an otherwise allowlisted node.
    pub fn foreign(alpn: &'static [u8], handler: impl ProtocolHandler) -> Self {
        Self {
            alpn,
            handler: Box::new(handler),
        }
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
    ///
    /// Takes `&self` rather than consuming, so a `Server` can live in an `Arc`
    /// and be shut down from whichever task notices first. iroh's
    /// `Router::shutdown` is idempotent, so a second call returns `Ok`.
    pub async fn shutdown(&self) -> Result<()> {
        self.router
            .shutdown()
            .await
            .map_err(|e| Error::HandlerPanic(e.to_string()))
    }

    /// The underlying iroh router.
    ///
    /// Note that protocols CANNOT be added through this handle: iroh's `Router`
    /// accepts protocols only through its builder, which `spawn` consumes. To
    /// put a foreign protocol on this node, register it with
    /// [`Service::foreign`] before spawning.
    pub fn router(&self) -> &Router {
        &self.router
    }
}

/// Collects the services one node will answer.
///
/// This exists because [`RouterBuilder::spawn`](iroh::protocol::RouterBuilder::spawn) calls
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
            if RESERVED_ALPNS.contains(&service.alpn) {
                return Err(Error::Config(ConfigError::ReservedAlpn(
                    String::from_utf8_lossy(service.alpn).into_owned(),
                )));
            }
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
    Req: Rpc + DeserializeOwned + Send + Sync + 'static,
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
    /// Attached to every call this client makes. See
    /// [`with_idempotency`](Self::with_idempotency).
    idempotency: Option<IdempotencyKey>,
    _p: PhantomData<fn(Req) -> Resp>,
}

impl<Req, Resp> RpcClient<Req, Resp> {
    /// The live connection, for crate code that observes it rather than uses it.
    ///
    /// An accessor rather than a `pub(crate)` field on purpose: a field can be
    /// assigned as well as read, and replacing a live connection underneath a
    /// client is never correct.
    pub(crate) fn connection(&self) -> &Connection {
        &self.conn
    }

    /// Scope this client to one idempotency key, so a call it makes runs at
    /// most once however many times it is sent.
    ///
    /// The key rides beside the request rather than inside it, so the generated
    /// method still takes domain arguments and nothing else:
    ///
    /// ```ignore
    /// let ticket = client
    ///     .with_idempotency(IdempotencyKey::new("reserve-4471")?)
    ///     .reserve("ADU-014".into(), 2)
    ///     .await?;
    /// ```
    ///
    /// Cheap: a `Connection` is a handle, so this shares one rather than
    /// opening another.
    ///
    /// # Make one call with it
    ///
    /// The key goes on EVERY call made through the returned client, so it is
    /// meant for one. A second, DIFFERENT call through the same handle is
    /// answered from the first call's record, and the generated client refuses
    /// a reply to a question it did not ask
    /// ([`Error::UnexpectedResponse`]). That is a loud failure rather than a
    /// wrong answer, but it is still a mistake worth not making.
    #[must_use]
    pub fn with_idempotency(&self, key: IdempotencyKey) -> Self {
        Self {
            conn: self.conn.clone(),
            idempotency: Some(key),
            _p: PhantomData,
        }
    }
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
            idempotency: None,
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

        wire::write_header(
            &mut send,
            body.declared_len(),
            self.idempotency.as_ref(),
            &request,
        )
        .await?;
        body.write_to(&mut send).await?;
        // The request is complete. Finishing tells the peer so without a
        // sentinel frame, and lets it start work before we read a byte.
        send.finish()?;

        let mut frames = FrameReader::new(recv);
        let Some(response) = frames.read_header::<WireResponse<Resp>>().await? else {
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
