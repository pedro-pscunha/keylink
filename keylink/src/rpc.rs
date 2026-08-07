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
use std::task::{Context as TaskContext, Poll, Waker};

use bytes::Bytes;
use iroh::{
    endpoint::{Connection, ConnectionError, RecvStream, SendStream},
    protocol::{AcceptError, DynProtocolHandler, ProtocolHandler, Router},
    EndpointAddr, EndpointId,
};
use n0_future::task::JoinSet;
use serde::{de::DeserializeOwned, Serialize};

use crate::admission::settle;
use crate::error::ConfigError;
// `Keyable` through the module rather than through the crate root, unlike its
// siblings below: the root re-export is a promise to a *consumer*, and
// `tests/api.rs` is the only place that writes it from a consumer's position. An
// in-crate `crate::Keyable` would make the library itself the thing that fails
// when the re-export goes, which would leave that test proving nothing.
use crate::idempotency::{Claim, Guard, Keyable, Ledger, Records};
use crate::middleware::{wrap, Reached};
use crate::wire::FrameReader;
use crate::{
    wire, Admission, AdmittedPeer, Allowlist, BlobHandle, BlobId, Blobs, Body, BodyReader, Context,
    Error, Granting, Idempotency, IdempotencyKey, IdempotencyRefusal, KeyState, Middleware, Next,
    Node, Outcome, RemoteError, Result, Rpc, WireRequest, WireResponse,
};

type BoxFut<T> = Pin<Box<dyn Future<Output = T> + Send>>;

/// Which kind of stream the accept loop just took.
enum Stream {
    /// A call that expects a reply.
    Bi((SendStream, RecvStream)),
    /// A `tell`, which does not.
    Uni(RecvStream),
}

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
pub(crate) struct RpcService<Req, Resp> {
    next: Next<Req, Resp>,
    /// The policy this service admits against. An `Arc<dyn ..>` rather than a
    /// generic parameter: the decision is made once per connection and does no
    /// I/O, and a generic would reach every `service!` user's type errors.
    admission: Arc<dyn Admission>,
    /// The content store this service grants against, when it declares `blob`
    /// methods. `None` for every service that does not, which is why an
    /// ordinary service pays nothing for this feature existing.
    blobs: Option<Blobs>,
    /// What this service does with an idempotency key: which methods accept
    /// one, and where the accepted ones are recorded.
    keys: Keys,
    _p: PhantomData<fn(Req) -> Resp>,
}

impl<Req, Resp> Clone for RpcService<Req, Resp> {
    fn clone(&self) -> Self {
        Self {
            next: self.next.clone(),
            admission: self.admission.clone(),
            blobs: self.blobs.clone(),
            keys: self.keys.clone(),
            _p: PhantomData,
        }
    }
}

/// One service's whole answer to "what happens to an idempotency key here?".
///
/// One value rather than a [`Keyable`] beside an `Option<Arc<Ledger>>`, because the
/// two are read together on every keyed call and handing one on without the other
/// is the mistake this makes unwritable. It also puts the invariant in one place
/// instead of at each site that reads it.
#[derive(Debug, Clone)]
struct Keys {
    /// Which methods accept a key. Consulted before the key is validated and
    /// before the ledger is touched.
    keyable: Keyable,
    /// Every recorded reply for THIS service. One per service rather than one
    /// per node, because a record is an encoded reply of this service's response
    /// type. See [`Ledger`].
    ///
    /// `None` when [`Keyable::None`] was configured — not an empty ledger, no
    /// ledger. The `Option` is also what makes the claim path unable to reach a
    /// ledger that does not exist without an `unwrap`: [`Keys::ledger_for`]
    /// returns the two facts at once or neither.
    ledger: Option<Arc<Ledger>>,
}

impl Keys {
    /// Build the policy and, if it needs one, the ledger it records into.
    ///
    /// The one place the two are decided together, which is what lets
    /// `keyed_none_builds_no_ledger` probe the absence directly rather than
    /// reaching through an erased handler for it.
    fn new(keyable: Keyable, records: Records) -> Self {
        let ledger = keyable.needs_a_ledger().then(|| Ledger::new(records));
        Self { keyable, ledger }
    }

    /// The ledger a key on `method` belongs in, or `None` if this service does
    /// not accept one there.
    ///
    /// Both conditions in one answer on purpose. [`Keyable::accepts`] is what
    /// decides every refusal, `Keyable::None` included; the missing ledger is a
    /// consequence of that variant rather than a second policy, and its `None`
    /// here is belt and braces over a decision already taken.
    fn ledger_for(&self, method: &'static str) -> Option<&Arc<Ledger>> {
        self.ledger
            .as_ref()
            .filter(|_| self.keyable.accepts(method))
    }
}

impl<Req, Resp> std::fmt::Debug for RpcService<Req, Resp> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RpcService").finish_non_exhaustive()
    }
}

/// What the caller's idempotency key is worth, once any waiting is over.
///
/// Deliberately has no `Running` arm. Whether a caller waits for a call already
/// in flight is decided in [`claim_key`], so nothing downstream of it has to
/// remember that the question exists.
///
/// It was called `Keyed`, which is a name this crate needs for something else
/// entirely — a keyed actor is one addressed by an [`ActorId`](crate::ActorId),
/// and this is about an idempotency key. Two unrelated keys, one word, one
/// crate. Renamed at the root rather than worked around at the call sites that
/// meet the other meaning.
///
/// **The word was asked for a second time and refused again.** The serving-side
/// policy deciding which methods accept an idempotency key was drafted as
/// `Keyed`, which would have put `keylink::Keyed` two lines under
/// `keylink::KeyedService` in one export list, meaning a different key. It ships
/// as [`Keyable`], whose own doc comment carries the argument. So this paragraph
/// is not history: it is the standing rule, and it has now decided two names.
enum Claimed {
    /// No key was sent. Nothing to record and nothing to release.
    None,
    /// The key is ours for this call. Settle it when the chain returns.
    Run(Guard),
    /// Write these bytes. The handler is not entered.
    Replay(Bytes),
    /// Refuse. The handler is not entered.
    Refuse(RemoteError),
}

/// Map a settled claim, treating "still running" as the refusal it has always
/// been. Reached only by a caller that did not ask to attach, or one whose
/// budget ran out — and `InFlight` is the truth in both cases.
fn resolve(claim: Claim) -> Claimed {
    match claim {
        Claim::Fresh(guard) => Claimed::Run(guard),
        Claim::Replay(frame) => Claimed::Replay(frame),
        Claim::Refused(refusal) => Claimed::Refuse(refusal),
        Claim::Running(_) => Claimed::Refuse(IdempotencyRefusal::InFlight.into()),
    }
}

/// Take the key, waiting for a call already in flight when the caller asked to.
///
/// # The serving side's own decision comes first
///
/// A key on a method this service does not key is [`NotKeyable`], and that is
/// settled **before** the key is validated and therefore before the ledger is
/// reached at all: a refused key can neither claim a slot nor evict somebody
/// else's record. The order is visible rather than incidental — the `validate`
/// below is the first step on the road to `Ledger::claim`, and the policy check is
/// above it.
///
/// Refused, never ignored. A caller that wrote `with_idempotency` believes the
/// call runs at most once; running it and recording nothing would hand it a
/// guarantee it does not have, on the one path it cannot check. See [`NotKeyable`]
/// for the whole argument, and [`Keyable`] for who decides.
///
/// **A [`Middleware`] cannot do this instead.** `serve_one` claims the key and
/// *then* calls the chain, so a layer never meets an unclaimed key — by the time
/// the outermost layer runs, the slot is already taken. That is why the decision
/// is a property of the service rather than a layer somebody installs.
///
/// [`NotKeyable`]: IdempotencyRefusal::NotKeyable
///
/// # The key itself
///
/// The `validate` here is not a second opinion about
/// [`IdempotencyKey::new`]. It is the only check this value has ever had:
/// `derive(Deserialize)` builds the string field by field and never calls a
/// constructor, so a peer's key arrives unchecked whatever our constructor
/// allows. Skipping it would key a map on a string a peer chose the length of.
///
/// # The wakeup, and the hazard it has to survive
///
/// `Notify::notify_waiters` wakes only the waiters registered when it is
/// called, so a waiter that registers *after* the settle sleeps through it
/// forever. That is the objection RFC-04 §3.3 raised against attach, and it was
/// right. [`Notified::enable`](tokio::sync::futures::Notified::enable) answers
/// it: the waiter is registered before the slot is re-read, so a settle landing
/// in between is caught by the re-read rather than lost.
///
/// One wait and one re-claim, never a loop, so the work here is bounded by the
/// caller's own budget and by nothing else.
///
/// # What the waiting itself is bounded by
///
/// A parked attacher holds no claim, so `MAX_RUNNING_PER_PEER` and `MAX_RUNNING`
/// bound neither it nor the stream pair it keeps open. It takes a `Waiting` from
/// the ledger instead, refused **before** it parks and given back by `Drop` —
/// because the ordinary way a waiter stops waiting is that this task is aborted.
async fn claim_key(
    keys: &Keys,
    method: &'static str,
    peer: EndpointId,
    idem: Option<Idempotency>,
) -> Claimed {
    let Some(idem) = idem else {
        return Claimed::None;
    };
    let Some(ledger) = keys.ledger_for(method) else {
        return Claimed::Refuse(
            IdempotencyRefusal::NotKeyable {
                method: method.to_owned(),
            }
            .into(),
        );
    };
    if let Err(error) = idem.key().validate() {
        return Claimed::Refuse(IdempotencyRefusal::Malformed(error.to_string()).into());
    }
    let key = idem.key().clone();

    let notify = match ledger.claim(peer, key.clone()) {
        Claim::Running(notify) => notify,
        settled => return resolve(settled),
    };
    let Some(budget) = idem.attach_budget() else {
        return Claimed::Refuse(IdempotencyRefusal::InFlight.into());
    };
    // Taken before anything parks, and held to the end of this function so it
    // covers the wait AND the re-claim after it. A caller that is not going to
    // wait never reaches here, so it never spends a place.
    let _waiting = match ledger.wait(peer) {
        Ok(waiting) => waiting,
        Err(refusal) => return Claimed::Refuse(refusal),
    };

    let notified = notify.notified();
    tokio::pin!(notified);
    // Registered HERE, before the re-read below. Reversing these two lines is
    // the dropped-wakeup bug, and it would pass every test that is not run
    // under load.
    notified.as_mut().enable();

    match ledger.claim(peer, key.clone()) {
        Claim::Running(_) => {}
        settled => return resolve(settled),
    }
    if n0_future::time::timeout(budget, notified).await.is_err() {
        return Claimed::Refuse(IdempotencyRefusal::InFlight.into());
    }
    resolve(ledger.claim(peer, key))
}

/// Write a recorded reply again, byte for byte.
async fn replay(send: &mut SendStream, frame: Bytes) -> Result<()> {
    wire::write_frame(send, frame).await?;
    send.finish()?;
    Ok(())
}

/// Refuse a call that was never entered. Carries no body, like every refusal.
async fn refuse<Resp: Serialize>(send: &mut SendStream, refusal: RemoteError) -> Result<()> {
    wire::write_header(
        send,
        Some(0),
        None,
        &WireResponse::<Resp>::Call(Outcome::Err(refusal)),
    )
    .await?;
    send.finish()?;
    Ok(())
}

/// Answer a question about this peer's ledger.
///
/// No handler, no layer and no body: it is a read of keylink's own bookkeeping,
/// and the only thing it could be authorized against is the connection's
/// admission, which already happened. Peer-scoped by construction — the id is
/// the one iroh proved here, so a peer cannot name another's key.
///
/// A key that would be refused as malformed is reported [`KeyState::Fresh`],
/// which is not a fallback: such a key can never have been claimed, because
/// [`claim_key`] refuses it before the ledger sees it.
///
/// A service with no ledger — [`Keyable::None`] — reports `Fresh` for the same
/// reason, and it is a fact there too: nothing was ever recorded, so no key was
/// ever used. What `Fresh` does NOT promise on such a service is that a call
/// carrying the key would run; it would be refused [`NotKeyable`]. `KeyState`'s own
/// doc comment says so, because a caller reading "a call with this key would run"
/// off a service that keys nothing would be reading a false claim.
///
/// [`NotKeyable`]: IdempotencyRefusal::NotKeyable
async fn answer_key_state<Resp: Serialize>(
    send: &mut SendStream,
    ledger: Option<&Ledger>,
    peer: EndpointId,
    key: &IdempotencyKey,
) -> Result<()> {
    let state = match ledger {
        Some(ledger) if key.validate().is_ok() => ledger.key_state(peer, key),
        Some(_) | None => KeyState::Fresh,
    };
    wire::write_header(send, Some(0), None, &WireResponse::<Resp>::KeyState(state)).await?;
    send.finish()?;
    Ok(())
}

/// Drop the handles of calls that have already finished.
///
/// Replaces `JoinSet::try_join_next`, which tokio has and `n0_future`'s browser
/// shim does not. `poll_join_next` exists on both, so this is the portable
/// spelling of the same non-blocking sweep.
///
/// It matters MORE in a browser, not less. The shim keeps a second `Vec` of
/// handles purely so it can cancel them, and pruning that `Vec` happens only
/// inside `poll_join_next`. Without this call the accept loop grows it for the
/// lifetime of the connection.
///
/// Do NOT replace this with a `select!` between `accept_bi()` and
/// `join_next()`. The shim documents that a `join_next` future held across a
/// `spawn` stays pending even after the new task finishes, so such a loop would
/// stop reaping on wasm. This helper builds a fresh `Context` each call and
/// holds no future across a spawn, which is why it is safe.
fn reap_finished<T: 'static>(calls: &mut JoinSet<T>) {
    let mut cx = TaskContext::from_waker(Waker::noop());
    while let Poll::Ready(Some(_)) = calls.poll_join_next(&mut cx) {}
}

/// Handle one `tell` on its own unidirectional stream.
///
/// The shape of `serve_one` minus every reply. Nothing is written back, so
/// there is no `SendStream`, no idempotency record — the ledger records a reply
/// frame, and there is none — and no body to stream out.
///
/// No [`Keyable`] check either, for the same reason: there is no stream to refuse
/// on. A key on the envelope of a `tell` is dropped rather than answered, which
/// the generated client cannot produce — `with_idempotency` returns an
/// [`IdempotentClient`], which has no `tell` — and which a hand-rolled peer gets
/// the `tell` guarantee for, namely that nothing comes back.
///
/// A failure here is deliberately dropped rather than propagated. The caller
/// has already gone: it wrote, finished the stream, and returned. There is
/// nobody to tell, which is exactly what `tell` promises and why a call that
/// needs an answer must be an `rpc`.
async fn serve_one_tell<Req, Resp>(
    next: Next<Req, Resp>,
    blobs: Option<Blobs>,
    peer: AdmittedPeer,
    recv: RecvStream,
) -> Result<()>
where
    Req: Rpc + DeserializeOwned,
    Resp: Serialize,
{
    let mut frames = FrameReader::new(recv);
    let Some(request) = frames.read_header::<WireRequest<Req>>().await? else {
        return Ok(());
    };
    // A ledger query on a unidirectional stream has nowhere to send its answer,
    // so it is dropped rather than half-answered. The generated client never
    // sends one; a hand-rolled peer that does gets the `tell` guarantee, which
    // is that nothing comes back.
    let WireRequest::Call(message) = request.message else {
        return Ok(());
    };

    let cx = Context::new(peer.id(), Req::SERVICE, message.method())
        .with_actor(message.actor().cloned());

    // The body reader is handed over for symmetry with `serve_one`: a `tell`
    // carries no body today, and a future kind that does will find the seam
    // already here rather than needing the signature changed.
    let (_outcome, _body) = next(cx, message, BodyReader::new(frames, request.body_len)).await;
    let _ = blobs;
    Ok(())
}

/// Answer one call on its own stream.
async fn serve_one<Req, Resp>(
    next: Next<Req, Resp>,
    keys: Keys,
    blobs: Option<Blobs>,
    admitted: AdmittedPeer,
    mut send: SendStream,
    recv: RecvStream,
) -> Result<()>
where
    Req: Rpc + DeserializeOwned,
    Resp: Serialize + Granting,
{
    let peer = admitted.id();
    let mut frames = FrameReader::new(recv);
    // A stream opened and finished without a request is not an error; the peer
    // simply changed its mind.
    let Some(request) = frames.read_header::<WireRequest<Req>>().await? else {
        return Ok(());
    };

    let message = match request.message {
        WireRequest::KeyState(key) => {
            return answer_key_state::<Resp>(&mut send, keys.ledger.as_deref(), peer, &key).await;
        }
        WireRequest::Call(message) => message,
    };

    let cx =
        Context::new(peer, Req::SERVICE, message.method()).with_actor(message.actor().cloned());
    // Kept while the chain runs, because the chain's own `Outcome` cannot say
    // whether the work was entered — see `Reached`.
    let progress = cx.progress();

    // Claimed BEFORE the handler is entered, which is the whole at-most-once
    // property: the only way back from a claimed key to a vacant one is a
    // release, and a release happens exactly when nothing ran.
    //
    // `keys` is also where this service says which methods accept a key at all,
    // and a key on any other is refused here rather than quietly dropped. An
    // UNKEYED call still costs nothing: `request.idempotency` is `None`, which is
    // the first thing `claim_key` reads and the only thing it needs.
    let keyed = match claim_key(&keys, message.method(), peer, request.idempotency).await {
        Claimed::None => None,
        // The guard watches the marker, so this task being ABORTED — the
        // connection ended, the server shut down — still settles honestly, even
        // though `Drop` is then the only code left running.
        Claimed::Run(guard) => Some(guard.watching(progress.clone())),
        Claimed::Replay(frame) => return replay(&mut send, frame).await,
        Claimed::Refuse(refusal) => return refuse::<Resp>(&mut send, refusal).await,
    };

    let (outcome, body) = next(cx, message, BodyReader::new(frames, request.body_len)).await;

    // The grant is recorded BEFORE the reply is written, and that order is the
    // whole property: the address reaches the caller only in this reply, so a
    // peer cannot race ahead and fetch content it has not been told about.
    //
    // This is also the only place both halves exist. The handler returned the
    // id and knows nothing about peers; `accept` proved the peer and knows
    // nothing about ids. Neither could record this on its own, which is why the
    // reply carries the address back here through `Granting` rather than the
    // store being threaded down into dispatch.
    if let (Some(blobs), Outcome::Ok(response)) = (&blobs, &outcome) {
        if let Some(id) = response.granted() {
            blobs.grant(&admitted, id);
        }
    }

    let frame = wire::encode_header(
        body.declared_len(),
        None,
        &WireResponse::<Resp>::Call(outcome),
    )?;
    // Settled before the reply is written, so a reply lost on the way back
    // still replays. That is the failure this exists for: the handler finished
    // and the caller never heard.
    //
    // Which way it settles is decided by how far the call got, NOT by whether
    // the chain returned an `Outcome::Err` — a refusing layer and a panicking
    // actor turn produce the same value and owe completely different things.
    //
    // A reply with a payload behind it is recorded as unrecordable rather than
    // as itself. Bodies are streamed and never held, and a header replayed
    // without its payload decodes into a body that never arrives.
    if let Some(guard) = keyed {
        match progress.reached() {
            Reached::Completed => guard.record(&frame, body.declared_len() != Some(0)),
            // A layer said no without calling through; or the work was entered,
            // or merely reached a mailbox, and produced nothing. Dropping the
            // guard is what settles those, and it settles them the same way
            // whether this task returned or was aborted.
            Reached::NotDispatched | Reached::Dispatched => drop(guard),
        }
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
    Resp: Serialize + Granting + Send + Sync + 'static,
{
    async fn accept(&self, connection: Connection) -> std::result::Result<(), AcceptError> {
        // The handshake proved this key. Whether it is welcome is the policy's
        // call, and what comes back is a value rather than a `bool` somebody
        // must remember to have consulted — see `Admittance`. The policy also
        // takes the live connection here, which is the whole of what makes a
        // revocation later able to reach it.
        let Some((admitted, _live)) = settle(self.admission.as_ref(), &connection) else {
            return Ok(());
        };

        let mut calls = JoinSet::new();
        loop {
            // Reap finished calls so the set tracks in-flight work, not history.
            reap_finished(&mut calls);

            // Both stream kinds, on one connection. A `tell` arrives on a
            // unidirectional stream precisely because there is no reply to
            // send back, and accepting only `bi` would leave every one of them
            // queued and unread rather than refused.
            //
            // `select!` is safe here: neither branch holds a future across a
            // spawn, which is the pattern `reap_finished`'s comment warns is
            // broken on the browser's `JoinSet` shim.
            let accepted = tokio::select! {
                bi = connection.accept_bi() => bi.map(Stream::Bi),
                uni = connection.accept_uni() => uni.map(Stream::Uni),
            };

            match accepted {
                Ok(Stream::Bi((send, recv))) => {
                    calls.spawn(serve_one(
                        self.next.clone(),
                        self.keys.clone(),
                        self.blobs.clone(),
                        admitted,
                        send,
                        recv,
                    ));
                }
                Ok(Stream::Uni(recv)) => {
                    calls.spawn(serve_one_tell(
                        self.next.clone(),
                        self.blobs.clone(),
                        admitted,
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

#[cfg(test)]
mod tests {
    #![allow(clippy::expect_used, clippy::unwrap_used)]

    use std::future::poll_fn;
    use std::str::FromStr;
    use std::time::Duration;

    // `Future` and `Poll` arrive through the glob; `poll_fn` does not.
    use super::*;
    use crate::idempotency::MAX_WAITING_PER_PEER;

    /// A valid endpoint id, reused from `idempotency.rs` for the same reason.
    const A: &str = "a09aa5f47a6759802ff955f8dc2d2a14a5c99d23be97f864127ff9383455a4f0";

    /// A policy that keys everything, wrapped around a ledger a test already has.
    ///
    /// The attach tests below are about the waiting caps and not about the policy,
    /// so they take the shape a default-configured service has.
    fn any(ledger: &Arc<Ledger>) -> Keys {
        Keys {
            keyable: Keyable::Any,
            ledger: Some(Arc::clone(ledger)),
        }
    }

    /// **`Keyable::None` builds no ledger — not an empty one, none at all.**
    ///
    /// Asserted on the field `build()` actually fills, through the one constructor
    /// `build()` calls, so a stub cannot satisfy it: the assertion is
    /// `Option::is_none` on the ledger a served call would claim in. The `is_some`
    /// half is what stops "always `None`" passing — a policy that allocated
    /// nothing ever would be a service where no key works at all.
    ///
    /// It cannot be asserted over the wire. A refusal looks identical whether it
    /// came from a missing ledger or an empty one, which is the whole reason this
    /// probe reaches for the field instead. The behaviour half is
    /// `tests/idempotency.rs::a_key_on_a_method_the_service_does_not_key_is_refused`.
    ///
    /// Made red by `scripts/break-once.sh` replacing the body of
    /// `Keyable::needs_a_ledger` — `!matches!(self, Self::None)` — with `true`.
    /// Watched: `panicked at .., `Keyable::None` must not allocate a ledger nothing
    /// can ever write to`, `0 passed; 1 failed;
    /// 138 filtered out`. No other test in the crate noticed the edit.
    #[test]
    fn keyed_none_builds_no_ledger() {
        assert!(
            Keys::new(Keyable::None, Records::Memory).ledger.is_none(),
            "`Keyable::None` must not allocate a ledger nothing can ever write to"
        );
        assert!(
            Keys::new(Keyable::Any, Records::Memory).ledger.is_some(),
            "and every other policy must have one, or no key works at all"
        );
    }

    /// The seam the whole of Wave 1 hangs on: one lookup answers *both* "is this
    /// method keyed?" and "where do its records go?", so no call site can take the
    /// ledger while ignoring the policy.
    ///
    /// Made red by `scripts/break-once.sh` replacing
    /// `self.ledger.as_ref().filter(|_| self.keyable.accepts(method))` with
    /// `self.ledger.as_ref()` — the edit that would make the policy decorative
    /// while every idempotency test that predates this wave stayed green. Watched:
    /// `panicked at .., a refused method must not, or the policy decides nothing`, `0 passed; 1 failed; 138 filtered out`.
    #[test]
    fn the_ledger_is_reachable_only_through_the_policy() {
        let keys = Keys::new(Keyable::Only(&["Reserve"]), Records::Memory);
        assert!(
            keys.ledger_for("Reserve").is_some(),
            "an accepted method must reach the ledger"
        );
        assert!(
            keys.ledger_for("Quote").is_none(),
            "a refused method must not, or the policy decides nothing"
        );
    }

    /// The cap has to be met *before* the task parks, or it is not a cap: a
    /// waiter that is refused after it waits has already spent the task and the
    /// stream pair the refusal exists to save.
    ///
    /// `start_paused` is what makes that assertion honest rather than a race.
    /// With the bound removed this call parks on its 30 second budget, tokio
    /// auto-advances to the timer, and the answer comes back `InFlight` — so
    /// the test fails on the variant rather than hanging.
    #[tokio::test(start_paused = true)]
    async fn an_attacher_over_the_cap_is_refused_before_it_parks() {
        let ledger = Ledger::new(Records::Memory);
        let peer = EndpointId::from_str(A).expect("a valid endpoint id");
        let key = IdempotencyKey::new("k").expect("a valid key");

        let Claim::Fresh(_running) = ledger.claim(peer, key.clone()) else {
            panic!("the first claim must be fresh");
        };
        let places: Vec<_> = (0..MAX_WAITING_PER_PEER)
            .map(|_| ledger.wait(peer).expect("a free place"))
            .collect();

        let keyed = claim_key(
            &any(&ledger),
            "Probe",
            peer,
            Some(Idempotency::attaching_for(key, Duration::from_secs(30))),
        )
        .await;

        // Exhaustive, with no catch-all: the arm that would hide a regression
        // here is the one nobody thought to name.
        let refused = match keyed {
            Claimed::Refuse(RemoteError::Idempotency(refusal)) => refusal,
            Claimed::Refuse(other) => panic!("expected an idempotency refusal, got {other:?}"),
            Claimed::None => panic!("expected a refusal, got no key at all"),
            Claimed::Run(_) => panic!("expected a refusal, got the key"),
            Claimed::Replay(_) => panic!("expected a refusal, got a replay"),
        };
        assert_eq!(
            refused,
            IdempotencyRefusal::TooManyRunning {
                limit: u32::try_from(MAX_WAITING_PER_PEER).expect("a cap that fits"),
            },
            "an attacher past the cap must be turned away, not parked"
        );
        drop(places);
    }

    /// Taking a place before the park is only half the property: it has to be
    /// **held** for the whole wait. `let _ = ledger.wait(peer)` takes one and
    /// drops it on the same line, which leaves a cap that is enforced and a
    /// bound that does not exist — every waiter frees its place before the next
    /// one asks, so the ledger parks as many tasks as the network sends it.
    ///
    /// Polled once, so the attach reaches its park, and then the ledger is
    /// asked for the place it should be holding. Deterministic on purpose:
    /// there is no second task and no timer to race, which is what a test of a
    /// concurrency bound usually gets wrong.
    #[tokio::test(start_paused = true)]
    async fn a_parked_attacher_keeps_its_place_for_the_whole_wait() {
        let ledger = Ledger::new(Records::Memory);
        let peer = EndpointId::from_str(A).expect("a valid endpoint id");
        let key = IdempotencyKey::new("k").expect("a valid key");

        let Claim::Fresh(_running) = ledger.claim(peer, key.clone()) else {
            panic!("the first claim must be fresh");
        };
        // Every place but one, so the attacher below takes the last.
        let places: Vec<_> = (0..MAX_WAITING_PER_PEER - 1)
            .map(|_| ledger.wait(peer).expect("a free place"))
            .collect();

        // Bound rather than inlined: `&any(&ledger)` is a temporary freed at the
        // end of the statement, and `attaching` outlives it (`E0716`).
        let keys = any(&ledger);
        let mut attaching = Box::pin(claim_key(
            &keys,
            "Probe",
            peer,
            Some(Idempotency::attaching_for(key, Duration::from_secs(30))),
        ));
        let answered = poll_fn(|cx| Poll::Ready(attaching.as_mut().poll(cx))).await;
        assert!(
            answered.is_pending(),
            "the attacher must be parked, not answered"
        );

        assert!(
            ledger.wait(peer).is_err(),
            "a parked attacher must still be holding its place, or the cap bounds nothing"
        );
        drop(places);
    }
}

/// Assembles one service: its allowlist, its handler, and the layers around it.
///
/// Returned by the generated `service()`, so a caller adds behaviour without
/// the macro knowing anything about it.
pub struct ServiceBuilder<Req, Resp> {
    alpn: &'static [u8],
    admission: Arc<dyn Admission>,
    base: Next<Req, Resp>,
    layers: Vec<Arc<dyn Middleware<Req, Resp>>>,
    blobs: Option<Blobs>,
    /// Set by generated code from `DECLARES_BLOBS`. Compared against `blobs` at
    /// spawn, so "this service serves content but was given no store" is a
    /// startup error rather than a refusal on the first call.
    declares_blobs: bool,
    /// Which methods accept an idempotency key. [`Keyable::Any`] by default, which
    /// is what this crate did before the type existed.
    keyable: Keyable,
    /// Where the replies of the accepted ones live. [`Records::Memory`] by
    /// default, for the same reason.
    records: Records,
}

impl<Req, Resp> ServiceBuilder<Req, Resp>
where
    Req: Rpc + DeserializeOwned + Send + Sync + 'static,
    Resp: Serialize + Granting + Send + Sync + 'static,
{
    /// Called by generated code. `dispatch` is the call into the user's handler.
    #[doc(hidden)]
    pub fn new(alpn: &'static [u8], allow: Allowlist, dispatch: Dispatch<Req, Resp>) -> Self {
        Self {
            alpn,
            admission: Arc::new(allow),
            base: dispatch,
            layers: Vec::new(),
            blobs: None,
            declares_blobs: false,
            // `Keyable::default()`, not `Keyable::Any` spelled out: the default
            // lives in one place, on the type, so a reader who wants to know what
            // a service that never names a policy does has one place to look.
            keyable: Keyable::default(),
            records: Records::default(),
        }
    }

    /// Called by generated code. Reports whether this service declares any
    /// `blob` method.
    ///
    /// A separate method rather than a fourth parameter on
    /// [`new`](Self::new), because `new` is also called from the actor path and
    /// an arity change there buys nothing.
    #[doc(hidden)]
    #[must_use]
    pub fn __keylink_declares_blobs(mut self, declares: bool) -> Self {
        self.declares_blobs = declares;
        self
    }

    /// Give this service the content store its `blob` methods serve from.
    ///
    /// Required exactly when the service declares a `blob` method: without it
    /// [`ServerBuilder::spawn`] raises
    /// [`ConfigError::MissingBlobStore`](crate::ConfigError::MissingBlobStore).
    /// A service with no `blob` methods never needs one, which is why this is a
    /// builder method rather than a parameter every service would carry.
    ///
    /// One node has one content store. Two services on one node may share a
    /// [`Blobs`], and two DIFFERENT stores are
    /// [`ConfigError::ConflictingBlobStores`](crate::ConfigError::ConflictingBlobStores):
    /// there is one blobs ALPN, so there can be one store behind it.
    #[must_use]
    pub fn with_blobs(mut self, blobs: &Blobs) -> Self {
        self.blobs = Some(blobs.clone());
        self
    }

    /// Admit against a policy rather than against the fixed list this was built
    /// with.
    ///
    /// The list passed to the generated `service(handler, allow)` is itself an
    /// [`Admission`] — it is the canonical one — so this replaces rather than
    /// adds. Reach for it when the answer has to be able to change while the
    /// service is running:
    ///
    /// ```ignore
    /// let policy = Arc::new(DynamicAdmission::new(Allowlist::only([first])));
    /// let service = inventory::service(handler, Allowlist::anyone())
    ///     .admission(policy.clone())
    ///     .build();
    /// // ... later, without a restart, and closing whoever is no longer welcome
    /// let closed = policy.replace(Allowlist::only([second]));
    /// ```
    #[must_use]
    pub fn admission(mut self, admission: Arc<dyn Admission>) -> Self {
        self.admission = admission;
        self
    }

    /// Add a layer. The first layer added is the outermost: it sees the request
    /// first and the reply last.
    #[must_use]
    pub fn layer(mut self, layer: impl Middleware<Req, Resp>) -> Self {
        self.layers.push(Arc::new(layer));
        self
    }

    /// Decide which of this service's methods accept an idempotency key.
    ///
    /// Without this a peer attaches a key to whatever it likes, and the records
    /// of the methods that needed one are evicted by the methods that did not.
    /// Read [`Keyable`] for the whole argument and for the `CamelCase` spelling the
    /// method names take.
    ///
    /// ```ignore
    /// till::service(handler, Allowlist::anyone())
    ///     .keyed(Keyable::Only(&["Reserve"]))
    ///     .build()
    /// ```
    ///
    /// The default is [`Keyable::Any`], so a service that never calls this behaves
    /// exactly as it did before the method existed. A key on a method the policy
    /// refuses is answered
    /// [`IdempotencyRefusal::NotKeyable`](crate::IdempotencyRefusal::NotKeyable) —
    /// refused rather than ignored, because a caller that attached a key believes
    /// it is protected.
    ///
    /// This is not something a [`Middleware`] could do instead: the key is claimed
    /// before the layer chain runs, so a layer never sees an unclaimed one.
    #[must_use]
    pub fn keyable(mut self, keyable: Keyable) -> Self {
        self.keyable = keyable;
        self
    }

    /// Decide where the replies recorded under a key live.
    ///
    /// [`Records::None`] is at-most-once without paying to store a reply: the key
    /// still claims, so the work still runs once, and every repeat is
    /// [`IdempotencyRefusal::Forgotten`](crate::IdempotencyRefusal::Forgotten)
    /// rather than the reply. Reach for it when the caller can read the result out
    /// of its own domain and only needed *"do not run this twice"*.
    ///
    /// ```ignore
    /// till::service(handler, Allowlist::anyone())
    ///     .keyable(Keyable::Only(&["Reserve"]))
    ///     .records(Records::None)
    ///     .build()
    /// ```
    ///
    /// The default is [`Records::Memory`], so a service that never calls this
    /// behaves exactly as it did before the method existed. **No setting here can
    /// weaken at-most-once** — the key state never leaves memory whatever this
    /// says, and [`Records`] is only about the reply bytes.
    #[must_use]
    pub fn records(mut self, records: Records) -> Self {
        self.records = records;
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
                admission: self.admission,
                blobs: self.blobs.clone(),
                keys: Keys::new(self.keyable, self.records),
                _p: PhantomData,
            }),
            blobs: self.blobs,
            declares_blobs: self.declares_blobs,
        }
    }
}

impl<Req, Resp> std::fmt::Debug for ServiceBuilder<Req, Resp> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ServiceBuilder")
            .field("alpn", &String::from_utf8_lossy(self.alpn))
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
/// The constant itself, not a copy of its value. A literal here would agree
/// with iroh-blobs today and diverge silently on the version bump that renamed
/// it, leaving the reservation pointed at an ALPN nobody serves.
const RESERVED_ALPNS: &[&[u8]] = &[iroh_blobs::ALPN];

/// One service, ready to register on a [`Server`].
///
/// The request and response types are erased here, which is what lets services
/// with completely unrelated schemas sit in one collection — and therefore on
/// one endpoint.
pub struct Service {
    alpn: &'static [u8],
    handler: Box<dyn DynProtocolHandler>,
    /// The store this service serves content from, if any. `spawn` collects
    /// these to decide what to mount on `iroh_blobs::ALPN`.
    blobs: Option<Blobs>,
    declares_blobs: bool,
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
            blobs: None,
            declares_blobs: false,
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
        let mut store: Option<Blobs> = None;
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
            // A service that serves content and was handed no store would
            // answer every `blob` call with an address nothing can fetch. That
            // is a configuration mistake, and keylink raises those at startup.
            if service.declares_blobs && service.blobs.is_none() {
                return Err(Error::Config(ConfigError::MissingBlobStore {
                    service: String::from_utf8_lossy(service.alpn).into_owned(),
                }));
            }
            if let Some(theirs) = &service.blobs {
                match &store {
                    // One node has one blobs ALPN, so it can serve one store.
                    // Sharing a `Blobs` between services is fine and normal;
                    // two DIFFERENT stores is a question with no right answer.
                    Some(ours) if !ours.is_same_store(theirs) => {
                        return Err(Error::Config(ConfigError::ConflictingBlobStores));
                    }
                    Some(_) => {}
                    None => store = Some(theirs.clone()),
                }
            }
            builder = builder.accept(service.alpn, service.handler);
        }
        // keylink mounts blobs itself, with the ledger attached. This is the
        // reason `iroh_blobs::ALPN` is reserved: there is exactly one way for
        // the protocol to reach a node, and it carries the policy.
        if let Some(blobs) = store {
            builder = builder.accept(iroh_blobs::ALPN, blobs.protocol());
        }
        Ok(Server {
            router: builder.spawn(),
        })
    }
}

/// A connected client.
///
/// `call` takes `&self` and holds no lock, so a client behind an `Arc` serves
/// any number of concurrent callers at full connection speed.
#[derive(Debug)]
pub struct RpcClient<Req, Resp> {
    conn: Connection,
    /// Kept so a `blob` reply can open the second connection. A `Connection`
    /// does not expose the endpoint it came from, and blobs speaks its own
    /// ALPN, so the handle needs a dialler of its own.
    endpoint: iroh::Endpoint,
    /// Attached to every call this client makes. See
    /// [`with_idempotency`](Self::with_idempotency).
    idempotency: Option<Idempotency>,
    _p: PhantomData<fn(Req) -> Resp>,
}

/// Every clone names the same connection, which is what lets a registry of
/// peers hold typed clients rather than raw connections.
///
/// Written out rather than derived: `derive(Clone)` would demand
/// `Req: Clone, Resp: Clone`, and neither appears in a field.
impl<Req, Resp> Clone for RpcClient<Req, Resp> {
    fn clone(&self) -> Self {
        Self {
            conn: self.conn.clone(),
            endpoint: self.endpoint.clone(),
            idempotency: self.idempotency.clone(),
            _p: PhantomData,
        }
    }
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

    /// Point a handle at content the peer we are talking to just named.
    ///
    /// Called by generated `blob` client code. The holder is this connection's
    /// peer, which is sound while the node that ANSWERED is the node that
    /// holds the bytes — true today, and RFC-01 §11.2c flags routing as the
    /// case that would break it.
    #[doc(hidden)]
    #[must_use]
    pub fn blob_handle(&self, id: BlobId) -> BlobHandle {
        BlobHandle::on(self.endpoint.clone(), self.conn.remote_id(), id)
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
    /// # Waiting for a call already in flight
    ///
    /// A bare key refuses on collision, which is what it has always done. Pass
    /// an [`Idempotency`] instead to wait for the call already running under it
    /// and take its answer:
    ///
    /// ```ignore
    /// client
    ///     .with_idempotency(Idempotency::attaching_for(key, Duration::from_secs(5)))
    ///     .reserve("ADU-014".into(), 2)
    ///     .await?;
    /// ```
    ///
    /// This is what makes [`Peer`](crate::Peer)'s single re-issue compose with
    /// a key: the re-issued call waits for the first one instead of meeting
    /// [`IdempotencyRefusal::InFlight`].
    ///
    /// # Make one call with it
    ///
    /// The key goes on EVERY call made through the returned client, so it is
    /// meant for one. A second, DIFFERENT call through the same handle is
    /// answered from the first call's record, and the generated client refuses
    /// a reply to a question it did not ask
    /// ([`Error::UnexpectedResponse`]). That is a loud failure rather than a
    /// wrong answer, but it is still a mistake worth not making.
    ///
    /// # Why this returns a different type
    ///
    /// [`IdempotentClient`] has no [`tell`](Self::tell). It used to return
    /// `Self`, and `client.with_idempotency(key).upload(name)` therefore
    /// compiled and sent no key — `tell` writes `None` on the wire and dropped
    /// the attached one on the floor. Two places in the README told a reader
    /// that shape was impossible, so somebody following the documentation to
    /// the letter could write it *to be safe*, believe they had at-most-once,
    /// and have nothing. The claim is now the type's, not the prose's.
    #[must_use]
    pub fn with_idempotency(
        &self,
        idempotency: impl Into<Idempotency>,
    ) -> IdempotentClient<Req, Resp> {
        IdempotentClient {
            inner: Self {
                conn: self.conn.clone(),
                endpoint: self.endpoint.clone(),
                idempotency: Some(idempotency.into()),
                _p: PhantomData,
            },
        }
    }
}

/// A connected client scoped to one idempotency key.
///
/// Built by [`RpcClient::with_idempotency`] and by the `with_idempotency` on
/// every generated client. It is the same connection — a `Connection` is a
/// handle — carrying a key on every call made through it.
///
/// # What it does not have, and why that is the whole point
///
/// There is no `tell`. A key is a promise that the work runs at most once, and
/// the ledger keeps that promise by recording the reply and answering a repeat
/// with it. A `tell` has no reply stream, so there is nothing to record and
/// nothing to answer with, and a key attached to one could only be discarded.
///
/// Discarding it silently is what this type exists to stop. The absence is
/// held by a probe in `representability.rs` rather than by this paragraph.
/// Fire-and-forget that must not run twice is an `rpc`.
#[derive(Debug)]
pub struct IdempotentClient<Req, Resp> {
    inner: RpcClient<Req, Resp>,
}

/// Every clone names the same connection and carries the same key. Written out
/// rather than derived for the reason [`RpcClient`]'s is.
impl<Req, Resp> Clone for IdempotentClient<Req, Resp> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

impl<Req, Resp> IdempotentClient<Req, Resp> {
    /// The live connection, for crate code that observes it. See
    /// [`RpcClient::connection`].
    pub(crate) fn connection(&self) -> &Connection {
        self.inner.connection()
    }

    /// Point a handle at content the peer we are talking to just named.
    ///
    /// Called by generated `blob` client code. See
    /// [`RpcClient::blob_handle`].
    #[doc(hidden)]
    #[must_use]
    pub fn blob_handle(&self, id: BlobId) -> BlobHandle {
        self.inner.blob_handle(id)
    }
}

impl<Req, Resp> IdempotentClient<Req, Resp>
where
    Req: Serialize + Send + Sync,
    Resp: DeserializeOwned + Send + Sync,
{
    /// Make one call, with this handle's key attached. See
    /// [`RpcClient::call`].
    ///
    /// # Errors
    ///
    /// Everything [`RpcClient::call`] reports, plus the refusals a key can
    /// meet — see [`IdempotencyRefusal`](crate::IdempotencyRefusal).
    pub async fn call(&self, request: Req, body: Body) -> Result<(Resp, BodyReader)> {
        self.inner.call(request, body).await
    }

    /// Ask what this peer's ledger knows about one of your keys. See
    /// [`RpcClient::key_state`].
    ///
    /// # Errors
    ///
    /// Everything [`RpcClient::key_state`] reports.
    pub async fn key_state(&self, key: IdempotencyKey) -> Result<KeyState> {
        self.inner.key_state(key).await
    }

    /// Close the connection gracefully.
    ///
    /// # Errors
    ///
    /// Never, today. See [`RpcClient::close`].
    pub async fn close(self) -> Result<()> {
        self.inner.close().await
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
            endpoint: node.endpoint().clone(),
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
            &WireRequest::Call(request),
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
            WireResponse::Call(Outcome::Ok(value)) => {
                Ok((value, BodyReader::new(frames, response.body_len)))
            }
            // A refusal carries no body: the handler never ran, or never
            // finished, so there is nothing to stream.
            WireResponse::Call(Outcome::Err(refused)) => Err(Error::Remote(refused)),
            other => Err(Error::UnexpectedResponse {
                expected: "a call reply",
                actual: other.arm(),
            }),
        }
    }

    /// Ask what the serving side's ledger knows about one of this client's
    /// idempotency keys.
    ///
    /// The one state a caller cannot discover by retrying is
    /// [`KeyState::Fresh`], because discovering it that way means executing.
    /// This is how to ask without submitting the order again.
    ///
    /// Peer-scoped by construction: the answer comes from the ledger of this
    /// connection's proven peer, so it can never report another peer's key.
    /// Advisory, like every observation of a live system — a key that is
    /// `Fresh` now can be claimed a microsecond later.
    ///
    /// It runs no handler and no layer: there is nothing to authorize past the
    /// admission this connection already passed.
    ///
    /// # Errors
    ///
    /// [`Error::Closed`] when the peer finishes the stream without answering,
    /// plus any transport or codec failure.
    pub async fn key_state(&self, key: IdempotencyKey) -> Result<KeyState> {
        let (mut send, recv) = self.conn.open_bi().await?;
        wire::write_header(&mut send, Some(0), None, &WireRequest::<Req>::KeyState(key)).await?;
        send.finish()?;

        let mut frames = FrameReader::new(recv);
        let Some(response) = frames.read_header::<WireResponse<Resp>>().await? else {
            return Err(Error::Closed);
        };
        match response.message {
            WireResponse::KeyState(state) => Ok(state),
            other => Err(Error::UnexpectedResponse {
                expected: "a key-state reply",
                actual: other.arm(),
            }),
        }
    }

    /// Send one message and return without waiting for anything.
    ///
    /// Opens a **unidirectional** stream, so there is no reply channel at all —
    /// not an ignored reply, an absent one. That is what makes the guarantee
    /// honest and also what makes it narrow.
    ///
    /// # What this guarantees
    ///
    /// The bytes were handed to the transport, in order relative to other
    /// `tell`s on this client.
    ///
    /// # What it does NOT guarantee, and cannot
    ///
    /// - **Delivery.** The connection may die between the write and the peer's
    ///   read, and nothing will report it.
    /// - **Handling.** The peer may accept the stream and refuse the message —
    ///   an allowlist, a middleware, a panic in the handler — and there is no
    ///   channel to say so.
    /// - **Ordering against `call`.** A `call` on a different stream may be
    ///   handled first, because streams are independent by design.
    ///
    /// An idempotency key is deliberately not accepted: the ledger records a
    /// reply so a retry can be answered with it, and a `tell` has no reply to
    /// record. Anything needing at-most-once wants a `rpc`.
    ///
    /// That is a claim the compiler holds, not a request to be careful.
    /// [`with_idempotency`](Self::with_idempotency) returns an
    /// [`IdempotentClient`], and this method is not on it.
    ///
    /// # Errors
    ///
    /// Only local ones — opening the stream, encoding, or writing. A refusal on
    /// the far side is not among them, by construction.
    pub async fn tell(&self, request: Req) -> Result<()> {
        let mut send = self.conn.open_uni().await?;
        // `None` for the key, and it is now the only value this line could
        // write: `self.idempotency` is `Some` on no client that has a `tell`.
        // `Some(0)` body length, because a `tell` carries no payload and an
        // open-ended stream would make the receiver wait for an end that says
        // nothing.
        wire::write_header(&mut send, Some(0), None, &WireRequest::Call(request)).await?;
        send.finish()?;
        Ok(())
    }

    /// Close the connection gracefully.
    pub async fn close(self) -> Result<()> {
        self.conn.close(0u32.into(), b"done");
        self.conn.closed().await;
        Ok(())
    }
}
