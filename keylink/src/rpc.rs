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
//! **That hundred is a DEPENDENCY's default, which makes it a different kind
//! of borrowed number from any constant this crate owns.** A limit here rots
//! when somebody edits a `const`, and a grep for the old value then finds
//! every sentence that repeated it. An external default rots on a version
//! bump instead: no file here changes, no gate reads it, and the sentence goes
//! on reading like a statement about keylink. `docs/rfc-04-actors.md` holds
//! the verification against the transport crate's own source at the version it
//! was read, which is the only honest form that citation can take. So rely on
//! the NAME — `turns.rs` bounds its queue by `max_concurrent_bidi_streams`
//! without spelling a number — and read the figure here as an order of
//! magnitude.
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
// siblings below: the root re-export is a promise to a *consumer*, and it is
// `tests/api.rs` that writes it from a consumer's position. An in-crate
// `crate::Keyable` would make the library itself the thing that fails when the
// re-export goes, which would leave that test proving nothing.
use crate::idempotency::{Attaching, Claim, Guard, Keyable, Ledger, Limits, Records};
use crate::middleware::{wrap, Reached};
use crate::wire::FrameReader;
use crate::{
    wire, Admission, AdmittedPeer, Allowlist, BlobHandle, BlobId, Blobs, Body, BodyReader, CallId,
    Context, Error, Granting, Idempotency, IdempotencyKey, IdempotencyRefusal, KeyState,
    Middleware, Next, Node, Outcome, RemoteError, Result, Rpc, WireRequest, WireResponse,
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

/// This surface reaches the actor through a [`Dispatch`], so the service's
/// [`Middleware`] stack and its [`Allowlist`] ran.
///
/// Write `impl Calls + Governed` where that must be true. `Calls` is the
/// per-service trait `service!` generates, so the bound a caller writes reads
/// `impl journal::Calls + Governed`. A `Handle` is refused, because a local call
/// reaches the mailbox directly and neither the stack nor the list is on that
/// path.
///
/// # It does NOT mean at-most-once
///
/// At-most-once is a property of a **key**, not of a type. It starts at
/// `Client::with_idempotency(key)` and holds only while the key is stable across
/// retries, and no bound can see a key's stability. So a plain `Client`
/// satisfies this bound, carries no key, reaches no ledger, and is at-least-once
/// under retry exactly as a `Handle` is. Name `IdempotentClient` in the
/// signature where the ledger is required; this marker cannot stand in for it.
///
/// # `GovernedSeal` is not a seal
///
/// `service!` writes this trait's impls in the **caller's** crate, where
/// `pub(crate)` does not reach, so `GovernedSeal` is `pub` for those impls to
/// name it. A downstream crate can therefore write
/// `impl keylink::GovernedSeal for MyType {}` and then
/// `impl keylink::Governed for MyType {}`, and nothing stops it. `#[doc(hidden)]`
/// keeps the supertrait out of the rendered docs and out of a caller's way; it
/// is not a visibility modifier and it forbids nothing. So this trait **cannot
/// be sealed**, and nothing here calls it sealed.
///
/// That is the tension `Progress` already carries, for the same reason and with
/// the same escape hatch: a token only generated code could build would itself
/// have to be `pub` to be named in the expansion, so the forgery moves by one
/// line. What makes it tolerable is the same thing too — the boundary this crate
/// defends is the **peer**, and no peer can reach any of this. A forged marker
/// tells you about your own binary.
///
/// # The refusal, held by a doctest
///
/// A `Handle` satisfies `Calls` and must not satisfy this. There is no call that
/// can assert that — the claim is an **absence**, and an absence has nothing to
/// drive — so it is held from a doctest, which compiles as its own crate and is
/// therefore the position a downstream author writes the bound from.
///
/// ```compile_fail
/// // Fails with `E0277`: `depot::Handle<Counter>` does not reach the actor
/// // through a `Dispatch`, which is this trait's own
/// // `#[diagnostic::on_unimplemented]` message.
/// //
/// // The code is named HERE and deliberately NOT on the fence. rustdoc does not
/// // check the code after `compile_fail`, so an annotated code asserts a
/// // verification it does not perform — this project has shipped five guards
/// // that were satisfiable without proving their claim, and a fence reading
/// // `compile_fail,E0277` is how the sixth would look.
/// //
/// // What stops this passing for the WRONG reason is the FIRST call below.
/// // `depot::Client` satisfies both halves of the bound, so the service, the
/// // generated trait, this trait's path, the actor impl and the function
/// // signature all have to resolve before the compiler ever reaches the second
/// // call. `Handle<Counter>` satisfies `Calls` too — it is one of the four
/// // implementors — so `Governed` is the one bound left to fail on.
/// keylink::service! {
///     service Depot;
///
///     rpc Count() -> u64;
/// }
///
/// struct Counter;
///
/// impl depot::Actor for Counter {
///     async fn count(&mut self, _cx: keylink::Caller<'_>) -> u64 {
///         0
///     }
/// }
///
/// fn a_governed_calling_surface<C: depot::Calls + keylink::Governed>(_: &C) {}
///
/// fn refused(served: &depot::Client, local: &depot::Handle<Counter>) {
///     a_governed_calling_surface(served);
///     a_governed_calling_surface(local);
/// }
/// ```
#[diagnostic::on_unimplemented(
    message = "`{Self}` does not reach the actor through a `Dispatch`",
    label = "not a governed surface",
    note = "a `Handle` calls the mailbox directly, so no `Middleware` and no \
            `Allowlist` ran. Take a `Client`, an `IdempotentClient` or an \
            `ActorRef`, or drop the `+ Governed` half of the bound."
)]
pub trait Governed: GovernedSeal {}

/// The supertrait [`Governed`] carries so that `impl Governed` alone does not
/// compile.
///
/// Not a seal — [`Governed`]'s own docs say why it cannot be one. What it buys
/// is that the two lines `service!` emits are a shape a hand-written impl has to
/// copy on purpose rather than reach by writing one obvious line.
#[doc(hidden)]
pub trait GovernedSeal {}

/// A compiled method policy checked before idempotency state is touched.
pub(crate) trait MethodAuthorization: Send + Sync + 'static {
    /// Bind a typed layer to the same policy object used by preflight.
    fn bind(&mut self, installed: std::sync::Weak<dyn MethodAuthorization>);

    /// Whether `peer` may call `method`.
    fn authorizes(&self, peer: EndpointId, method: &'static str) -> bool;
}

/// Serves one protocol: authorizes the peer once per connection, then answers
/// every stream that connection opens, concurrently.
pub(crate) struct RpcService<Req, Resp> {
    next: Next<Req, Resp>,
    /// The policy this service admits against. An `Arc<dyn ..>` rather than a
    /// generic parameter: the decision is made once per connection and does no
    /// I/O, and a generic would reach every `service!` user's type errors.
    admission: Arc<dyn Admission>,
    required_admission: Vec<Arc<dyn Admission>>,
    required_authorization: Arc<[Arc<dyn MethodAuthorization>]>,
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
            required_admission: self.required_admission.clone(),
            required_authorization: self.required_authorization.clone(),
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
/// is the mistake this makes unwritable.
///
/// **The CLAIM path has one reader**, [`Keys::ledger_for`], and that is the
/// load-bearing half: it is the only route to a ledger a key can be recorded in, so
/// there is no site that can take the ledger while ignoring the policy.
/// `answer_key_state` reads `ledger` directly and does not go through it, which is
/// total rather than a hole — its parameter is `Option<&Ledger>` and both arms are
/// answered — and it is why this says "the claim path" rather than "every site".
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
    ///
    /// It is also the one place that **discards** a configured [`Limits`]: under
    /// [`Keyable::None`] there is no ledger to hold it. A doc line on
    /// [`ServiceBuilder::limits`] says so, because both methods appear in the
    /// crate's own examples in one chain.
    fn new(keyable: Keyable, records: Records, limits: Limits) -> Self {
        let ledger = keyable
            .needs_a_ledger()
            .then(|| Ledger::new(limits, records));
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
    // `ledger.attach_cap()` and not `MAX_ATTACH`: the ceiling is this service's
    // `Limits::attach`, and this is the one place that holds the peer's request and
    // the ledger at the same time. `ledger` came from the `ledger_for` above and is
    // used again below, so reading the cap here repeats no lookup.
    //
    // TWO refusals, one wire answer, and they are separate lines on purpose. This one
    // is the OPERATOR's: `Attaching::Forbidden` means this service admits no attacher.
    // The next is the CALLER's: it did not ask to be held. Both answer `InFlight` and
    // both return before `ledger.wait`, so neither spends a waiting place.
    //
    // Folding this into `attach_budget`'s `None` would give the same wire answer and
    // cost a guard: `a_bare_key_does_not_attach` asserts that `None` means *the caller
    // did not ask*, and a second producer makes it pass for two reasons at once while
    // its name still claims one. `Attaching::Forbidden`'s own doc carries the rest.
    //
    // A `match` and not a `let ... else`, so that each act is ONE line a break harness
    // can replace: reintroducing the constant on the first arm and collapsing the
    // second into a zero hold are the two edits `an_attach_ceiling_below_the_peers_request_governs`
    // and `a_forbidden_ceiling_is_refused_without_a_waiting_place` record. A `let-else`
    // spreads the refusal over three lines and neither break fits on one of them.
    let cap = match ledger.attach_cap() {
        Attaching::UpTo(cap) => cap,
        Attaching::Forbidden => return Claimed::Refuse(IdempotencyRefusal::InFlight.into()),
    };
    let Some(budget) = idem.attach_budget(cap) else {
        return Claimed::Refuse(IdempotencyRefusal::InFlight.into());
    };
    // Taken before anything parks, and held to the end of this function so it
    // covers the wait AND the re-claim after it. A caller that did not ASK to attach
    // never reaches here, so it never spends a place.
    //
    // But "will not wait" is not the same as "did not ask": a service with
    // `Limits::attach` at zero gets `Some(ZERO)` above, reaches this line, spends a
    // place, and expires at the timeout below. So a zero ceiling is refused
    // `TooManyRunning` rather than `InFlight` once the waiting pool is full, which is
    // what `Attaching`'s own doc says instead of "turns the feature off".
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
/// # This query is NOT policed per method, and it cannot be
///
/// [`WireRequest::KeyState`] carries a key and no method name, so
/// [`Keyable::Only`] and [`Keyable::Except`] have nothing to check it against. The
/// policy covers the call path and not this one, and that is a property of the wire
/// type rather than a check somebody forgot.
///
/// Nothing leaks by it. The query is peer-scoped by construction, and it can only
/// report keys that were actually claimed — which under `Only` are only keys on
/// accepted methods. A peer asking about a method it may not key is told `Fresh`,
/// which is the truth.
///
/// Refusing the query under a policy was considered and rejected: [`NotKeyable`]
/// carries the method a key was attached to, and there is no method here, so
/// refusing would mean inventing one. Answering the fact beats naming a method the
/// caller never sent.
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
    required_authorization: Arc<[Arc<dyn MethodAuthorization>]>,
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

    // Required method policy runs before key validation, claim, attach, replay,
    // or refusal. The ledger is scoped to `(peer, key)`, so a replay cannot be
    // allowed to answer a method this policy denies.
    for policy in required_authorization.iter() {
        if !policy.authorizes(peer, message.method()) {
            return refuse::<Resp>(
                &mut send,
                RemoteError::Unauthorized(message.method().to_owned()),
            )
            .await;
        }
    }

    // Named BEFORE the key is consumed below, and by reference rather than by
    // clone: this is the one place both halves of the digest exist, exactly as it
    // is the one place both halves of a blob grant do.
    //
    // Computed for a key that has not been validated yet, which is harmless and
    // worth saying: `claim_key` refuses a malformed key before the chain is
    // entered, so no handler ever sees a `CallId` derived from one.
    let mut cx = Context::new(peer, Req::SERVICE, message.method())
        .with_actor(message.actor().cloned())
        .with_call(
            request
                .idempotency
                .as_ref()
                .map(|idem| CallId::of(peer, idem.key())),
        );
    if !required_authorization.is_empty() {
        cx = cx.with_method_authorized(required_authorization.clone());
    }
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
        let Some((admitted, _live)) = settle(
            self.admission.as_ref(),
            &self.required_admission,
            &connection,
        ) else {
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
                        self.required_authorization.clone(),
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
    use crate::idempotency::{MAX_ATTACH, MAX_WAITING_PER_PEER};

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
    /// Made red by `scripts/break-once.sh` forcing `Keyable::needs_a_ledger` to
    /// return `true` for every variant. Watched: `panicked at .., `Keyable::None`
    /// must not allocate a ledger nothing can ever write to`.
    ///
    /// It also reddens `idempotency::tests::policy::keyed_none_accepts_no_method`.
    /// **An earlier version of this record said "no other test in the crate noticed
    /// the edit", which was false** — and the replacement names the reddened tests
    /// rather than counting them or claiming to have found all of them. A name stays
    /// true when somebody adds a test; "the only one" does not.
    #[test]
    fn keyed_none_builds_no_ledger() {
        assert!(
            Keys::new(Keyable::None, Records::Memory, Limits::default())
                .ledger
                .is_none(),
            "`Keyable::None` must not allocate a ledger nothing can ever write to"
        );
        assert!(
            Keys::new(Keyable::Any, Records::Memory, Limits::default())
                .ledger
                .is_some(),
            "and every other policy must have one, or no key works at all"
        );
    }

    /// The seam the whole of Wave 1 hangs on: one lookup answers *both* "is this
    /// method keyed?" and "where do its records go?", so no call site can take the
    /// ledger while ignoring the policy.
    ///
    /// Made red by `scripts/break-once.sh` dropping the `.filter(..)` from
    /// `Keys::ledger_for`, so it answers the ledger without consulting the policy —
    /// the edit that makes the policy decorative while every idempotency test
    /// predating this wave stays green. Watched: `panicked at .., a refused method
    /// must not, or the policy decides nothing`.
    ///
    /// **Described rather than quoted, and that is not a style choice.** This record
    /// used to quote the one-line form `self.ledger.as_ref().filter(..)`, which
    /// `rustfmt` split across three lines when the signature took `&'static str` —
    /// so `break-once.sh` could no longer find its anchor and the recorded run was
    /// unreplayable. A quoted anchor in the file being broken fails twice over: it
    /// makes the literal non-unique, and it rots when the formatter rewraps the
    /// target.
    #[test]
    fn the_ledger_is_reachable_only_through_the_policy() {
        let keys = Keys::new(
            Keyable::Only(&["Reserve"]),
            Records::Memory,
            Limits::default(),
        );
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
    /// the test fails on the variant rather than hanging. That budget is the duration
    /// this test asks for below, not [`MAX_ATTACH`], which currently holds the same
    /// number — worth saying because a sweep reading the figure alone cannot tell the
    /// two apart, and one read it as the constant.
    #[tokio::test(start_paused = true)]
    async fn an_attacher_over_the_cap_is_refused_before_it_parks() {
        let ledger = Ledger::new(Limits::default(), Records::Memory);
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
        let ledger = Ledger::new(Limits::default(), Records::Memory);
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

    /// Park an attacher against a claim that never settles, and answer **how long
    /// the accept path held it**.
    ///
    /// The figure is read off tokio's PAUSED clock, so it is the budget
    /// `claim_key` chose and not a wall-clock measurement: under `start_paused`
    /// the runtime advances to the pending timer once the attach is the only thing
    /// left to run, so the elapsed time is exactly the deadline. An upper bound on
    /// the wall clock is the flake `CONTRIBUTING.md` forbids; this cannot see the
    /// wall at all.
    ///
    /// The refusal is asserted here rather than at each caller. Nothing settles the
    /// claim, so an expired budget is the only honest outcome, and returning a
    /// duration for a `Run` or a `Replay` would let one of those read as a wait.
    async fn held_for(limits: Limits, asked: Duration) -> Duration {
        let ledger = Ledger::new(limits, Records::Memory);
        let peer = EndpointId::from_str(A).expect("a valid endpoint id");
        let key = IdempotencyKey::new("k").expect("a valid key");

        let Claim::Fresh(_running) = ledger.claim(peer, key.clone()) else {
            panic!("the first claim must be fresh");
        };

        let started = tokio::time::Instant::now();
        let keyed = claim_key(
            &any(&ledger),
            "Probe",
            peer,
            Some(Idempotency::attaching_for(key, asked)),
        )
        .await;
        let held = started.elapsed();

        // Exhaustive, with no catch-all, for the reason
        // `an_attacher_over_the_cap_is_refused_before_it_parks` gives: the arm that
        // hides a regression is the one nobody thought to name.
        match keyed {
            Claimed::Refuse(RemoteError::Idempotency(IdempotencyRefusal::InFlight)) => held,
            Claimed::Refuse(RemoteError::Idempotency(other)) => {
                panic!("expected an expired budget to answer InFlight, got {other:?}")
            }
            Claimed::Refuse(other) => panic!("expected an idempotency refusal, got {other:?}"),
            Claimed::None => panic!("expected a refusal, got no key at all"),
            Claimed::Run(_) => panic!("expected a refusal, got the key"),
            Claimed::Replay(_) => panic!("expected a refusal, got a replay"),
        }
    }

    /// **The property the whole of `Limits::attach` exists for**: the ceiling a
    /// peer's budget meets is the one this service configured, not the constant.
    ///
    /// A peer asks for thirty seconds — the default ceiling exactly, so a service
    /// that ignored its own setting would look right — and a service that set one
    /// second holds it for one.
    ///
    /// Made red by `scripts/break-once.sh` replacing `Attaching::UpTo(cap) => cap,`
    /// with `Attaching::UpTo(_) => crate::MAX_ATTACH,`, which is the hard-coded ceiling
    /// this bound removed, reintroduced at the one call site. Fully qualified in the
    /// replacement because `rpc.rs` does not import the constant. The search literal
    /// carries its eight spaces of indentation, so this record's unindented copy is not
    /// a second occurrence of it. Watched, under `cargo test`:
    /// `assertion `left == right` failed: the ceiling must be this service's
    /// Limits::attach`, `left: 30s`, `right: 1s`.
    ///
    /// **That `left` has two owners holding one value, and only one of them is local,
    /// so it is recorded rather than replaced.** The replacement puts [`MAX_ATTACH`]
    /// back as the cap and `attach_budget` then clamps the request to it, so the hold
    /// is the SMALLER of this test's own thirty second request and the constant. The
    /// two are equal deliberately — the paragraph above says the peer asks the default
    /// ceiling exactly — so the number is reproducible from this file, and it still
    /// moves if the constant ever drops BELOW the request. That is the difference from
    /// `a_peer_over_the_attach_ceiling_is_held_for_the_default` and
    /// `a_peer_under_the_attach_ceiling_keeps_its_own_budget`, where `left` or `right`
    /// **is** the constant and no local literal reproduces it.
    ///
    /// **The line this quotes moved twice, and the break set moved with it.** It was
    /// `let Some(budget) = idem.attach_budget(ledger.attach_cap()) else {` when the cap
    /// was read and clamped on one line; the read is its own `match` arm now, because
    /// [`Attaching::Forbidden`] needed a line of its own that a harness could replace.
    ///
    /// **MEASURED at this tree, whole-lib, one run: TWO guards red.** This one and
    /// `an_attach_ceiling_of_zero_refuses_the_attacher_at_once`.
    /// `a_peer_over_the_attach_ceiling_is_held_for_the_default` and
    /// `a_peer_under_the_attach_ceiling_keeps_its_own_budget` stay **green**, and the
    /// reason is not that they are weak: both configure `Limits::default()`, whose cap
    /// **is** [`MAX_ATTACH`], so this replacement is a no-op for them. A break that
    /// substitutes the default is invisible to every guard that uses the default —
    /// which is why the guard that sets one second is the one that carries this
    /// property.
    #[tokio::test(start_paused = true)]
    async fn an_attach_ceiling_below_the_peers_request_governs() {
        assert_eq!(
            held_for(
                Limits::default().attach(Attaching::UpTo(Duration::from_secs(1))),
                Duration::from_secs(30),
            )
            .await,
            Duration::from_secs(1),
            "the ceiling must be this service's Limits::attach"
        );
    }

    /// The default ceiling still caps, so moving the clamp did not drop it.
    ///
    /// Derived from [`MAX_ATTACH`] rather than written as thirty seconds, because a
    /// literal here rots in the one direction nothing catches: lower the constant
    /// and a hard-coded 30 would assert a ceiling the ledger no longer has.
    ///
    /// Made red by `scripts/break-once.sh` replacing
    /// `attach: Attaching::UpTo(MAX_ATTACH),` in `impl Default for Limits` with
    /// `attach: Attaching::UpTo(Duration::from_secs(1)),` — the same
    /// edit `idempotency.rs`'s `the_default_attach_ceiling_is_max_attach` records,
    /// and this is the guard that says the default reaches the accept path rather
    /// than only the struct. Watched, under `cargo test`:
    /// `assertion `left == right` failed: a request over the ceiling must be held
    /// for the ceiling`, where `left: 1s` is the replacement's own duration, quoted
    /// above, and `right` is [`MAX_ATTACH`] — the constant the assertion below names
    /// rather than spells.
    ///
    /// **This record made the argument above and then broke it in its own watched
    /// output.** That paragraph says a hard-coded thirty rots in the one direction
    /// nothing catches, and the transcription beneath it went on to spell that very
    /// number out, in the same block, for the same constant. The general form is what
    /// to keep, because it says where to look rather than what happened here: **a
    /// reader who has just been convinced by an argument is the least likely person to
    /// audit the transcription beneath it.** So check a record against the rule it
    /// argues for LAST, and never read the argument as evidence that the record already
    /// complies with it.
    #[tokio::test(start_paused = true)]
    async fn a_peer_over_the_attach_ceiling_is_held_for_the_default() {
        assert_eq!(
            held_for(Limits::default(), Duration::from_secs(3600)).await,
            MAX_ATTACH,
            "a request over the ceiling must be held for the ceiling"
        );
    }

    /// The clamp is a `min` and not an assignment, so a modest request is not
    /// rounded **up** to the service's ceiling.
    ///
    /// **This is the guard the other three cannot be**, and the two break sets are
    /// disjoint: cap removal reddens the other three and leaves this one green, while
    /// the edit below reddens this one alone. All four measured one run at a time,
    /// because the first version of this claim said "two" in a record one file away.
    ///
    /// Made red by `scripts/break-once.sh` replacing
    /// `.map(|millis| Duration::from_millis(millis).min(cap))` with
    /// `.map(|_| cap)` in `keylink/src/idempotency.rs`, which is the ceiling
    /// swallowing the request. Watched, under `cargo test`:
    /// `assertion `left == right` failed: a request under the ceiling must be kept`,
    /// where `left` is [`MAX_ATTACH`] — the default ceiling returned whole, because the
    /// replacement discards the request — and `right: 1s` is the budget this test asks
    /// for.
    #[tokio::test(start_paused = true)]
    async fn a_peer_under_the_attach_ceiling_keeps_its_own_budget() {
        assert_eq!(
            held_for(Limits::default(), Duration::from_secs(1)).await,
            Duration::from_secs(1),
            "a request under the ceiling must be kept"
        );
    }

    /// Zero is representable inside [`Attaching::UpTo`] — [`Duration`] has no
    /// `NonZero` — so what it does is asserted rather than argued.
    ///
    /// **What it measures is the hold, which is zero, and NOT that attaching is
    /// turned off.** The attacher still spends a waiting place and re-claims once
    /// before expiring — this test gets
    /// [`InFlight`](IdempotencyRefusal::InFlight) because the pool has a place free,
    /// and a zero ceiling meeting an exhausted pool is refused `TooManyRunning`
    /// instead. [`Attaching`] carries that distinction; the guard deliberately
    /// covers one arm of it, and naming which arm is the point of this paragraph.
    /// [`Attaching::Forbidden`] is the setting that does turn it off, and
    /// `a_forbidden_ceiling_is_refused_without_a_waiting_place` holds that.
    ///
    /// `representability.rs`'s `no_ledger_bound_can_be_asked_to_be_zero` names this
    /// test as one of its two stated exceptions, `ttl`'s being the other.
    ///
    /// Made red by `scripts/break-once.sh` replacing
    /// `.map(|millis| Duration::from_millis(millis).min(cap))` with
    /// `.map(|millis| Duration::from_millis(millis))` in
    /// `keylink/src/idempotency.rs` — the cap going away rather than moving.
    /// Watched, under `cargo test`:
    /// `assertion `left == right` failed: a zero ceiling must refuse without
    /// waiting`, `left: 30s`, `right: 0ns`. **Both values are this file's own, and a
    /// sweep for borrowed numbers will flag that thirty wrongly** — one already did.
    /// This break DELETES the clamp rather than moving the cap, so the hold is exactly
    /// the request the test makes below and [`MAX_ATTACH`] never enters the result at
    /// all.
    #[tokio::test(start_paused = true)]
    async fn an_attach_ceiling_of_zero_refuses_the_attacher_at_once() {
        assert_eq!(
            held_for(
                Limits::default().attach(Attaching::UpTo(Duration::ZERO)),
                Duration::from_secs(30)
            )
            .await,
            Duration::ZERO,
            "a zero ceiling must refuse without waiting"
        );
    }

    /// Fill every waiting place this peer has, then attach, and answer the refusal.
    ///
    /// **The exhausted pool is what makes the two settings tell each other apart**,
    /// and it is the only observation available: [`Limits`] offers no getters and the
    /// ledger publishes no waiter count. A setting that needs a place cannot get one
    /// here and is refused [`TooManyRunning`](IdempotencyRefusal::TooManyRunning) by
    /// `Ledger::wait`; a setting that needs none never reaches that call and is
    /// refused [`InFlight`](IdempotencyRefusal::InFlight). So the refusal *names*
    /// which side of `ledger.wait` the call was turned away on.
    ///
    /// One fixture for all three, following `actors.rs`'s `gated_behind`: settings that
    /// owe a caller different things in the same scenario have to be compared through
    /// one fixture, or the scenario drifts between them.
    ///
    /// `asked` is what the CALLER sent, and it is an [`Option`] because that is the
    /// shape of the wire field under test — `Idempotency::attach_for_millis` is
    /// `Option<u64>`, and `None` there *is* "did not ask". It is not an invented flag.
    ///
    /// `start_paused` for `held_for`'s reason — a setting that DOES park must reach
    /// its answer without a wall-clock wait.
    async fn refused_with_no_places(
        attaching: Attaching,
        asked: Option<Duration>,
    ) -> IdempotencyRefusal {
        let ledger = Ledger::new(Limits::default().attach(attaching), Records::Memory);
        let peer = EndpointId::from_str(A).expect("a valid endpoint id");
        let key = IdempotencyKey::new("k").expect("a valid key");

        let Claim::Fresh(_running) = ledger.claim(peer, key.clone()) else {
            panic!("the first claim must be fresh");
        };
        let places: Vec<_> = (0..MAX_WAITING_PER_PEER)
            .map(|_| ledger.wait(peer).expect("a free place"))
            .collect();

        let idem = match asked {
            Some(budget) => Idempotency::attaching_for(key, budget),
            None => key.into(),
        };
        let keyed = claim_key(&any(&ledger), "Probe", peer, Some(idem)).await;

        // Exhaustive, with no catch-all, for the reason
        // `an_attacher_over_the_cap_is_refused_before_it_parks` gives: the arm that
        // hides a regression is the one nobody thought to name.
        let refused = match keyed {
            Claimed::Refuse(RemoteError::Idempotency(refusal)) => refusal,
            Claimed::Refuse(other) => panic!("expected an idempotency refusal, got {other:?}"),
            Claimed::None => panic!("expected a refusal, got no key at all"),
            Claimed::Run(_) => panic!("expected a refusal, got the key"),
            Claimed::Replay(_) => panic!("expected a refusal, got a replay"),
        };
        drop(places);
        refused
    }

    /// **The property [`Attaching::Forbidden`] exists for**: the operator refuses to
    /// ADMIT an attacher, not merely to hold one.
    ///
    /// The pool has no place left, and the answer is
    /// [`InFlight`](IdempotencyRefusal::InFlight) rather than
    /// [`TooManyRunning`](IdempotencyRefusal::TooManyRunning) — which can only happen
    /// if `Ledger::wait` was never reached. That is the assertion: no waiting place is
    /// spent, so nothing parks and nothing re-claims.
    ///
    /// Paired with `a_zero_hold_still_spends_a_waiting_place`, which is the same
    /// scenario at the setting this one is not.
    ///
    /// Made red by `scripts/break-once.sh` replacing
    /// `Attaching::Forbidden => return Claimed::Refuse(IdempotencyRefusal::InFlight.into()),`
    /// with `Attaching::Forbidden => std::time::Duration::ZERO,`, which is
    /// [`Attaching::Forbidden`] collapsing into the zero hold it is not. Fully qualified
    /// in the replacement because `rpc.rs` does not import `Duration` — the unqualified
    /// form was tried and is `E0433`. The search literal carries its eight spaces of
    /// indentation, so this record's unindented copy is not a second occurrence.
    ///
    /// **MEASURED at this tree, whole-lib, one run: ONE guard red, this one.**
    /// `a_zero_hold_still_spends_a_waiting_place` stays green, which is what says the
    /// pair is not one test twice. Watched, under `cargo test`:
    /// `assertion `left == right` failed: a forbidden setting must be refused before a
    /// place is asked for`, `left: TooManyRunning`, `right: InFlight`.
    ///
    /// **The `limit` field the real line carries is left out of that transcription, and
    /// not for brevity.** This record owns the message above and the `right` it names —
    /// both are in this file, a few lines below — and it owns nothing at all about the
    /// count. `Ledger::wait` builds the refusal from `self.limits.waiting_per_peer`,
    /// which `Limits`'s `Default` fills from
    /// [`MAX_WAITING_PER_PEER`]; a service can then move that value through
    /// `Limits::waiting_per_peer` without touching the constant at all. Recording the
    /// number would also point at the wrong owner as readily as the right one:
    /// `MAX_RUNNING_PER_PEER` holds the same value **by intent** and says so in its own
    /// doc, so the printed count alone does not say which cap was reached.
    /// `left: TooManyRunning` is a substring of the real line, which is what a replay
    /// matches, and `a_zero_hold_still_spends_a_waiting_place` drops the count from its
    /// mirrored output for this same reason.
    #[tokio::test(start_paused = true)]
    async fn a_forbidden_ceiling_is_refused_without_a_waiting_place() {
        assert_eq!(
            refused_with_no_places(Attaching::Forbidden, Some(Duration::from_secs(30))).await,
            IdempotencyRefusal::InFlight,
            "a forbidden setting must be refused before a place is asked for"
        );
    }

    /// **The collapse, pinned rather than described.** A caller that never asked to
    /// attach reaches the SAME refusal, through the line below the operator's one —
    /// so an unexhausted pool is not what distinguishes them and nothing on the wire
    /// does either.
    ///
    /// The ceiling here is the default, so the only thing turning this call away is
    /// that it did not ask. Read against
    /// `a_forbidden_ceiling_is_refused_without_a_waiting_place`, which sends the same
    /// key with a budget and gets the same answer, the pair is what makes
    /// [`Attaching::Forbidden`]'s documented collapse a checked fact.
    ///
    /// No break is recorded, and the reason is that it has none worth having: this is
    /// `a_bare_key_does_not_attach`'s property observed one layer out, and that guard
    /// carries the break. What this adds is the accept path, which the other cannot
    /// see.
    #[tokio::test(start_paused = true)]
    async fn a_caller_that_did_not_ask_is_refused_the_same_way() {
        assert_eq!(
            refused_with_no_places(Attaching::UpTo(MAX_ATTACH), None).await,
            IdempotencyRefusal::InFlight,
            "a caller that did not ask must be refused before a place is asked for too"
        );
    }

    /// The other half, and the whole reason zero is not spelled `Forbidden`: a zero
    /// hold still ASKS for a waiting place, so an exhausted pool turns it away with
    /// the count it exceeded.
    ///
    /// [`Attaching`]'s own doc claims exactly this — *"`TooManyRunning` when the
    /// waiting pool was already full"* — and nothing asserted it until here.
    ///
    /// Made red by `scripts/break-once.sh` appending `.filter(|held| !held.is_zero())`
    /// to `.map(|millis| Duration::from_millis(millis).min(cap))` in
    /// `keylink/src/idempotency.rs`, which is a zero hold collapsing into
    /// [`Attaching::Forbidden`] — the inverse of the break above, and the reason these
    /// two are a pair rather than one test twice. That is A2's registered literal with
    /// a replacement of its own, so it adds no row. **MEASURED at this tree, whole-lib,
    /// one run: ONE guard red, this one.** Watched, under `cargo test`:
    /// `assertion `left == right` failed: a zero hold must still spend a waiting
    /// place`, `left: InFlight`, `right: TooManyRunning`. The count that line prints is
    /// left out for the reason
    /// `a_forbidden_ceiling_is_refused_without_a_waiting_place` sets out: it is
    /// [`MAX_WAITING_PER_PEER`]'s number arriving here through `Limits`, and this record
    /// owns none of it. The assertion below names that constant rather than a number,
    /// which is where a reader should go.
    #[tokio::test(start_paused = true)]
    async fn a_zero_hold_still_spends_a_waiting_place() {
        assert_eq!(
            refused_with_no_places(
                Attaching::UpTo(Duration::ZERO),
                Some(Duration::from_secs(30))
            )
            .await,
            IdempotencyRefusal::TooManyRunning {
                limit: u32::try_from(MAX_WAITING_PER_PEER).expect("a cap that fits"),
            },
            "a zero hold must still spend a waiting place"
        );
    }
}

/// Assembles one service: its allowlist, its handler, and the layers around it.
///
/// Returned by the generated `service()`, so a caller adds behaviour without
/// the macro knowing anything about it.
pub struct ServiceBuilder<Req, Resp> {
    alpn: &'static [u8],
    admission: Arc<dyn Admission>,
    required_admission: Vec<Arc<dyn Admission>>,
    required_authorization: Vec<Arc<dyn MethodAuthorization>>,
    base: Next<Req, Resp>,
    layers: Vec<Arc<dyn Middleware<Req, Resp>>>,
    required_layers: Vec<Arc<dyn Middleware<Req, Resp>>>,
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
    /// Every bound the ledger enforces. [`Limits::default`] unless
    /// [`ServiceBuilder::limits`] replaced it, and discarded under
    /// [`Keyable::None`], which builds no ledger to hold it.
    limits: Limits,
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
            required_admission: Vec::new(),
            required_authorization: Vec::new(),
            base: dispatch,
            layers: Vec::new(),
            required_layers: Vec::new(),
            blobs: None,
            declares_blobs: false,
            // Spelled out rather than reached through `Default`, and neither type
            // has one. `Keyable::Any` is the permissive setting and `Records::Memory`
            // holds a caller's bytes for ten minutes, so both are decisions somebody
            // makes — `Allowlist`'s doc makes the same argument about `anyone()`.
            // This line IS the default, and it is the only place to look for it.
            //
            // `Limits` is the exception on this list, and it DOES have a `Default`:
            // nine of its ten bounds refuse visibly when they are set wrong, so the
            // safe configuration is the default one. Its own rustdoc argues that per
            // field, including the field that is not like the others.
            keyable: Keyable::Any,
            records: Records::Memory,
            limits: Limits::default(),
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
    /// [`ConfigError::MissingBlobStore`].
    /// A service with no `blob` methods never needs one, which is why this is a
    /// builder method rather than a parameter every service would carry.
    ///
    /// One node has one content store. Two services on one node may share a
    /// [`Blobs`], and two DIFFERENT stores are
    /// [`ConfigError::ConflictingBlobStores`]:
    /// there is one blobs ALPN, so there can be one store behind it.
    #[must_use]
    pub fn with_blobs(mut self, blobs: &Blobs) -> Self {
        self.blobs = Some(blobs.clone());
        self
    }

    pub(crate) fn require_admission(mut self, admission: impl Admission) -> Self {
        self.required_admission.push(Arc::new(admission));
        self
    }

    pub(crate) fn require_authorization<A>(mut self, mut authorization: A) -> Self
    where
        A: MethodAuthorization + Middleware<Req, Resp>,
    {
        let authorization = Arc::new_cyclic(|installed| {
            authorization.bind(installed.clone() as std::sync::Weak<dyn MethodAuthorization>);
            authorization
        });
        self.required_authorization.push(authorization.clone());
        self.required_layers.push(authorization);
        self
    }

    /// Admit against a policy rather than against the fixed list this was built
    /// with.
    ///
    /// The list passed to the generated `service(handler, allow)` is itself an
    /// [`Admission`] — it is the canonical one — so this replaces the caller
    /// policy rather than adding another. Required policies installed by
    /// [`Compiled`](crate::Compiled) are intersected with it and cannot be replaced.
    /// Reach for this when the answer has to be able to change while the
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

    /// Add a layer. The first ordinary layer added is the outermost: it sees the
    /// request first and the reply last. Required layers installed by
    /// [`Compiled`](crate::Compiled) remain outside every ordinary layer.
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
    ///     .keyable(Keyable::Only(&["Reserve"]))
    ///     .build()
    /// ```
    ///
    /// The default is [`Keyable::Any`], so a service that never calls this behaves
    /// exactly as it did before the method existed. A key on a method the policy
    /// refuses is answered
    /// [`IdempotencyRefusal::NotKeyable`] —
    /// refused rather than ignored, because a caller that attached a key believes
    /// it is protected.
    ///
    /// This is not something a [`Middleware`] could do instead, and the act it
    /// cannot do is **refusing a key that is present**: the key is claimed before
    /// the layer chain runs, so a layer never sees an unclaimed one. Reading that a
    /// key is **absent** is a different act, and the section below is a layer doing
    /// it.
    ///
    /// # A layer requires a key where this policy permits one
    ///
    /// This decides which methods *accept* a key, and it cannot demand one. A
    /// [`Middleware`] can: [`Context::call`](crate::Context::call) answers `None`
    /// for a call that carried no key, and it answers before any layer runs —
    /// `claim_key` builds the value from the request that arrived, and `serve_one`
    /// enters the chain after it. So the absence is already correct when a layer
    /// reads it, and [`RemoteError::Unauthorized`] is what refuses
    /// it — **a reuse rather than a fit, and the difference is worth knowing before
    /// you copy this.** No variant is named for *this method requires a key*, and
    /// minting one is not cheap: [`RemoteError`] sits inside every service's hashed
    /// contract, so `reply.rs` records that adding a variant is
    /// wire-visible for every service, singletons included. `Unauthorized`'s own
    /// documented meaning is *the peer is known, but not for this method*, which is
    /// about the peer's standing rather than about a missing key. It is still the right
    /// choice over [`Failed`](crate::RemoteError::Failed): a peer can `matches!` on it,
    /// while a `Failed` is prose. A `Failed` whose text came from a caller's own
    /// `Middleware` is outside every guard in this crate and always will be — so a
    /// `Failed` string here is one nothing protects.
    ///
    /// ```ignore
    /// struct RequireKey;
    ///
    /// impl<Resp: Send + 'static> Middleware<till::Request, Resp> for RequireKey {
    ///     fn handle(
    ///         &self,
    ///         cx: Context,
    ///         req: till::Request,
    ///         body: BodyReader,
    ///         next: Next<till::Request, Resp>,
    ///     ) -> BoxFut<(Outcome<Resp>, Body)> {
    ///         Box::pin(async move {
    ///             // In step with `Keyable::Only(&["Reserve"])` above, and that is
    ///             // the whole of the caveat below.
    ///             if cx.method() == "Reserve" && cx.call().is_none() {
    ///                 return (
    ///                     Outcome::Err(RemoteError::Unauthorized(
    ///                         "Reserve needs an idempotency key".into(),
    ///                     )),
    ///                     Body::empty(),
    ///                 );
    ///             }
    ///             next(cx, req, body).await
    ///         })
    ///     }
    /// }
    /// ```
    ///
    /// **The method test is what makes a shorter layer wrong.** On a method
    /// outside this policy a keyed call is refused
    /// [`NotKeyable`](crate::IdempotencyRefusal::NotKeyable) *before* the chain is
    /// entered and an unkeyed one arrives with `None`, so `cx.call()` can never
    /// answer `Some` there — [`Context::call`](crate::Context::call) states that and
    /// gives the construction. A layer that drops the `cx.method()` test therefore
    /// refuses **every** call on every method this policy leaves out.
    ///
    /// **A `tell` breaks such a layer two ways, and the `cx.method()` test is what
    /// saves it.** `serve_one_tell` never calls `with_call`, so `cx.call()` is
    /// `None` for *every* `tell` whatever the caller sent — and it discards the layer
    /// chain's outcome, so a refusal there reaches nobody. A `tell` cannot carry a key
    /// at all, which [`Context::call`](crate::Context::call) says on its own; the
    /// consequence for this layer is that a `tell` method must never be in the list it
    /// tests.
    ///
    /// **Under [`Keyable::None`] such a layer refuses everything**, by the same
    /// construction as a method outside the list: no ledger is built at all, so no call
    /// can arrive with a key. `rpc::tests::the_ledger_is_reachable_only_through_the_policy`
    /// and `Keys::new`'s own guard hold that no ledger exists there.
    ///
    /// **What such a layer polices is the key's presence, and nothing further about
    /// the ledger.** [`Limits::attach`](crate::Limits::attach) records the ordering
    /// this rests on: `serve_one` finishes `claim_key` — the park, and the re-claim
    /// after it — *before* it calls the chain, so no layer can bound how long an
    /// attaching call was held.
    ///
    /// **Served paths only.** `middleware::wrap` is `pub(crate)` and reached from
    /// [`build`](Self::build) alone, so a call made through a local
    /// [`Handle`](crate::LocalActor) meets no layer and this guard does not exist for
    /// it. A key is a wire concept, so that is consistent rather than a hole — but a
    /// layer is not a place to put an invariant the local path shares.
    ///
    /// **A keyed actor system takes this layer with its impl header changed**, and the
    /// change is not optional. The generated `Registry` service path hands back a
    /// [`ServiceBuilder`]`<`[`Addressed`](crate::Addressed)`<Request>, Response>`, and
    /// [`layer`](Self::layer) accepts a [`Middleware`] over that builder's own `Req` — so
    /// the header becomes `Middleware<Addressed<till::Request>, Resp>`, and the form
    /// written above does not satisfy it. An operator who copies the header unchanged
    /// onto a keyed system gets a trait-bound error rather than a layer.
    ///
    /// **The body is unchanged.** [`Context::method`](crate::Context::method) answers the
    /// actor's own method name on that path rather than a constant, which is what makes
    /// the `cx.method()` test above mean the same thing there.
    /// `tests/actors.rs`'s `PerMethod` mounts a `Middleware<Addressed<..>, ..>` on a
    /// `Registry::service` and reads that name, so the keyed header shape is compiled
    /// somewhere in this tree.
    ///
    /// The layer above is held on an ordinary service by
    /// `tests/protocol.rs::a_layer_can_require_an_idempotency_key`. **No test mounts this
    /// layer on a keyed system**, so the keyed header is stated here rather than
    /// compiled with this body.
    #[must_use]
    pub fn keyable(mut self, keyable: Keyable) -> Self {
        self.keyable = keyable;
        self
    }

    /// Decide whether this service holds a caller's reply bytes at all.
    ///
    /// [`Records::None`] is at-most-once with no retention: the key still claims, so
    /// the work still runs once, and every repeat is
    /// [`IdempotencyRefusal::Unrecordable`]
    /// rather than the reply. Reach for it when the reply is data this service would
    /// rather not keep, and the caller can read the result out of its own domain.
    ///
    /// ```ignore
    /// till::service(handler, Allowlist::anyone())
    ///     .keyable(Keyable::Only(&["Reserve"]))
    ///     .records(Records::None)
    ///     .build()
    /// ```
    ///
    /// The default is [`Records::Memory`], so a service that never calls this
    /// behaves exactly as it did before the method existed.
    ///
    /// # It is not free, and [`Records`] is where the cost is written
    ///
    /// The key state never leaves memory whatever this says. **What
    /// [`Records::None`] does shorten is the at-most-once *window*, by about a
    /// ninth**, because a settled key skips the record pool and enters the tombstone
    /// pool directly. That lands on the one degradation a caller cannot detect: past
    /// [`MAX_TOMBSTONES`](crate::MAX_TOMBSTONES) a key reads `Fresh` again, and `Fresh`
    /// is what a key that was never used looks like too. `README.md` argues it under
    /// its tombstone limit — named by subject here, because what stood in this sentence
    /// was the ORDINAL "limit 4", and an ordinal is a positional anchor pointed at
    /// prose rather than at a literal. Any item inserted above it in that numbered list
    /// repoints this sentence silently, and neither a count nor a gate can see that.
    /// `.mise/tasks/anchors` numbers the same rot mode three for quoted lines. The
    /// ratio above is derived from [`MAX_RECORDS`](crate::MAX_RECORDS) against both
    /// pools together, so it moves whenever either constant does. [`Records`] carries
    /// the operands and the argument; read it before choosing `None` for a privacy
    /// requirement, because the sentence
    /// that used to sit here — *"no setting here can weaken at-most-once"* — was
    /// false, and this is the doc a caller reads at the instant it types the call.
    #[must_use]
    pub fn records(mut self, records: Records) -> Self {
        self.records = records;
        self
    }

    /// Replace the bounds this service's idempotency ledger enforces.
    ///
    /// The default is [`Limits::default`], so a service that never calls this
    /// behaves exactly as it did before the method existed. [`Limits`] carries the
    /// argument for each bound and what setting it to its smallest value would do.
    ///
    /// ```ignore
    /// till::service(handler, Allowlist::anyone())
    ///     .limits(Limits::default().pool_records(thousands))
    ///     .build()
    /// ```
    ///
    /// # Two ways to call this and have it bound nothing
    ///
    /// Both compile, and both read as configured:
    ///
    /// - **`.keyable(Keyable::None)`** builds **no ledger at all**
    ///   (`Keys::new` is where that is decided), so the whole value is
    ///   discarded. Nothing warns, because a service with no keys has no bounds to
    ///   set.
    /// - **`.records(Records::None)`** holds no reply, so
    ///   [`pool_records`](Limits::pool_records),
    ///   [`frame_bytes`](Limits::frame_bytes) and
    ///   [`pool_bytes`](Limits::pool_bytes) bound nothing. The running, waiting and
    ///   tombstone bounds still apply.
    ///
    /// These are worth naming here rather than only on [`Limits`], because both
    /// methods and this one appear in this crate's own examples in a single chain.
    #[must_use]
    pub fn limits(mut self, limits: Limits) -> Self {
        self.limits = limits;
        self
    }

    /// Freeze the chain and erase the schema.
    pub fn build(self) -> Service {
        let configuration =
            self.keyable
                .first_unknown(Req::METHODS)
                .map(|named| ConfigError::UnknownMethod {
                    service: Req::SERVICE,
                    named: named.to_owned(),
                });
        let mut chain = self.base;
        for layer in self.layers.into_iter().rev() {
            chain = wrap(layer, chain);
        }
        for layer in self.required_layers.into_iter().rev() {
            chain = wrap(layer, chain);
        }
        Service {
            alpn: self.alpn,
            handler: Box::new(RpcService {
                next: chain,
                admission: self.admission,
                required_admission: self.required_admission,
                required_authorization: self.required_authorization.into(),
                blobs: self.blobs.clone(),
                keys: Keys::new(self.keyable, self.records, self.limits),
                _p: PhantomData,
            }),
            blobs: self.blobs,
            declares_blobs: self.declares_blobs,
            configuration,
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
    /// A typed-builder configuration error retained through schema erasure.
    configuration: Option<ConfigError>,
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
    /// does NOT cover it, and neither does [`Middleware`].
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
            configuration: None,
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
            if let Some(error) = service.configuration {
                return Err(Error::Config(error));
            }
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
    /// meet — see [`IdempotencyRefusal`].
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
