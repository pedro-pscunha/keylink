//! Cross-cutting behaviour, added from outside the generated code.
//!
//! A [`Middleware`] wraps one service's dispatch. It sees the request *typed*,
//! so a per-method rule is an ordinary `match` on the real enum rather than a
//! string comparison, and it can refuse a call by returning
//! [`Outcome::Err`] instead of calling [`Next`].
//!
//! Layers are the reason the reply carries an [`Outcome`]: a timeout or an
//! authorization rule has to be able to say no, and it cannot do that by
//! inventing a return value for a method it knows nothing about.

use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::Arc;
use std::time::Duration;

use iroh::EndpointId;
use n0_future::time::Instant;

use crate::{ActorId, Body, BodyReader, CallId, Outcome, RemoteError};

pub(crate) type BoxFut<T> = Pin<Box<dyn Future<Output = T> + Send>>;

/// The rest of the chain: the next layer, or the handler itself.
pub type Next<Req, Resp> =
    Arc<dyn Fn(Context, Req, BodyReader) -> BoxFut<(Outcome<Resp>, Body)> + Send + Sync>;

/// Implemented by every generated `Request` enum, so a layer can name the
/// method it is looking at without knowing the service's types.
pub trait Rpc {
    /// The service name, as written in `service!`.
    const SERVICE: &'static str;

    /// Every method present in this binary's service contract.
    const METHODS: &'static [&'static str];

    /// The method this request selects, in the `CamelCase` spelling from
    /// `service!`.
    fn method(&self) -> &'static str;

    /// Which actor this request addresses, for a request that names one.
    ///
    /// `None` for an ordinary service, where the whole handler is the address.
    /// Overridden by [`Addressed`](crate::Addressed), which is what a keyed
    /// request arrives inside, so that a layer can apply a **per-actor** rule
    /// before dispatch.
    ///
    /// This method survived a proposal to delete it, and the reversal is worth
    /// recording because the reason it was written down is no longer the reason
    /// it is here. It used to be the *only* thing a layer could discriminate on
    /// over a keyed system: the method name lived inside a sealed payload no
    /// layer could read before a hash check, so [`Self::method`] answered the
    /// constant `"Message"` and a per-method rule was impossible. A keyed
    /// request is typed now and `method` reports the real name, so this is no
    /// longer a consolation prize — it is the *other* axis, and a per-actor rule
    /// is a thing a caller genuinely wants beside a per-method one. Three lines
    /// with a default beat a permanent fleet-wide wire tax.
    fn actor(&self) -> Option<&ActorId> {
        None
    }
}

/// How far one call got before the chain handed an answer back.
///
/// The idempotency ledger cannot read this off the [`Outcome`]: `Outcome::Err`
/// is what a refusing layer returns *and* what a panicking actor turn returns,
/// so the value genuinely cannot say who produced it. See RFC-05 §2.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Reached {
    /// No layer called through. Nothing ran, so a claimed key owes nothing and
    /// is given back.
    NotDispatched,
    /// The work may have started and did not report finishing — a `Timeout`
    /// that dropped the wait for an actor turn already running *or* already
    /// queued, or a connection that died. Whether it took effect is unknown,
    /// which is what `Interrupted` says.
    ///
    /// "May have" and not "did", since a message sitting in a mailbox is one
    /// nobody can promise did not run. Answering that with a release is the
    /// at-least-once defect RFC-04 §16.12 records.
    Dispatched,
    /// The work produced the answer the chain returned. The key settles as that
    /// answer.
    Completed,
}

/// The progress of one call, shared by every clone of its [`Context`].
///
/// `Context` travels **by value** down [`Next`], so each layer holds its own
/// clone; the marker is behind an `Arc` precisely so that all of them, and the
/// accept path that built the first one, look at one value.
///
/// Marked by generated dispatch rather than by a wrapper around the base,
/// because only generated code knows when the *work* began. On the actor path
/// the base is entered while the message is still queued, so a wrapper would
/// report `Dispatched` for a turn that a [`QueueTimeout`](crate::QueueTimeout)
/// then refused before it ever ran.
///
/// # A middleware can lie, and this is the wrong place to stop it
///
/// This type and [`Context::progress`] are `pub` — hidden, but `pub` — because
/// the code that marks them is expanded into the *caller's* crate, where
/// `pub(crate)` does not reach. So a [`Middleware`] holding a [`Context`] can
/// mark [`completed`](Self::completed) and then refuse without calling `next`,
/// and the accept path will record that refusal as the call's settled answer
/// and replay it to every retry — the defect the whole marker exists to
/// prevent, re-opened one layer up.
///
/// There is nothing to seal it with. A token only generated code could build
/// would itself have to be `pub` to be named in the expansion, so the forgery
/// moves by one line. Cross-checking against the chain's `Outcome` is worse
/// than useless: a layer that runs `next` and *then* rewrites `Ok` into `Err`
/// produced the frame the caller actually received, and replaying that frame
/// byte for byte is correct — demoting it would break a legitimate layer to
/// inconvenience a dishonest one.
///
/// What makes it tolerable is that a `Middleware` is not a plugin. You write
/// it, you compile it into your binary, and it already sits between every call
/// and the handler: a layer willing to forge this marker can return a wrong
/// answer outright and save itself the trouble. The boundary this crate
/// defends is the **peer**, and no peer can reach any of this.
#[doc(hidden)]
#[derive(Debug, Clone)]
pub struct Progress(Arc<AtomicU8>);

/// Facts about one call, not positions along it. `fetch_or` sets a bit and
/// never clears one, which is the same property `fetch_max` was chosen for — a
/// late mark cannot walk the state backwards, so marks made from the actor's
/// task need no ordering against the accept task's read — without needing the
/// facts to sit in one total order.
///
/// They cannot. What each fact owes a claimed key alternates: nothing marked
/// releases, `ENQUEUED` settles, `UNRUN` releases, `DISPATCHED` settles. A
/// threshold read can only answer something monotone along the order, and
/// `UNRUN` happens *after* `ENQUEUED` and owes *less* than it. Ordering them
/// was the wrong constraint, not the missing one.
const ENQUEUED: u8 = 1;
const UNRUN: u8 = 2;
const DISPATCHED: u8 = 4;
const COMPLETED: u8 = 8;

impl Progress {
    pub(crate) fn new() -> Self {
        Self(Arc::new(AtomicU8::new(0)))
    }

    /// The message is in a mailbox and its fate is no longer this task's to
    /// know. Marked by whichever code got the permit, because only that code
    /// can tell a message that queued from one whose send was cancelled.
    ///
    /// **Not a settlement**, which is the whole of why marking here does not
    /// re-open RFC-04 §16.6 and §16.7: [`unrun`](Self::unrun) outranks it, so a
    /// [`QueueTimeout`](crate::QueueTimeout) refusing a still-queued message
    /// still gives the key back. What it does buy is the case nothing else
    /// covers — a caller that gives up on a queued message and never hears
    /// which way it went, whose key would otherwise be released under a turn
    /// that then runs.
    #[doc(hidden)]
    pub fn enqueued(&self) {
        self.0.fetch_or(ENQUEUED, Ordering::Release);
    }

    /// The message came off a mailbox and no user code ran. Called by generated
    /// dispatch, from every arm that ends a message without entering it.
    ///
    /// Silence would mean [`enqueued`](Self::enqueued), so an arm that knows
    /// nothing ran has to say so rather than say nothing.
    #[doc(hidden)]
    pub fn unrun(&self) {
        self.0.fetch_or(UNRUN, Ordering::Release);
    }

    /// The work is about to begin. Called by generated dispatch.
    #[doc(hidden)]
    pub fn dispatched(&self) {
        self.0.fetch_or(DISPATCHED, Ordering::Release);
    }

    /// The work produced its answer. Called by generated dispatch.
    #[doc(hidden)]
    pub fn completed(&self) {
        self.0.fetch_or(COMPLETED, Ordering::Release);
    }

    /// Read most-known first, so there is still no catch-all arm and no
    /// unreachable state to name: every bit pattern maps to exactly one answer.
    ///
    /// The order is also what makes a double mark harmless rather than
    /// ambiguous. A message that is both `DISPATCHED` and `UNRUN` is read as
    /// dispatched, which is the conservative answer, and it is why an arm may
    /// mark `unrun()` without first proving that nothing above it ran.
    pub(crate) fn reached(&self) -> Reached {
        let raw = self.0.load(Ordering::Acquire);
        if raw & COMPLETED != 0 {
            Reached::Completed
        } else if raw & DISPATCHED != 0 {
            Reached::Dispatched
        } else if raw & UNRUN != 0 {
            Reached::NotDispatched
        } else if raw & ENQUEUED != 0 {
            Reached::Dispatched
        } else {
            Reached::NotDispatched
        }
    }
}

/// Who is calling, and what they are calling.
///
/// `peer` is the key iroh proved during the handshake — not a claim carried in
/// the request body, which is why it is safe to authorize against.
/// `credential` is the authority a trust guard verified for this call.
#[derive(Clone)]
pub struct Context {
    peer: EndpointId,
    service: &'static str,
    method: &'static str,
    actor: Option<ActorId>,
    progress: Progress,
    queueing: Option<Queueing>,
    call: Option<CallId>,
    credential: Option<Arc<crate::trust::Credential>>,
}

impl std::fmt::Debug for Context {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Context")
            .field("peer", &self.peer)
            .field("service", &self.service)
            .field("method", &self.method)
            .field("actor", &self.actor)
            .field("progress", &self.progress)
            .field("queueing", &self.queueing)
            .field("call", &self.call)
            .finish()
    }
}

impl Context {
    pub(crate) fn new(peer: EndpointId, service: &'static str, method: &'static str) -> Self {
        Self {
            peer,
            service,
            method,
            actor: None,
            progress: Progress::new(),
            queueing: None,
            call: None,
            credential: None,
        }
    }

    /// The budget a [`QueueTimeout`] put on this call, for generated actor
    /// dispatch to honour.
    ///
    /// `None` on every call no `QueueTimeout` layer touched, which is every
    /// call by default.
    #[doc(hidden)]
    #[must_use]
    pub fn queueing(&self) -> Option<Queueing> {
        self.queueing
    }

    /// The progress marker for this call, for generated dispatch to mark.
    ///
    /// A clone shares the marker, which is what lets an actor's turn — running
    /// on another task entirely — report that it started and finished.
    ///
    /// Reachable from a [`Middleware`], which is a hole [`Progress`] documents
    /// rather than one it closes.
    #[doc(hidden)]
    #[must_use]
    pub fn progress(&self) -> Progress {
        self.progress.clone()
    }

    /// Record which actor the request addresses. Called by the accept loop for
    /// every request whose type names one.
    pub(crate) fn with_actor(mut self, actor: Option<ActorId>) -> Self {
        self.actor = actor;
        self
    }

    /// Name this call, when it arrived carrying an idempotency key. Called by the
    /// accept loop, from the one place both halves of the digest exist.
    pub(crate) fn with_call(mut self, call: Option<CallId>) -> Self {
        self.call = call;
        self
    }

    pub(crate) fn with_credential(
        mut self,
        credential: Option<Arc<crate::trust::Credential>>,
    ) -> Self {
        self.credential = credential;
        self
    }

    /// The authority verified by a trust guard, or `None` on an unguarded service.
    pub fn credential(&self) -> Option<&crate::trust::Credential> {
        self.credential.as_deref()
    }

    /// The proven public key of the calling peer.
    pub fn peer(&self) -> EndpointId {
        self.peer
    }

    /// The service being called.
    pub fn service(&self) -> &'static str {
        self.service
    }

    /// The method being called.
    ///
    /// For a keyed actor this **is** the actor's own method, the same name a
    /// singleton reports, because a keyed request is
    /// [`Addressed<Request>`](crate::Addressed) and its
    /// [`Rpc::method`] delegates inward. It answered the constant `"Message"`
    /// for every keyed call while the payload was opaque bytes, which is what
    /// made per-method middleware and per-method authorization impossible there.
    pub fn method(&self) -> &'static str {
        self.method
    }

    /// The actor this message addresses, when the service is an actor system.
    ///
    /// As it arrived. The length cap is enforced by the registry, not here, so
    /// a value seen from a layer may be longer than [`crate::MAX_KEY_LEN`] —
    /// in which case no actor can match it and the call is refused.
    pub fn actor(&self) -> Option<&ActorId> {
        self.actor.as_ref()
    }

    /// The name of this call, when the caller put an
    /// [`IdempotencyKey`](crate::IdempotencyKey) on it.
    ///
    /// `None` when it did not, and `None` for every `tell`, which cannot carry a
    /// key at all. Never a peer's key in the clear: see [`CallId`].
    ///
    /// # `None` for a method this service does not key, and that is a footgun
    ///
    /// [`Keyable`](crate::Keyable) decides which methods accept a key at all. On a
    /// method outside that policy a keyed call is refused
    /// [`NotKeyable`](crate::IdempotencyRefusal::NotKeyable) **before the chain is
    /// entered**, and an unkeyed one arrives here as `None` — so this accessor can
    /// never answer `Some` there, by construction rather than by chance.
    ///
    /// Which means an [`Applied`](crate::Applied) sitting in a handler whose method
    /// the service has not made keyable **never fires, and nothing says so**. That
    /// is the same silent at-least-once the type exists to prevent, reached through
    /// a serving-side setting instead of through a caller forgetting
    /// `with_idempotency`. If you deduplicate in a handler, put its method in the
    /// policy.
    ///
    /// The refusal itself is held by
    /// `tests/idempotency.rs::a_key_on_a_method_the_service_does_not_key_is_refused`.
    /// What is added here is only where that leaves this accessor, which is a
    /// consequence of `claim_key` returning before `next` and needs no second test.
    ///
    /// This is what makes `README.md`'s third limit — *"read the result out of
    /// your own domain; do not re-run the work"* — something a handler can act on.
    /// [`Applied`](crate::Applied) is where to put it. The ledger stops a
    /// duplicate for as long as the process lives; **this is for the restart it
    /// cannot cross**, where the state came back and the ledger did not.
    ///
    /// # A `#[defer]` reply released in a later turn does not carry this one
    ///
    /// `parking.rs` puts it plainly: a [`Deferred`](crate::Deferred) *"is parked in
    /// turn 1 and sent in turn 5"*. During turn 5 this accessor answers **turn
    /// 5's** call — `approve`'s key, or `None` if `approve` carried none — and not
    /// the parked `withdraw`'s. That is the only answer that can be right: turn 5
    /// is a call of its own, with its own caller and its own key, and a
    /// [`Deferred`](crate::Deferred) reached from a spawned task is not inside any
    /// turn at all.
    ///
    /// So a handler that wants to dedupe a `#[defer]` method records `cx.call()`
    /// **in the turn that parks**, which is the turn that has it. What it records
    /// there cannot be an answer, because that turn produced none — so `#[defer]`
    /// gets `Applied<()>` and at-most-once, never the replayed answer an ordinary
    /// `rpc` gets. Across a restart it gets less still: a parked reply channel is
    /// not `Durable` and cannot be, so the restart drops it and the original
    /// caller is never answered by anybody.
    ///
    /// Reachable from a [`Middleware`], which is a hole this documents rather than
    /// one it closes — the same shape as [`progress`](Self::progress), and much
    /// less interesting: a layer can already read the whole request.
    #[must_use]
    pub fn call(&self) -> Option<CallId> {
        self.call
    }
}

/// Where a message reached an actor from.
///
/// An actor is reachable two ways — a local [`Handle`](crate::LocalActor) and a
/// served ALPN — so "who is calling" has an answer that a concurrent
/// [`Handler`] never needs: nobody, because the call came from this process.
///
/// **Not `Option<&Context>`.** `authz.rs` records what an `Option` on an
/// identity costs, and this crate's rule is that a known value set is an enum.
/// `None` also invites `cx.map(Context::peer)` at the one call site where being
/// wrong means authorizing a local message against the last peer that happened
/// to dial in.
///
/// ```ignore
/// async fn submit(&mut self, cx: Caller<'_>, order: Order) -> Fill {
///     let who = match cx {
///         Caller::Local => Who::House,
///         Caller::Peer(cx) => Who::Trader(cx.peer()),
///     };
///     // ...
/// }
/// ```
///
/// Exhaustive, like [`Departure`](crate::Departure). A wildcard arm here is the
/// collapse the type exists to prevent.
///
/// [`Handler`]: crate::Middleware
#[derive(Debug, Clone, Copy)]
pub enum Caller<'a> {
    /// From inside this process, through the generated `Handle`. There is no
    /// peer, no handshake and no proven key, so there is nothing to attribute
    /// it to — and pretending otherwise is the failure this variant exists to
    /// make impossible.
    Local,
    /// From a peer iroh proved at the handshake.
    Peer(&'a Context),
}

impl<'a> Caller<'a> {
    /// The calling peer's context, when a peer is calling.
    ///
    /// A convenience for the common "log it, or ignore it" case. Reach for the
    /// `match` when the local case needs its own answer, which is most of the
    /// time an actor is served both ways.
    #[must_use]
    pub fn peer(self) -> Option<&'a Context> {
        match self {
            Self::Local => None,
            Self::Peer(cx) => Some(cx),
        }
    }
}

/// A [`QueueTimeout`]'s budget, as the two things the serving side needs: when
/// it runs out, and how long it was, for the refusal to report.
///
/// Carried on [`Context`] rather than kept inside the layer, because a layer
/// cannot tell a queued message from a running one — only the actor's own task
/// can, and that is where the deadline is checked.
#[doc(hidden)]
#[derive(Debug, Clone, Copy)]
pub struct Queueing {
    deadline: Instant,
    millis: u64,
}

impl Queueing {
    /// When the budget runs out.
    #[doc(hidden)]
    #[must_use]
    pub fn deadline(self) -> Instant {
        self.deadline
    }

    /// The budget, for [`RemoteError::TimedOut`].
    #[doc(hidden)]
    #[must_use]
    pub fn millis(self) -> u64 {
        self.millis
    }

    /// Whether the budget is spent. Asked here so that both dispatch paths ask
    /// one question rather than each writing its own comparison.
    ///
    /// Generated code cannot write it: `deadline` is an
    /// `n0_future::time::Instant` — tokio's natively, `web_time`'s in a browser,
    /// and `std::time::Instant` on neither, because that one panics on
    /// `wasm32-unknown-unknown` — and nothing in this crate's public surface
    /// re-exports a clock for `service!` to name in the caller's crate. The
    /// keyed loop reached for the one spelling available to it,
    /// `!deadline().elapsed().is_zero()`, which is `now > deadline` and not the
    /// `now >= deadline` the singleton's own dispatch applied to the same budget.
    /// That divergence was written down twice, in the macro and in RFC-04, as "a
    /// budget of exactly zero". It is every case where `now == deadline`, and
    /// there are two ways to land on one:
    ///
    /// - **A paused clock.** `tokio::time::pause` advances only to timer edges,
    ///   so it lands exactly on the deadline at any budget. There this is the
    ///   ordinary case rather than an edge one, and a test written that way
    ///   would watch one path refuse a message the other ran.
    /// - **A browser.** On `wasm32-unknown-unknown` the clock is
    ///   `web_time::Instant`, and its `now()` is `Performance.now()` — clamped
    ///   to a tick, typically 1 ms and as coarse as 100 ms under Firefox's
    ///   resist-fingerprinting. `deadline` is stamped from that same clamped
    ///   clock, so it sits on a tick too, and `now` reaches it exactly for any
    ///   budget that is a whole number of ticks. Under the usual 1 ms clamp
    ///   that is every budget written in whole milliseconds. Not a test
    ///   artefact: a target this crate compiles for.
    ///
    /// It is the native clock that hides all of this. Nanosecond resolution and
    /// a channel hop between the stamp and the check mean `now == deadline` is
    /// unreachable there at any budget — which is why the whole test suite
    /// stays green whichever way this one comparison is written.
    ///
    /// `>=` rather than `>`, because the other half of this budget is
    /// `n0_future::time::timeout` around the wait for a mailbox permit, and a
    /// timer fires *at* its deadline. A message that has waited its whole budget
    /// is already refused by that half; the half that runs on the actor's task
    /// has to say the same thing about the same message.
    #[doc(hidden)]
    #[must_use]
    pub fn expired(self) -> bool {
        Instant::now() >= self.deadline
    }
}

/// Refuses a message that is still **queued** when its budget expires, and
/// never one that has started.
///
/// # Why this is not `Timeout`
///
/// A layer runs outside the mailbox, so a [`Timeout`]'s budget expiring drops
/// the *wait for the reply*, not the turn: the turn runs to completion whatever
/// the caller does. Ten callers get `TimedOut` at 100 ms and the actor still
/// spends a hundred seconds producing replies nobody is waiting for. That is
/// not load shedding; it is load shedding's shadow.
///
/// This one sheds real load, because it refuses **before** the work starts. The
/// deadline travels on the [`Context`] and is checked in two places, on both of
/// the ways an actor is served: once around the wait for a mailbox permit, and
/// once on the actor's own task immediately before the turn begins. So a message
/// it refuses is a message that never ran, which is also what makes it the only
/// bound that composes with an [`IdempotencyKey`](crate::IdempotencyKey) — the
/// key stays claimable.
///
/// # Neither check substitutes for the other, and the two paths got them apart
///
/// The second check bounds a message queued behind a busy actor. The first
/// bounds a message that cannot be queued at all, because the mailbox is full —
/// which is the state a load-shedding budget exists for. Named per path rather
/// than left as one sentence about "the deadline", because the sentence above
/// was true of one path and false of the other for as long as both existed:
///
/// - A singleton served through the former inherent `Handle::service` door had
///   both from the start, in what was then the whole of that path's dispatch.
/// - A keyed [`Registry`](crate::Registry) had **neither**: the layer compiled on
///   `Registry::service` and guarded nothing there at any budget, because its loop
///   never read the [`Context`]. RFC-04 §16.7. It gained the turn-side check
///   first and the permit-side one after that, separately, because the first fix
///   looked complete.
///
/// **Both paths now make each check in one place, and neither writes it.** The
/// permit-side bound is in `deliver_keyed` for a keyed actor and in
/// `deliver_unkeyed` for a singleton; the turn-side one is in `keylink::turn`,
/// which every generated mailbox arm calls and which reads
/// [`Queueing::expired`](Queueing::expired) off the origin the message arrived
/// with.
/// So the two paths cannot disagree about a comparison neither of them spells,
/// which is what the three hand-written ones above turned out to be worth.
///
/// Why a [`QueueTimeout`] could not represent its deadline on the target clock.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("queue timeout budget is too large for the target clock")]
pub struct QueueTimeoutError;

/// A queue budget applied by [`Middleware`].
///
/// # On a `Handler` it refuses nothing, and that is not a lie
///
/// A [`Handler`](crate::Middleware) is dispatched immediately and never queues,
/// so no message it serves is ever "still queued when the budget expired". The
/// answer is honestly zero rather than falsely reported. Use [`Timeout`] there,
/// where dropping the future genuinely cancels the work.
#[derive(Debug, Clone, Copy)]
pub struct QueueTimeout {
    budget: Duration,
}

impl QueueTimeout {
    /// Refuse any message still waiting to start after `budget`.
    ///
    /// The budget is checked against the target clock at construction time. A
    /// deadline is computed again when a request reaches the layer; that second
    /// computation uses the same checked operation and conservatively expires
    /// immediately if the clock has since reached its representable limit.
    ///
    /// # Errors
    ///
    /// Returns [`QueueTimeoutError`] when the target clock cannot represent a
    /// deadline starting now after `budget`.
    pub fn try_after(budget: Duration) -> Result<Self, QueueTimeoutError> {
        Instant::now()
            .checked_add(budget)
            .map(|_| Self { budget })
            .ok_or(QueueTimeoutError)
    }
}

impl<Req: Send + 'static, Resp: Send + 'static> Middleware<Req, Resp> for QueueTimeout {
    fn handle(
        &self,
        mut cx: Context,
        req: Req,
        body: BodyReader,
        next: Next<Req, Resp>,
    ) -> BoxFut<(Outcome<Resp>, Body)> {
        // The deadline is stamped HERE, when the call reached this layer, so an
        // outer layer that took its time is spent out of this budget rather
        // than added to it.
        let now = Instant::now();
        // `try_after` checks this at construction time. Re-check here because a
        // long-lived layer can outlive the clock range it was created in; an
        // unrepresentable deadline is safest as an already-expired budget, not a
        // panic in the dispatch task.
        let deadline = now.checked_add(self.budget).unwrap_or(now);
        cx.queueing = Some(Queueing {
            deadline,
            millis: u64::try_from(self.budget.as_millis()).unwrap_or(u64::MAX),
        });
        next(cx, req, body)
    }
}

/// One layer of behaviour around a service.
///
/// Make the impl generic over `Req: Rpc` and it applies to every service you
/// ever define, which is what keeps a logging or metrics layer from being
/// rewritten per service.
///
/// # Leave the progress marker alone
///
/// The [`Context`] a layer is handed carries the marker the idempotency ledger
/// reads to decide whether the work was entered. Marking it and then refusing
/// records the refusal as the call's answer and replays it to every retry. It
/// is `#[doc(hidden)]` and it cannot be sealed; the reasoning is on `Progress`.
pub trait Middleware<Req, Resp>: Send + Sync + 'static {
    /// Handle one call. Call `next` to continue, or return an
    /// [`Outcome::Err`] to refuse without reaching the handler.
    fn handle(
        &self,
        cx: Context,
        req: Req,
        body: BodyReader,
        next: Next<Req, Resp>,
    ) -> BoxFut<(Outcome<Resp>, Body)>;
}

/// Refuses a call that outlives its budget.
///
/// Bundled because a timeout is the one layer that is identical for every
/// service and impossible to express without the error channel.
///
/// # Cancellation
///
/// When the budget expires the handler's future is dropped where it stands. A
/// handler that must not be interrupted half way has to do that work in a task
/// the drop cannot reach.
///
/// # On an actor it cancels nothing, and a key it gives up on stays refused
///
/// An actor's turn is exactly the work "a task the drop cannot reach" describes,
/// because that is what a mailbox is. This layer runs outside it, so the budget
/// expiring drops the *wait for the reply* and nothing else: the message is
/// already queued and the turn runs to completion whatever the caller does. Ten
/// callers get `TimedOut` at 100 ms and the actor still spends a hundred seconds
/// producing replies nobody is waiting for. That is not load shedding.
///
/// Put an [`IdempotencyKey`](crate::IdempotencyKey) on the same call and the
/// answer is honest, permanent, and probably not what the caller hoped for. A
/// key settles on **what ran**, read the instant the chain answers, and this
/// layer answers while the message is somewhere it cannot see — queued, or in a
/// turn. Both settle
/// [`Interrupted`](crate::IdempotencyRefusal::Interrupted), so every retry of
/// that call is refused for as long as the ledger keeps the entry and the
/// caller has to reconcile out of band.
///
/// That the two cases now agree is the fix, not an accident. A message still
/// queued when the budget expired used to leave the key **released** — nothing
/// had marked it — and then the turn ran, and the retry the released key invited
/// ran the same work a second time: at-least-once, out of the module whose one
/// promise is at-most-once. Reaching a mailbox is a fact the marker records, so
/// giving up on a message that reached one can no longer be mistaken for giving
/// up on one that never did. RFC-04 §16.12.
///
/// [`QueueTimeout`] is still the bound for an actor: it refuses only a message
/// that has not started, which is what leaves the key *claimable* rather than
/// settled, so the retry is a first attempt instead of a refusal. Keep this one
/// for a [`Handler`](crate::Middleware), where dropping the future genuinely
/// cancels the work — and for the one job on an actor that nothing else in the
/// crate does, below.
///
/// # The only bound that answers a caller behind a turn that never returns
///
/// Both dispatch paths bound the wait for a mailbox *permit*, and neither
/// bounds the wait for the *answer*. So a turn that never returns — a self-call
/// deadlock, an await on something that never fires — holds its caller forever,
/// and [`QueueTimeout`] cannot help: its permit-side check has already passed,
/// and its turn-side check runs when the message is taken, which never happens.
///
/// This layer is what answers that caller. It is not a worse `QueueTimeout`; it
/// is the liveness backstop for the case `QueueTimeout` structurally cannot
/// see, which is why a marker bound refusing it on actor builders was rejected.
/// RFC-04 §16.12.
#[derive(Debug, Clone, Copy)]
pub struct Timeout {
    budget: Duration,
}

impl Timeout {
    /// Refuse any call still running after `budget`.
    pub fn after(budget: Duration) -> Self {
        Self { budget }
    }
}

impl<Req: Send + 'static, Resp: Send + 'static> Middleware<Req, Resp> for Timeout {
    fn handle(
        &self,
        cx: Context,
        req: Req,
        body: BodyReader,
        next: Next<Req, Resp>,
    ) -> BoxFut<(Outcome<Resp>, Body)> {
        let budget = self.budget;
        Box::pin(async move {
            match n0_future::time::timeout(budget, next(cx, req, body)).await {
                Ok(reply) => reply,
                Err(_) => (
                    Outcome::Err(RemoteError::TimedOut {
                        millis: u64::try_from(budget.as_millis()).unwrap_or(u64::MAX),
                    }),
                    Body::empty(),
                ),
            }
        })
    }
}

/// Wrap `inner` in `layer`. Used by [`ServiceBuilder`](crate::ServiceBuilder).
pub(crate) fn wrap<Req: Send + 'static, Resp: Send + 'static>(
    layer: Arc<dyn Middleware<Req, Resp>>,
    inner: Next<Req, Resp>,
) -> Next<Req, Resp> {
    Arc::new(move |cx, req, body| layer.handle(cx, req, body, inner.clone()))
}

/// A wire request that carries `Req`, directly or in an actor address.
#[doc(hidden)]
pub trait Carries<Req>: Rpc {
    /// The typed request inside this wire request.
    fn carried(&self) -> &Req;
}
impl<R: Rpc> Carries<R> for R {
    fn carried(&self) -> &R {
        self
    }
}

#[cfg(test)]
mod tests {
    //! One question, because [`Queueing::expired`] answers one thing that no
    //! integration test in this repo can see: which side of `==` a spent budget
    //! falls on.

    #![allow(clippy::expect_used, clippy::unwrap_used)]

    use super::*;

    #[test]
    fn an_unrepresentable_budget_is_rejected() {
        assert!(matches!(
            QueueTimeout::try_after(Duration::MAX),
            Err(QueueTimeoutError),
        ));
    }

    /// A budget is spent **at** its deadline, not a tick later.
    ///
    /// Every other test in this repo is green either way, and none of them is
    /// wrong to be: they run on a native clock, where nanosecond resolution and
    /// a channel hop put `now == deadline` out of reach at any budget. A paused
    /// clock advances only to timer edges, so it lands exactly on the deadline
    /// for **every** budget — which is what makes the choice checkable here and
    /// nowhere else in CI.
    ///
    /// It is reachable in production, on the one target CI cannot run: a
    /// browser's `Performance.now()` is clamped to a tick, `deadline` is stamped
    /// from that same clamped clock, and `now` therefore hits it exactly for any
    /// whole-tick budget. `Queueing::expired` carries that argument; this test
    /// is the only place it is executed.
    ///
    /// `>=` and not `>`, because the other half of the same budget is a
    /// `n0_future::time::timeout` around the wait for a mailbox permit, and a
    /// timer fires *at* its deadline. A message that has waited its whole budget
    /// is already refused by that half.
    #[tokio::test(start_paused = true)]
    async fn a_budget_is_spent_at_its_deadline() {
        let budget = Duration::from_millis(100);
        let queueing = Queueing {
            deadline: Instant::now() + budget,
            millis: 100,
        };

        assert!(!queueing.expired(), "a budget just stamped is not spent");

        n0_future::time::sleep(budget).await;
        assert!(
            queueing.expired(),
            "a message that waited its whole budget must be refused, or the two halves of one \
             QueueTimeout disagree about the same message"
        );
    }
}
