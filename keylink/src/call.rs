//! The name a keyed call is known by inside its own handler.
//!
//! # The hole this fills, and why it was a hole and not a bug
//!
//! The ledger in `idempotency.rs` keeps a handler out of the way of a repeat for
//! as long as the process lives, and says plainly what ends that: the state is
//! durable and the ledger is memory, so a retry that arrives after a restart is
//! dispatched into a handler whose state already contains the work. The ledger
//! cannot close that from the inside. A durable ledger has to commit on the same
//! write as the state or the two disagree in the other direction, which is a
//! journal, and this crate is not one.
//!
//! What the *handler* can do about it, keylink never gave it the means to do.
//! `README.md`'s third limit already tells a caller in exactly this position to
//! **"read the result out of your own domain; do not re-run the work"** — and then
//! [`Context`](crate::Context) arrives carrying the peer, the service, the method,
//! the actor, the progress marker and the queueing budget, and not the key. There
//! was nothing to read the result *under*.
//!
//! [`CallId`] is that name and [`Applied`] is somewhere to keep it. Neither is held
//! by this crate: the set lives in the caller's own state, beside the balance it is
//! protecting, and is written by that state's own `Durable` snapshot — `Serialize`
//! and `Deserialize` are implemented so that a `postcard::to_allocvec(self)` snapshot
//! keeps compiling when the field is added, and so that a hand-written one needs a
//! line rather than a loop.
//!
//! That placement is the design rather than an implementation detail, and it buys
//! two things. The bound on how far back the dedupe reaches is argued in the one
//! domain that knows what a duplicate costs it. And the record shares a turn with
//! the work it is a record *of*, so the observer chain stores both or neither —
//! which no framework-held set could promise.
//!
//! # What this does not buy
//!
//! Three costs, and they are the price of not building the journal:
//!
//! - **A duplicate still enters the handler, and still pays a turn.** Refusing
//!   before dispatch is what a durable ledger would buy; this does not. On a
//!   [`Persist`](crate::Persist)-backed actor that turn is an `fsync` of a state
//!   the handler did not change — about 7.6 ms, measured and dated in that type's
//!   own module header.
//! - **[`key_state`](crate::RpcClient::key_state) answers
//!   [`Fresh`](crate::KeyState::Fresh) after a restart on a
//!   [`Records::Memory`](crate::Records) service**, because the ledger is what
//!   that query reads. So a peer learns nothing there about a key this state is
//!   about to deduplicate. Asking is still safe — it runs no handler — it is just
//!   uninformative.
//!
//!   This paragraph used to say *as the tree stands*, and predicted that "a
//!   durable record store would give the answer back without anything here
//!   changing". [`Records::Durable`](crate::Records) is that store, and nothing
//!   here changed: the query answers [`Done`](crate::KeyState::Done) after a
//!   restart. What did **not** come back is the cost above it — a duplicate
//!   entering the handler is a consequence of the record living in the caller's
//!   state, and no store on a disk alters that.
//! - **The dedupe is yours to write.** Three lines, in the handler you are already
//!   editing. keylink will not write them for you, because the only correct
//!   version knows what your answer is and whether replaying it is better than
//!   refusing.
//!
//! # And one it buys only halfway
//!
//! The guarantee rests on the **calling peer keeping its identity across the
//! restart**, because the peer is folded into the digest. A client that reconnects
//! on a fresh keypair hashes to a different [`CallId`], is not recognised, and
//! runs the work again — silently, and indistinguishably from a first call.
//! `README.md`'s fourth limit already names that shape of failure as the one a
//! caller cannot detect; this is a second way to reach it. A long-lived
//! [`SecretKey`](crate::SecretKey) on the calling side is not a deployment
//! preference here, it is the precondition.

use std::collections::hash_map::Entry;
use std::collections::{HashMap, VecDeque};
use std::num::NonZeroUsize;

use iroh::EndpointId;
use serde::{Deserialize, Serialize};

use crate::IdempotencyKey;

/// Bytes of digest a [`CallId`] keeps.
///
/// Sixteen, out of blake3's thirty-two — 128 bits, so about `2^-128` per pair of
/// distinct calls. The population that matters is the entries of one [`Applied`],
/// which is [`APPLIED_BOUND`] of them, so the chance that *any* pair inside one
/// collides is about `4096²/2^129`, or `2^-105`. The other sixteen bytes would buy
/// nothing a caller could reach.
///
/// # What folding the peer in does and does not do
///
/// It does **not** make cross-peer collisions impossible, and an earlier version of
/// this comment said it did. A digest is not a namespace: two calls from two peers
/// can land on the same sixteen bytes exactly as readily as two from one, and one
/// [`Applied`] holds the ids of every peer that called that state — so an accidental
/// cross-peer false hit is possible, at the `2^-105` above.
///
/// What it buys is the part that was ever a security property: a peer cannot
/// **choose** to collide with another's key. The ledger gets that structurally, from
/// per-peer buckets of plaintext keys, where a cross-peer hit is not representable.
/// Here it is cryptographic — peer A would need a second preimage against a
/// 128-bit truncation of blake3, which is `2^128` work, not the `2^64` a birthday
/// search would cost if it could pick both sides, because B's key is fixed and not
/// A's to move.
///
/// That is a genuine, if unreachable, weakening against the ledger's structural
/// version, and it is the price of not exporting the scoping rule to every caller
/// who wants to deduplicate. Naming it is cheaper than discovering it.
const DIGEST_BYTES: usize = 16;

/// Calls an [`Applied`] remembers before it starts forgetting the oldest.
///
/// Judgement, not measurement — and the thing it is judged against is the **window
/// a retry has to arrive in**, never a byte budget.
///
/// **The unit is calls, not seconds, and that is the honest way to state it.** An id
/// is displaced by the *distinct keyed calls this state accepts after it*, so 4096
/// is "4096 other pieces of keyed work may land between yours and its retry". Every
/// conversion of that into a duration needs a turn rate, and the turn rate is not
/// keylink's to promise.
///
/// # The seconds, and what they are conditional on
///
/// On a [`Persist`](crate::Persist) actor over `keylink_persist::file::Files`, a
/// turn costs about 7.6 ms — two `fsync`s and a `rename`, measured and dated in
/// `Persist`'s own module header — so roughly 130 turns per second, and 4096 entries
/// is about **31 seconds** of back-to-back, all-distinct, fully saturated keyed
/// traffic.
///
/// **That number belongs to that adapter.** `Persist`'s header invites *"an adapter
/// whose `store` does not sync"* and measures the same turn with no observer at
/// **12.8 µs** in release. An actor storing at anything like that rate does tens of
/// thousands of turns a second, and 4096 entries is then well under a second, not
/// 31. So: measure your own adapter's turn rate and divide. If you have no
/// measurement, the count is still the guarantee and the seconds are not.
///
/// And read the cost section below before choosing a bigger bound for that adapter,
/// because on a turn that cheap the set is no longer free.
///
/// The comparison this used to draw against [`MAX_ATTACH`](crate::MAX_ATTACH) is
/// gone, and it was wrong rather than merely loose: `MAX_ATTACH` bounds how long
/// keylink holds a stream waiting on a call that is **in flight on a live process**,
/// which is not the window a post-restart retry crosses. It was an unrelated
/// quantity that happened to be near 31.
///
/// # A restart is when the window is most likely to be spent, not least
///
/// While the process is down this state accepts nothing, so the downtime itself
/// consumes none of the bound. What follows it is the opposite of quiet: a restart
/// **concentrates** arrivals, because every peer's backoff fires at once, so the
/// busiest traffic this actor ever sees is exactly the traffic racing the retries
/// that need the bound.
///
/// **What actually drains the set is first-time keyed work, and nothing else.** A
/// recognised retry costs no place: [`insert`](Applied::insert) returns on
/// `Entry::Occupied` *before* it touches the eviction order, so a repeat neither
/// replaces an answer nor displaces a neighbour —
/// `a_recognised_retry_does_not_displace_anything` is the guard. A herd of pure
/// retries could therefore arrive at any size and drain nothing.
///
/// So the drain rate is the rate of **distinct keys this state has not seen before**,
/// which is bounded above by the adapter's turn rate and is usually well below it. A
/// reconnecting client supplies both kinds — it re-issues its pending call and then
/// carries on working — and it is only the second half that spends the bound. That
/// makes the post-restart burst the worst window for reasons of arrival *rate*, not
/// because retries eat their own records.
///
/// # Cost — and it is CPU per turn, not just memory
///
/// The memory is the easy half: 4096 ids is 64 KiB, plus one `T` each and a
/// `HashMap`'s overhead.
///
/// **The half that bites is that the whole set re-encodes on every turn.**
/// `Persist::after_turn` calls `Snapshot::of(state)` for every `Completed` turn
/// unconditionally, so a `Durable::snapshot` carrying an `Applied` pays for every
/// entry it holds whether that turn touched one or not. Linear in entries, measured
/// through `Till::snapshot` in
/// `tests/persist.rs::the_per_turn_cost_of_the_applied_set`:
///
/// | entries | snapshot bytes | µs/turn |
/// |---|---|---|
/// | 64 | 1,092 | 2.1 |
/// | 1,024 | 18,310 | 26.6 |
/// | **4,096** | **73,606** | **91.7** |
/// | 16,384 | 294,792 | 329.6 |
/// | 65,536 | 1,228,680 | 1,507.7 |
///
/// Release profile, Apple M5 Max (`Mac17,6`), macOS 27.0 build 26A5378n,
/// 2026-08-07 — the same machine and day as `Persist`'s own table, so the two are
/// comparable.
///
/// **Read that against your turn, not against a byte budget.** Beside a 7.6 ms
/// `fsync` turn, 91.7 µs is 1.2% and invisible. Beside the fast adapter this very
/// section invites it is **6.5× the entire turn**, and the set is then the dominant
/// cost of the thing it is riding on.
///
/// That ratio divides by **14.0 µs, not by the 12.8 µs tabled in
/// [`Persist`](crate::Persist)** — two figures for one quantity, so which one and
/// why. 14.0 µs is what `the_per_turn_store_cost` printed for its unobserved turn
/// **on the same `--ignored` run that produced the 91.7 µs above**, and a ratio
/// wants both halves from one run: same machine state, same thermal state, same
/// build. Against the tabled 12.8 µs the same 91.7 µs is 7.2×, and if you are
/// checking the arithmetic against `persist.rs`'s table that is the number you
/// will get. Both are right about different runs; neither is wrong.
///
/// The µs vary 6–11% run to run while the byte counts reproduce exactly, which is
/// the ordinary shape of a timing measurement and the reason the ratio is stated to
/// one figure rather than two.
///
/// The turn rate a caller is told to measure two paragraphs up is therefore
/// **partly a function of the bound they are choosing**, which is circular, and the
/// way out is to measure at the bound you intend to run.
///
/// So: raise it with [`Applied::bounded`] if your callers back off for longer than
/// your actor takes to fill it — and **lower it if your turns are cheap**, because
/// on a non-syncing adapter a bound of 65,536 puts 1.5 ms of encoding on a turn that
/// was doing 14 µs of work. The previous version of this paragraph said there was no
/// reason to lower it. There is.
pub const APPLIED_BOUND: NonZeroUsize = match NonZeroUsize::new(4096) {
    Some(bound) => bound,
    // Unreachable: 4096 is not zero. The arm exists because `NonZeroUsize::new`
    // is total and this crate denies `unwrap` in a `const` as everywhere else.
    None => NonZeroUsize::MIN,
};

/// One keyed call, named so that a handler can recognise its own retry.
///
/// `blake3(peer || key)`, truncated to sixteen bytes. Present on
/// [`Context::call`](crate::Context::call) for every call that arrived carrying an
/// [`IdempotencyKey`], and absent for every call that did not — including every
/// `tell`, which cannot carry one.
///
/// # Opaque on purpose
///
/// The obvious alternative is to hand the handler back what the accept path
/// already has: the [`EndpointId`] and the key. That exports
/// the ledger's peer-scoping rule to every caller who wants to dedupe, and
/// `idempotency.rs` calls that scoping *"the whole security
/// property"* — one peer's key must never be able to name another's. Folding the
/// peer into the digest means the rule is applied once, here, and a peer can only
/// ever collide with itself.
///
/// So the failure direction is fixed by construction. A collision is a **false
/// hit** — one caller's key B recognised as its own key A, and answered from A's
/// record. It is never a false miss: the digest is a total function of exactly the
/// pair, so the same peer and the same key always name the same call, and a retry
/// cannot fail to be recognised because the id came out different.
///
/// # `Serialize` yes, `Schema` never — and the difference is the whole rule
///
/// It is derived on the serving side from something the peer sent, and it never
/// goes back on keylink's wire. An earlier version of this paragraph refused serde
/// outright, saying it "would put a 16-byte digest into a hashed contract that
/// would then have to hold still". That conflated two traits and cost the caller a
/// hand-rolled encoder.
///
/// The contract keylink hashes is [`postcard_schema::Schema`], via
/// `derive_alpn<C: Schema>` — and a type with no `Schema` impl **cannot appear in a
/// `service!` signature at all**. So `Schema` is the one that would freeze the
/// digest, and it is deliberately absent. `Serialize`/`Deserialize` cannot reach an
/// ALPN, and they are what let a state whose `snapshot` is
/// `postcard::to_allocvec(self)` keep compiling when an [`Applied`] field is added
/// to it.
///
/// [`as_bytes`](Self::as_bytes) and [`from_bytes`](Self::from_bytes) remain, for a
/// hand-written snapshot that wants the sixteen bytes and nothing around them.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct CallId([u8; DIGEST_BYTES]);

impl CallId {
    /// Name the call this peer made under this key.
    ///
    /// The peer goes in first and is a fixed thirty-two bytes, which is what makes
    /// the concatenation unambiguous without a length prefix: no `(peer, key)`
    /// pair can produce the same byte string as another.
    ///
    /// # The service and the method are absent, and that is decided
    ///
    /// **A key names a call, not a method.** The digest is scoped exactly as the
    /// ledger's slots are, and that is the whole reason to leave both out.
    ///
    /// - **Method**, because the ledger does not scope by one either:
    ///   `Bucket::slots` is a `HashMap<IdempotencyKey, Entry>`, so a caller that
    ///   sends one key to two methods already gets one call out of the ledger — the
    ///   second is answered with the first's recorded reply. Folding the method in
    ///   here would make [`Applied`] *disagree* with the ledger, and the caller
    ///   would meet one rule before a restart and another after it.
    /// - **Service**, because an `Applied` lives inside one state, which is served
    ///   behind one ALPN. The dimension is carried by where the set sits rather than
    ///   by what is in the digest, and a `Ledger` is per built `Service` for the
    ///   same reason.
    ///
    /// The consequence is reachable and belongs to the caller: two methods sharing
    /// one `Applied` field will answer the second from the first's record, exactly
    /// as the live ledger already would. If two methods must not share a key, give
    /// them different keys — or different `Applied` fields.
    pub(crate) fn of(peer: EndpointId, key: &IdempotencyKey) -> Self {
        let mut id = [0u8; DIGEST_BYTES];
        id.copy_from_slice(&Self::digest(peer, key)[..DIGEST_BYTES]);
        Self(id)
    }

    /// The full digest, before it is cut down.
    ///
    /// Split out so the unit tests can cut it somewhere else and watch what a
    /// narrower id does — see `a_shortened_digest_fails_towards_a_false_hit`.
    fn digest(peer: EndpointId, key: &IdempotencyKey) -> [u8; 32] {
        let mut hasher = blake3::Hasher::new();
        hasher.update(peer.as_bytes());
        hasher.update(key.as_str().as_bytes());
        *hasher.finalize().as_bytes()
    }

    /// The id as bytes, for a state that has to write down what it applied.
    #[must_use]
    pub fn as_bytes(&self) -> &[u8; DIGEST_BYTES] {
        &self.0
    }

    /// The same bytes, read back out of a snapshot.
    ///
    /// Total, and it has to be: every sixteen bytes is a possible digest, so there
    /// is nothing here to refuse. A snapshot holding bytes that were never a
    /// [`CallId`] describes a call that will never arrive again, which costs one
    /// entry and no correctness.
    #[must_use]
    pub fn from_bytes(bytes: [u8; DIGEST_BYTES]) -> Self {
        Self(bytes)
    }
}

/// Hex, because the one place a `CallId` is read by a human is a log line beside
/// the key it was derived from.
impl std::fmt::Display for CallId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for byte in self.0 {
            write!(f, "{byte:02x}")?;
        }
        Ok(())
    }
}

/// Delegates to [`Display`](std::fmt::Display) rather than deriving, which would
/// print sixteen decimal numbers.
impl std::fmt::Debug for CallId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "CallId({self})")
    }
}

/// The calls a state has already applied, and what each of them answered.
///
/// Put one in your own state, next to what it is protecting, and the dedupe is
/// three lines:
///
/// ```ignore
/// async fn credit(&mut self, cx: Caller<'_>, amount: u64) -> u64 {
///     let call = cx.peer().and_then(Context::call);
///     // Or, for a method that must NOT run without a key — see the paragraph on
///     // what an unkeyed call gets, because the default here is silent:
///     //
///     //     let Some(call) = cx.peer().and_then(Context::call) else {
///     //         return self.balance;   // or refuse; this method needs a key
///     //     };
///     //     let call = Some(call);
///     if let Some(&balance) = self.applied.get(call) {
///         return balance;
///     }
///     self.balance += amount;
///     self.applied.insert(call, self.balance);
///     self.balance
/// }
/// ```
///
/// `T` is your answer, so a recognised retry is *answered* rather than refused —
/// effectively-once, across the restart the ledger cannot cross. `Applied<()>` is
/// the other shape: it holds no answer to give back, which is at-most-once and is
/// all a `#[defer]` method can have. See [`seen`](Self::seen).
///
/// # Insert in the turn that APPLIES, not the turn that hears about it
///
/// An entry means "this state applied that call", so it belongs in the same turn as
/// the mutation. Put the two together and the observer chain stores the work and
/// the record on one snapshot and one `fsync`, or stores neither — which is the
/// reason this type is embedded rather than framework-held, and the one thing about
/// it that is load-bearing.
///
/// Splitting them is a real defect and not a style point. A `#[defer]` method that
/// inserts when it **parks** has recorded that the request arrived, not that it was
/// applied: a restart before the releasing turn then leaves a record with no work
/// behind it, and the retry is answered out of a state nothing was ever applied to
/// — a lost operation reported as a success. The parking turn is where the
/// [`CallId`] is *available*; park it beside the [`Deferred`](crate::Deferred) and
/// let the turn that applies do the recording.
///
/// # It is yours, and that is the point
///
/// keylink does not hold this, look at it, or bound it on your behalf. That is
/// what makes [`APPLIED_BOUND`] arguable at all: a bound inside the framework
/// would have to be right for every state at once, and the number that matters is
/// how long *your* callers back off for.
///
/// # Snapshotting it
///
/// `Serialize` and `Deserialize` are implemented by hand, so a state whose `snapshot` is
/// `postcard::to_allocvec(self)` keeps compiling when this field is added, and one
/// whose snapshot is hand-written needs one line rather than a loop. The encoding
/// carries the **bound** as well as the entries, so a round trip restores the
/// same bound and the same next victim rather than silently reverting to
/// [`APPLIED_BOUND`].
///
/// That matters more than it sounds: a hand-written snapshot that forgets this
/// field compiles, passes every test, and starts empty after every restart — which
/// is precisely the silent at-least-once this module exists to prevent. There is no
/// encoder to get wrong now, which is the only defence available against a mistake
/// nothing can observe.
///
/// [`iter`](Self::iter) is still there for a snapshot that wants the pairs itself;
/// it reports oldest-first, so replaying it through [`insert`](Self::insert) into an
/// `Applied` **of the same bound** rebuilds the same eviction order.
///
/// # What an unkeyed call gets
///
/// Nothing, deliberately. [`get`](Self::get) and [`insert`](Self::insert) take the
/// `Option<CallId>` that [`Context::call`](crate::Context::call) hands out, so the
/// caller writes no `match`; a `None` reads as absent and stores as a no-op. A
/// caller that sent no key asked for no de-duplication and gets none — the same
/// answer `claim_key` gives it one layer up, which is why the shape is this one and
/// not a `Result`.
///
/// **The default is silent, and for money it is the wrong default.** A peer that
/// forgot `with_idempotency` — or reached the method through a `tell`, which cannot
/// carry a key — runs with no de-duplication at all and nothing anywhere says so.
/// If a method must not run unkeyed, say it in the handler, which is why the example
/// above carries the `let ... else` form as well as the three-line one. keylink
/// cannot say it for you: `Applied` is in your state and the decision is about your
/// domain, not about the transport.
#[derive(Debug, Clone)]
pub struct Applied<T> {
    answers: HashMap<CallId, T>,
    /// The ids in `answers`, oldest first. The eviction order, and the order
    /// [`Applied::iter`] reports so that a snapshot round-trips.
    order: VecDeque<CallId>,
    bound: NonZeroUsize,
}

impl<T> Applied<T> {
    /// Remember up to [`APPLIED_BOUND`] calls.
    #[must_use]
    pub fn new() -> Self {
        Self::bounded(APPLIED_BOUND)
    }

    /// Remember up to `bound` calls.
    ///
    /// **`bounded` and not `with_capacity`.** std's `with_capacity` reserves room
    /// and changes no behaviour; this reserves nothing, allocates nothing, and is
    /// the point at which entries start being forgotten. Borrowing the std spelling
    /// for the opposite meaning is how a caller ends up believing a bigger number is
    /// only a performance knob.
    ///
    /// [`NonZeroUsize`] rather than `usize`, because a bound of zero is an `Applied`
    /// that accepts every insert and recognises nothing — at-least-once wearing the
    /// type that says otherwise, which is exactly the silent degradation this whole
    /// module exists to give a caller a way out of.
    #[must_use]
    pub fn bounded(bound: NonZeroUsize) -> Self {
        Self {
            answers: HashMap::new(),
            order: VecDeque::new(),
            bound,
        }
    }

    /// What this call answered last time, if this state has applied it.
    ///
    /// `None` for a call that has not been applied, for one whose id has been
    /// displaced, and for a call that carried no key.
    #[must_use]
    pub fn get(&self, call: Option<CallId>) -> Option<&T> {
        self.answers.get(&call?)
    }

    /// Whether this call has been applied, without asking what it answered.
    ///
    /// **This is `get(call).is_some()`, and it is kept for reading rather than for
    /// capability.** Said plainly because the duplication is the kind a reviewer
    /// should challenge, and the answer is not that it does something more. It is
    /// that `Applied<()>`'s `get` returns `Option<&()>` — a value with no
    /// information in it — so `self.applied.get(call).is_some()` asks the right
    /// question through a type that cannot express it, and
    /// `self.applied.seen(call)` asks it directly.
    ///
    /// The narrower surface is a real alternative and the cost of taking it is one
    /// awkward line at every `Applied<()>` call site. That trade is the whole
    /// argument; there is nothing else on this side of it.
    #[must_use]
    pub fn seen(&self, call: Option<CallId>) -> bool {
        call.is_some_and(|call| self.answers.contains_key(&call))
    }

    /// Record that this call was applied, and what it answered.
    ///
    /// The **first** answer for an id is the one kept: a second insert under the
    /// same id is ignored, because the first is the one a caller may already be
    /// holding, and handing a retry a different answer than the original call got
    /// is worse than handing it the original one.
    ///
    /// A `None` call stores nothing. See the type's own paragraph on why that is
    /// the answer and not a panic.
    pub fn insert(&mut self, call: Option<CallId>, answer: T) {
        let Some(call) = call else { return };
        match self.answers.entry(call) {
            Entry::Occupied(_) => return,
            Entry::Vacant(slot) => {
                slot.insert(answer);
            }
        }
        self.order.push_back(call);
        while self.order.len() > self.bound.get() {
            if let Some(oldest) = self.order.pop_front() {
                self.answers.remove(&oldest);
            }
        }
    }

    /// How many calls are remembered.
    #[must_use]
    pub fn len(&self) -> usize {
        self.answers.len()
    }

    /// Whether nothing has been applied yet.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.answers.is_empty()
    }

    /// The bound this one was built with.
    ///
    /// It exists so that the round trip's promise is **assertable** rather than
    /// decorative: this type's `Serialize` claims to carry the bound, and
    /// `a_snapshot_of_what_was_applied_round_trips_with_its_bound` is what holds it
    /// to that. Without an accessor the claim could only be checked behaviourally,
    /// by observing an eviction, which is the weaker of the two readings.
    ///
    /// A caller who serialises an `Applied` and wants to know what bound came back
    /// has the same need, which is why this is `pub` rather than `pub(crate)`.
    #[must_use]
    pub fn bound(&self) -> NonZeroUsize {
        self.bound
    }

    /// Every remembered call and its answer, **oldest first**.
    ///
    /// The order is the contract, not an accident of the container: it is the
    /// eviction order, so a snapshot written in it and replayed through
    /// [`insert`](Self::insert) — into an `Applied` of the **same bound** —
    /// restores the same set *and* the same next victim. `Serialize` carries the
    /// bound for you, which is why it is the one to reach for first.
    pub fn iter(&self) -> impl Iterator<Item = (CallId, &T)> {
        self.order
            .iter()
            .filter_map(|call| self.answers.get(call).map(|answer| (*call, answer)))
    }
}

impl<T> Default for Applied<T> {
    fn default() -> Self {
        Self::new()
    }
}

/// The bound **and** the entries, oldest first, as a pair.
///
/// Hand-written rather than derived, for two reasons the derive cannot give. The
/// `HashMap`'s iteration order is not stable, so a derived impl would write
/// different bytes for one unchanged state and lose the eviction order with them;
/// and the bound is not recoverable from the entries, so a decode that did not
/// carry it would silently substitute [`APPLIED_BOUND`] for whatever the state was
/// built with.
impl<T: Serialize> Serialize for Applied<T> {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        (self.bound, self.iter().collect::<Vec<_>>()).serialize(serializer)
    }
}

/// Replayed through [`Applied::insert`] in the order it was written, so the decoded
/// value is one an ordinary sequence of inserts could have produced. Nothing here
/// can construct a state the public API cannot.
impl<'de, T: Deserialize<'de>> Deserialize<'de> for Applied<T> {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let (bound, entries): (NonZeroUsize, Vec<(CallId, T)>) =
            Deserialize::deserialize(deserializer)?;
        let mut applied = Self::bounded(bound);
        for (call, answer) in entries {
            applied.insert(Some(call), answer);
        }
        Ok(applied)
    }
}

/// # Replaying the break records below
///
/// Every one of them quotes a line **without its leading indentation**, because a
/// doc comment reads better that way. Pass the line to `scripts/break-once.sh`
/// **with** its indentation: this file then contains the bare text twice — once as
/// code and once in the record — and the harness aborts on a match that is not
/// unique, which is the correct behaviour and the reason this paragraph exists
/// rather than a softer literal.
///
/// # Which records quote their anchor, and which describe it
///
/// Quoting an anchor is the more precise record and it has one failure mode:
/// **`rustfmt` owns where a line breaks, and a record does not.** An anchor inside
/// a formatter-wrapped chain stops existing when a neighbour grows, and the run
/// then aborts rather than going red — which reads as green to anybody checking
/// counts instead of anchors.
///
/// So the split here is measured, not by taste. Every anchor in this module was
/// re-checked under two plausible edits — a nearby signature gaining an argument,
/// and the `answers` field being renamed — with `cargo fmt` run after each:
///
/// - **A quoted anchor is a single short statement or arm on one line.** Those are
///   the ones the formatter has no reason to reflow, and none of them moved.
/// - **[`Applied::iter`]'s chain is the one that rotted**, from the field rename
///   alone — a change that does not touch that function's text at all. Its record
///   describes the edit instead, and says so at the site.
///
/// A record that quotes an anchor spanning more than one line is therefore already
/// wrong, whatever the formatter has done to it so far: the break it depends on was
/// never the author's to promise. That is the test to apply to a new one, and it
/// needs no inventory here — an inventory would be one more value this file does
/// not control.
///
/// The replacement half is never at risk and is always quoted verbatim:
/// `break-once.sh` greps and counts the search literal only, so what a record says
/// to substitute cannot affect uniqueness in any file.
#[cfg(test)]
mod tests {
    #![allow(clippy::expect_used)]

    use std::str::FromStr;

    use super::*;

    /// Two peers, spelled out rather than generated, so a failure names the same
    /// bytes every time. Borrowed from `idempotency.rs`'s own fixtures.
    const A: &str = "a09aa5f47a6759802ff955f8dc2d2a14a5c99d23be97f864127ff9383455a4f0";
    const B: &str = "d04ab232742bb4ab3a1368bd4615e4e6d0224ab71a016baf8520a332c9778737";

    fn peer(hex: &str) -> EndpointId {
        EndpointId::from_str(hex).expect("a valid endpoint id")
    }

    fn key(text: &str) -> IdempotencyKey {
        IdempotencyKey::new(text).expect("a valid key")
    }

    /// A `CallId` cut to `width` bytes instead of [`DIGEST_BYTES`], so a
    /// collision is exhibitable at a width where one is cheap to find.
    ///
    /// Test-only. The production path has exactly one width and no argument for
    /// it.
    fn narrow(peer: EndpointId, key: &IdempotencyKey, width: usize) -> CallId {
        let mut id = [0u8; DIGEST_BYTES];
        id[..width].copy_from_slice(&CallId::digest(peer, key)[..width]);
        CallId::from_bytes(id)
    }

    /// **The security property, restated as an identity.** One peer's key must
    /// never name another peer's call, because the ledger's whole peer-scoping
    /// rule is what `CallId` folds in so that no caller has to re-implement it.
    ///
    /// Made red by taking the peer out of the digest — replace
    /// `hasher.update(peer.as_bytes());` with `let _ = peer;`. Watched:
    /// `the same key from two peers must be two calls`, with `left` and `right`
    /// the same `CallId`.
    #[test]
    fn one_peers_key_cannot_name_another_peers_call() {
        let shared = key("reserve-4471");
        assert_ne!(
            CallId::of(peer(A), &shared),
            CallId::of(peer(B), &shared),
            "the same key from two peers must be two calls",
        );
    }

    /// The other half of the same pair: within one peer, two keys are two calls.
    ///
    /// Without this, dropping the *key* from the digest passes the test above —
    /// which is the shape the verification lens warned about, where a stub that
    /// always answers one way satisfies half a suite.
    ///
    /// Made red by replacing `hasher.update(key.as_str().as_bytes());` with
    /// `let _ = key;`. Watched: `two keys from one peer must be two calls`, with
    /// `left` and `right` the same `CallId`.
    #[test]
    fn two_keys_from_one_peer_are_two_calls() {
        assert_ne!(
            CallId::of(peer(A), &key("reserve-4471")),
            CallId::of(peer(A), &key("reserve-4472")),
            "two keys from one peer must be two calls",
        );
    }

    /// **The failure direction, and the wrong answer it hands back.**
    ///
    /// A collision at sixteen bytes is not exhibitable and this test does not
    /// pretend to exhibit one. It forces one at a width where the pigeonhole
    /// guarantees a pair inside 257 keys, and then asserts the **consequence**:
    /// `Applied` answers one key's call with **another key's answer**. That is a
    /// wrong answer returned as if it were right, which is a worse failure than the
    /// refusal a short digest is sometimes assumed to produce, and it is the reason
    /// the width is sixteen bytes rather than eight.
    ///
    /// # Which assertion is the guard, and which is the demonstration
    ///
    /// Written down because the first version of this test was two-thirds
    /// tautology — it re-asserted the predicate `find` had just matched on, and
    /// then compared one call to itself.
    ///
    /// - The **guard** is the width assertion. Made red by narrowing
    ///   [`DIGEST_BYTES`] to one: search
    ///   `const DIGEST_BYTES: usize = 16;`
    ///   — **with its trailing newline, which is what makes it unique** — and replace
    ///   with `const DIGEST_BYTES: usize = 1;`. This record lives in the file it
    ///   breaks and the declaration sits at **column 0**, so there is no indentation
    ///   for this quote to lack and the bare form counts 2. What disambiguates
    ///   instead is that the record's copy is closed by a **backtick** where the
    ///   declaration's is closed by a **newline**. The pair that collides at one byte
    ///   then collides at the real width too. Watched:
    ///   `at DIGEST_BYTES the same pair must be two calls / left: CallId(4a) right: CallId(4a)`.
    /// - The **demonstration** is the `Applied` half. It passes broken and
    ///   unbroken, because it is asserted at the narrowed width on purpose, and it
    ///   is here to state the consequence in executable form rather than in prose.
    ///   It is not decoration: it goes red if `Applied` ever stops keying on the
    ///   `CallId` alone, which is the only way this consequence could change.
    ///
    /// A false **miss** has no assertion and cannot have one: the digest is a total
    /// function of exactly `(peer, key)`, so there is no input that makes one id
    /// into two. What could still produce one is `CallId`'s `Hash` and `Eq`
    /// disagreeing, and that is guarded by
    /// [`a_keyed_call_is_answered_from_what_it_applied`].
    #[test]
    fn a_shortened_digest_fails_towards_a_false_hit() {
        let keys: Vec<IdempotencyKey> = (0..300).map(|n| key(&format!("k-{n}"))).collect();
        let (left, right) = keys
            .iter()
            .enumerate()
            .flat_map(|(i, left)| keys[i + 1..].iter().map(move |right| (left, right)))
            .find(|(left, right)| narrow(peer(A), left, 1) == narrow(peer(A), right, 1))
            .expect("257 keys cannot all differ in one byte");

        // The consequence: a state that applied `left` answers `right` with
        // `left`'s answer. Nobody is refused and nobody is told.
        let mut applied: Applied<u64> = Applied::new();
        applied.insert(Some(narrow(peer(A), left, 1)), 50);
        assert_eq!(
            applied.get(Some(narrow(peer(A), right, 1))),
            Some(&50),
            "a digest too short answers one key's call with ANOTHER key's answer, \
             and reports it as that call's own",
        );

        // The guard: at the real width they are two calls, so the consequence
        // above is a property of the narrowing and not of the design.
        assert_ne!(
            CallId::of(peer(A), left),
            CallId::of(peer(A), right),
            "at DIGEST_BYTES the same pair must be two calls",
        );
    }

    /// A call that carried no key is not de-duplicated, and cannot be made to be
    /// by accident.
    ///
    /// Made red by replacing `let Some(call) = call else { return };` with
    /// `let call = call.unwrap_or_else(|| CallId::from_bytes([0; DIGEST_BYTES]));`
    /// — every unkeyed call then shares one id. Watched: `and nothing was stored`.
    ///
    /// That break reaches the third assertion and not the first two, and the
    /// reason is worth writing down rather than papering over: [`Applied::get`]
    /// and [`Applied::seen`] short-circuit on the same `Option`, so with `insert`
    /// alone broken there is still nothing for them to find. Both arms have to be
    /// broken together to make `get(None)` answer, which is a two-literal edit and
    /// therefore not this harness's shape. The first two assertions are here as
    /// the statement of the rule; the third is the one with a break behind it.
    #[test]
    fn an_unkeyed_call_is_never_deduplicated() {
        let mut applied: Applied<u64> = Applied::new();
        applied.insert(None, 1);
        applied.insert(None, 2);
        assert!(applied.get(None).is_none(), "there is nothing to find");
        assert!(!applied.seen(None), "and nothing to have seen");
        assert!(applied.is_empty(), "and nothing was stored");
    }

    /// A keyed call is, which is the positive this pairs with.
    ///
    /// Made red by `self.answers.get(&call?)` → `let _ = call?; None`, when a state
    /// recognises nothing and every retry re-applies. Watched:
    /// `left: None right: Some(50)`.
    #[test]
    fn a_keyed_call_is_answered_from_what_it_applied() {
        let mut applied: Applied<u64> = Applied::new();
        let call = Some(CallId::of(peer(A), &key("debit-1")));
        assert!(applied.get(call).is_none(), "nothing applied yet");
        applied.insert(call, 50);
        assert_eq!(applied.get(call), Some(&50));
        assert!(applied.seen(call));
        assert_eq!(applied.len(), 1);
    }

    /// The bound evicts the **oldest**, so the newest calls — the ones whose
    /// retries are still plausible — are the ones kept.
    ///
    /// Made red by making [`Applied::insert`]'s eviction take from the **newest** end
    /// of `order` rather than the oldest. Search the whole line —
    /// `if let Some(oldest) = self.order.pop_front() {`
    /// — and replace `pop_front()` in it with `pop_back()`. The fragment
    /// `pop_front()` alone counts 2 and no amount of indentation makes it unique,
    /// which is why the anchor is the LINE: at its own indentation the line counts 1,
    /// and this quote sits unindented so it is not a second occurrence. The set then
    /// forgets what just happened and remembers what nobody will ask about again.
    /// Watched: `the first call is the one displaced`.
    #[test]
    fn the_oldest_call_is_the_one_forgotten() {
        let two = NonZeroUsize::new(2).expect("2 is not zero");
        let mut applied: Applied<u64> = Applied::bounded(two);
        let calls: Vec<Option<CallId>> = (0..3)
            .map(|n| Some(CallId::of(peer(A), &key(&format!("debit-{n}")))))
            .collect();
        for (n, call) in calls.iter().enumerate() {
            applied.insert(*call, n as u64);
        }

        assert_eq!(applied.len(), 2, "the bound holds");
        assert!(
            applied.get(calls[0]).is_none(),
            "the first call is the one displaced",
        );
        assert_eq!(applied.get(calls[1]), Some(&1));
        assert_eq!(applied.get(calls[2]), Some(&2));
    }

    /// The first answer wins, because it is the one a caller may already hold.
    ///
    /// Made red by replacing `Entry::Occupied(_) => return,` with
    /// `Entry::Occupied(mut slot) => { slot.insert(answer); return }` — the retry
    /// then overwrites the answer the original call was given. Watched:
    /// `left: Some(999) right: Some(50)`.
    #[test]
    fn the_first_answer_for_a_call_is_the_one_kept() {
        let mut applied: Applied<u64> = Applied::new();
        let call = Some(CallId::of(peer(A), &key("debit-1")));
        applied.insert(call, 50);
        applied.insert(call, 999);
        assert_eq!(applied.get(call), Some(&50));
        assert_eq!(applied.len(), 1, "and it is one entry, not two");
    }

    /// **A recognised retry displaces nothing**, which is the mechanism the bound's
    /// own docs rest on when they say a post-restart herd of retries cannot drain
    /// the set.
    ///
    /// The eviction order has to not grow on a repeat, and that is a *separate*
    /// fact from the answer being kept: `Entry::Occupied` returns before
    /// `order.push_back`, and nothing about `len()` would notice if it did not,
    /// because the extra entry lands in `order` and not in `answers`. Which is why
    /// this is its own test at a bound of two rather than another assertion on
    /// [`the_first_answer_for_a_call_is_the_one_kept`].
    ///
    /// Made red by moving the push above the match — `Entry::Occupied(_) => return,`
    /// → `Entry::Occupied(_) => { self.order.push_back(call); return }`. The repeat
    /// then costs a place, `B` pushes the length past the bound, and `A` is evicted
    /// by its own retry. Watched:
    /// `a retry must not cost the set a place ... left: None right: Some(10)`.
    #[test]
    fn a_recognised_retry_does_not_displace_anything() {
        let two = NonZeroUsize::new(2).expect("2 is not zero");
        let mut applied: Applied<u64> = Applied::bounded(two);
        let first = Some(CallId::of(peer(A), &key("credit-1")));
        let second = Some(CallId::of(peer(A), &key("credit-2")));

        applied.insert(first, 10);
        applied.insert(first, 10); // the retry
        applied.insert(second, 20);

        assert_eq!(
            applied.get(first),
            Some(&10),
            "a retry must not cost the set a place, or the retry of A is what \
             evicts A",
        );
        assert_eq!(applied.get(second), Some(&20));
        assert_eq!(applied.len(), 2);
    }

    /// **The property that makes this durable at all**: what comes back has the
    /// same entries, the same eviction order **and the same bound**.
    ///
    /// Through `postcard`, which is what a state whose `snapshot` is
    /// `postcard::to_allocvec(self)` gets for free and what a hand-written one
    /// should reach for first.
    ///
    /// The bound half is the one worth stating. **Nothing on the decode side names
    /// a bound**: the value comes back through `from_bytes` alone, so if the
    /// encoding did not carry it the decode would fall to [`APPLIED_BOUND`] — 4096
    /// where this state was built with 2 — and every assertion about entries would
    /// still pass while the bound had silently widened by three orders of magnitude.
    ///
    /// Two assertions catch that, deliberately: `bound()` reads it directly, and the
    /// eviction at the end reads it *behaviourally* — a restored set with the wrong
    /// bound does not evict on the third insert. The direct one fires first, so the
    /// behavioural one is not watched here; reaching it needs the bound assertion
    /// removed as well, which is a two-literal edit and not this harness's shape.
    ///
    /// Made red two ways:
    ///
    /// - **reversing [`Applied::iter`]** — so the restored set forgets the wrong
    ///   entry. Search, with its indentation:
    ///   `.filter_map(|call| self.answers.get(call).map(|answer| (*call, answer)))`
    ///   and replace it with `.rev()`, `.collect::<Vec<_>>()`, `.into_iter()` and
    ///   that same line, in that order — the `collect` being what keeps the closure's
    ///   lifetime the same shape.
    ///
    ///   **A one-line search with a multi-line replacement**, which is the shape this
    ///   record was missing. `break-once.sh` searches ONE literal and substitutes
    ///   anything, so the chain being wrapped across three lines bounds the ANCHOR
    ///   and not the edit: measured at `7a147c0`, `.iter()` alone counts 2 and is
    ///   unusable, while this `.filter_map` line counts **1**. The earlier reason —
    ///   that the site spans three lines so no anchor exists — was true of the site
    ///   and false of the anchor.
    ///
    ///   Renaming the `answers` field rewraps this chain, which is how the previous
    ///   anchor here was lost. It also rewrites this line, so the anchor goes to
    ///   count 0 and `mise run anchors` says so — loudly, which is the point of
    ///   quoting it rather than describing it.
    ///
    ///   Watched: `a restored set must evict in the order it was written`.
    /// - dropping the bound from the encoding: `(self.bound, self.iter()` →
    ///   `(APPLIED_BOUND, self.iter()`, which is what a derived impl would have
    ///   done and which the `bound()` assertion catches.
    ///
    ///   Watched: `a round trip that lost the bound would silently widen it to
    ///   APPLIED_BOUND and no assertion about entries would notice`.
    #[test]
    fn a_snapshot_of_what_was_applied_round_trips_with_its_bound() {
        let two = NonZeroUsize::new(2).expect("2 is not zero");
        let mut applied: Applied<u64> = Applied::bounded(two);
        let calls: Vec<Option<CallId>> = (0..2)
            .map(|n| Some(CallId::of(peer(A), &key(&format!("debit-{n}")))))
            .collect();
        applied.insert(calls[0], 10);
        applied.insert(calls[1], 20);

        let written = postcard::to_stdvec(&applied).expect("encodes");
        let mut restored: Applied<u64> = postcard::from_bytes(&written).expect("decodes");

        assert_eq!(restored.get(calls[0]), Some(&10));
        assert_eq!(restored.get(calls[1]), Some(&20));
        assert_eq!(
            restored.bound(),
            two,
            "a round trip that lost the bound would silently widen it to \
             APPLIED_BOUND and no assertion about entries would notice",
        );

        // And the next insert displaces the same entry the original would have.
        let third = Some(CallId::of(peer(A), &key("debit-2")));
        restored.insert(third, 30);
        assert!(
            restored.get(calls[0]).is_none(),
            "a restored set must evict in the order it was written",
        );
        assert_eq!(restored.get(calls[1]), Some(&20));
    }

    /// The bytes are stable across encodings of one unchanged value.
    ///
    /// A derived impl over the inner `HashMap` would not be: its iteration order is
    /// unspecified, so one state would encode to different bytes run to run, and the
    /// eviction order would be whatever the map felt like that time. `Persist`
    /// rewrites the snapshot every turn either way, so nothing breaks loudly — the
    /// eviction order just stops being the one [`Applied::iter`] promises.
    ///
    /// Made red by `(self.bound, self.iter().collect::<Vec<_>>())` →
    /// `(self.bound, self.answers.iter().map(|(c, a)| (*c, a)).collect::<Vec<_>>())`,
    /// which is the derive's behaviour written out — the `map` only because
    /// `HashMap::iter` hands back `&CallId`. Watched:
    /// `and a decoded set must re-encode to the same bytes`.
    ///
    /// **The registered anchor is the whole line, and both widths are quoted here
    /// so that the row can name either one.** The whole-line pair is
    /// `(self.bound, self.iter().collect::<Vec<_>>()).serialize(serializer)` →
    /// `(self.bound,
    /// self.answers.iter().map(|(c, a)| (*c, a)).collect::<Vec<_>>()).serialize(serializer)`.
    /// The fragment above resolves today and has no remedy the day it stops,
    /// because indentation cannot disambiguate a mid-line span. Quoting both
    /// widths keeps this record valid whichever width the row currently carries.
    ///
    /// **Do not rewrap the search quote except at a space it already contains.**
    /// `mise run anchors` flattens a quoted span by joining its `///` lines with
    /// one space, then compares it whole against the registered literal. A wrap
    /// before `.serialize` inserts a space the line does not have, and the row
    /// then reports `literal is NOT QUOTED by its own record`. That quote is 77
    /// columns at this indent, and nothing here reflows it — `rustfmt.toml` pins
    /// `edition` alone, so `wrap_comments` is off. The replacement quote is never
    /// searched, so its wrap is free; it is wrapped at a space the line really
    /// has anyway, so promoting it to an anchor later needs no relayout.
    ///
    /// **It is the second assertion that goes red, not the first**, and the reason is
    /// worth knowing before reading a failure here: two encodings of one *live* value
    /// walk the same map and agree even under the break. What exposes the hash order
    /// is a **decode**, which rebuilds the map and lands the entries somewhere else.
    /// So the first assertion is the weaker of the pair and the round trip is what
    /// carries the guard.
    #[test]
    fn one_unchanged_set_encodes_to_one_sequence_of_bytes() {
        let mut applied: Applied<u64> = Applied::new();
        for n in 0..32 {
            applied.insert(Some(CallId::of(peer(A), &key(&format!("debit-{n}")))), n);
        }
        let once = postcard::to_stdvec(&applied).expect("encodes");
        let twice = postcard::to_stdvec(&applied).expect("encodes");
        assert_eq!(once, twice, "the encoding must not depend on a hash order");

        let restored: Applied<u64> = postcard::from_bytes(&once).expect("decodes");
        assert_eq!(
            postcard::to_stdvec(&restored).expect("encodes"),
            once,
            "and a decoded set must re-encode to the same bytes",
        );
    }
}
