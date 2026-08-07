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
//! [`CallId`] is that name and [`Applied`] is somewhere to keep it. Neither is
//! held by this crate: the set lives in the caller's own state, beside the balance
//! it is protecting, and travels with it into whatever `Durable` snapshot that
//! state already writes. That placement is the design rather than an
//! implementation detail — the bound on how far back the dedupe reaches is then
//! argued in the one domain that knows what a duplicate costs it.
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
//! - **[`key_state`](crate::RpcClient::key_state) still answers
//!   [`Fresh`](crate::KeyState::Fresh) after a restart.** The ledger is what that
//!   query reads and the ledger genuinely knows nothing, so a peer learns nothing
//!   about a key the domain is about to dedupe. Asking is still safe — it runs no
//!   handler — it is just no longer informative across a restart.
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

use crate::IdempotencyKey;

/// Bytes of digest a [`CallId`] keeps.
///
/// Sixteen, out of blake3's thirty-two. Two peers cannot collide with each other
/// at all — the peer is hashed in — so this is a birthday bound over **one
/// caller's own keys**, at 2^64 of them, against a ledger that will not hold more
/// than [`MAX_RECORDS`](crate::MAX_RECORDS) at a time. The other sixteen bytes
/// would buy nothing a caller could reach.
const DIGEST_BYTES: usize = 16;

/// Calls an [`Applied`] remembers before it starts forgetting the oldest.
///
/// Judgement, not measurement — and the thing it is judged against is the
/// **window a retry has to arrive in**, never a byte budget.
///
/// An id is displaced by the *distinct keyed calls this state accepts after it*,
/// so the question is how much other traffic can land between the work and its
/// retry. The ceiling on that rate is the durable actor's own: 7.6 ms per turn,
/// about 130 turns per second, measured and dated in [`Persist`](crate::Persist)'s
/// own module header. At that
/// ceiling, 4096 entries is **about 31 seconds of back-to-back, all-distinct,
/// fully saturated keyed traffic** — longer than
/// [`MAX_ATTACH`](crate::MAX_ATTACH), which is the longest keylink itself will
/// hold a stream open for a call it is waiting on. A retry later than that is one
/// the caller's own backoff produced.
///
/// The restart is *not* part of that window, which is what makes the margin wider
/// than it looks: while the process is down this state accepts nothing, so nothing
/// displaces anything. The 31 seconds is traffic arriving after the new process is
/// serving and before the retry reaches it.
///
/// What it costs is a consequence and not the argument: 4096 ids is 64 KiB, plus
/// one `T` each and a `HashMap`'s overhead. Raise it with
/// [`Applied::with_capacity`] if your callers back off for longer than your actor
/// takes to fill it; there is no reason to lower it.
pub const APPLIED_CAPACITY: NonZeroUsize = match NonZeroUsize::new(4096) {
    Some(capacity) => capacity,
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
/// already has: the [`EndpointId`](crate::EndpointId) and the key. That exports
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
/// # Not a wire type
///
/// It is derived on the serving side from something the peer sent, and it never
/// goes back. There are no `Serialize`, `Deserialize` or `Schema` impls, and
/// adding them would put a 16-byte digest into a hashed contract that would then
/// have to hold still. [`as_bytes`](Self::as_bytes) and
/// [`from_bytes`](Self::from_bytes) are what a state's own snapshot uses, which is
/// the one place these bytes are meant to be written down.
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct CallId([u8; DIGEST_BYTES]);

impl CallId {
    /// Name the call this peer made under this key.
    ///
    /// The peer goes in first and is a fixed thirty-two bytes, which is what makes
    /// the concatenation unambiguous without a length prefix: no `(peer, key)`
    /// pair can produce the same byte string as another.
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
/// async fn debit(&mut self, cx: Caller<'_>, amount: u64) -> u64 {
///     let call = cx.peer().and_then(Context::call);
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
/// the other shape: it remembers that the work happened and holds no answer to
/// give back, which is at-most-once and is all a `#[defer]` method can have. See
/// [`seen`](Self::seen).
///
/// # It is yours, and that is the point
///
/// keylink does not hold this, look at it, or bound it on your behalf. That is
/// what makes [`APPLIED_CAPACITY`] arguable at all: a bound inside the framework
/// would have to be right for every state at once, and the number that matters is
/// how long *your* callers back off for.
///
/// It is also why nothing here touches durability. Your state already knows how to
/// snapshot itself; [`iter`](Self::iter) hands the entries back oldest-first so a
/// restore that replays them with [`insert`](Self::insert) rebuilds the same
/// eviction order, and [`CallId::as_bytes`] is the sixteen bytes to write.
///
/// # What an unkeyed call gets
///
/// Nothing, deliberately. [`get`](Self::get) and [`insert`](Self::insert) take the
/// `Option<CallId>` that [`Context::call`](crate::Context::call) hands out, so the
/// caller writes no `match`; a `None` reads as absent and stores as a no-op. A
/// caller that sent no key asked for no de-duplication and gets none — the same
/// answer `claim_key` gives it one layer up.
#[derive(Debug, Clone)]
pub struct Applied<T> {
    answers: HashMap<CallId, T>,
    /// The ids in `answers`, oldest first. The eviction order, and the order
    /// [`Applied::iter`] reports so that a snapshot round-trips.
    order: VecDeque<CallId>,
    capacity: NonZeroUsize,
}

impl<T> Applied<T> {
    /// Remember up to [`APPLIED_CAPACITY`] calls.
    #[must_use]
    pub fn new() -> Self {
        Self::with_capacity(APPLIED_CAPACITY)
    }

    /// Remember up to `capacity` calls.
    ///
    /// [`NonZeroUsize`] rather than `usize`, because a capacity of zero is an
    /// `Applied` that accepts every insert and recognises nothing — at-least-once
    /// wearing the type that says otherwise, which is exactly the silent
    /// degradation this whole module exists to give a caller a way out of.
    #[must_use]
    pub fn with_capacity(capacity: NonZeroUsize) -> Self {
        Self {
            answers: HashMap::new(),
            order: VecDeque::new(),
            capacity,
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
    /// For `Applied<()>`, where there is no answer to hand back and the only
    /// question is whether the work already happened.
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
        while self.order.len() > self.capacity.get() {
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
    #[must_use]
    pub fn capacity(&self) -> NonZeroUsize {
        self.capacity
    }

    /// Every remembered call and its answer, **oldest first**.
    ///
    /// The order is the contract, not an accident of the container: it is the
    /// eviction order, so a snapshot written in it and replayed through
    /// [`insert`](Self::insert) restores the same set *and* the same next victim.
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

/// # Replaying the break records below
///
/// Every one of them quotes a line **without its leading indentation**, because a
/// doc comment reads better that way. Pass the line to `scripts/break-once.sh`
/// **with** its indentation: this file then contains the bare text twice — once as
/// code and once in the record — and the harness aborts on a match that is not
/// unique, which is the correct behaviour and the reason this paragraph exists
/// rather than a softer literal.
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
    /// `hasher.update(peer.as_bytes());` with `let _ = peer;` and the two ids
    /// become equal.
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
    /// `let _ = key;`.
    #[test]
    fn two_keys_from_one_peer_are_two_calls() {
        assert_ne!(
            CallId::of(peer(A), &key("reserve-4471")),
            CallId::of(peer(A), &key("reserve-4472")),
            "two keys from one peer must be two calls",
        );
    }

    /// **The failure direction, at a width where the failure is reachable.**
    ///
    /// A collision at sixteen bytes is not exhibitable and this test does not
    /// pretend to exhibit one. What it pins is the *direction*: a digest too short
    /// produces a **false hit** — two of one peer's keys becoming one id, so the
    /// second is answered from the first's record — and never a false miss, which
    /// is unreachable at any width because the digest is a total function of
    /// exactly `(peer, key)`.
    ///
    /// One byte, so the pigeonhole guarantees a pair inside 257 keys.
    ///
    /// Made red by `DIGEST_BYTES: usize = 16` → `1`: the pair that collides at one
    /// byte then collides at the real width too, and the second assertion fails.
    #[test]
    fn a_shortened_digest_fails_towards_a_false_hit() {
        let keys: Vec<IdempotencyKey> = (0..300).map(|n| key(&format!("k-{n}"))).collect();
        let collision = keys
            .iter()
            .enumerate()
            .flat_map(|(i, left)| keys[i + 1..].iter().map(move |right| (left, right)))
            .find(|(left, right)| narrow(peer(A), left, 1) == narrow(peer(A), right, 1))
            .expect("257 keys cannot all differ in one byte");

        let (left, right) = collision;
        assert_eq!(
            narrow(peer(A), left, 1),
            narrow(peer(A), right, 1),
            "a one-byte id is what a false hit looks like: two keys, one call",
        );
        assert_ne!(
            CallId::of(peer(A), left),
            CallId::of(peer(A), right),
            "at DIGEST_BYTES the same pair must be two calls",
        );
        assert_eq!(
            CallId::of(peer(A), left),
            CallId::of(peer(A), left),
            "a false MISS is what would break a retry, and it is unreachable: \
             the digest is a function of the pair and of nothing else",
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
    /// Made red by `pop_front()` → `pop_back()`: the set then forgets what just
    /// happened and remembers what nobody will ask about again.
    #[test]
    fn the_oldest_call_is_the_one_forgotten() {
        let two = NonZeroUsize::new(2).expect("2 is not zero");
        let mut applied: Applied<u64> = Applied::with_capacity(two);
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

    /// **The property that makes this durable at all**: the entries can be written
    /// down and read back, and what comes back has the same eviction order.
    ///
    /// This is the shape a state's own `Durable::snapshot` and `restore` take, so
    /// the round trip goes through [`CallId::as_bytes`] and
    /// [`CallId::from_bytes`] rather than cloning the value.
    ///
    /// Made red by reversing [`Applied::iter`] — `self.order\n.iter()` →
    /// `self.order\n.iter()\n.rev()\n.collect::<Vec<_>>()\n.into_iter()`, the
    /// `collect` being what keeps the closure's lifetime the same shape — when the
    /// restored set forgets the wrong entry. Watched:
    /// `a restored set must evict in the order it was written`.
    #[test]
    fn a_snapshot_of_what_was_applied_round_trips_with_its_eviction_order() {
        let two = NonZeroUsize::new(2).expect("2 is not zero");
        let mut applied: Applied<u64> = Applied::with_capacity(two);
        let calls: Vec<Option<CallId>> = (0..2)
            .map(|n| Some(CallId::of(peer(A), &key(&format!("debit-{n}")))))
            .collect();
        applied.insert(calls[0], 10);
        applied.insert(calls[1], 20);

        // What a `snapshot` writes: sixteen bytes and an answer, in order.
        let written: Vec<([u8; DIGEST_BYTES], u64)> = applied
            .iter()
            .map(|(call, answer)| (*call.as_bytes(), *answer))
            .collect();
        // What a `restore` does with them.
        let mut restored: Applied<u64> = Applied::with_capacity(two);
        for (bytes, answer) in written {
            restored.insert(Some(CallId::from_bytes(bytes)), answer);
        }

        assert_eq!(restored.get(calls[0]), Some(&10));
        assert_eq!(restored.get(calls[1]), Some(&20));

        // And the next insert displaces the same entry the original would have.
        let third = Some(CallId::of(peer(A), &key("debit-2")));
        restored.insert(third, 30);
        assert!(
            restored.get(calls[0]).is_none(),
            "a restored set must evict in the order it was written",
        );
        assert_eq!(restored.get(calls[1]), Some(&20));
    }
}
