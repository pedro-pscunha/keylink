//! The one adapter the [`Observer`] port is owed: a state written after every
//! turn, in front of the reply.
//!
//! # "In front of the reply" means the reply the **loop** sends
//!
//! Say the exception before the rule, because the rule is the quotable half.
//! An ordinary `rpc`, a `blob`, a `recv`, a feed item: the handler returns, the
//! observer chain runs, the store completes, and only then is the caller told
//! anything. For those, an answered call is a stored call.
//!
//! A `#[defer]` method that answers through [`Deferred::send`](crate::Deferred)
//! is **not** one of those. That call hands the value straight to the caller
//! from inside the running handler — *"this turn's observers neither see it nor
//! gate it"* — so the caller holds its answer for the whole of the ~7.6 ms
//! below, and a crash in that window loses state somebody was already told
//! about. It is a real limit, not a caveat: `tests/persist.rs::
//! a_deferred_reply_is_answered_before_its_turn_is_stored` demonstrates it with
//! a receipt for a debit that is not on the disk.
//!
//! A `#[defer]` method that answers with [`Reply::send`](crate::Reply) inside
//! its own turn is gated normally and keeps the property.
//!
//! `keylink-persist` states the standard this module exists to meet — a port is
//! justified by one adapter written on purpose — and until this file existed
//! every implementor of [`Observer`] in the workspace was a test or a
//! compile-time probe. This is the consumer the port was designed for, and the
//! shape of [`Observer::after_turn`] is the shape it needed.
//!
//! # Where it lives, and why it is not a third crate
//!
//! A `keylink-durable` crate whose entire content is one adapter is a seam
//! nobody asked for. The dependency-direction objection does not survive
//! either: the hexagon's rule is that the **port** must not know the framework,
//! and `keylink → keylink-persist` is the application depending on the port,
//! which is the direction the arrow is supposed to point. `keylink-persist` has
//! no idea this file exists and still compiles alone.
//!
//! What was left was the browser, and that is a feature problem rather than a
//! crate problem. `keylink-persist` arrives here `optional = true,
//! default-features = false`, so:
//!
//! | feature | what it adds | browser |
//! |---|---|---|
//! | `persist` | this module, over the port | **builds** |
//! | `persist-file` | `persist` plus the file adapter | `compile_error!` |
//!
//! `persist` reaches nothing but the frame and the two port traits, so it
//! compiles for `wasm32-unknown-unknown` and `.mise/tasks/wasm` builds it there.
//! `persist-file` turns on `tokio/fs`, which does not exist in a browser, and
//! `lib.rs` refuses it on that target beside the same refusal for `blocking`,
//! `fs` and `env`.
//!
//! # What it costs per turn — measured, on a named machine, on a stated day
//!
//! RFC-06 §7 asked for this number and never got one. It is much larger than
//! anybody guessed, so it is the first thing in this file after the placement.
//!
//! **7.6 ms per turn**, against **18 µs** for the same turn with no observer.
//! The store is essentially all of it: **~7.6 ms**, or **about 130 turns per
//! second per actor**.
//!
//! | profile | no observer | with `Persist` | runs |
//! |---|---|---|---|
//! | debug (`cargo test`) | 16.5 – 20.2 µs | 7,520 / 7,652 / 7,927 µs | 3 |
//! | release | 12.8 µs | 7,083 µs | 1 |
//!
//! Apple M5 Max (`Mac17,6`), macOS 27.0 build 26A5378n, APFS on the internal
//! SSD, 2026-08-07. 1,000 turns of a fixed-size 8-byte state, so the payload is
//! 20 framed bytes on every turn and the number is not a function of how far the
//! run got. The debug and release columns agree to within 8%, which is the
//! evidence that this is a syscall cost and not a code-generation one.
//!
//! **7.6 ms is a mean, and there is no tail in this table and no bound
//! anywhere.** Nothing wraps [`Observer::after_turn`] in a timeout — not this
//! adapter, not the loop — so the store's worst case is the actor's worst case.
//! A store that hangs wedges the single loop that owns the state: no further
//! turn runs, no feed item is taken, and `stop().await` never returns because
//! it waits for the loop to reach its exit. The only escape is
//! [`abort`](crate::LocalActor::abort), which cancels the task at its next
//! await and therefore skips [`Observer::finish`] for every observer.
//!
//! That is a property of a filesystem `fsync` on a healthy local disk, where it
//! is a fair trade. It is a different property against a network store, an NFS
//! mount, or a full one — and an adapter for any of those should carry its own
//! deadline, because there is nowhere else to put one.
//!
//! **It is two `fsync`s and a `rename`, and it is *supposed* to be expensive.**
//! `keylink_persist::file::Files` syncs the temporary file and then the
//! directory, which is what makes "an answered call is a stored call" survive a
//! power cut rather than merely a process restart. An adapter that skipped the
//! second `fsync` would report a much better number and quietly mean something
//! weaker.
//!
//! So the number is not a defect to fix here; it is the price of the promise, and
//! the knob that trades it away is a debounce — which nothing here implements
//! and nothing here half-implements; see the type's own paragraph on why not.
//! A caller who wants the other trade today has it without any new machinery:
//! write a state whose turns are cheap to lose, or reach for an adapter whose
//! `store` does not sync.
//!
//! Reproduce it with
//! `cargo test -p keylink --test persist -- --ignored --nocapture`, which prints
//! the same numbers and asserts nothing about any of them. There is no
//! wall-clock assertion anywhere in this wave.

use std::marker::PhantomData;

use keylink_persist::versioned::{Snapshot, Versioned};
use keylink_persist::{Durable, Snapshots, StateId};

use crate::{Observer, Stop, TurnFut, TurnInfo, TurnOutcome};

/// Stores the state after every turn it accepts, and refuses a turn that
/// panicked.
///
/// One of these owns one [`Claim`](keylink_persist::Claim) — the actor owns the
/// observer, so the claim dies with the actor and is free before
/// [`stop`](crate::LocalActor::stop) returns. That is what [`Observer`]'s "the
/// actor owns its observers" paragraph is for, and it is why this type is not
/// `Clone` and cannot be shared between two actors: two writers of one state is
/// the situation the claim exists to make impossible.
///
/// ```no_run
/// # use keylink::{Caller, Persist};
/// # use keylink_persist::{Durable, RestoreError, StateId};
/// # use keylink_persist::file::Files;
/// # keylink::service! { service Ledger; rpc Post(entry: u64) -> u64; }
/// # #[derive(Default)]
/// # struct Ledger { entries: Vec<u64> }
/// # impl ledger::Actor for Ledger {
/// #     async fn post(&mut self, _cx: Caller<'_>, entry: u64) -> u64 {
/// #         self.entries.push(entry);
/// #         self.entries.len() as u64
/// #     }
/// # }
/// # impl Durable for Ledger {
/// #     const VERSION: u32 = 1;
/// #     fn snapshot(&self) -> Vec<u8> { Vec::new() }
/// #     fn restore(_: &[u8]) -> Result<Self, RestoreError> { Ok(Self::default()) }
/// #     fn migrate(_: u32, _: &[u8]) -> Result<Option<Vec<u8>>, RestoreError> { Ok(None) }
/// # }
/// # async fn run() -> Result<(), Box<dyn std::error::Error>> {
/// let store = Files::open("/var/lib/keylink").await?;
/// let id = StateId::new("ledgers/sp-01")?;
///
/// // The claim is taken here and held until the actor ends. `Ledger` is named
/// // because nothing in the arguments says which state is being claimed — see
/// // `open`. `start_from` is the only way to reach the observer, and it hands
/// // over the restored state in the same expression.
/// let (state, saver) = Persist::<Ledger, _>::open(&store, &id)
///     .await?
///     .start_from(Ledger::default);
/// let ledger = ledger::actor_with(state, vec![Box::new(saver)]);
///
/// // An ordinary `rpc`, so the store is in front of the reply: this returning
/// // `Ok` means the bytes are fsynced. A `#[defer]` method answers from inside
/// // the handler and does NOT get that — see "the `#[defer]` window" below.
/// ledger.post(1).await?;
/// # Ok(())
/// # }
/// ```
///
/// # What is stored is exactly what was answered — **both** directions
///
/// The first direction is the wave's headline: a turn is written before the
/// loop tells its caller anything, so an answered `rpc` is a stored `rpc` —
/// with the [`Deferred::send`](crate::Deferred) exception this module's header
/// states first. The converse matters just as much and is easier to lose: **a
/// turn that was not answered is not stored either.**
///
/// A `store` that returns `Err` refuses the turn, which ends the actor and
/// hands the caller
/// [`TurnLost::Refused`](crate::TurnLost::Refused) — whose text is *"it ran and
/// was not committed"*, and which crosses the wire to a peer as
/// `observer refused: persist` under the same sentence. That is a promise, and
/// the only thing the caller has to act on. A caller who believes it replays;
/// an operator who reads it replays.
///
/// So **this adapter does not write anything on the way out.** It implements
/// no [`Observer::finish`], and it keeps no flag that would give one something
/// to do. A state written after the actor ended could only be the state of the
/// turn whose store just failed — the turn somebody was told was not committed
/// — and writing it turns one lost turn into a **double apply** on the replay
/// the refusal invites. For a `Post(entry)` on a ledger that is money.
///
/// It is the same trade the panicked turn takes, one line up: losing a turn is
/// the conservative loss, and reproducing one on restart is not.
///
/// **A feed turn has no caller, so retrying its failed store would be
/// harmless** — and this adapter still does not, because it cannot tell the two
/// apart. [`TurnInfo`] carries the outcome and the method name and nothing
/// about who is owed a reply. Fail closed.
///
/// # RFC-06 §7's debounce is **not** partly built here, on purpose
///
/// A debounce returns `Ok` from `after_turn` without writing, so the very first
/// thing it gives up is the property above: an answered call stops being a
/// stored call. Its `finish` is therefore answering a different question than
/// this one would — "which of these several answered-and-unwritten turns do I
/// still owe the disk, and what do I do when the last of them is also the one
/// that failed" — and a `finish` written here in advance would be an answer to
/// the question nobody asked yet. It would also have been wrong: the version
/// this replaced retried the failed turn.
///
/// # `with_state` is a read, and a state that lies about that is not stored
///
/// A generated `Handle::with_state` — [`LocalActor::read`](crate::LocalActor)
/// underneath — runs a closure against the state without going through the
/// observer chain, because a read has nothing for an observer to accept or
/// refuse. Nothing enforces that it *is* a read: a state whose fields are
/// mutable through `&self` — a [`Cell`], a [`RefCell`], an [`AtomicU64`] — can
/// be changed by that closure, and the change is answered and never stored.
///
/// This is not hypothetical for this crate. `tests/persist.rs`'s `Counter` holds
/// exactly those types, on purpose, to prove `D: Sync` is not required — and its
/// `Durable::snapshot` reads the `Cell`. A `with_state` closure bumping it would
/// be lost.
///
/// Stated rather than guarded, because the alternative is `A: Sync`-shaped: the
/// property "this closure does not mutate" is not one the type system holds for
/// interior mutability, and running the observer chain on every read would pay
/// the ~7.6 ms below for something that changed nothing. **Mutate in a turn.**
///
/// [`Cell`]: std::cell::Cell
/// [`RefCell`]: std::cell::RefCell
/// [`AtomicU64`]: std::sync::atomic::AtomicU64
///
/// # Put it **last** in the chain
///
/// The first observer to return `Err` stops the rest, so an observer placed
/// before this one can refuse a turn and prevent the write. One placed *after*
/// it cannot: the write has already happened, and the caller is still told
/// [`TurnLost::Refused`](crate::TurnLost::Refused). That asymmetry is the
/// chain's, not this type's — but "last" is the position in which it means what
/// a reader expects, so it is written here rather than left to be discovered.
///
/// # Two parameters, and why the second one is the **store**
///
/// `D` is the state and `S` is where its snapshots live. The claim type would be
/// the narrower parameter, and it is deliberately not the one here:
/// `Versioned<C>` demands `C: Claim`, a bound this file would have to name, and
/// `keylink_persist::Claim` is the trait whose one method writes **unframed**
/// bytes. `.mise/tasks/framing` reads exactly that — a file that never brings the
/// trait into scope cannot call its methods, whatever it calls its variables — so
/// naming the store instead of the claim keeps the strongest guarantee available
/// about this file: it has no route to an unframed write at all.
/// `Versioned<S::Claim>` needs no bound, because [`Snapshots`] already promises
/// one.
pub struct Persist<D, S: Snapshots> {
    /// The claim, wrapped so that everything written through it is framed.
    /// Owned, with no way back out to the raw claim.
    ///
    /// **The only field with a value in it, and that is the design.** An
    /// adapter that remembered anything about the last turn would have to
    /// decide what to do with that memory on the way out, and the one decision
    /// available to it — write the state one last time — is the decision
    /// [`Observer::finish`] is deliberately not used for here. See this type's
    /// "what is stored is what was answered" paragraph.
    durable: Versioned<S::Claim>,
    /// `fn() -> D` rather than `D`, so `Send` and `Sync` here are properties of
    /// the claim and nothing else — the same reason
    /// [`Snapshot`] gives for its own.
    ///
    /// `D` is in the type rather than only in the impl because a `Persist` is
    /// bound to one state type: the frame it writes stamps
    /// [`D::VERSION`](Durable::VERSION), and an adapter that could observe two
    /// different states would write two shapes into one claim under one version
    /// number.
    state: PhantomData<fn() -> D>,
}

impl<D: Durable, S: Snapshots> Persist<D, S> {
    /// Claim the state, read whatever is stored, and keep the claim.
    ///
    /// **This is the only way to build one, and it does not hand one over on its
    /// own.** A constructor that took a claim and skipped the read would let a
    /// caller start an actor from `Default` and have the first turn overwrite a
    /// perfectly good stored state — silently, and exactly once per deploy.
    ///
    /// This used to return `(Option<D>, Self)` under a sentence claiming that
    /// pairing the two "makes the read impossible to leave out". It did not:
    /// `Result` is `#[must_use]` but **a tuple element is not**, so
    /// `let (_restored, saver) = ..` compiled clean and was the shortest form to
    /// copy. It returns an [`Opened`] instead, which is one value, and the only
    /// method that yields a `Persist` yields the state in the same expression.
    ///
    /// The claim is exclusive from here until that value is dropped. A second
    /// `open` on one [`StateId`] is [`Error::Busy`](keylink_persist::Error::Busy),
    /// not a second writer.
    ///
    /// # `D` appears only in the return, so it usually has to be named
    ///
    /// `Persist::<Ledger, _>::open(&store, &id)`. Nothing in the arguments says
    /// which state is being claimed, and inference reaches the observer end but
    /// not always the state end: handing the restored value to a generic
    /// `actor_with<A: Actor>` leaves `D` ambiguous even where exactly one type
    /// implements that service's `Actor`, because rustc will not pick an impl to
    /// resolve a type parameter. The turbofish reads as "open the Ledger's
    /// state", which is what the line means anyway.
    ///
    /// # Errors
    ///
    /// - [`Error::Busy`](keylink_persist::Error::Busy) when another holder — in
    ///   this process or another — already owns this state. **Not** a transient
    ///   error to retry in a loop.
    /// - [`Error::Io`](keylink_persist::Error::Io) when the store cannot be
    ///   claimed or read.
    /// - [`Error::Restore`](keylink_persist::Error::Restore) when the stored
    ///   bytes are not a frame this build can read, are from a newer build, or
    ///   [`Durable::restore`] refused them. Loud, and the stored bytes are left
    ///   alone.
    pub async fn open(store: &S, id: &StateId) -> keylink_persist::Result<Opened<D, S>> {
        let durable = Versioned::new(store.claim(id).await?);
        let restored = durable.load::<D>().await?;
        Ok(Opened {
            restored,
            saver: Self {
                durable,
                state: PhantomData,
            },
        })
    }
}

/// A claimed state, read, with the adapter that will keep writing it.
///
/// What [`Persist::open`] returns, and the reason it is a type rather than a
/// tuple: **there is no way to take the observer without taking the state.**
/// [`start_from`](Self::start_from) is the only method that hands over a
/// [`Persist`], and it hands over the state in the same expression, so the
/// shortest thing a reader can copy is the correct one.
///
/// Holding one of these holds the claim, which is what
/// `tests/persist.rs::a_second_open_on_one_state_is_busy` leans on.
#[must_use = "this holds the claim AND the state that was read back; dropping it \
              starts the next run from nothing"]
pub struct Opened<D, S: Snapshots> {
    restored: Option<D>,
    saver: Persist<D, S>,
}

impl<D: Durable, S: Snapshots> Opened<D, S> {
    /// Whether anything was stored. `false` is the first ever run, which is not
    /// a failure.
    ///
    /// Takes `&self`, so asking cannot lose the answer — the distinction an
    /// operator logs and a test asserts, without a route to the state.
    #[must_use]
    pub fn was_stored(&self) -> bool {
        self.restored.is_some()
    }

    /// The state to start the actor from, and the observer that will keep it
    /// current.
    ///
    /// What was stored, migrated through [`Durable::migrate`] if it is older
    /// than this build — or `first()` when nothing ever was. `first` is a
    /// closure rather than a value so a restart does not build a starting state
    /// it then throws away.
    ///
    /// ```ignore
    /// let (state, saver) = Persist::<Ledger, _>::open(&store, &id).await?.start_from(Ledger::default);
    /// let ledger = ledger::actor_with(state, vec![Box::new(saver)]);
    /// ```
    pub fn start_from(self, first: impl FnOnce() -> D) -> (D, Persist<D, S>) {
        (self.restored.unwrap_or_else(first), self.saver)
    }
}

impl<D, S: Snapshots> std::fmt::Debug for Opened<D, S> {
    /// Whether there was anything to restore, and nothing else. The state is the
    /// state's business.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Opened")
            .field("was_stored", &self.restored.is_some())
            .finish_non_exhaustive()
    }
}

impl<D, S> Observer<D> for Persist<D, S>
where
    D: Durable + 'static,
    S: Snapshots,
{
    fn name(&self) -> &'static str {
        "persist"
    }

    /// Take the snapshot synchronously, then write it before the reply.
    ///
    /// # The snapshot is taken **outside** the future, and that is the design
    ///
    /// [`Observer::after_turn`] is a plain `fn` returning a boxed future rather
    /// than an `async fn` precisely so this line can run before the future
    /// exists. The naive spelling —
    /// `Box::pin(async move { self.durable.store(&Snapshot::of(state)).await })` —
    /// holds `&D` across an await, so the future is `Send` only when `D: Sync`,
    /// and that bound would then be demanded of **every** state in the crate.
    /// Wave 0 probed the trap and Wave 2d hit it again on
    /// [`finish`](Observer::finish); `tests/persist.rs` keeps a state that is
    /// `Send` and not `Sync` and persists it, which is what makes this paragraph
    /// checkable rather than remembered.
    ///
    /// # A panicked turn is refused and never stored
    ///
    /// A handler that panicked half-way through leaves the state half-applied,
    /// and a half-applied state written to the disk is worse than the turn being
    /// lost: a restart would reproduce it. So the turn is refused and the actor
    /// ends.
    ///
    /// The [`Stop`] reason is not what an operator reads, on purpose. The loop
    /// folds a refusal of a *panicked* turn into
    /// [`Ended::Panicked`](crate::Ended::Panicked) carrying the handler's own
    /// message, because reporting the durability adapter for a fault in a
    /// `service!` method points at the wrong code.
    ///
    /// # A `match`, and not an `if let`, because the default here is **write**
    ///
    /// [`TurnOutcome`] is exhaustive and deliberately not `#[non_exhaustive]`.
    /// An `if let` on the one outcome that must not be stored puts every future
    /// outcome in the `else` branch, and this `else` branch commits bytes to a
    /// disk. A third outcome — `Cancelled`, say — would be persisted under a
    /// rule nobody wrote and nothing would say so. The `match` makes it `E0004`
    /// here, in keylink, at the line that decides; `representability.rs`'s
    /// `every_outcome_is_named` holds the same tripwire for a build without
    /// this feature.
    fn after_turn<'a>(&'a mut self, state: &'a D, turn: TurnInfo<'a>) -> TurnFut<'a> {
        let snapshot = match turn.outcome {
            TurnOutcome::Completed => Snapshot::of(state),
            TurnOutcome::Panicked { .. } => {
                return Box::pin(async {
                    Err(Stop::new(
                        "a panicked turn is not stored: the state may be half-applied",
                    ))
                })
            }
        };

        Box::pin(async move {
            self.durable.store(&snapshot).await?;
            Ok(())
        })
    }

    // No `finish`. The default — do nothing — is this adapter's policy, and the
    // paragraph on the type says why: a state written after the actor ended
    // could only be a state some caller was told was refused.
}

impl<D, S: Snapshots> std::fmt::Debug for Persist<D, S> {
    /// The name and nothing else, because there is nothing else. The claim is
    /// an adapter's business, the snapshot bytes are the state's, and this type
    /// deliberately remembers nothing between turns.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Persist").finish_non_exhaustive()
    }
}
